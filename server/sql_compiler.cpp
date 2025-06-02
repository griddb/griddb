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
#include "sql_compiler_internal.h"

#include "sql_processor.h" 
#include "sql_execution.h" 
#include "meta_type.h"

#include "json.h"
#include "picojson.h"
#include "sql_utils.h"

SQLTableInfo::SQLTableInfo(util::StackAllocator &alloc) :
		dbName_(alloc),
		tableName_(alloc),
		hasRowKey_(false),
		writable_(false),
		isView_(false),
		isExpirable_(false),
		columnInfoList_(alloc),
		partitioning_(NULL),
		indexInfoList_(NULL),
		basicIndexInfoList_(NULL),
		nosqlColumnInfoList_(alloc),
		nosqlColumnOptionList_(alloc),
		sqlString_(alloc),
		cardinalityList_(alloc),
		nullsStats_(alloc) {
}

SQLTableInfo::IdInfo::IdInfo() :
		id_(std::numeric_limits<Id>::max()),
		type_(SQLType::TABLE_CONTAINER),
		dbId_(0),
		partitionId_(0),
		containerId_(0),
		schemaVersionId_(0),
		subContainerId_(-1),
		partitioningVersionId_(-1),
		isNodeExpansion_(false),
		approxSize_(-1) {
}

bool SQLTableInfo::IdInfo::isEmpty() const {
	return (id_ == std::numeric_limits<Id>::max());
}

bool SQLTableInfo::IdInfo::operator==(const IdInfo &another) const {
	return id_ == another.id_ &&
			type_ == another.type_ &&
			dbId_ == another.dbId_ &&
			partitionId_ == another.partitionId_ &&
			containerId_ == another.containerId_ &&
			schemaVersionId_ == another.schemaVersionId_ &&
			subContainerId_ == another.subContainerId_ &&
			partitioningVersionId_ == another.partitioningVersionId_ &&
			approxSize_ == another.approxSize_;
}

SQLTableInfo::SubInfo::SubInfo() :
		partitionId_(0),
		containerId_(0),
		schemaVersionId_(0),
		nodeAffinity_(-1),
		approxSize_(-1) {
}

SQLTableInfo::PartitioningInfo::PartitioningInfo(util::StackAllocator &alloc) :
		alloc_(alloc),
		partitioningType_(0),
		partitioningColumnId_(-1),
		subPartitioningColumnId_(-1),
		subInfoList_(alloc_),
		tableName_(alloc),
		isCaseSensitive_(false),
		partitioningCount_(0),
		clusterPartitionCount_(0),
		intervalValue_(0),
		nodeAffinityList_(alloc),
		availableList_(alloc) {
}

void SQLTableInfo::PartitioningInfo::copy(
		const SQLTableInfo::PartitioningInfo &partitioningInfo) {
	partitioningType_ = partitioningInfo.partitioningType_;
	partitioningColumnId_ = partitioningInfo.partitioningColumnId_;
	subPartitioningColumnId_ = partitioningInfo.subPartitioningColumnId_;
	for (size_t pos = 0; pos < partitioningInfo.subInfoList_.size(); pos++) {
		subInfoList_.push_back(partitioningInfo.subInfoList_[pos]);
	}
	tableName_ = partitioningInfo.tableName_.c_str();
	isCaseSensitive_ = partitioningInfo.isCaseSensitive_;
	partitioningCount_ = partitioningInfo.partitioningCount_;
	clusterPartitionCount_ = partitioningInfo.clusterPartitionCount_;
	intervalValue_ = partitioningInfo.intervalValue_;
}

SQLTableInfo::SQLIndexInfo::SQLIndexInfo(util::StackAllocator &alloc) :
		columns_(alloc) {
}

SQLTableInfo::BasicIndexInfoList::BasicIndexInfoList(
		util::StackAllocator &alloc) :
		columns_(alloc) {
}

void SQLTableInfo::BasicIndexInfoList::assign(
		util::StackAllocator &alloc, const IndexInfoList &src) {
	util::Set<ColumnId> columnSet(alloc);
	for (IndexInfoList::const_iterator it = src.begin(); it != src.end(); ++it) {
		if (it->columns_.empty()) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
		}
		columnSet.insert(it->columns_.front());
	}
	columns_.assign(columnSet.begin(), columnSet.end());
}

const util::Vector<ColumnId>&
SQLTableInfo::BasicIndexInfoList::getFirstColumns() const {
	return columns_;
}

bool SQLTableInfo::BasicIndexInfoList::isIndexed(
		ColumnId firstColumnId) const {
	return std::binary_search(
			columns_.begin(), columns_.end(), firstColumnId);
}

SQLTableInfoList::SQLTableInfoList(
		util::StackAllocator &alloc, SQLIndexStatsCache *indexStats) :
		list_(alloc),
		defaultDBName_(NULL),
		indexStats_(indexStats) {
}

const SQLTableInfo& SQLTableInfoList::add(const SQLTableInfo &info) {
	const SQLTableInfo::Id id = static_cast<SQLTableInfo::Id>(list_.size());
	list_.push_back(info);
	list_.back().idInfo_.id_ = id;

	if (info.indexInfoList_ != NULL) {
		util::StackAllocator &alloc = getAllocator();
		BasicIndexInfoList *basicIndexInfoList =
				ALLOC_NEW(alloc) BasicIndexInfoList(alloc);
		basicIndexInfoList->assign(alloc, *info.indexInfoList_);
		list_.back().basicIndexInfoList_ = basicIndexInfoList;
	}

	return list_.back();
}

uint32_t SQLTableInfoList::size() const {
	return static_cast<uint32_t>(list_.size());
}

bool SQLCompiler::Meta::isNodeExpansion(ContainerId containerId) {
	const bool forCore = true;
	const MetaType::InfoTable &infoTable = MetaType::InfoTable::getInstance();
	const MetaContainerInfo *info = infoTable.findInfo(containerId, forCore);
	if (info == NULL) {
		return false;
	}

	return info->nodeDistribution_;
}

const SQLTableInfo* SQLTableInfoList::prepareMeta(
		const QualifiedName &name, const SQLTableInfo::IdInfo &coreIdInfo,
		DatabaseId dbId, uint32_t partitionCount) {
	if (coreIdInfo.containerId_ == UNDEF_CONTAINERID) {
		return NULL;
	}

	if (name.table_ == NULL) {
		assert(false);
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	util::StackAllocator &alloc = getAllocator();
	QualifiedName metaName(alloc);

	SQLCompiler::Meta::getCoreMetaTableName(alloc, coreIdInfo, metaName);

	{
		SQLTableInfo info(alloc);

		if (name.db_ != NULL) {
			info.dbName_ = *name.db_;
		}
		else if (defaultDBName_ != NULL) {
			info.dbName_ = *defaultDBName_;
		}
		else {
			assert(false);
		}
		info.tableName_ = *metaName.table_;

		info.idInfo_ = coreIdInfo;
		info.idInfo_.dbId_ = dbId;
		bool isNodeExpansion = SQLCompiler::Meta::isNodeExpansion(info.idInfo_.containerId_);
		if (isNodeExpansion) {
			info.idInfo_.isNodeExpansion_ = true;
		}

		const bool forCore = true;
		SQLCompiler::Meta::getMetaColumnInfo(
				alloc, coreIdInfo, forCore, info.columnInfoList_, -1);

		{
			SQLTableInfo::PartitioningInfo *partitioning =
					ALLOC_NEW(alloc) SQLTableInfo::PartitioningInfo(alloc);

			partitioning->partitioningCount_ = partitionCount;
			partitioning->clusterPartitionCount_ = partitionCount;

			SQLTableInfo::SubInfoList &subInfoList = partitioning->subInfoList_;
			subInfoList.resize(partitionCount);
			for (uint32_t i = 0; i < partitionCount; i++) {
				subInfoList[i].partitionId_ = i;
				subInfoList[i].containerId_ = coreIdInfo.containerId_;
				subInfoList[i].nodeAffinity_ = i;
			}

			info.partitioning_ = partitioning;
		}

		return &add(info);
	}

	return NULL;
}

void SQLTableInfoList::setDefaultDBName(const char8_t *dbName) {
	util::StackAllocator &alloc = getAllocator();
	defaultDBName_ = ALLOC_NEW(alloc) util::String(dbName, alloc);
}

const char8_t* SQLTableInfoList::resolveDBName(
		const QualifiedName &name) const {
	if (name.db_ == NULL) {
		if (defaultDBName_ == NULL) {
			SQL_COMPILER_THROW_ERROR(
					GS_ERROR_SQL_COMPILE_DATABASE_NOT_FOUND, NULL,
					"Database not found because "
					"default database is not specified");
		}
		return defaultDBName_->c_str();
	}
	else {
		return name.db_->c_str();
	}
}

bool SQLTableInfoList::resolveDBCaseSensitive(const QualifiedName &name) {
	return (name.db_ == NULL || name.dbCaseSensitive_);
}

const SQLTableInfo& SQLTableInfoList::resolve(SQLTableInfo::Id id) const {
	if (id >= list_.size()) {
		assert(false);
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	return list_[id];
}

const SQLTableInfo& SQLTableInfoList::resolve(
		const QualifiedName &name) const {
	if (name.table_ == NULL) {
		assert(false);
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	return resolve(
			resolveDBName(name), name.table_->c_str(),
			resolveDBCaseSensitive(name), name.tableCaseSensitive_);
}

const SQLTableInfo& SQLTableInfoList::resolve(
		const char8_t *dbName, const char8_t *tableName,
		bool dbCaseSensitive, bool tableCaseSensitive) const {
	const SQLTableInfo *info =
			find(dbName, tableName, dbCaseSensitive, tableCaseSensitive);

	if (info == NULL) {
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_TABLE_NOT_FOUND, NULL,
				"Table not found (database=" << dbName <<
				", table=" << tableName << ")");
	}

	return *info;
}

const SQLTableInfo* SQLTableInfoList::find(const QualifiedName &name) const {
	if (name.table_ == NULL) {
		assert(false);
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	return find(
			resolveDBName(name), name.table_->c_str(),
			resolveDBCaseSensitive(name), name.tableCaseSensitive_);
}

const SQLTableInfo* SQLTableInfoList::find(
		const char8_t *dbName, const char8_t *tableName,
		bool dbCaseSensitive, bool tableCaseSensitive) const {
	for (List::const_iterator it = list_.begin(); it != list_.end(); ++it) {
		const SQLTableInfo &info = *it;
		if (SQLCompiler::compareStringWithCase(
					info.dbName_.c_str(), dbName, dbCaseSensitive) == 0 &&
					SQLCompiler::compareStringWithCase(
							info.tableName_.c_str(), tableName,
							tableCaseSensitive) == 0) {
			return &info;
		}
	}

	return NULL;
}

const SQLTableInfo::BasicIndexInfoList* SQLTableInfoList::getIndexInfoList(
		const SQLTableInfo::IdInfo &idInfo) const {
	const SQLTableInfo &tableInfo = resolve(idInfo.id_);
	return tableInfo.basicIndexInfoList_;
}

SQLIndexStatsCache& SQLTableInfoList::getIndexStats() const {
	if (indexStats_ == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}
	return *indexStats_;
}

util::StackAllocator& SQLTableInfoList::getAllocator() {
	return *list_.get_allocator().base();
}

SQLPreparedPlan::SQLPreparedPlan(util::StackAllocator &alloc) :
		nodeList_(alloc),
		parameterList_(alloc),
		currentTimeRequired_(false),
		hintInfo_(NULL) {
}

SQLPreparedPlan::~SQLPreparedPlan() {
}

util::StackAllocator& SQLPreparedPlan::getAllocator() const {
	return *nodeList_.get_allocator().base();
}

SQLPreparedPlan::Node& SQLPreparedPlan::append(SQLType::Id type) {
	const Node::Id id = static_cast<Node::Id>(nodeList_.size());

	nodeList_.push_back(Node(getAllocator()));

	Node &node = back();
	node.id_ = id;
	node.type_ = type;

	return node;
}

SQLPreparedPlan::Node& SQLPreparedPlan::back() {
	assert(!nodeList_.empty());
	return nodeList_.back();
}

const SQLPreparedPlan::Node& SQLPreparedPlan::back() const {
	assert(!nodeList_.empty());
	return nodeList_.back();
}

bool SQLPreparedPlan::isDisabledNode(const Node &node) {
	return (node.type_ == getDisabledNodeType());
}

bool SQLPreparedPlan::isDisabledExpr(const SyntaxTree::Expr &expr) {
	return (expr.op_ == getDisabledExprType());
}

SQLType::Id SQLPreparedPlan::getDisabledNodeType() {
	return SQLType::START_EXEC;
}

SQLType::Id SQLPreparedPlan::getDisabledExprType() {
	return SQLType::START_EXPR;
}

void SQLPreparedPlan::removeEmptyNodes() {
	util::StackAllocator &alloc = getAllocator();

	uint32_t nextId = 0;
	util::Vector<uint32_t> idMap(alloc);
	for (NodeList::iterator nodeIt = nodeList_.begin();
			nodeIt != nodeList_.end(); ++nodeIt) {
		Node &node = *nodeIt;

		if (isDisabledNode(node)) {
			idMap.push_back(SyntaxTree::UNDEF_INPUTID);
		}
		else {
			idMap.push_back(nextId);
			nextId++;
		}
	}

	if (nextId == nodeList_.size()) {
		return;
	}

	NodeList nodeList(alloc);
	for (NodeList::iterator nodeIt = nodeList_.begin();
			nodeIt != nodeList_.end(); ++nodeIt) {
		Node &node = *nodeIt;

		if (isDisabledNode(node)) {
			continue;
		}

		nodeList.push_back(node);

		nodeList.back().id_ = static_cast<Node::Id>(nodeList.size() - 1);
		Node::IdList &inputList = nodeList.back().inputList_;

		for (Node::IdList::iterator it = inputList.begin();
				it != inputList.end(); ++it) {
			assert(idMap[*it] != SyntaxTree::UNDEF_INPUTID);
			*it = idMap[*it];
		}
	}

	nodeList_.swap(nodeList);
}

void SQLPreparedPlan::removeEmptyColumns() {
	util::StackAllocator &alloc = getAllocator();

	bool found = false;
	util::Vector<const util::Vector<ColumnId>*> allColumnMap(alloc);

	for (NodeList::iterator nodeIt = nodeList_.begin();
			nodeIt != nodeList_.end(); ++nodeIt) {
		Node &node = *nodeIt;

		if (isDisabledNode(node)) {
			allColumnMap.push_back(NULL);
			continue;
		}

		util::Vector<ColumnId> *columnMap =
				ALLOC_NEW(alloc) util::Vector<uint32_t>(alloc);

		ColumnId nextId = 0;
		for (Node::ExprList::iterator it = node.outputList_.begin();
				it != node.outputList_.end(); ++it) {
			if (isDisabledExpr(*it)) {
				columnMap->push_back(std::numeric_limits<ColumnId>::max());
				found = true;
			}
			else {
				columnMap->push_back(nextId);
				nextId++;
			}
		}

		allColumnMap.push_back(columnMap);
	}

	if (!found) {
		return;
	}

	for (NodeList::iterator nodeIt = nodeList_.begin();
			nodeIt != nodeList_.end(); ++nodeIt) {
		Node &node = *nodeIt;

		if (isDisabledNode(node)) {
			continue;
		}

		if (node.tableIdInfo_.isEmpty()) {
			node.remapColumnId(allColumnMap, node.predList_);
			node.remapColumnId(allColumnMap, node.outputList_);

			if (node.updateSetList_ != NULL) {
				node.remapColumnId(allColumnMap, *node.updateSetList_);
			}
		}

		for (Node::ExprList::iterator it = node.outputList_.begin();
				it != node.outputList_.end();) {
			if (isDisabledExpr(*it)) {
				it = node.outputList_.erase(it);
			}
			else {
				++it;
			}
		}
	}
}

void SQLPreparedPlan::validateReference(
		const SQLTableInfoList *tableInfoList) const {
	Node::Id resultId = std::numeric_limits<Node::Id>::max();

	for (NodeList::const_iterator nodeIt = nodeList_.begin();
			nodeIt != nodeList_.end(); ++nodeIt) {
		const Node &node = *nodeIt;

		if (isDisabledNode(node)) {
			continue;
		}

		node.validateReference(*this, tableInfoList);

		if (node.type_ == SQLType::EXEC_RESULT) {
			if (resultId < nodeList_.size()) {
				assert(false);
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
			}
			resultId = static_cast<Node::Id>(nodeIt - nodeList_.begin());
		}
	}

	if (resultId >= nodeList_.size()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}

	{
		util::StackAllocator &alloc = getAllocator();
		util::StackAllocator::Scope scope(alloc);

		const size_t nodeCount = nodeList_.size();

		typedef util::Vector<bool> VisitedList;
		VisitedList visitedList(nodeList_.size(), false, alloc);

		typedef std::pair<const Node*, uint32_t> PathEntry;
		typedef util::Vector<PathEntry> NodePath;

		NodePath nodePath(alloc);
		nodePath.push_back(PathEntry(&nodeList_[resultId], 0));
		do {
			if (nodePath.size() > nodeCount) {
				assert(false);
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
			}

			PathEntry &entry = nodePath.back();

			const Node *node = entry.first;
			uint32_t &inputId = entry.second;

			const Node::Id nodeId =
					static_cast<Node::Id>(node - &nodeList_[0]);
			visitedList[nodeId] = true;

			if (inputId < node->inputList_.size()) {
				const Node::Id inNodeId = node->inputList_[inputId];
				if (inNodeId >= nodeCount) {
					assert(false);
					GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
				}

				if (!visitedList[inNodeId]) {
					nodePath.push_back(PathEntry(&nodeList_[inNodeId], 0));
				}
				++inputId;
			}
			else {
				nodePath.pop_back();
			}
		}
		while (!nodePath.empty());

		for (NodeList::const_iterator nodeIt = nodeList_.begin();
				nodeIt != nodeList_.end(); ++nodeIt) {
			const Node &node = *nodeIt;
			const Node::Id nodeId =
					static_cast<Node::Id>(nodeIt - nodeList_.begin());
			const bool visited = visitedList[nodeId];

			if (!isDisabledNode(node) == !visited) {
				assert(false);
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
			}
		}
	}
}


void SQLPreparedPlan::setUpHintInfo(
		const SyntaxTree::ExprList* hintList, SQLTableInfoList &tableInfoList) {
	static_cast<void>(tableInfoList); 

	assert(hintInfo_ == NULL);
	if (hintList) {
		util::StackAllocator &alloc = getAllocator();
		SQLHintInfo *info =  ALLOC_NEW(alloc) SQLHintInfo(alloc);
		info->makeHintMap(hintList);
		hintInfo_ = info;
	}
}

void SQLPreparedPlan::dump(std::ostream &os, bool asJson) const {
	if (asJson) {
		picojson::value jsonValue;
		JsonUtils::OutStream out(jsonValue);
		TupleValue::coder(util::ObjectCoder(), NULL).encode(out, *this);
		const std::string &str = jsonValue.serialize();
		os.write(str.c_str(), static_cast<std::streamsize>(str.size()));
	}
	else {
		TupleValue::coder(util::ObjectCoder(), NULL).encode(os, *this);
	}
}

void SQLPreparedPlan::dumpToFile(const char8_t *name, bool asJson) const {
	util::NormalOStringStream oss;
	dump(oss, asJson);

	util::NamedFile file;
	file.open(name,
			util::FileFlag::TYPE_READ_WRITE | util::FileFlag::TYPE_CREATE |
			util::FileFlag::TYPE_TRUNCATE);
	file.lock();
	file.write(oss.str().c_str(), oss.str().size());
	file.close();
}

const char8_t SQLPreparedPlan::Node::Config::UNSPECIFIED_COLUMN_NAME[] = "";

const SQLPreparedPlan::Node::CommandOptionFlag
		SQLPreparedPlan::Node::Config::CMD_OPT_SCAN_INDEX = 1 << 0;

const SQLPreparedPlan::Node::CommandOptionFlag
		SQLPreparedPlan::Node::Config::CMD_OPT_JOIN_LEADING_LEFT = 1 << 1;

const SQLPreparedPlan::Node::CommandOptionFlag
		SQLPreparedPlan::Node::Config::CMD_OPT_JOIN_LEADING_RIGHT = 1 << 2;

const SQLPreparedPlan::Node::CommandOptionFlag
		SQLPreparedPlan::Node::Config::CMD_OPT_SCAN_EXPIRABLE = 1 << 3;

const SQLPreparedPlan::Node::CommandOptionFlag
		SQLPreparedPlan::Node::Config::CMD_OPT_JOIN_NO_INDEX = 1 << 4;

const SQLPreparedPlan::Node::CommandOptionFlag
		SQLPreparedPlan::Node::Config::CMD_OPT_JOIN_USE_INDEX = 1 << 5;

const SQLPreparedPlan::Node::CommandOptionFlag
		SQLPreparedPlan::Node::Config::CMD_OPT_SCAN_MULTI_INDEX = 1 << 6;

const SQLPreparedPlan::Node::CommandOptionFlag
		SQLPreparedPlan::Node::Config::CMD_OPT_WINDOW_SORTED = 1 << 7;

const SQLPreparedPlan::Node::CommandOptionFlag
		SQLPreparedPlan::Node::Config::CMD_OPT_GROUP_RANGE = 1 << 8;

const SQLPreparedPlan::Node::CommandOptionFlag
		SQLPreparedPlan::Node::Config::CMD_OPT_JOIN_DRIVING_NONE_LEFT = 1 << 9;
const SQLPreparedPlan::Node::CommandOptionFlag
		SQLPreparedPlan::Node::Config::CMD_OPT_JOIN_DRIVING_SOME = 1 << 10;

SQLPreparedPlan::Node::Node(util::StackAllocator &alloc) :
		id_(0),
		type_(Type()),
		qName_(NULL),
		inputList_(alloc),
		predList_(alloc),
		outputList_(alloc),
		indexInfoList_(NULL),
		offset_(0),
		limit_(-1),
		subOffset_(0),
		subLimit_(-1),
		aggPhase_(AggregationPhase()),
		joinType_(JoinType()),
		unionType_(UnionType()),
		cmdOptionFlag_(CommandOptionFlag()),
		updateSetList_(NULL),
		createTableOpt_(NULL),
		createIndexOpt_(NULL),
		partitioningInfo_(NULL),
		commandType_(SyntaxTree::CMD_NONE),
		cmdOptionList_(NULL),
		insertColumnMap_(NULL),
		insertSetList_(NULL),
		nosqlTypeList_(NULL),
		nosqlColumnOptionList_(NULL),
		profile_(NULL),
		profileKey_(-1) {
}

util::StackAllocator& SQLPreparedPlan::Node::getAllocator() const {
	return *inputList_.get_allocator().base();
}

SQLPreparedPlan::Node& SQLPreparedPlan::Node::getInput(
		SQLPreparedPlan &plan, size_t index) {
	return plan.nodeList_[inputList_[index]];
}

const SQLPreparedPlan::Node& SQLPreparedPlan::Node::getInput(
		const SQLPreparedPlan &plan, size_t index) const {
	return plan.nodeList_[inputList_[index]];
}

void SQLPreparedPlan::Node::setName(
		const util::String &table, const util::String *db, int64_t nsId) {
	util::StackAllocator &alloc = getAllocator();

	QualifiedName *name = ALLOC_NEW(alloc) SyntaxTree::QualifiedName(alloc);
	name->table_ = ALLOC_NEW(alloc) util::String(table);
	name->nsId_ = nsId;
	if (db != NULL) {
		name->db_ = ALLOC_NEW(alloc) util::String(*db);
	}

	qName_ = name;
}

void SQLPreparedPlan::Node::getColumnNameList(
		util::Vector<util::String> &nameList) const {
	util::StackAllocator &alloc = *nameList.get_allocator().base();

	for (ExprList::const_iterator it = outputList_.begin();
			it != outputList_.end(); ++it) {
		const QualifiedName *qName = it->qName_;

		const char8_t *columnName;
		if (qName != NULL && qName->name_ != NULL) {
			columnName = qName->name_->c_str();
		}
		else {
			columnName = Config::UNSPECIFIED_COLUMN_NAME;
		}

		nameList.push_back(util::String(columnName, alloc));
	}
}

void SQLPreparedPlan::Node::remapColumnId(
		const util::Vector<const util::Vector<ColumnId>*> &allColumnMap,
		ExprList &exprList) {
	assert(tableIdInfo_.isEmpty());

	for (ExprList::iterator it = exprList.begin(); it != exprList.end(); ++it) {
		Expr *expr = &(*it);

		if (isDisabledExpr(*expr)) {
			continue;
		}

		remapColumnId(allColumnMap, expr);

		if (expr != &(*it)) {
			*it = *expr;
		}
	}
}

void SQLPreparedPlan::Node::remapColumnId(
		const util::Vector<const util::Vector<ColumnId>*> &allColumnMap,
		Expr *&expr) {
	assert(expr != NULL);
	assert(!isDisabledExpr(*expr));
	assert(tableIdInfo_.isEmpty());

	if (expr->op_ != SQLType::EXPR_COLUMN &&
			expr->left_ == NULL &&
			expr->right_ == NULL &&
			expr->next_ == NULL) {
		return;
	}

	util::StackAllocator &alloc = expr->alloc_;
	expr = ALLOC_NEW(alloc) Expr(*expr);

	if (expr->op_ == SQLType::EXPR_COLUMN) {
		const uint32_t inputId = expr->inputId_;
		if (inputId >= inputList_.size()) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
		}

		const Id inNodeId = inputList_[inputId];
		if (inNodeId >= allColumnMap.size()) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
		}

		const util::Vector<ColumnId> *columnMap = allColumnMap[inNodeId];
		if (columnMap == NULL || expr->columnId_ >= columnMap->size()) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
		}
		expr->columnId_ = (*columnMap)[expr->columnId_];
	}

	if (expr->left_ != NULL) {
		remapColumnId(allColumnMap, expr->left_);
	}

	if (expr->right_ != NULL) {
		remapColumnId(allColumnMap, expr->right_);
	}

	if (expr->next_ != NULL) {
		expr->next_ = ALLOC_NEW(alloc) SyntaxTree::ExprList(
				expr->next_->begin(), expr->next_->end(), alloc);

		for (SyntaxTree::ExprList::iterator it = expr->next_->begin();
				it != expr->next_->end(); ++it) {
			remapColumnId(allColumnMap, *it);
		}
	}
}

void SQLPreparedPlan::Node::validateReference(
		const SQLPreparedPlan &plan,
		const SQLTableInfoList *tableInfoList) const {
	if (id_ != static_cast<Id>(this - &plan.nodeList_[0])) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}

	for (IdList::const_iterator inIt = inputList_.begin();;) {
		if (inIt != inputList_.end() && *inIt >= plan.nodeList_.size()) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
		}

		const Node *inNode = NULL;
		if (inIt != inputList_.end()) {
			inNode = &plan.nodeList_[*inIt];
		}

		if ((type_ == SQLType::EXEC_LIMIT || type_ == SQLType::EXEC_UNION) &&
				inNode != NULL && outputList_.size() != inNode->outputList_.size()) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
		}

		const Id *idRef = NULL;
		if (type_ == SQLType::EXEC_LIMIT || type_ == SQLType::EXEC_UNION) {
			if (inNode == NULL) {
				assert(false);
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
			}
			idRef = &(*inIt);
		}

		if (inIt == inputList_.begin() || idRef != NULL) {
			for (ExprList::const_iterator it = predList_.begin();
					it != predList_.end(); ++it) {
				validateReference(plan, tableInfoList, idRef, *it);
			}

			size_t count = 0;
			for (ExprList::const_iterator it = outputList_.begin();
					it != outputList_.end(); ++it) {
				if ((type_ == SQLType::EXEC_LIMIT || type_ == SQLType::EXEC_UNION) &&
						inNode != NULL) {
					if (isDisabledExpr(*it)) {
						if (!isDisabledExpr(
								inNode->outputList_[it - outputList_.begin()])) {
							assert(false);
							GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
						}
					}
					else {
						if (it->op_ != SQLType::EXPR_COLUMN || it->inputId_ != 0 ||
							it->columnId_ != static_cast<size_t>(it - outputList_.begin())) {
							assert(false);
							GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
						}
					}
				}

				if (isDisabledExpr(*it)) {
					continue;
				}
				validateReference(plan, tableInfoList, idRef, *it);
				count++;
			}

			if (count == 0) {
				switch (type_) {
				case SQLType::EXEC_GROUP:
				case SQLType::EXEC_JOIN:
				case SQLType::EXEC_LIMIT:
				case SQLType::EXEC_SCAN:
				case SQLType::EXEC_SELECT:
				case SQLType::EXEC_SORT:
				case SQLType::EXEC_UNION:
					assert(false);
					GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
					break;
				default:
					break;
				}
			}
		}

		if (inIt == inputList_.end() || ++inIt == inputList_.end()) {
			break;
		}
	}
}

void SQLPreparedPlan::Node::validateReference(
		const SQLPreparedPlan &plan, const SQLTableInfoList *tableInfoList,
		const Id *idRef, const Expr &expr) const {

	if (isDisabledExpr(expr)) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}
	else if (expr.op_ == SQLType::EXPR_COLUMN) {
		const uint32_t tableCount = getInputTableCount();

		if (tableCount > 0 && expr.inputId_ == 0) {
			if (tableInfoList == NULL) {
				assert(false);
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
			}

			const SQLTableInfo &tableInfo =
					tableInfoList->resolve(tableIdInfo_.id_);

			if (expr.columnId_ >= tableInfo.columnInfoList_.size()) {
				assert(false);
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
			}

			return;
		}

		const uint32_t pos = expr.inputId_ - tableCount;
		if (pos >= inputList_.size()) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
		}

		const Id id = (idRef == NULL ? inputList_[pos] : *idRef);
		if (id >= plan.nodeList_.size() ||
				isDisabledNode(plan.nodeList_[id])) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
		}

		const Node &node = plan.nodeList_[id];
		if (expr.columnId_ >= node.outputList_.size()) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
		}

		if (isDisabledExpr(node.outputList_[expr.columnId_])) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
		}

		return;
	}

	for (SQLCompiler::ExprArgsIterator<> it(expr); it.exists(); it.next()) {
		validateReference(plan, tableInfoList, idRef, it.get());
	}
}

uint32_t SQLPreparedPlan::Node::getInputTableCount() const {
	return (tableIdInfo_.isEmpty() || type_ != SQLType::EXEC_SCAN ? 0 : 1);
}

const char8_t SQLCompiler::Meta::DRIVER_TABLES_NAME[] =
	"#_driver_tables";
const char8_t SQLCompiler::Meta::DRIVER_COLUMNS_NAME[] =
	"#_driver_columns";
const char8_t SQLCompiler::Meta::DRIVER_PRIMARY_KEYS_NAME[] =
	"#_driver_primary_keys";
const char8_t SQLCompiler::Meta::DRIVER_TYPE_INFO_NAME[] =
	"#_driver_type_info";
const char8_t SQLCompiler::Meta::DRIVER_INDEX_INFO_NAME[] =
	"#_driver_index_info";
const char8_t SQLCompiler::Meta::DRIVER_CLUSTER_INFO_NAME[] =
	"#_driver_cluster_info";
const char8_t SQLCompiler::Meta::DRIVER_TABLE_TYPES_NAME[] =
	"#_driver_table_types";

const util::NameCoderEntry<SQLPreparedPlan::Constants::StringConstant>
SQLPreparedPlan::Constants::CONSTANT_LIST[] = {
	UTIL_NAME_CODER_ENTRY(STR_REORDER_JOIN),
	UTIL_NAME_CODER_ENTRY(STR_REVERSED_COST),

	UTIL_NAME_CODER_ENTRY(STR_TREE_WEIGHT),
	UTIL_NAME_CODER_ENTRY(STR_FILTERED_TREE_WEIGHT),
	UTIL_NAME_CODER_ENTRY(STR_NODE_DEGREE),

	UTIL_NAME_CODER_ENTRY(STR_FILTER),
	UTIL_NAME_CODER_ENTRY(STR_FILTER_LEVEL),
	UTIL_NAME_CODER_ENTRY(STR_FILTER_DEGREE),
	UTIL_NAME_CODER_ENTRY(STR_FILTER_WEAKNESS),

	UTIL_NAME_CODER_ENTRY(STR_EDGE),
	UTIL_NAME_CODER_ENTRY(STR_EDGE_LEVEL),
	UTIL_NAME_CODER_ENTRY(STR_EDGE_DEGREE),
	UTIL_NAME_CODER_ENTRY(STR_EDGE_WEAKNESS),

	UTIL_NAME_CODER_ENTRY(STR_NONE),
	UTIL_NAME_CODER_ENTRY(STR_TABLE),
	UTIL_NAME_CODER_ENTRY(STR_INDEX),
	UTIL_NAME_CODER_ENTRY(STR_SCHEMA),
	UTIL_NAME_CODER_ENTRY(STR_SYNTAX),

	UTIL_NAME_CODER_ENTRY(STR_HINT)
};

const util::NameCoder<
		SQLPreparedPlan::Constants::StringConstant,
		SQLPreparedPlan::Constants::END_STR>
		SQLPreparedPlan::Constants::CONSTANT_CODER(CONSTANT_LIST, 1);

SQLPreparedPlan::TotalProfile::TotalProfile() :
		plan_(NULL) {
}

bool SQLPreparedPlan::TotalProfile::clearIfEmpty(TotalProfile *&profile) {
	if (profile != NULL && Profile::clearIfEmpty(profile->plan_)) {
		profile = NULL;
	}
	return (profile == NULL);
}

SQLPreparedPlan::Profile::Profile() :
		optimization_(NULL) {
}

bool SQLPreparedPlan::Profile::clearIfEmpty(Profile *&profile) {
	if (profile != NULL && OptProfile::clearIfEmpty(profile->optimization_)) {
		profile = NULL;
	}
	return (profile == NULL);
}

SQLPreparedPlan::OptProfile::OptProfile() :
		joinReordering_(NULL),
		joinDriving_(NULL) {
}

template<>
util::Vector<SQLPreparedPlan::OptProfile::Join>*&
SQLPreparedPlan::OptProfile::getEntryList() {
	return joinReordering_;
}

template<>
util::Vector<SQLPreparedPlan::OptProfile::JoinDriving>*&
SQLPreparedPlan::OptProfile::getEntryList() {
	return joinDriving_;
}

bool SQLPreparedPlan::OptProfile::clearIfEmpty(OptProfile *&profile) {
	if (profile != NULL &&
			isListEmpty(profile->joinReordering_) &&
			isListEmpty(profile->joinDriving_)) {
		profile = NULL;
	}
	return (profile == NULL);
}

template<typename T>
bool SQLPreparedPlan::OptProfile::isListEmpty(const util::Vector<T> *list) {
	return (list == NULL || list->empty());
}

SQLPreparedPlan::OptProfile::Join::Join() :
		nodes_(NULL),
		tree_(NULL),
		candidates_(NULL),
		reordered_(false),
		tableCostAffected_(false),
		costBased_(true),
		profileKey_(-1) {
}

SQLPreparedPlan::OptProfile::JoinNode::JoinNode() :
		ordinal_(-1),
		qName_(NULL),
		approxSize_(-1) {
}

SQLPreparedPlan::OptProfile::JoinTree::JoinTree() :
		ordinal_(-1),
		left_(NULL),
		right_(NULL),
		criterion_(Constants::END_STR),
		cost_(NULL),
		best_(NULL),
		other_(NULL),
		target_(NULL) {
}

SQLPreparedPlan::OptProfile::JoinCost::JoinCost() :
		filterLevel_(0),
		filterDegree_(0),
		filterWeakness_(0),
		edgeLevel_(0),
		edgeDegree_(0),
		edgeWeakness_(0),
		approxSize_(-1) {
}

SQLPreparedPlan::OptProfile::JoinDriving::JoinDriving() :
		driving_(NULL),
		inner_(NULL),
		left_(NULL),
		right_(NULL),
		profileKey_(-1) {
}

SQLPreparedPlan::OptProfile::JoinDrivingInput::JoinDrivingInput() :
		qName_(NULL),
		cost_(NULL),
		tableScanCost_(NULL),
		indexScanCost_(NULL),
		criterion_(Constants::END_STR),
		indexed_(false),
		reducible_(false) {
}

const util::NameCoderEntry<SQLCompiler::Meta::StringConstant>
SQLCompiler::Meta::CONSTANT_LIST[] = {
	UTIL_NAME_CODER_ENTRY(STR_AUTO_INCREMENT),
	UTIL_NAME_CODER_ENTRY(STR_BUFFER_LENGTH),
	UTIL_NAME_CODER_ENTRY(STR_CASE_SENSITIVE),
	UTIL_NAME_CODER_ENTRY(STR_CHAR_OCTET_LENGTH),
	UTIL_NAME_CODER_ENTRY(STR_COLUMN_DEF),
	UTIL_NAME_CODER_ENTRY(STR_COLUMN_NAME),
	UTIL_NAME_CODER_ENTRY(STR_COLUMN_SIZE),
	UTIL_NAME_CODER_ENTRY(STR_CREATE_PARAMS),
	UTIL_NAME_CODER_ENTRY(STR_DATA_TYPE),
	UTIL_NAME_CODER_ENTRY(STR_DECIMAL_DIGITS),
	UTIL_NAME_CODER_ENTRY(STR_FIXED_PREC_SCALE),
	UTIL_NAME_CODER_ENTRY(STR_IS_AUTOINCREMENT),
	UTIL_NAME_CODER_ENTRY(STR_IS_GENERATEDCOLUMN),
	UTIL_NAME_CODER_ENTRY(STR_IS_NULLABLE),
	UTIL_NAME_CODER_ENTRY(STR_KEY_SEQ),
	UTIL_NAME_CODER_ENTRY(STR_LITERAL_PREFIX),
	UTIL_NAME_CODER_ENTRY(STR_LITERAL_SUFFIX),
	UTIL_NAME_CODER_ENTRY(STR_LOCAL_TYPE_NAME),
	UTIL_NAME_CODER_ENTRY(STR_MINIMUM_SCALE),
	UTIL_NAME_CODER_ENTRY(STR_MAXIMUM_SCALE),
	UTIL_NAME_CODER_ENTRY(STR_NULLABLE),
	UTIL_NAME_CODER_ENTRY(STR_NUM_PREC_RADIX),
	UTIL_NAME_CODER_ENTRY(STR_ORDINAL_POSITION),
	UTIL_NAME_CODER_ENTRY(STR_PK_NAME),
	UTIL_NAME_CODER_ENTRY(STR_PRECISION),
	UTIL_NAME_CODER_ENTRY(STR_REF_GENERATION),
	UTIL_NAME_CODER_ENTRY(STR_REMARKS),
	UTIL_NAME_CODER_ENTRY(STR_SCOPE_CATALOG),
	UTIL_NAME_CODER_ENTRY(STR_SCOPE_SCHEMA),
	UTIL_NAME_CODER_ENTRY(STR_SCOPE_TABLE),
	UTIL_NAME_CODER_ENTRY(STR_SEARCHABLE),
	UTIL_NAME_CODER_ENTRY(STR_SELF_REFERENCING_COL_NAME),
	UTIL_NAME_CODER_ENTRY(STR_SOURCE_DATA_TYPE),
	UTIL_NAME_CODER_ENTRY(STR_SQL_DATA_TYPE),
	UTIL_NAME_CODER_ENTRY(STR_SQL_DATETIME_SUB),
	UTIL_NAME_CODER_ENTRY(STR_TABLE),
	UTIL_NAME_CODER_ENTRY(STR_TABLE_CAT),
	UTIL_NAME_CODER_ENTRY(STR_TABLE_SCHEM),
	UTIL_NAME_CODER_ENTRY(STR_TABLE_NAME),
	UTIL_NAME_CODER_ENTRY(STR_TABLE_TYPE),
	UTIL_NAME_CODER_ENTRY(STR_TYPE_CAT),
	UTIL_NAME_CODER_ENTRY(STR_TYPE_SCHEM),
	UTIL_NAME_CODER_ENTRY(STR_TYPE_NAME),
	UTIL_NAME_CODER_ENTRY(STR_UNSIGNED_ATTRIBUTE),
	UTIL_NAME_CODER_ENTRY(STR_YES),
	UTIL_NAME_CODER_ENTRY(STR_NO),
	UTIL_NAME_CODER_ENTRY(STR_UNKNOWN),
	UTIL_NAME_CODER_ENTRY(STR_EMPTY),
	UTIL_NAME_CODER_ENTRY(STR_NON_UNIQUE),
	UTIL_NAME_CODER_ENTRY(STR_INDEX_QUALIFIER),
	UTIL_NAME_CODER_ENTRY(STR_INDEX_NAME),
	UTIL_NAME_CODER_ENTRY(STR_TYPE),
	UTIL_NAME_CODER_ENTRY(STR_ASC_OR_DESC),
	UTIL_NAME_CODER_ENTRY(STR_CARDINALITY),
	UTIL_NAME_CODER_ENTRY(STR_PAGES),
	UTIL_NAME_CODER_ENTRY(STR_FILTER_CONDITION),
	UTIL_NAME_CODER_ENTRY(STR_DATABASE_MAJOR_VERSION),
	UTIL_NAME_CODER_ENTRY(STR_DATABASE_MINOR_VERSION),
	UTIL_NAME_CODER_ENTRY(STR_EXTRA_NAME_CHARACTERS),
	UTIL_NAME_CODER_ENTRY(STR_VIEW)
};

const util::NameCoder<
		SQLCompiler::Meta::StringConstant, SQLCompiler::Meta::END_STR>
SQLCompiler::Meta::CONSTANT_CODER(CONSTANT_LIST, 1);

const SQLCompiler::Meta::ColumnDefinition
SQLCompiler::Meta::DRIVER_TABLES_COLUMN_LIST[] = {
	defineColumn(TupleList::TYPE_STRING, STR_TABLE_CAT),
	defineColumn(TupleList::TYPE_STRING, STR_TABLE_SCHEM),
	defineColumn(TupleList::TYPE_STRING, STR_TABLE_NAME),
	defineColumn(TupleList::TYPE_STRING, STR_TABLE_TYPE),
	defineColumn(TupleList::TYPE_STRING, STR_REMARKS),
	defineColumn(TupleList::TYPE_STRING, STR_TYPE_CAT),
	defineColumn(TupleList::TYPE_STRING, STR_TYPE_SCHEM),
	defineColumn(TupleList::TYPE_STRING, STR_TYPE_NAME),
	defineColumn(TupleList::TYPE_STRING, STR_SELF_REFERENCING_COL_NAME),
	defineColumn(TupleList::TYPE_STRING, STR_REF_GENERATION),
};

const SQLCompiler::Meta::ColumnDefinition
SQLCompiler::Meta::DRIVER_COLUMNS_COLUMN_LIST[] = {
	defineColumn(TupleList::TYPE_STRING, STR_TABLE_CAT),
	defineColumn(TupleList::TYPE_STRING, STR_TABLE_SCHEM),
	defineColumn(TupleList::TYPE_STRING, STR_TABLE_NAME),
	defineColumn(TupleList::TYPE_STRING, STR_COLUMN_NAME),
	defineColumn(TupleList::TYPE_INTEGER, STR_DATA_TYPE),
	defineColumn(TupleList::TYPE_STRING, STR_TYPE_NAME),
	defineColumn(TupleList::TYPE_INTEGER, STR_COLUMN_SIZE),
	defineColumn(TupleList::TYPE_INTEGER, STR_BUFFER_LENGTH),
	defineColumn(TupleList::TYPE_INTEGER, STR_DECIMAL_DIGITS),
	defineColumn(TupleList::TYPE_INTEGER, STR_NUM_PREC_RADIX),
	defineColumn(TupleList::TYPE_INTEGER, STR_NULLABLE),
	defineColumn(TupleList::TYPE_STRING, STR_REMARKS),
	defineColumn(TupleList::TYPE_STRING, STR_COLUMN_DEF),
	defineColumn(TupleList::TYPE_INTEGER, STR_SQL_DATA_TYPE),
	defineColumn(TupleList::TYPE_INTEGER, STR_SQL_DATETIME_SUB),
	defineColumn(TupleList::TYPE_INTEGER, STR_CHAR_OCTET_LENGTH),
	defineColumn(TupleList::TYPE_INTEGER, STR_ORDINAL_POSITION),
	defineColumn(TupleList::TYPE_STRING, STR_IS_NULLABLE),
	defineColumn(TupleList::TYPE_STRING, STR_SCOPE_CATALOG),
	defineColumn(TupleList::TYPE_STRING, STR_SCOPE_SCHEMA),
	defineColumn(TupleList::TYPE_STRING, STR_SCOPE_TABLE),
	defineColumn(TupleList::TYPE_SHORT, STR_SOURCE_DATA_TYPE),
	defineColumn(TupleList::TYPE_STRING, STR_IS_AUTOINCREMENT),
	defineColumn(TupleList::TYPE_STRING, STR_IS_GENERATEDCOLUMN)
};

const SQLCompiler::Meta::ColumnDefinition
SQLCompiler::Meta::DRIVER_PRIMARY_KEYS_COLUMN_LIST[] = {
	defineColumn(TupleList::TYPE_STRING, STR_TABLE_CAT),
	defineColumn(TupleList::TYPE_STRING, STR_TABLE_SCHEM),
	defineColumn(TupleList::TYPE_STRING, STR_TABLE_NAME),
	defineColumn(TupleList::TYPE_STRING, STR_COLUMN_NAME),
	defineColumn(TupleList::TYPE_SHORT, STR_KEY_SEQ),
	defineColumn(TupleList::TYPE_STRING, STR_PK_NAME)
};

const SQLCompiler::Meta::ColumnDefinition
SQLCompiler::Meta::DRIVER_TYPE_INFO_COLUMN_LIST[] = {
	defineColumn(TupleList::TYPE_STRING, STR_TYPE_NAME),
	defineColumn(TupleList::TYPE_INTEGER, STR_DATA_TYPE),
	defineColumn(TupleList::TYPE_INTEGER, STR_PRECISION),
	defineColumn(TupleList::TYPE_STRING, STR_LITERAL_PREFIX),
	defineColumn(TupleList::TYPE_STRING, STR_LITERAL_SUFFIX),
	defineColumn(TupleList::TYPE_STRING, STR_CREATE_PARAMS),
	defineColumn(TupleList::TYPE_SHORT, STR_NULLABLE),
	defineColumn(TupleList::TYPE_BOOL, STR_CASE_SENSITIVE),
	defineColumn(TupleList::TYPE_SHORT, STR_SEARCHABLE),
	defineColumn(TupleList::TYPE_BOOL, STR_UNSIGNED_ATTRIBUTE),
	defineColumn(TupleList::TYPE_BOOL, STR_FIXED_PREC_SCALE),
	defineColumn(TupleList::TYPE_BOOL, STR_AUTO_INCREMENT),
	defineColumn(TupleList::TYPE_STRING, STR_LOCAL_TYPE_NAME),
	defineColumn(TupleList::TYPE_SHORT, STR_MINIMUM_SCALE),
	defineColumn(TupleList::TYPE_SHORT, STR_MAXIMUM_SCALE),
	defineColumn(TupleList::TYPE_INTEGER, STR_SQL_DATA_TYPE),
	defineColumn(TupleList::TYPE_INTEGER, STR_SQL_DATETIME_SUB),
	defineColumn(TupleList::TYPE_INTEGER, STR_NUM_PREC_RADIX)
};

const SQLCompiler::Meta::ColumnDefinition
SQLCompiler::Meta::DRIVER_INDEX_INFO_COLUMN_LIST[] = {
	defineColumn(TupleList::TYPE_STRING, STR_TABLE_CAT),
	defineColumn(TupleList::TYPE_STRING, STR_INDEX_NAME),
	defineColumn(TupleList::TYPE_STRING, STR_TABLE_NAME),
	defineColumn(TupleList::TYPE_LONG, STR_PAGES),			
	defineColumn(TupleList::TYPE_INTEGER, STR_MINIMUM_SCALE),	
	defineColumn(TupleList::TYPE_SHORT, STR_ORDINAL_POSITION),  
	defineColumn(TupleList::TYPE_STRING, STR_COLUMN_NAME),	
	defineColumn(TupleList::TYPE_SHORT, STR_TYPE),			
	defineColumn(TupleList::TYPE_INTEGER, STR_CARDINALITY),	
};

const SQLCompiler::Meta::ColumnDefinition
SQLCompiler::Meta::DRIVER_CLUSTER_INFO_COLUMN_LIST[] = {
	defineColumn(TupleList::TYPE_INTEGER, STR_DATABASE_MAJOR_VERSION),
	defineColumn(TupleList::TYPE_INTEGER, STR_DATABASE_MINOR_VERSION),
	defineColumn(TupleList::TYPE_STRING, STR_EXTRA_NAME_CHARACTERS)
};

const SQLCompiler::Meta::ColumnDefinition
SQLCompiler::Meta::DRIVER_TABLE_TYPES_COLUMN_LIST[] = {
	defineColumn(TupleList::TYPE_STRING, STR_TABLE_TYPE)
};

const SQLCompiler::PlanNode::Id SQLCompiler::UNDEF_PLAN_NODEID =
		std::numeric_limits<PlanNode::Id>::max();
const ColumnId SQLCompiler::REPLACED_AGGR_COLUMN_ID = MAX_COLUMNID;

const SQLCompiler::JoinNodeId SQLCompiler::UNDEF_JOIN_NODEID =
		std::numeric_limits<JoinNodeId>::max();

const uint64_t SQLCompiler::JOIN_SCORE_MAX = UINT64_C(1) << 10;
const uint64_t SQLCompiler::DEFAULT_MAX_INPUT_COUNT =
		std::numeric_limits<uint64_t>::max();
const uint64_t SQLCompiler::DEFAULT_MAX_EXPANSION_COUNT = 100;

const SQLPlanningVersion SQLCompiler::LEGACY_JOIN_REORDERING_VERSION(5, 4);
const SQLPlanningVersion SQLCompiler::LEGACY_JOIN_DRIVING_VERSION(5, 7);

SQLCompiler::SQLCompiler(
		TupleValue::VarContext &varCxt, const CompileOption *option) :
		alloc_(varContextToAllocator(varCxt)),
		varCxt_(varCxt),
		init_(option),
		tableInfoList_(init_ ?
				init_->tableInfoList_ : emptyPtr<TableInfoList>()),
		subqueryStack_(makeSubqueryStack(alloc_)),
		lastSrcId_(init_ ? init_->lastSrcId_ : 0),
		pendingColumnList_(init_ ?
				init_->pendingColumnList_ : PendingColumnList(alloc_)),
		metaTableInfoIdList_(init_ ?
				init_->metaTableInfoIdList_ : TableInfoIdList(alloc_)),
		parameterTypeList_(init_ ?
				init_->parameterTypeList_ : emptyPtr<Plan::ValueTypeList>()),
		inputSql_(init_ ? init_->inputSql_ : emptyPtr<const char8_t>()),
		subqueryLevel_(init_ ? init_->subqueryLevel_ : 0),
		explainType_(init_ ? init_->explainType_ : SyntaxTree::EXPLAIN_NONE),
		queryStartTime_(init_ ? init_->queryStartTime_ : util::DateTime()),
		planSizeLimitRatio_(init_ ? init_->planSizeLimitRatio_ : 5 * 100),
		generatedScanCount_(init_ ?
				init_->generatedScanCount_ :
				std::numeric_limits<uint64_t>::max()),
		expandedJoinCount_(init_ ? init_->expandedJoinCount_ : 0),
		tableNarrowingList_(init_ ?
				SQLCompiler::TableNarrowingList::tryDuplicate(
						init_->tableNarrowingList_) :
				emptyPtr<TableNarrowingList>()),
		compileOption_(init_ ? init_->compileOption_ : option),
		optimizationOption_(makeOptimizationOption(
				alloc_, (init_ ?
						*init_->optimizationOption_ :
						OptimizationOption(&init_.profilerManager())))),
		currentTimeRequired_(init_ ? init_->currentTimeRequired_ : false),
		isMetaDataQuery_(init_ ? init_->isMetaDataQuery_ : false),
		indexScanEnabled_(init_ ?
				init_->indexScanEnabled_ : isIndexScanEnabledDefault()),
		multiIndexScanEnabled_(init_ ? init_->multiIndexScanEnabled_ : true),
		expansionLimitReached_(init_ ? init_->expansionLimitReached_ : false),
		predicatePushDownApplied_(init_ ? init_->predicatePushDownApplied_ : false),
		substrCompatible_(init_ ? init_->substrCompatible_ : false),
		varianceCompatible_(init_ ? init_->varianceCompatible_ : false),
		experimentalFuncEnabled_(init_ ? init_->experimentalFuncEnabled_ : false),
		recompileOnBindRequired_(init_ ? init_->recompileOnBindRequired_ : false),
		neverReduceExpr_(init_ ? init_->neverReduceExpr_ : false),
		profiler_(init_ ?
				SQLCompiler::Profiler::tryDuplicate(init_->profiler_) :
				init_.profilerManager().tryCreateProfiler(alloc_)),
		nextProfileKey_(init_ ? init_->nextProfileKey_ : -1),
		initEnd_(init_) {
}

SQLCompiler::~SQLCompiler() {
}

void SQLCompiler::setTableInfoList(const TableInfoList *infoList) {
	tableInfoList_ = infoList;
}

void SQLCompiler::setParameterTypeList(
		Plan::ValueTypeList *parameterTypeList) {
	parameterTypeList_ = parameterTypeList;
}

void SQLCompiler::setInputSql(const char* sql) {
	inputSql_ = sql;
}

void SQLCompiler::setMetaDataQueryFlag(bool isMetaDataQuery) {
	isMetaDataQuery_ = isMetaDataQuery;
}

bool SQLCompiler::isMetaDataQuery() {
	return isMetaDataQuery_;
}

void SQLCompiler::setExplainType(SyntaxTree::ExplainType explainType) {
	explainType_ = explainType;
}

void SQLCompiler::setQueryStartTime(int64_t time) {
	queryStartTime_ = util::DateTime(time);
}

void SQLCompiler::setPlanSizeLimitRatio(size_t ratio) {
	planSizeLimitRatio_ = ratio;
}

void SQLCompiler::setIndexScanEnabled(bool enabled) {
	indexScanEnabled_ = enabled;
}

void SQLCompiler::setMultiIndexScanEnabled(bool enabled) {
	multiIndexScanEnabled_ = enabled;
}

void SQLCompiler::compile(const Select &in, Plan &plan, SQLConnectionControl *control) {
	{
		std::string value;

		if (control->getEnv(
				SQLPragma::PRAGMA_INTERNAL_COMPILER_EXECUTE_AS_META_DATA_QUERY,
				value)) {
			setMetaDataQueryFlag((value == SQLPragma::VALUE_TRUE));
		}

		if (control->getEnv(SQLPragma::PRAGMA_INTERNAL_COMPILER_SCAN_INDEX, value)) {
			setIndexScanEnabled((value == SQLPragma::VALUE_TRUE));
		}

		if (control->getEnv(SQLPragma::PRAGMA_EXPERIMENTAL_SCAN_MULTI_INDEX, value)) {
			setMultiIndexScanEnabled((value == SQLPragma::VALUE_TRUE));
		}
		else {
			setMultiIndexScanEnabled(control->getConfig().isMultiIndexScan());
		}

		if (control->getEnv(
				SQLPragma::PRAGMA_INTERNAL_COMPILER_SUBSTR_COMPATIBLE, value)) {
			substrCompatible_ = (value == SQLPragma::VALUE_TRUE);
		}
		if (control->getEnv(
				SQLPragma::PRAGMA_INTERNAL_COMPILER_VARIANCE_COMPATIBLE, value)) {
			varianceCompatible_ = (value == SQLPragma::VALUE_TRUE);
		}
		if (control->getEnv(
				SQLPragma::PRAGMA_INTERNAL_COMPILER_EXPERIMENTAL_FUNCTIONS, value)) {
			experimentalFuncEnabled_ = (value == SQLPragma::VALUE_TRUE);
		}
	}

	applyPlanningOption(plan);

	switch (in.cmdType_) {
	case SyntaxTree::CMD_SELECT:
		{
			PlanNodeId productedNodeId = UNDEF_PLAN_NODEID;
			genSelect(in, false, productedNodeId, plan);
		}
		break;
	case SyntaxTree::CMD_INSERT:
		genInsert(in, plan);
		break;
	case SyntaxTree::CMD_UPDATE:
		genUpdate(in, plan);
		break;
	case SyntaxTree::CMD_DELETE:
		genDelete(in, plan);
		break;
	case SyntaxTree::CMD_CREATE_TABLE:
	case SyntaxTree::CMD_CREATE_INDEX:
	case SyntaxTree::CMD_CREATE_DATABASE:
	case SyntaxTree::CMD_DROP_DATABASE:
	case SyntaxTree::CMD_DROP_TABLE:
	case SyntaxTree::CMD_DROP_INDEX:
	case SyntaxTree::CMD_ALTER_TABLE_DROP_PARTITION:  
	case SyntaxTree::CMD_ALTER_TABLE_ADD_COLUMN:      
	case SyntaxTree::CMD_ALTER_TABLE_RENAME_COLUMN:   
	case SyntaxTree::CMD_CREATE_USER:
	case SyntaxTree::CMD_CREATE_ROLE:
	case SyntaxTree::CMD_DROP_USER:
	case SyntaxTree::CMD_SET_PASSWORD:
	case SyntaxTree::CMD_GRANT:
	case SyntaxTree::CMD_REVOKE:
	case SyntaxTree::CMD_DROP_VIEW:
		genDDL(in, plan);
		break;
	case SyntaxTree::CMD_CREATE_VIEW:
		{
			if (in.cmdOptionValue_ != 1 && in.insertSet_ && in.insertSet_->right_) {
				Plan *tmpPlan = ALLOC_NEW(alloc_) SQLPreparedPlan(alloc_);
				PlanNodeId productedNodeId = UNDEF_PLAN_NODEID;
				const Select &viewSelect = *in.insertSet_->right_;
				genSelect(viewSelect, false, productedNodeId, *tmpPlan);
			}
			genDDL(in, plan);
		}
		break;
	case SyntaxTree::CMD_BEGIN:
	case SyntaxTree::CMD_COMMIT:
	case SyntaxTree::CMD_ROLLBACK:
	default:
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_UNSUPPORTED, NULL,
				"Unsupported command: commandType=" << static_cast<uint32_t>(in.cmdType_));
		break;
	}

	genResult(plan);


	plan.validateReference(tableInfoList_);

	bool tableCostAffected = false;
	if (optimizationOption_->isSomeEnabled()) {
		OptimizationContext cxt(*this, plan);

		{
			OptimizationUnit unit(cxt, OPT_FACTORIZE_PREDICATE);
			unit(unit() && factorizePredicate(plan));
		}

		makeJoinHintMap(plan);

		{
			OptimizationUnit unit(cxt, OPT_MAKE_SCAN_COST_HINTS);
			unit(unit() && makeScanCostHints(plan));
		}

		{
			OptimizationUnit unit(cxt, OPT_REORDER_JOIN);
			unit(unit() && reorderJoin(plan, tableCostAffected));
		}

		applyJoinOption(plan);

		{
			OptimizationUnit unit(cxt, OPT_PUSH_DOWN_PREDICATE);
			unit(unit() && pushDownPredicate(plan, true));
		}
	}


	const bool costCheckOnly = false;
	genDistributedPlan(plan, costCheckOnly);

	finalizeScanCostHints(plan, tableCostAffected);

	applyScanOption(plan);


	optimize(plan);

	plan.validateReference(tableInfoList_);

	const bool binding = false;
	refreshAllColumnTypes(plan, binding, NULL);

	checkLimit(plan);

	if (profiler_ != NULL) {
		profiler_->setPlan(plan);
		profiler_->setTableInfoList(getTableInfoList());
		ProfilerManager::getInstance().addProfiler(*profiler_);
	}
	relocateOptimizationProfile(plan);
}

bool SQLCompiler::isRecompileOnBindRequired() const {
	return recompileOnBindRequired_;
}

void SQLCompiler::bindParameterTypes(Plan &plan) {
	const bool binding = true;
	bool insertOrUpdateFound;
	refreshAllColumnTypes(plan, binding, &insertOrUpdateFound);
	checkModificationColumnConversions(plan, insertOrUpdateFound);
}

bool SQLCompiler::reducePartitionedTargetByTQL(
		util::StackAllocator &alloc, const TableInfo &tableInfo,
		const Query &query, bool &uncovered,
		util::Vector<int64_t> *affinityList,
		util::Vector<uint32_t> *subList) {

	TupleValue::VarContext varCxt;
	varCxt.setStackAllocator(&alloc);

	TableInfoList infoList(alloc, NULL);
	const TableInfo addedTableInfo = infoList.add(tableInfo);

	SQLCompiler compiler(varCxt);
	compiler.setTableInfoList(&infoList);
	compiler.neverReduceExpr_ = true;

	Plan plan(alloc);
	PlanNode &node = plan.append(SQLType::EXEC_SCAN);
	node.tableIdInfo_ = addedTableInfo.idInfo_;

	const Expr &inExpr = ProcessorUtils::tqlToPredExpr(alloc, query);
	const Expr &expr = compiler.genScalarExpr(
			inExpr, plan, MODE_NORMAL_SELECT, false, NULL);

	bool placeholderAffected;
	util::Set<int64_t> unmatchAffinitySet(alloc);
	return ProcessorUtils::reducePartitionedTarget(
			varCxt, addedTableInfo, expr, uncovered, affinityList, subList,
			NULL, placeholderAffected, unmatchAffinitySet);
}

FullContainerKey* SQLCompiler::predicateToContainerKeyByTQL(
		util::StackAllocator &alloc, TransactionContext &txn,
		DataStoreV4 &dataStore, const Query &query, DatabaseId dbId,
		ContainerId metaContainerId, PartitionId partitionCount,
		util::String &dbNameStr, bool &fullReduced,
		PartitionId &reducedPartitionId) {

	TupleValue::VarContext varCxt;
	varCxt.setStackAllocator(&alloc);

	return ProcessorUtils::predicateToContainerKey(
			varCxt, txn, dataStore, query, dbId, metaContainerId,
			partitionCount, dbNameStr, fullReduced, reducedPartitionId);
}

bool SQLCompiler::isDBOnlyMatched(
		const QualifiedName &name1, const QualifiedName &name2) {
	assert(name1.db_ != NULL);
	assert(name2.db_ != NULL);
	return compareStringWithCase(
			name1.db_->c_str(), name2.db_->c_str(),
			(name1.dbCaseSensitive_ || name2.dbCaseSensitive_)) == 0;
}

bool SQLCompiler::isTableOnlyMatched(
		const QualifiedName &name1, const QualifiedName &name2) {
	assert(name1.table_ != NULL);
	assert(name2.table_ != NULL);
	return compareStringWithCase(
			name1.table_->c_str(), name2.table_->c_str(),
			(name1.tableCaseSensitive_ || name2.tableCaseSensitive_)) == 0;
}

bool SQLCompiler::isNameOnlyMatched(
		const QualifiedName &name1, const QualifiedName &name2) {
	assert(name1.name_ != NULL);
	assert(name2.name_ != NULL);
	return compareStringWithCase(
			name1.name_->c_str(), name2.name_->c_str(),
			(name1.nameCaseSensitive_ || name2.nameCaseSensitive_)) == 0;
}

int32_t SQLCompiler::compareStringWithCase(
		const char8_t *str1, const char8_t *str2, bool caseSensitive) {
	if (caseSensitive) {
		return strcmp(str1, str2);
	}
	else {
		return SQLProcessor::ValueUtils::strICmp(str1, str2);
	}
}

void SQLCompiler::genDistributedPlan(Plan &plan, bool costCheckOnly) {
	util::Vector<PlanNodeId> largeScanNodeIdList(alloc_);
	SQLPreparedPlan::NodeList::iterator itr = plan.nodeList_.begin();
	for (; itr != plan.nodeList_.end(); ++itr) {
		if (itr->type_ == SQLType::EXEC_SCAN) {
			SQLTableInfo::IdInfo &idInfo = itr->tableIdInfo_;
			const SQLTableInfo &tableInfo = tableInfoList_->resolve(idInfo.id_);
			const SQLTableInfo::SubInfoList *subInfoList =
					(tableInfo.partitioning_ == NULL ?
					NULL :	&tableInfo.partitioning_->subInfoList_);
			if (subInfoList != NULL && !subInfoList->empty()) {
				if (metaTableInfoIdList_.size() > 0) {
					bool found = false;
					TableInfoIdList::iterator metaIdInfoItr = metaTableInfoIdList_.begin();
					for (; metaIdInfoItr != metaTableInfoIdList_.end(); ++metaIdInfoItr) {
						if (metaIdInfoItr->id_ == itr->tableIdInfo_.id_) {
							found = true;
							break;
						}
					}
					if (found) {
						if (costCheckOnly) {
							continue;
						}
						int64_t subContainerId = static_cast<int64_t>(metaIdInfoItr->partitionId_);
						itr->tableIdInfo_ = *metaIdInfoItr;
						itr->outputList_[0] = genConstExpr(
								plan, TupleValue(subContainerId), MODE_NORMAL_SELECT);
						metaTableInfoIdList_.erase(metaIdInfoItr);
						continue;
					}
				}
				largeScanNodeIdList.push_back(itr->id_);
			}
		}
	}
	util::Vector<PlanNodeId>::iterator nodeIdItr = largeScanNodeIdList.begin();
	for (; nodeIdItr != largeScanNodeIdList.end(); ++nodeIdItr) {
		PlanNodeRef orgNodeRef = toRef(plan, &plan.nodeList_[*nodeIdItr]);

		const SQLTableInfo::Id tableInfoId = orgNodeRef->tableIdInfo_.id_;
		const SQLTableInfo &tableInfo = tableInfoList_->resolve(tableInfoId);
		const SQLTableInfo::IdInfo &idInfo = tableInfo.idInfo_;
		assert(tableInfo.partitioning_ != NULL);

		const bool containerExpirable = tableInfo.isExpirable_;
		const SQLTableInfo::SubInfoList &subInfoList =
				tableInfo.partitioning_->subInfoList_;

		const SQLTableInfo::BasicIndexInfoList *indexInfoList;
		bool uncovered;
		util::Vector<uint32_t> subList(alloc_);
		util::Set<int64_t> unmatchSubSet(alloc_);
		{
			const SQLPreparedPlan::Node &orgNode = *orgNodeRef;
			const Expr &predExpr = (orgNode.predList_.empty() ?
					genConstExpr(plan, makeBoolValue(true), MODE_WHERE) :
					orgNode.predList_.front());

			const Plan::ValueList *parameterList = &plan.parameterList_;
			if (parameterList->empty()) {
				parameterList = NULL;
			}

			bool placeholderAffected;
			ProcessorUtils::reducePartitionedTarget(
					getVarContext(), tableInfo, predExpr, uncovered, NULL,
					&subList, parameterList, placeholderAffected,
					unmatchSubSet);

			recompileOnBindRequired_ |= placeholderAffected;
			indexInfoList = orgNode.indexInfoList_;
		}

		if (subList.empty()) {
			subList.push_back(0);
		}

		int64_t subApproxSize = -1;
		util::Vector<PlanNodeId> subScanIdList(alloc_);
		for (util::Vector<uint32_t>::iterator it = subList.begin();
				it != subList.end(); ++it) {
			const SQLTableInfo::SubInfo &subInfo = subInfoList[*it];
			if (costCheckOnly) {
				if (subInfo.approxSize_ >= 0) {
					subApproxSize =
							std::max<int64_t>(subApproxSize, 0) +
							subInfo.approxSize_;
				}
				continue;
			}

			const int32_t subContainerId = static_cast<int32_t>(*it);
			const bool subUnmatch = (
					unmatchSubSet.find(subInfo.nodeAffinity_) !=
					unmatchSubSet.end());

			PlanNode &subScan = plan.append(SQLType::EXEC_SCAN);
			const PlanNode &orgNode = *orgNodeRef; 
			subScanIdList.push_back(subScan.id_);

			SQLTableInfo::IdInfo subIdInfo = idInfo;
			subIdInfo.partitionId_ = subInfo.partitionId_;
			subIdInfo.containerId_ = subInfo.containerId_;
			subIdInfo.schemaVersionId_ = subInfo.schemaVersionId_;
			if (uncovered) {
				subIdInfo.partitioningVersionId_ = idInfo.partitioningVersionId_;
			}
			subIdInfo.subContainerId_ = subContainerId;

			subScan.tableIdInfo_ = subIdInfo;
			subScan.indexInfoList_ = indexInfoList;
			subScan.outputList_ = tableInfoToOutput(
					plan, tableInfo, MODE_NORMAL_SELECT, subContainerId);
			subScan.predList_.assign(
					orgNode.predList_.begin(), orgNode.predList_.end());
			if (containerExpirable) {
				subScan.cmdOptionFlag_ |= PlanNode::Config::CMD_OPT_SCAN_EXPIRABLE;
			}
			if (subUnmatch) {
				subScan.limit_ = 0;
			}
		}

		if (costCheckOnly) {
			if (subApproxSize >= 0) {
				orgNodeRef->tableIdInfo_.approxSize_ = subApproxSize;
			}
			continue;
		}

		if (subList.size() == 1) {
			const PlanNodeId orgNodeId = orgNodeRef->id_;
			plan.nodeList_[orgNodeId] = plan.nodeList_.back();
			plan.nodeList_[orgNodeId].id_ = orgNodeId;
			plan.nodeList_.erase(plan.nodeList_.end() - 1);
			continue;
		}

		genUnionAllShallow(subScanIdList, *nodeIdItr, plan);
	}
}

void SQLCompiler::genUnionAllShallow(
		const util::Vector<PlanNodeId> &inputIdList,
		PlanNodeId replaceNodeId, Plan &plan) {
	typedef util::Vector<PlanNodeId> PlanNodeIdList;
	size_t upperLimit = 0; 
	if (plan.hintInfo_) {
		util::Vector<TupleValue> hintValueList(alloc_);
		plan.hintInfo_->getHintValueList(
				SQLHint::MAX_DEGREE_OF_TASK_INPUT, hintValueList);
		if (hintValueList.size() == 1
			&& hintValueList[0].getType() == TupleList::TYPE_LONG) {
			int64_t hintValue = hintValueList[0].get<int64_t>();
			if (hintValue > 1) { 
				upperLimit = static_cast<size_t>(hintValue);
			}
			else {
				upperLimit = 2;
			}
		}
		else if (hintValueList.size() != 0) {
			plan.hintInfo_->reportWarning("Invalid value for Hint(MaxDegreeOfTaskInput)");
			upperLimit = 0;
		}
	}
	if (upperLimit > 0) {
		PlanNodeIdList lastLevelNodeIdList(alloc_);
		lastLevelNodeIdList.assign(inputIdList.begin(), inputIdList.end());
		PlanNodeIdList nextLevelNodeIdList(alloc_);
		if (inputIdList.size() > upperLimit) {
			while (true) {
				PlanNodeIdList::iterator it = lastLevelNodeIdList.begin();
				size_t remain = static_cast<size_t>(lastLevelNodeIdList.end() - it);
				assert (remain != 0);
				while(remain >= upperLimit) {
					SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_UNION);
					node.unionType_ = SQLType::UNION_ALL;
					node.inputList_.insert(
							node.inputList_.end(), it, it + upperLimit);
					const uint32_t inputId = 0;
					node.outputList_ = inputListToOutput(plan, node, &inputId);
					nextLevelNodeIdList.push_back(node.id_);
					it += upperLimit;
					remain = static_cast<size_t>(lastLevelNodeIdList.end() - it);
				}
				if (nextLevelNodeIdList.size() > 0 && remain > 1) {
					SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_UNION);
					node.unionType_ = SQLType::UNION_ALL;
					node.inputList_.insert(
							node.inputList_.end(), it, it + remain);
					const uint32_t inputId = 0;
					node.outputList_ = inputListToOutput(plan, node, &inputId);
					nextLevelNodeIdList.push_back(node.id_);
					it += remain;
				}
				else if (remain == 1) {
					nextLevelNodeIdList.push_back(*it);
					it += 1;
				}
				assert(it == lastLevelNodeIdList.end());
				if (nextLevelNodeIdList.size() <= upperLimit) {
					break;
				}
				lastLevelNodeIdList.swap(nextLevelNodeIdList);
				nextLevelNodeIdList.clear();
			}
		}
		else {
			nextLevelNodeIdList.swap(lastLevelNodeIdList);
		}
		if (replaceNodeId != UNDEF_PLAN_NODEID) {
			SQLPreparedPlan::Node *origNode = &plan.nodeList_[replaceNodeId];
			origNode->type_ = SQLType::EXEC_UNION;
			origNode->unionType_ = SQLType::UNION_ALL;
			origNode->tableIdInfo_ = SQLTableInfo::IdInfo(); 
			origNode->predList_.clear();
			origNode->inputList_.insert(
					origNode->inputList_.end(),
					nextLevelNodeIdList.begin(), nextLevelNodeIdList.end());
			const uint32_t inputId = 0;
			origNode->outputList_ = inputListToOutput(plan, *origNode, &inputId);
		}
		else {
			SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_UNION);
			node.unionType_ = SQLType::UNION_ALL;
			node.inputList_.insert(
					node.inputList_.end(),
					nextLevelNodeIdList.begin(), nextLevelNodeIdList.end());
			const uint32_t inputId = 0;
			node.outputList_ = inputListToOutput(plan, node, &inputId);
		}
	}
	else {
		if (replaceNodeId != UNDEF_PLAN_NODEID) {
			SQLPreparedPlan::Node &replaceNode = plan.nodeList_[replaceNodeId];
			replaceNode.type_ = SQLType::EXEC_UNION;
			replaceNode.unionType_ = SQLType::UNION_ALL;
			replaceNode.tableIdInfo_ = SQLTableInfo::IdInfo(); 
			replaceNode.predList_.clear();
			replaceNode.inputList_.insert(
					replaceNode.inputList_.end(),
					inputIdList.begin(), inputIdList.end());
			const uint32_t inputId = 0;
			replaceNode.outputList_ = inputListToOutput(plan, replaceNode, &inputId);
		}
		else {
			SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_UNION);
			node.unionType_ = SQLType::UNION_ALL;
			node.inputList_.insert(
					node.inputList_.end(), inputIdList.begin(), inputIdList.end());
			const uint32_t inputId = 0;
			node.outputList_ = inputListToOutput(plan, node, &inputId);
		}
	}
}

void SQLCompiler::genSelect(
		const Select &select, bool innerGenSet,
		PlanNodeId &productedNodeId, Plan &plan, bool leaveInternalIdColumn) {

	GenRAContext cxt(alloc_, select);
	cxt.whereExpr_ = select.where_;
	cxt.basePlanSize_ = plan.nodeList_.size();
	productedNodeId = UNDEF_PLAN_NODEID;
	cxt.productedNodeId_ = UNDEF_PLAN_NODEID;

	validateWindowFunctionOccurrence(
			select, cxt.genuineWindowExprCount_, cxt.pseudoWindowExprCount_);
	if (select.from_) {
		genFrom(select, cxt.whereExpr_, plan);
	}
	else {
		genEmptyRootNode(plan);
	}
	if (cxt.whereExpr_) {
		genWhere(cxt, plan);
	}
	Expr outExpr(alloc_);
	if (select.having_ && !select.groupByList_) {
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
				"GROUP BY clause is required before HAVING");
	}

	if (select.groupByList_) {
		ExprRef havingExprRef;

		genGroupBy(cxt, havingExprRef, plan);

		if (select.groupByList_ && select.having_) {
			genHaving(cxt, havingExprRef, plan);
		}
	}
	else {
		genSplittedSelectProjection(cxt, *select.selectList_, plan, leaveInternalIdColumn);
		if (cxt.pseudoWindowExprCount_ > 0) {
			genPseudoWindowFunc(cxt, plan);
		}
		if (cxt.aggrExprCount_ > 0) {
			genAggrFunc(cxt, plan);
		}
	}
	if (cxt.genuineWindowExprCount_ > 0) {
		genOver(cxt, plan);
		const PlanNodeId inputId = plan.back().id_;
		SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_SELECT);
		node.inputList_.push_back(inputId);
		node.outputList_ = inputListToOutput(plan);
	}

	if (!leaveInternalIdColumn) {
		genLastOutput(cxt, plan);
	}
	if (select.selectOpt_ == SyntaxTree::AGGR_OPT_DISTINCT) {
		PlanNodeId inputId = plan.back().id_;

		SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_UNION);
		node.unionType_ = SQLType::UNION_DISTINCT;
		node.inputList_.push_back(inputId);
		node.outputList_ = inputListToOutput(plan);
	}
	productedNodeId = cxt.productedNodeId_;
	assert(productedNodeId != UNDEF_PLAN_NODEID);
	if (innerGenSet) {
		return;
	}
	if (select.orderByList_) {
		ExprRef innerTopExprRef;
		genOrderBy(cxt, innerTopExprRef, plan);
	}
	if (select.limitList_) {
		const Expr* limitExpr = NULL;
		const Expr* offsetExpr = NULL;

		if (select.limitList_->size() > 0) {
			limitExpr = (*select.limitList_)[0];
		}
		if (select.limitList_->size() > 1) {
			offsetExpr = (*select.limitList_)[1];
		}
		genLimitOffset(limitExpr, offsetExpr, plan);
	}
}

void SQLCompiler::genLastOutput(GenRAContext &cxt, Plan &plan) {
	SQLPreparedPlan::Node &node = plan.back();
	PlanExprList baseOutputList = node.outputList_;
	PlanExprList::iterator inItr = baseOutputList.begin() + cxt.selectTopColumnId_;

	node.outputList_.clear();
	for (size_t pos = 0;
		 pos < cxt.selectList_.size()
		 && inItr != baseOutputList.end(); ++inItr, ++pos) {
		(*inItr).autoGenerated_ = false;
		node.outputList_.push_back(*inItr);
	}
}

void SQLCompiler::genResult(Plan &plan) {
	const PlanNode::Id lastId = plan.back().id_;
	PlanNode &node = plan.append(SQLType::EXEC_RESULT);

	node.inputList_.push_back(lastId);

	const PlanExprList &exprList = inputListToOutput(plan, false);
	node.outputList_.assign(exprList.begin(), exprList.end());

	if (currentTimeRequired_) {
		plan.currentTimeRequired_ = true;
	}
}

void SQLCompiler::dumpPlanNode(Plan &plan) {
	SQLPreparedPlan::NodeList::iterator itr = plan.nodeList_.begin();
	for (; itr != plan.nodeList_.end(); ++itr) {
		SQLPreparedPlan::Node &node = (*itr);
		util::NormalOStringStream oss;
		oss << "id," << node.id_ <<
				",type," << static_cast<int32_t>(node.type_) << ",name,";
		if (node.qName_ && node.qName_->name_) {
			oss << node.qName_->name_->c_str();
		}
		oss << ",in,{";
		{
			SQLPreparedPlan::Node::IdList::iterator itr = node.inputList_.begin();
			for (; itr != node.inputList_.end(); ++itr) {
				oss << *itr << ",";
			}
			oss << "},";
		}
		oss << ",pred,{";
		{
			SQLPreparedPlan::Node::ExprList::iterator itr = node.predList_.begin();
			size_t pos = 0;
			for (; itr != node.predList_.end(); ++itr, ++pos) {
				oss << pos << ":";
				SourceId srcId = itr->srcId_;
				if (srcId != -1) {
					oss << "<s" << (int32_t)srcId << ">";
				}
				oss << "(" << (int32_t)itr->inputId_ << "," << (int32_t)itr->columnId_ << ")";
				oss << ",";
			}
			oss << "},";
		}
		oss << ",out,{";
		{
			SQLPreparedPlan::Node::ExprList::iterator itr = node.outputList_.begin();
			size_t pos = 0;
			for (; itr != node.outputList_.end(); ++itr, ++pos) {
				oss << pos << ":";
				SourceId srcId = itr->srcId_;
				if (srcId != -1) {
					oss << "<s" << (int32_t)srcId << ">";
				}
				oss << "(" << (int32_t)itr->inputId_ << "," << (int32_t)itr->columnId_ << ")";
				oss << ",";
			}
			oss << "},";
		}
		std::cout << oss.str().c_str() << std::endl;
	}
}

void SQLCompiler::genInsert(const Select &select, Plan &plan) {

	assert(select.targetName_ != NULL);
	const SQLTableInfo& tableInfo = tableInfoList_->resolve(*select.targetName_);
	InsertColumnMap *insertColumnMap = NULL;

	if (tableInfo.isView_) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_CONTAINER_ATTRIBUTE,
			"Target table '" << select.targetName_->table_->c_str() << "' is VIEW");
	}

	util::Vector<TupleList::TupleColumnType> columnTypeList(alloc_);
	columnTypeList.assign(tableInfo.columnInfoList_.size(), TupleList::TYPE_NULL);
	size_t realColumnCount = 0;
	if (select.insertList_ != NULL) {
		insertColumnMap = ALLOC_NEW(alloc_) InsertColumnMap(alloc_);
		realColumnCount = makeInsertColumnMap(tableInfo, select.insertList_,
				*insertColumnMap, columnTypeList);
	}
	else {
		realColumnCount = tableInfo.columnInfoList_.size();
	}

	if (select.insertSet_ != NULL &&
			select.insertSet_->type_ == SyntaxTree::SET_OP_NONE) {

		PlanNodeId productedNodeId = UNDEF_PLAN_NODEID;
		genSelect(*select.insertSet_->right_, false, productedNodeId, plan);
	}
	else if (select.insertSet_ != NULL &&
			select.insertSet_->type_ == SyntaxTree::SET_OP_UNION_ALL) {
		genSet(*select.insertSet_, NULL, plan);

		PlanNodeId inputId = plan.back().id_;
		SQLPreparedPlan::Node *projectNode = &plan.append(SQLType::EXEC_SELECT);
		projectNode->inputList_.push_back(inputId);
		projectNode->outputList_ = inputListToOutput(plan);
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL,
				"Invalid syntax tree of insert sql");
	}

	PlanExprList output = getOutput(plan);
	if (output.size() != realColumnCount) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_MISMATCH_SCHEMA,
				"Specified column list count is unmatch with target table, expected="
				<< realColumnCount << ", actual=" << output.size());
	}

	if (insertColumnMap != NULL && insertColumnMap->size() > 0) {
		if (columnTypeList.size() != realColumnCount) {
			util::Vector<TupleList::TupleColumnType> realTypeList(alloc_);
			for (size_t pos = 0; pos < realColumnCount; pos++) {
				realTypeList.push_back(columnTypeList[pos]);
			}
			genPromotedProjection(plan, realTypeList);
		}
		else {
			genPromotedProjection(plan, columnTypeList);
		}
	}
	else {
		columnTypeList.clear();
		for (size_t pos = 0; pos < tableInfo.columnInfoList_.size(); pos++) {
			columnTypeList.push_back(tableInfo.columnInfoList_[pos].first);
		}
		genPromotedProjection(plan, columnTypeList);
	}
	PlanNodeId inputId = plan.back().id_;

	SQLPreparedPlan::Node &insertNode = plan.append(SQLType::EXEC_INSERT);
	insertNode.inputList_.push_back(inputId);
	if (select.insertList_ != NULL) {
		if (insertColumnMap->size() > 0) {
			insertNode.insertColumnMap_ = insertColumnMap;
		}
		else {
			ALLOC_DELETE(alloc_, insertColumnMap);
		}
	}

	insertNode.predList_ = inputListToOutput(plan);
	insertNode.cmdOptionFlag_ = static_cast<int32_t>(select.cmdOptionValue_);

	const Expr outExpr =
		genConstExpr(plan, TupleValue(INT64_C(0)), MODE_NORMAL_SELECT);
	insertNode.outputList_.push_back(outExpr);

	setTargetTableInfo(select.targetName_->table_->c_str(),
			select.targetName_->tableCaseSensitive_, tableInfo, insertNode);
}

void SQLCompiler::genUpdate(const Select &select, Plan &plan) {

	assert(select.targetName_ != NULL);
	const SQLTableInfo& tableInfo = tableInfoList_->resolve(*select.targetName_);

	if (tableInfo.isView_) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_CONTAINER_ATTRIBUTE,
			"Target table '" << select.targetName_->table_->c_str() << "' is VIEW");
	}

	Select updateSelect(alloc_);
	const ExprList *exprList = select.updateSetList_;
	updateSelect.selectList_ = ALLOC_NEW(alloc_) SyntaxTree::ExprList(alloc_);

	util::Map<util::String, Expr *> updateColumnMap(alloc_);
	std::pair<util::Map<util::String, Expr*>::iterator, bool> retVal;
	std::pair<util::Set<util::String>::iterator, bool> outResult;
	util::Set<util::String> realColumnSet(alloc_);
	size_t columnId;
	for (columnId = 0; columnId < tableInfo.columnInfoList_.size(); columnId++) {
		util::String tmpKey(static_cast<const char*>(
				tableInfo.columnInfoList_[columnId].second.c_str()), alloc_);
		const util::String &key = normalizeName(alloc_, tmpKey.c_str());
		realColumnSet.insert(key);
	}

	for (size_t i = 0; i < exprList->size(); i++) {
		Expr *columnExpr = (*exprList)[i]->left_;
		Expr *valueExpr = (*exprList)[i]->right_;
		Expr *targetExpr = (*exprList)[i];
		if (findSubqueryExpr(valueExpr)) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
					"Subqueries in the update value are not supported");
		}
		if (findAggrExpr(*valueExpr, false)) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
					"Aggregation in the update value are not supported");
		}
		assert(columnExpr->qName_->name_);
		const char *orgColumnName = columnExpr->qName_->name_->c_str();
		util::String tmpKey(orgColumnName, alloc_);
		const util::String &key = normalizeName(alloc_, tmpKey.c_str());
		retVal = updateColumnMap.insert(std::make_pair(key, targetExpr));
		if (!retVal.second) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
					"Specified update column='" << orgColumnName << "' is duplicated");
		}
		util::Set<util::String>::iterator it = realColumnSet.find(key);
		if (it == realColumnSet.end()) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
					"Specified update column='" << orgColumnName << "' is not found");
		}
	}

	for (size_t columnId = 0; columnId < tableInfo.columnInfoList_.size(); columnId++) {
		const char *orgColumnName = tableInfo.columnInfoList_[columnId].second.c_str();
		util::String tmpKey(orgColumnName, alloc_);
		const util::String &key = normalizeName(alloc_, tmpKey.c_str());
		util::Map<util::String, Expr *>::iterator it = updateColumnMap.find(key);
		if (it != updateColumnMap.end()) {
			Expr *targetExpr = (*it).second;
			if (targetExpr->left_->qName_->nameCaseSensitive_) {
				if (tableInfo.columnInfoList_[columnId].second.compare(
						*targetExpr->left_->qName_->name_)) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
							"Specified update column='" << orgColumnName<< "' is not found");
				}
			}
			if (tableInfo.nosqlColumnOptionList_[columnId] == SyntaxTree::COLUMN_OPT_VIRTUAL) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
						"Virtual column='" << tableInfo.columnInfoList_[columnId].second.c_str()
						<< "' must not be specifed in set statement");
			}

	
			const SQLTableInfo::PartitioningInfo *partitioning =
					tableInfo.partitioning_;
			if (partitioning != NULL &&
					columnId == static_cast<ColumnId>(
							partitioning->partitioningColumnId_)) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_COMPILE_PARTITIONING_KEY_NOT_UPDATABLE,
						"Partitioning column='" << orgColumnName << "' is not updatable");
			}
			if (partitioning != NULL &&
					columnId == static_cast<ColumnId>(
							partitioning->subPartitioningColumnId_)) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_COMPILE_PARTITIONING_KEY_NOT_UPDATABLE,
						"Sub partitioning column='" << orgColumnName << "' is not updatable");
			}
			updateSelect.selectList_->push_back((*it).second->right_);
		}
		else {
			Expr *outColumnExpr = ALLOC_NEW(alloc_) Expr(alloc_);
			outColumnExpr->op_ = SQLType::EXPR_COLUMN;
			outColumnExpr->qName_ = ALLOC_NEW(alloc_) SyntaxTree::QualifiedName(alloc_);
			outColumnExpr->qName_->name_ = ALLOC_NEW(alloc_) util::String(key.c_str(), alloc_);
			updateSelect.selectList_->push_back(outColumnExpr);
		}
	}

	updateSelect.where_ = select.where_;

	updateSelect.from_ = select.from_;  

	PlanNodeId productedNodeId = UNDEF_PLAN_NODEID;
	genSelect(updateSelect, false, productedNodeId, plan, true);

	util::Vector<TupleList::TupleColumnType> columnTypeList(alloc_);
	columnTypeList.push_back(TupleList::TYPE_LONG);
	columnTypeList.push_back(TupleList::TYPE_LONG);
	for (size_t pos = 0; pos < tableInfo.columnInfoList_.size(); pos++) {
		columnTypeList.push_back(tableInfo.columnInfoList_[pos].first);
	}
	genPromotedProjection(plan, columnTypeList);

	PlanNodeId inputId = plan.back().id_;

	SQLPreparedPlan::Node &updateNode = plan.append(SQLType::EXEC_UPDATE);
	updateNode.inputList_.push_back(inputId);
	updateNode.predList_ = inputListToOutput(plan);

	const Expr outExpr
			= genConstExpr(plan, TupleValue(INT64_C(0)), MODE_NORMAL_SELECT);
	updateNode.outputList_.push_back(outExpr);

	setTargetTableInfo(select.targetName_->table_->c_str(),
			select.targetName_->tableCaseSensitive_, tableInfo, updateNode);
}

void SQLCompiler::genDelete(const Select &select, Plan &plan) {

	assert(select.targetName_ != NULL);
	const SQLTableInfo& tableInfo = tableInfoList_->resolve(*select.targetName_);

	if (tableInfo.isView_) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_CONTAINER_ATTRIBUTE,
			"Target table '" << select.targetName_->table_->c_str() << "' is VIEW");
	}
	size_t genuineWindowExprCount = 0;
	size_t pseudoWindowExprCount = 0;
	size_t windowExprCount = validateWindowFunctionOccurrence(
			select, genuineWindowExprCount, pseudoWindowExprCount);
	if (windowExprCount > 0) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_PARAMETER,
			"Window function is not supported");
	}

	Select deleteSelect(alloc_);
	deleteSelect.where_ = select.where_;

	SyntaxTree::Expr* fromExpr = SyntaxTree::Expr::makeTable(alloc_,
		select.targetName_, NULL);

	deleteSelect.from_ = fromExpr;

	GenRAContext cxt(alloc_, select);
	cxt.whereExpr_ = select.where_;
	cxt.basePlanSize_ = plan.nodeList_.size();
	cxt.productedNodeId_ = UNDEF_PLAN_NODEID;

	PlanExprList selectList(alloc_);
	const Expr* whereExpr = deleteSelect.where_;
	const size_t basePlanSize = plan.nodeList_.size();
	if (deleteSelect.from_) {
		genFrom(deleteSelect, whereExpr, plan);
	}
	else {
		genEmptyRootNode(plan);
	}
	if (whereExpr) {
		genWhere(cxt, plan);
	}
	PlanNodeId inputId = (basePlanSize != plan.nodeList_.size()) ?
			plan.back().id_ : SyntaxTree::UNDEF_INPUTID;
	SQLPreparedPlan::Node *node = &plan.append(SQLType::EXEC_SELECT);
	node->inputList_.push_back(inputId);
	PlanExprList outputList(alloc_);
	outputList = inputListToOutput(plan);
	node->outputList_.insert(
			node->outputList_.end(), outputList.begin(), outputList.begin() + 2);

	inputId = plan.back().id_;
	SQLPreparedPlan::Node &deleteNode = plan.append(SQLType::EXEC_DELETE);

	deleteNode.inputList_.push_back(inputId);
	deleteNode.predList_ = inputListToOutput(plan);

	const Expr outExpr =
			genConstExpr(plan, TupleValue(INT64_C(0)), MODE_NORMAL_SELECT);
	deleteNode.outputList_.push_back(outExpr);
	setTargetTableInfo(select.targetName_->table_->c_str(),
			select.targetName_->tableCaseSensitive_, tableInfo, deleteNode);
}

void SQLCompiler::validateCreateTableOption(
		const SyntaxTree::CreateTableOption &option) {
	if (option.columnInfoList_) {
		SyntaxTree::ColumnInfoList::const_iterator itr = option.columnInfoList_->begin();
		for (; itr != option.columnInfoList_->end(); ++itr) {
			if ((*itr)->isVirtual()) {
				SQL_COMPILER_THROW_ERROR(
					GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
					"Virtual column is allowed for virtual table");
			}
			if (!checkAcceptableTupleType((*itr)->type_)) {
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
						"Target column type='" << ValueProcessor::getTypeName(
						convertTupleTypeToNoSQLType((*itr)->type_))
						<< "' is not specified in current version");
			}
		}
	}
	else {
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
				"Column definition does not exist");
	}
}

void SQLCompiler::validateScanOptionForVirtual(const PlanExprList &scanOptionList, bool isVirtual) {
	PlanExprList::const_iterator itr = scanOptionList.begin();
	if (!isVirtual && scanOptionList.size() > 0) {
		SQL_COMPILER_THROW_ERROR(
			GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
			"Scan option is allowed for virtual table");
	}
	for (; itr != scanOptionList.end(); ++itr) {
		if (itr->op_ != SQLType::EXPR_CONSTANT
			&& itr->op_ != SQLType::EXPR_PLACEHOLDER
			&& itr->op_ != SQLType::EXPR_COLUMN) {
			SQL_COMPILER_THROW_ERROR(
					GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, &(*itr),
					"Invalid argument");
		}
	}
}

void SQLCompiler::validateCreateIndexOptionForVirtual(
	SyntaxTree::QualifiedName &indexName, SyntaxTree::CreateIndexOption &option, Plan::ValueTypeList *typeList) {
	const SQLTableInfo &targetTableInfo = tableInfoList_->resolve(indexName);
	if (option.extensionName_ == NULL) {
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
				"Invalid extension name");
	}
	if (option.columnNameList_ == NULL || option.columnNameList_->size() != 1) {
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
				"Index column name is not specified");
	}
	const util::String &columnName = *((*(option.columnNameList_))[0]->name_);
	SQLTableInfo::SQLColumnInfoList::const_iterator itr
		= targetTableInfo.columnInfoList_.begin();
	bool found = false;
	for (; itr != targetTableInfo.columnInfoList_.end(); ++itr) {
		if (SQLProcessor::ValueUtils::strICmp(
			columnName.c_str(), itr->second.c_str()) == 0) {
				found = true;
				break;
		}
	}
	if (!found) {
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
				"Index column is not found");
	}

	if (option.extensionOptionList_ == NULL) {
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
				"VNN Index option is not specified");
	}
	for (size_t pos = 0; pos < option.extensionOptionList_->size(); pos++) {
		if (option.extensionOptionList_->at(0) == NULL) {
			SQL_COMPILER_THROW_ERROR(
					GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
					"VNN Index option is not specified");
		}
		if (option.extensionOptionList_->at(pos)->op_ == SQLType::EXPR_CONSTANT) {
		}
		else if (option.extensionOptionList_->at(pos)->op_ == SQLType::EXPR_PLACEHOLDER) {
			typeList->push_back(TupleList::TYPE_ANY);
		}
		else {
			SQL_COMPILER_THROW_ERROR(
					GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
					"VNN Index option must be constant value or placeholder");
		}
	}
}


void SQLCompiler::genDDL(const Select &select, Plan &plan) {
	SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_DDL);

	const Expr outExpr =
		genConstExpr(plan, TupleValue(INT64_C(0)), MODE_NORMAL_SELECT);
	node.outputList_.push_back(outExpr);

	switch (select.cmdType_) {
	case SyntaxTree::CMD_CREATE_TABLE:
		validateCreateTableOption(*select.createTableOpt_);
		node.qName_ = select.targetName_;
		break;
	case SyntaxTree::CMD_DROP_TABLE:
		node.qName_ = select.targetName_;
		break;
	case SyntaxTree::CMD_CREATE_VIEW:
		node.qName_ = select.targetName_;
		break;
	case SyntaxTree::CMD_DROP_VIEW:
		node.qName_ = select.targetName_;
		break;
	case SyntaxTree::CMD_CREATE_INDEX: {
		assert(select.targetName_->table_);
		node.qName_ = select.targetName_;
	}
		break;
	case SyntaxTree::CMD_DROP_INDEX:
		node.qName_ = select.targetName_;
		break;
	case SyntaxTree::CMD_ALTER_TABLE_DROP_PARTITION: 
		node.qName_ = select.targetName_;
		break;
	case SyntaxTree::CMD_ALTER_TABLE_ADD_COLUMN: 
		node.qName_ = select.targetName_;
		break;
	case SyntaxTree::CMD_ALTER_TABLE_RENAME_COLUMN: 
		node.qName_ = select.targetName_;
		break;
	default:
		break;
	}

	node.commandType_ = select.cmdType_;
	node.cmdOptionList_ = select.cmdOptionList_;
	node.createTableOpt_ = select.createTableOpt_;
	node.createIndexOpt_ = select.createIndexOpt_;
	node.cmdOptionFlag_ = select.cmdOptionValue_;

}

int32_t SQLCompiler::makeInsertColumnMap(const SQLTableInfo &tableInfo,
		ExprList *insertList, InsertColumnMap &insertColumnMap,
		util::Vector<TupleList::TupleColumnType> &columnTypeList) {

	int32_t columnId;
	util::Map<util::String, int32_t> realColumnMap(alloc_);
	util::Map<util::String, int32_t> realSensitiveColumnMap(alloc_);
	int32_t realColumnCount = 0;

	for (columnId = 0; columnId <
			static_cast<int32_t>(tableInfo.columnInfoList_.size()); columnId++) {
		util::String key(static_cast<const char*>(
				tableInfo.columnInfoList_[columnId].second.c_str()), alloc_);
		const util::String &normalizedKey = normalizeName(alloc_, key.c_str());
		realColumnMap.insert(std::make_pair(normalizedKey, columnId));
		realSensitiveColumnMap.insert(std::make_pair(key, columnId));
		if (tableInfo.nosqlColumnOptionList_[columnId] != SyntaxTree::COLUMN_OPT_VIRTUAL) {
		}
	}
	assert(tableInfo.columnInfoList_.size() > 0);
	insertColumnMap.assign(tableInfo.columnInfoList_.size(), -1);

	util::Set<ColumnId> checkColumnSet(alloc_);
	std::pair<util::Set<ColumnId>::iterator, bool> outResult;
	bool isChange = false;
	for (size_t pos = 0; pos < insertList->size(); pos++) {
		Expr *insertExpr = (*insertList)[pos];
		
		util::String key(static_cast<const char*>(insertExpr->qName_->name_->c_str()),
				strlen(insertExpr->qName_->name_->c_str()), alloc_);

		ColumnId foundColumnId = static_cast<ColumnId>(-1);
		if (insertExpr->qName_->nameCaseSensitive_) {
			util::Map<util::String, int32_t>::iterator it = realSensitiveColumnMap.find(key);
			if (it == realSensitiveColumnMap.end()) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
						"Specified insert column='" << key.c_str() << "' is not found");
			}
			foundColumnId = (*it).second;
		}
		else {
			const util::String &normalizedKey = normalizeName(alloc_, key.c_str());
			util::Map<util::String, int32_t>::iterator it = realColumnMap.find(normalizedKey);
			if (it == realColumnMap.end()) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
						"Specified insert column='" << key.c_str() << "' is not found");
			}
			foundColumnId = (*it).second;
		}
		if (foundColumnId != static_cast<ColumnId>(-1)) {
			if (tableInfo.nosqlColumnOptionList_[foundColumnId] == SyntaxTree::COLUMN_OPT_VIRTUAL) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INVALID_INSERT_COLUMN,
						"INSERT operation disallowed on virtual columns");
			}	
			insertColumnMap[foundColumnId] = static_cast<int32_t>(pos);
			columnTypeList[pos] = tableInfo.columnInfoList_[foundColumnId].first;
			outResult = checkColumnSet.insert(foundColumnId);
			if (!outResult.second) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
						"Specified insert column='" << key.c_str() << "' is duplicated");
			}
			realColumnCount++;
		}
	}
	for (size_t i = 0; i < insertColumnMap.size(); i++) {
		if (insertColumnMap[i] == -1 ||
				insertColumnMap[i] != static_cast<ptrdiff_t>(i)) {
			isChange = true;
			break;
		}
	}
	if (!isChange) {
		insertColumnMap.clear();
	}
	return realColumnCount;
}

void SQLCompiler::setTargetTableInfo(const char *tableName, bool isCaseSensitive,
		const SQLTableInfo &tableInfo, SQLPreparedPlan::Node &node) {

	for (size_t pos = 0; pos < tableInfo.nosqlColumnInfoList_.size(); pos++) {
		if (!checkNoSQLTypeToTupleType(tableInfo.nosqlColumnInfoList_[pos])) {
			SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_UNSUPPORTED, NULL,
					"Target column type='" << ValueProcessor::getTypeName(
					tableInfo.nosqlColumnInfoList_[pos])
					<< "' is not writable in current version");
		}
	}
	if (SQLCompiler::Meta::isSystemTable(tableName)) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_METATABLE_NOT_UPDATABLE,
				"Meta table '" << tableName << "' is not updatable");
	}

	node.tableIdInfo_.dbId_ = tableInfo.idInfo_.dbId_;
	node.tableIdInfo_.containerId_ = tableInfo.idInfo_.containerId_;
	node.tableIdInfo_.schemaVersionId_ = tableInfo.idInfo_.schemaVersionId_;
	node.tableIdInfo_.partitionId_ = tableInfo.idInfo_.partitionId_;
	node.tableIdInfo_.id_ = tableInfo.idInfo_.id_;
	node.tableIdInfo_.approxSize_ = tableInfo.idInfo_.approxSize_;

	PlanNode::PartitioningInfo *destPartitioning =
			ALLOC_NEW(alloc_) SQLTableInfo::PartitioningInfo(alloc_);
	node.partitioningInfo_ = destPartitioning;

	destPartitioning->tableName_ = tableName;
	destPartitioning->isCaseSensitive_ = isCaseSensitive;

	const PlanNode::PartitioningInfo *srcPartitioning = tableInfo.partitioning_;
	if (srcPartitioning != NULL && !srcPartitioning->subInfoList_.empty()) {
		destPartitioning->partitioningType_ =
				srcPartitioning->partitioningType_;
		destPartitioning->subInfoList_ = srcPartitioning->subInfoList_;
		destPartitioning->partitioningColumnId_ =
				srcPartitioning->partitioningColumnId_;
		destPartitioning->subPartitioningColumnId_ =
				srcPartitioning->subPartitioningColumnId_;
		destPartitioning->partitioningCount_ = 
				srcPartitioning->partitioningCount_;
	}
	else {
		SQLTableInfo::SubInfo subInfo;
		subInfo.containerId_ = tableInfo.idInfo_.containerId_;
		subInfo.partitionId_ = tableInfo.idInfo_.partitionId_;
		subInfo.schemaVersionId_ = tableInfo.idInfo_.schemaVersionId_;
		subInfo.approxSize_ = tableInfo.idInfo_.approxSize_;

		destPartitioning->subInfoList_.push_back(subInfo);
		destPartitioning->partitioningCount_ = 1;
	}
	node.nosqlTypeList_ = &tableInfo.nosqlColumnInfoList_;
	node.nosqlColumnOptionList_ = &tableInfo.nosqlColumnOptionList_;
}

void SQLCompiler::genFrom(
		const Select &select, const Expr* &selectWhereExpr, Plan &plan) {
	StringSet aliases(alloc_);
	const Expr &from = *select.from_;
	bool whereExprHasSubquery = findSubqueryExpr(selectWhereExpr);
	const Expr* whereExpr = whereExprHasSubquery ? NULL : selectWhereExpr;

	if (from.op_ == SQLType::EXPR_JOIN) {
		genTableOrSubquery(from, whereExpr, true, aliases, plan);
		whereExpr = NULL; 
	}
	else {
		genTableOrSubquery(from, whereExpr, false, aliases, plan);
	}
	selectWhereExpr = (whereExprHasSubquery) ? selectWhereExpr : whereExpr;
	SQLPreparedPlan::Node &node = plan.back();
	for (PlanExprList::iterator itr = node.outputList_.begin();
			itr != node.outputList_.end(); ++itr) {
		if (itr->subqueryLevel_ < subqueryLevel_) {
			itr->subqueryLevel_ = subqueryLevel_;
		}
	}
}

void SQLCompiler::genWhere(GenRAContext &cxt, Plan &plan) {
	const Expr* selectWhereExpr = cxt.whereExpr_;
	const size_t basePlanSize = cxt.basePlanSize_;

	if (basePlanSize == plan.nodeList_.size()) {
		genSelectProjection(cxt, plan);
		assert(cxt.productedNodeId_ != UNDEF_PLAN_NODEID);
	}
	if (basePlanSize == plan.nodeList_.size()) {
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
			"No tables specified");
	}
	PlanNodeId inputId = plan.back().id_;
	Expr* scalarExpr = NULL;
	Expr* subExpr = NULL;
	bool splitted = splitSubqueryCond(*selectWhereExpr, scalarExpr, subExpr);
	if (splitted) {
		if (scalarExpr != NULL) {
			SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_SELECT);
			node.inputList_.push_back(inputId);
			node.outputList_ = inputListToOutput(plan);

			const Expr& whereExpr = genExpr(*scalarExpr, plan, MODE_WHERE);
			assert(node.id_ == plan.back().id_);
			node.predList_.push_back(whereExpr);
			cxt.scalarWhereNodeRef_ = toRef(plan, &node);
		}
		assert(subExpr != NULL);
		inputId = plan.back().id_;
		SQLPreparedPlan::Node* node = &plan.append(SQLType::EXEC_SELECT);
		node->inputList_.push_back(inputId);
		node->outputList_ = inputListToOutput(plan);

		size_t orgSize = node->outputList_.size();
		const Expr& whereExpr = genExpr(*subExpr, plan, MODE_WHERE);
		assert(node->id_ != plan.back().id_);
		node = &plan.back();
		node->predList_.push_back(whereExpr);
		node->outputList_ = inputListToOutput(plan);
		trimExprList(orgSize, node->outputList_);
	}
	else {
		assert(subExpr == NULL);
		SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_SELECT);
		node.inputList_.push_back(inputId);
		node.outputList_ = inputListToOutput(plan);

		const Expr& whereExpr = genExpr(*selectWhereExpr, plan, MODE_WHERE);
		assert(node.id_ == plan.back().id_);
		node.predList_.push_back(whereExpr);
		cxt.scalarWhereNodeRef_ = toRef(plan, &node);
	}
	cxt.whereExpr_ = NULL;
}

void SQLCompiler::hideSelectListName(
		GenRAContext &cxt, SQLPreparedPlan::Node &inputNode, Plan &plan) {
	static_cast<void>(plan);
	ExprList::const_iterator inItr = cxt.outExprList_.begin();
	assert(cxt.selectTopColumnId_ + cxt.outExprList_.size()
			<= inputNode.outputList_.size());
	PlanExprList::iterator outItr;
	outItr = inputNode.outputList_.begin() + cxt.selectTopColumnId_;
	for (; inItr != cxt.outExprList_.end(); ++inItr, ++outItr) {
		if ((*inItr) && (*inItr)->aliasName_) {
			outItr->qName_ = NULL; 
		}
	}
}

void SQLCompiler::restoreSelectListName(
		GenRAContext &cxt, PlanExprList &restoreList, Plan &plan) {
	SQLPreparedPlan::Node &node = plan.back();
	PlanExprList::const_iterator inItr = restoreList.begin() + cxt.selectTopColumnId_;
	assert(cxt.selectTopColumnId_ + cxt.outExprList_.size()
			<= node.outputList_.size());
	ExprList::const_iterator checkItr = cxt.outExprList_.begin();
	PlanExprList::iterator outItr;
	outItr = node.outputList_.begin() + cxt.selectTopColumnId_;
	for (; checkItr != cxt.outExprList_.end(); ++checkItr, ++inItr, ++outItr) {
		if ((*checkItr) && (*checkItr)->aliasName_) {
			if (inItr->qName_) {
				outItr->qName_ = inItr->qName_->duplicate();
			}
		}
	}
}

void SQLCompiler::genAggrFunc(GenRAContext &cxt, Plan &plan) {
	PlanExprList aggrList(alloc_);
	{
		assert(cxt.aggrExprList_.size() > 0);
		SQLPreparedPlan::Node *node = NULL;
		const PlanNodeId inputId = plan.back().id_;
		node = &plan.append(SQLType::EXEC_SELECT);
		node->inputList_.push_back(inputId);
		node->outputList_ = inputListToOutput(plan); 
		size_t baseSize = node->outputList_.size();
		SQLPreparedPlan::Node &inputNode = node->getInput(plan, 0);
		PlanExprList saveOutputList(alloc_);
		saveOutputList = inputNode.outputList_;
		PlanNodeId saveOutputNodeId = inputNode.id_;
		hideSelectListName(cxt, inputNode, plan);
		aggrList = genColumnExprList(
				cxt.aggrExprList_, plan, MODE_AGGR_SELECT, &inputId);

		plan.nodeList_[saveOutputNodeId].outputList_ = saveOutputList;
		node = &plan.back();
		for (PlanExprList::iterator itr = aggrList.begin(); itr != aggrList.end(); ++itr) {
			if (itr->subqueryLevel_ < subqueryLevel_) {
				itr->subqueryLevel_ = subqueryLevel_;
			}
		}
		node->outputList_.clear();
		node->outputList_ = inputListToOutput(plan);
		restoreSelectListName(cxt, saveOutputList, plan);
		while (node->outputList_.size() > baseSize) {
			node->outputList_.pop_back();
		}
		PlanExprList::const_iterator inItr = aggrList.begin();
		PlanExprList::iterator outItr = cxt.selectList_.begin();
		for (; inItr != aggrList.end(); ++inItr, ++outItr) {
			bool isAggrExpr = findAggrExpr(*inItr, true);
			if (isAggrExpr) {
				*outItr = *inItr;
			}
		}
		{
			assert(cxt.selectTopColumnId_ != UNDEF_COLUMNID);
			assert(cxt.selectTopColumnId_ + aggrList.size() <= node->outputList_.size());
			inItr = aggrList.begin();
			outItr = node->outputList_.begin() + cxt.selectTopColumnId_;
			for (; inItr != aggrList.end(); ++inItr, ++outItr) {
				bool isAggrExpr = findAggrExpr(*inItr, true);
				if (isAggrExpr) {
					*outItr = *inItr;
				}
			}
		}
		while (node->outputList_.size() > baseSize) {
			assert(false);
			node->outputList_.pop_back();
		}
	}
}

bool SQLCompiler::genSplittedSelectProjection(
		GenRAContext &cxt, const ExprList &inSelectList,
		Plan &plan, bool leaveInternalIdColumn) {
	const size_t basePlanSize = cxt.basePlanSize_;
	bool inSubQuery = cxt.inSubQuery_;
	Type type = cxt.type_;

	PlanNodeId inputId = (basePlanSize != plan.nodeList_.size())
			? plan.back().id_ : SyntaxTree::UNDEF_INPUTID;
	SQLPreparedPlan::Node *node = &plan.append(SQLType::EXEC_SELECT);
	if (inputId != SyntaxTree::UNDEF_INPUTID) {
		node->inputList_.push_back(inputId);
		node->outputList_ = inputListToOutput(plan); 
	}
	PlanExprList prevOutputList(alloc_);
	prevOutputList.assign(node->outputList_.begin(), node->outputList_.end());
	splitAggrWindowExprList(cxt, inSelectList, plan);
	bool hasAggrExpr = (cxt.aggrExprCount_ > 0);
	assert(cxt.outExprList_.size() > 0 || cxt.aggrExprList_.size() > 0 || cxt.windowExprList_.size() > 0);

	PlanExprList list =
			genColumnExprList(cxt.outExprList_, plan, MODE_NORMAL_SELECT, NULL);
	if (inSubQuery && type != SQLType::EXPR_EXISTS &&
			list.size() != inSelectList.size()) {
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
				"Subquery has too many columns");
	}
	node = &plan.back();
	assert(node->type_ == SQLType::EXEC_SELECT);
	for (PlanExprList::iterator itr = list.begin(); itr != list.end(); ++itr) {
		if (itr->subqueryLevel_ < subqueryLevel_) {
			itr->subqueryLevel_ = subqueryLevel_;
		}
	}
	cxt.selectList_.assign(list.begin(), list.end());

	if (!inSubQuery) {
		node->outputList_.clear();
		cxt.selectTopColumnId_ = 0;
		if (leaveInternalIdColumn) {
			node->outputList_ = inputListToOutput(plan);
			if (node->outputList_.size() < 2) {
				SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
			}
			node->outputList_.erase(node->outputList_.begin() + 2, node->outputList_.end());
			cxt.selectTopColumnId_ = static_cast<ColumnId>(node->outputList_.size());
		}
		node->outputList_.insert(node->outputList_.end(), list.begin(), list.end());
		for (size_t pos = 0; pos < inSelectList.size(); ++pos) {
			cxt.selectExprRefList_.push_back(toRef(plan, node->outputList_[cxt.selectTopColumnId_ + pos]));
		}
	}
	else {
		if (leaveInternalIdColumn) {
			SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
		}
		node->outputList_.clear();
		node->outputList_ = inputListToOutput(plan);

		util::Set<SourceId> srcIdSet(alloc_);
		PlanExprList::iterator itr = list.begin();
		for (; itr != list.end(); ++itr) {
			if (itr->srcId_ != -1 && itr->subqueryLevel_ == subqueryLevel_) {
				srcIdSet.insert(itr->srcId_);
			}
		}
		itr = node->outputList_.begin();
		while(itr != node->outputList_.end()) {
			if (itr->srcId_ != -1 && itr->subqueryLevel_ == subqueryLevel_) {
				if (srcIdSet.find(itr->srcId_) != srcIdSet.end()) {
					itr = node->outputList_.erase(itr);
					continue;
				}
			}
			++itr;
		}
		cxt.selectTopColumnId_ = static_cast<ColumnId>(node->outputList_.size());
		node->outputList_.insert(node->outputList_.end(), list.begin(), list.end());
		for (size_t pos = 0; pos < inSelectList.size(); ++pos) {
			cxt.selectExprRefList_.push_back(toRef(plan, node->outputList_[cxt.selectTopColumnId_ + pos]));
		}
	}
	if (!leaveInternalIdColumn) {
		cxt.otherTopPos_ = node->outputList_.size();
		cxt.otherColumnCount_ = 0;

		util::Set<ColumnExprId> checkSet(alloc_);
		PlanExprList::iterator itr = node->outputList_.begin();
		for (; itr != node->outputList_.end(); ++itr) {
			if (itr->op_ == SQLType::EXPR_COLUMN) {
				ColumnExprId id(itr->srcId_, itr->subqueryLevel_, itr->qName_);
				checkSet.insert(id);
			}
		}
		itr = prevOutputList.begin();
		for (; itr != prevOutputList.end(); ++itr) {
			if (itr->op_ == SQLType::EXPR_COLUMN) {
				ColumnExprId id(itr->srcId_, itr->subqueryLevel_, itr->qName_);
				if (checkSet.find(id) == checkSet.end()) {
					checkSet.insert(id);
					node->outputList_.push_back(*itr);
					++cxt.otherColumnCount_;
				}
			}
			else {
				node->outputList_.push_back(*itr);
				++cxt.otherColumnCount_;
			}
		}
	}
	cxt.productedNodeId_ = node->id_;

	return hasAggrExpr;
}


void SQLCompiler::genGroupBy(GenRAContext &cxt, ExprRef &havingExprRef, Plan &plan) {
	if (genRangeGroup(cxt, havingExprRef, plan)) {
		return;
	}

	const Select &select = cxt.select_;
	bool inSubQuery = cxt.inSubQuery_;
	const Expr* selectHaving = select.having_;

	ExprList inSelectList(alloc_);
	inSelectList.reserve(select.selectList_->size() + 1);
	inSelectList.assign(select.selectList_->begin(), select.selectList_->end());
	if (select.having_) {
		inSelectList.push_back(select.having_);
	}
	SQLPreparedPlan::Node *node = NULL;
	genSplittedSelectProjection(cxt, inSelectList, plan);
	if (select.having_) {
		assert(cxt.otherTopPos_ > 0);
		size_t havingColId = cxt.otherTopPos_ - 1;
		assert(plan.back().outputList_.size() > havingColId);
		havingExprRef = toRef(plan, plan.back().outputList_[havingColId]);
	}
	if (cxt.pseudoWindowExprCount_ > 0) {
		genPseudoWindowFunc(cxt, plan);

		PlanNodeId inputId = plan.back().id_;
		SQLPreparedPlan::Node *node = &plan.append(SQLType::EXEC_SELECT);
		node->inputList_.push_back(inputId);
		node->outputList_ = inputListToOutput(plan);
	}
	const PlanNodeId inputId = plan.back().id_;
	PlanExprList aggrList(alloc_);
	{
		assert(cxt.aggrExprList_.size() > 0);
		node = &plan.append(SQLType::EXEC_GROUP);
		node->inputList_.push_back(inputId);
		node->outputList_ = inputListToOutput(plan); 

		ColumnId havingColumnId = UNDEF_COLUMNID;
		if (select.having_) {
			havingExprRef.update();
			havingExprRef = toRef(plan, *havingExprRef);
			havingColumnId = static_cast<ColumnId>(
					havingExprRef.get() - &node->outputList_[0]);
		}
		size_t origOutputListSize = node->outputList_.size();
		assert(inputId != UNDEF_PLAN_NODEID);
		aggrList = genColumnExprList(
				cxt.aggrExprList_, plan, MODE_GROUP_SELECT, &inputId);
		node = &plan.back();
		for (PlanExprList::iterator itr = aggrList.begin(); itr != aggrList.end(); ++itr) {
			if (itr->subqueryLevel_ < subqueryLevel_) {
				itr->subqueryLevel_ = subqueryLevel_;
			}
		}
		node->outputList_.clear();
		node->outputList_ = inputListToOutput(plan);
		assert(cxt.selectTopColumnId_ != UNDEF_COLUMNID);
		trimExprList(origOutputListSize, node->outputList_);

		PlanExprList::const_iterator inItr = aggrList.begin();
		PlanExprList::iterator outItr = cxt.selectList_.begin();
		for (; inItr != aggrList.end(); ++inItr, ++outItr) {
			bool isAggrExpr = findAggrExpr(*inItr, true);
			if (isAggrExpr) {
				*outItr = *inItr;
			}
		}
		if (selectHaving) {
			cxt.selectList_.pop_back();
			if (cxt.selectExprRefList_.size() > 1) {
				cxt.selectExprRefList_.pop_back();
			}
		}
		if (cxt.inSubQuery_ && aggrList.size() > 0) {
			assert(cxt.innerIdExprRef_.get() != NULL);
			Expr innerIdColumnExpr(alloc_);
			innerIdColumnExpr = cxt.innerIdExprRef_.generate();
			inItr = aggrList.begin();
			outItr = node->outputList_.begin() + cxt.selectTopColumnId_;
			size_t pos = 0;
			for (; inItr != aggrList.end(); ++inItr, ++outItr, ++pos) {
				bool isAggrExpr = findAggrExpr(*inItr, true);
				if (isAggrExpr) {
					if (pos < cxt.selectExprRefList_.size()) {
						cxt.selectExprRefList_[pos].update();
						Expr *selectExpr = cxt.selectExprRefList_[pos].get();
						*selectExpr = *inItr;
						if (inItr == aggrList.begin()) {
							applySubqueryId(innerIdColumnExpr, *selectExpr);
							*outItr = *selectExpr;
						}
						cxt.selectExprRefList_[pos] = toRef(plan, *selectExpr);
					}
					if (inItr != aggrList.begin()) {
						*outItr = *inItr;
					}
				}
			}
			if (havingColumnId != UNDEF_COLUMNID) {
				havingExprRef = toRef(plan, node->outputList_[havingColumnId]);
			}
		}
		else {
			inItr = aggrList.begin();
			outItr = node->outputList_.begin() + cxt.selectTopColumnId_;
			for (; inItr != aggrList.end(); ++inItr, ++outItr) {
				bool isAggrExpr = findAggrExpr(*inItr, true);
				if (isAggrExpr) {
					*outItr = *inItr;
				}
			}
			if (havingColumnId != UNDEF_COLUMNID) {
				havingExprRef = toRef(plan, node->outputList_[havingColumnId]);
			}
		}
		while (node->outputList_.size() > origOutputListSize) {
			assert(false);
			node->outputList_.pop_back();
		}
	}
	PlanExprList baseOutputList = node->outputList_;
	assert(node->type_ == SQLType::EXEC_GROUP);
	PlanExprList groupByList0(alloc_);
	size_t orgSize = node->outputList_.size();
	if (select.groupByList_) {
		assert(cxt.productedNodeId_ != UNDEF_PLAN_NODEID);
		groupByList0 = genExprList(
				*select.groupByList_, plan, MODE_GROUPBY, &inputId);
	}
	node = &plan.back();
	assert(node->type_ == SQLType::EXEC_GROUP);
	node->outputList_ = inputListToOutput(plan);
	trimExprList(orgSize, node->outputList_);
	PlanExprList groupByList(alloc_);
	if (inSubQuery) {
		groupByList.reserve(groupByList0.size() + 2);
		groupByList.push_back(genColumnExpr(plan, 0, 0));
	}
	groupByList.insert(groupByList.end(), groupByList0.begin(), groupByList0.end());

	PlanExprList groupByList2(alloc_);
	int32_t index = 0;
	PlanExprList::const_iterator itr = groupByList.begin();
	for (; itr != groupByList.end(); ++itr) {
		if (SQLType::EXPR_CONSTANT == itr->op_
			&& TupleList::TYPE_LONG == itr->value_.getType()) {
			int64_t columnPos = size_t(itr->value_.get<int64_t>());
			if (columnPos > static_cast<int64_t>(cxt.selectList_.size())
				|| columnPos <= 0) {
				SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, &(*itr),
						"Invalid column reference (" << columnPos << ")");
			}
			ColumnId colId = static_cast<ColumnId>(columnPos - 1);
			if (findAggrExpr(cxt.selectList_[colId], true)) {
				SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, &(*itr),
						"Aggregate functions are not allowed in GROUP BY");
			}
			if (inSubQuery) {
				if (colId < cxt.selectExprRefList_.size()) {
					groupByList2.push_back(cxt.selectExprRefList_[colId].generate());
				}
				else {
					SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
				}
			}
			else {
				Expr col = genColumnExpr(plan, 0, colId); 
				groupByList2.push_back(col); 
			}
		}
		else if (SQLType::EXPR_COLUMN == itr->op_) {
			groupByList2.push_back(*itr);
		}
		else {
			SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, &(*itr),
					"Grouping key must be column or column reference");
		}
		index++;
	}
	if (!inSubQuery) {
		validateGroupBy(groupByList2, cxt.selectList_);
	}
	node->predList_.insert(node->predList_.end(), groupByList2.begin(), groupByList2.end());
	PlanExprList::const_iterator inItr = baseOutputList.begin();
	PlanExprList::iterator outItr = node->outputList_.begin();
	for (; inItr != baseOutputList.end(); ++inItr, ++outItr) {
		*outItr = *inItr;
	}
}

void SQLCompiler::genHaving(GenRAContext &cxt, const ExprRef &havingExprRef, Plan &plan) {
	static_cast<void>(cxt);
	PlanNodeId inputId = plan.back().id_;

	SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_SELECT);
	node.inputList_.push_back(inputId);
	node.outputList_ = inputListToOutput(plan);
	Expr havingExpr(alloc_);
	havingExpr = havingExprRef.generate();
	node.predList_.push_back(havingExpr);
}

bool SQLCompiler::genRangeGroup(
		GenRAContext &cxt, ExprRef &havingExprRef, Plan &plan) {
	const Select &select = cxt.select_;

	if (!isRangeGroupPredicate(select.groupByList_)) {
		return false;
	}

	if (cxt.inSubQuery_) {
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
				"GROUP BY RANGE clause in the subquery is not supported");
	}

	ExprList inSelectList(alloc_);
	inSelectList.reserve(select.selectList_->size() + 1);
	inSelectList.assign(select.selectList_->begin(), select.selectList_->end());
	if (select.having_) {
		inSelectList.push_back(select.having_);
	}


	if (findSubqueryExpr(inSelectList)) {
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
				"Subqueries in the SELECT " <<
				(select.having_ == NULL ? "" : "or HAVING ") <<
				"clause with GROUP BY RANGE clause "
				"are not supported");
	}

	if (findDistinctAggregation(&inSelectList)) {
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
				"Distinct aggregation functions with GROUP BY RANGE clause "
				"are not supported");
	}

	const Expr *windowExpr = NULL;

	if (findWindowExpr(inSelectList, false, windowExpr) &&
			calcFunctionCategory(*windowExpr, false) !=
					FUNCTION_PSEUDO_WINDOW) {
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
				"Window functions with GROUP BY RANGE clause "
				"are not supported");
	}

	genSplittedSelectProjection(cxt, inSelectList, plan);

	ExprRef keyExprRef;
	{
		const PlanNodeId inputId = plan.back().id_;
		const PlanNodeId *productedNodeId = &inputId;
		PlanNode &node = plan.append(SQLType::EXEC_SORT);
		node.inputList_.push_back(inputId);

		ExprList inPredList(alloc_);
		inPredList.push_back(select.groupByList_->front()->next_->front());
		assert(inPredList.front()->op_ == SQLType::EXPR_COLUMN);

		PlanExprList baseList = genExprList(
				inPredList, plan, MODE_GROUPBY, productedNodeId);

		keyExprRef = toRef(
				plan,
				plan.nodeList_[inputId].outputList_[baseList.front().columnId_],
				&plan.nodeList_[inputId]);
		plan.nodeList_.pop_back();
	}

	const bool withPseudoWindow = (cxt.pseudoWindowExprCount_ > 0);
	if (withPseudoWindow) {
		genPseudoWindowFunc(cxt, plan);

		PlanNodeId inputId = plan.back().id_;
		SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_SELECT);
		node.inputList_.push_back(inputId);
		node.outputList_ = inputListToOutput(plan);
	}
	else {
		const PlanNodeId inputId = plan.back().id_;
		const PlanNodeId *productedNodeId = &inputId;
		PlanNode &node = plan.append(SQLType::EXEC_SORT);
		node.inputList_.push_back(inputId);
		node.outputList_ = inputListToOutput(plan);

		ExprList inPredList(alloc_);
		inPredList.push_back(select.groupByList_->front()->next_->front());
		assert(inPredList.front()->op_ == SQLType::EXPR_COLUMN);

		node.predList_ = genExprList(
				inPredList, plan, MODE_GROUPBY, productedNodeId);
	}

	{
		const PlanNodeId inputId = plan.back().id_;
		const PlanNodeId *productedNodeId = &inputId;
		PlanNode &node = plan.append(SQLType::EXEC_GROUP);
		node.cmdOptionFlag_ |= PlanNode::Config::CMD_OPT_GROUP_RANGE;
		node.inputList_.push_back(inputId);
		node.outputList_ = inputListToOutput(plan); 

		ExprList inTotalAggrList(alloc_);
		inTotalAggrList.insert(
				inTotalAggrList.end(),
				cxt.aggrExprList_.begin(), cxt.aggrExprList_.end());
		inTotalAggrList.insert(
				inTotalAggrList.end(),
				cxt.windowExprList_.begin(), cxt.windowExprList_.end());

		PlanExprList totalAggrList = genColumnExprList(
				inTotalAggrList, plan, MODE_GROUP_SELECT, productedNodeId);
		assert(&node == &plan.back());
		assert(
				totalAggrList.size() ==
				cxt.aggrExprList_.size() + cxt.windowExprList_.size());

		PlanExprList::const_iterator aggrBegin = totalAggrList.begin();
		PlanExprList::const_iterator aggrEnd = aggrBegin + cxt.aggrExprList_.size();
		{
			PlanExprList::const_iterator inIt = aggrBegin;
			PlanExprList::iterator outIt = cxt.selectList_.begin();
			for (; inIt != aggrEnd; ++inIt, ++outIt) {
				if (findAggrExpr(*inIt, true)) {
					*outIt = *inIt;
				}
			}
		}
		{
			PlanExprList::const_iterator inIt = aggrBegin;
			PlanExprList::iterator outIt =
					node.outputList_.begin() + cxt.selectTopColumnId_;
			for (; inIt != aggrEnd; ++inIt, ++outIt) {
				if (findAggrExpr(*inIt, true)) {
					*outIt = *inIt;
				}
			}
		}
		if (select.having_) {
			cxt.selectList_.pop_back();
			if (cxt.selectExprRefList_.size() > 1) {
				cxt.selectExprRefList_.pop_back();
			}
		}

		PlanNodeRef &whereNode = cxt.scalarWhereNodeRef_;
		const PlanExprList *wherePred =
				(whereNode.get() == NULL ? NULL : &whereNode->predList_);
		if (wherePred == NULL || wherePred->empty()) {
			SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
					"WHERE clause must be specified for GROUP BY RANGE clause");
		}

		const Expr keyExprInCond = *keyExprRef;
		keyExprRef.update();

		const Expr *optionExpr = select.groupByList_->front();
		node.predList_.push_back(resolveRangeGroupPredicate(
				plan, wherePred->front(), *keyExprRef, keyExprInCond,
				*optionExpr));
		whereNode = PlanNodeRef();

		adjustRangeGroupOutput(plan, node, *keyExprRef);

		cxt.selectTopColumnId_ = 0;
		cxt.productedNodeId_ = node.id_;
	}

	if (select.having_) {
		assert(cxt.otherTopPos_ > 0);
		const size_t havingColId = cxt.otherTopPos_ - 1;
		assert(plan.back().outputList_.size() > havingColId);
		havingExprRef = toRef(plan, plan.back().outputList_[havingColId]);
	}

	return true;
}

bool SQLCompiler::genRangeGroupIdNode(
		Plan &plan, const Select &select, int32_t &rangeGroupIdPos) {
	rangeGroupIdPos = -1;

	if (!isRangeGroupPredicate(select.groupByList_)) {
		return false;
	}
	const Expr *optionExpr = select.groupByList_->front();

	{
		Plan::Node &node = plan.back();
		node.outputList_.push_back(makeRangeGroupIdExpr(plan, *optionExpr));
		rangeGroupIdPos = static_cast<int32_t>(node.outputList_.size() - 1);
	}
	const PlanNodeId inputId = plan.back().id_;

	{
		Plan::Node &node = plan.append(SQLType::EXEC_SELECT);
		node.inputList_.push_back(inputId);
		node.outputList_ = inputListToOutput(plan);
	}

	return true;
}

bool SQLCompiler::isRangeGroupPredicate(const ExprList *groupByList) {
	if (groupByList == NULL || groupByList->size() != 1) {
		return false;
	}

	const Expr *optionExpr = groupByList->front();
	if (optionExpr->op_ != SQLType::EXPR_RANGE_GROUP) {
		return false;
	}

	return true;
}

void SQLCompiler::adjustRangeGroupOutput(
		Plan &plan, PlanNode &node, const Expr &keyExpr) {
	PlanExprList &outExprList = node.outputList_;
	assert(keyExpr.op_ == SQLType::EXPR_COLUMN);

	for (PlanExprList::iterator it = outExprList.begin();
			it != outExprList.end(); ++it) {
		adjustRangeGroupOutput(*it, keyExpr);
	}

	refreshColumnTypes(plan, node, outExprList, true, false);
	for (PlanExprList::iterator it = outExprList.begin();
			it != outExprList.end(); ++it) {
	}
}

void SQLCompiler::adjustRangeGroupOutput(
		Expr &outExpr, const Expr &keyExpr) {
	const Type type = outExpr.op_;
	do {
		Type destType;
		if (SQLType::START_AGG < type && type < SQLType::END_AGG) {
			destType = SQLType::EXPR_RANGE_FILL;
		}
		else if (type == SQLType::EXPR_COLUMN) {
			destType = (outExpr.columnId_ == keyExpr.columnId_ ?
					SQLType::EXPR_RANGE_KEY : SQLType::EXPR_RANGE_FILL);
		}
		else {
			break;
		}

		Expr *subExpr = ALLOC_NEW(alloc_) Expr(outExpr);
		outExpr = Expr(alloc_);
		outExpr.op_ = destType;
		outExpr.left_ = subExpr;
		outExpr.autoGenerated_ = true;
		setUpExprAttributes(
				outExpr, subExpr, subExpr->columnType_, MODE_GROUP_SELECT,
				SQLType::AGG_PHASE_ALL_PIPE);
		outExpr.autoGenerated_ = subExpr->autoGenerated_;

		outExpr.qName_ = subExpr->qName_;
		outExpr.aliasName_ = subExpr->aliasName_;

		return;
	}
	while (false);

	for (ExprArgsIterator<false> it(outExpr); it.exists(); it.next()) {
		adjustRangeGroupOutput(it.get(), keyExpr);
	}
}

SQLCompiler::Expr SQLCompiler::resolveRangeGroupPredicate(
		const Plan &plan, const Expr &whereCond, const Expr &keyExpr,
		const Expr &keyExprInCond, const Expr &optionExpr) {
	const ColumnType keyType = keyExpr.columnType_;
	{
		if (!ColumnTypeUtils::isNull(keyType) &&
				!ProcessorUtils::isRangeGroupSupportedType(keyType)) {
			SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
					"Unsuppoted column type for GROUP BY RANGE key (type=" <<
					TupleList::TupleColumnTypeCoder()(keyType) << ")");
		}
	}

	TupleValue lower;
	const bool lowerFound =
			findRangeGroupBoundary(whereCond, keyExprInCond, lower, true);

	TupleValue upper;
	const bool upperFound =
			findRangeGroupBoundary(whereCond, keyExprInCond, upper, false);

	if (!lowerFound || !upperFound) {
		if (!lowerFound && !upperFound) {
			SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
					"Boundary conditions in the WHERE clause "
					"must be specified for GROUP BY RANGE clause");
		}
		else {
			SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
					(lowerFound ? "Upper" : "Lower") << " "
					"boundary condition in the WHERE clause "
					"not specified for GROUP BY RANGE clause");
		}
	}

	Expr *inKeyExpr;
	Type fillType;
	int64_t baseInterval;
	util::DateTime::FieldType unit;
	int64_t baseOffset;
	util::TimeZone timeZone;
	getRangeGroupOptions(
			optionExpr, inKeyExpr, fillType, baseInterval, unit, baseOffset,
			timeZone);

	TupleValue interval;
	TupleValue offset;
	ProcessorUtils::resolveRangeGroupInterval(
			alloc_, keyType, baseInterval, baseOffset, unit, timeZone,
			interval, offset);

	if (!ProcessorUtils::adjustRangeGroupBoundary(
			alloc_, lower, upper, interval, offset)) {
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
				"Boundary value for GROUP BY RANGE clause must not be "
				"less than offset");
	}

	const int64_t limit = getMaxGeneratedRows(plan);
	return makeRangeGroupPredicate(
			keyExpr, fillType, interval, lower, upper, limit);
}

SQLCompiler::Expr SQLCompiler::makeRangeGroupIdExpr(
		Plan &plan, const Expr &optionExpr) {
	Expr *inKeyExpr;
	Type fillType;
	int64_t baseInterval;
	util::DateTime::FieldType unit;
	int64_t baseOffset;
	util::TimeZone timeZone;
	getRangeGroupOptions(
			optionExpr, inKeyExpr, fillType, baseInterval, unit, baseOffset,
			timeZone);

	const ColumnType keyType = inKeyExpr->columnType_;
	TupleValue interval;
	TupleValue offset;
	ProcessorUtils::resolveRangeGroupInterval(
			alloc_, keyType, baseInterval, baseOffset, unit, timeZone,
			interval, offset);

	ExprList inPredList(alloc_);
	inPredList.push_back(inKeyExpr);
	assert(inPredList.front()->op_ == SQLType::EXPR_COLUMN);
	Expr* outKeyExpr = ALLOC_NEW(alloc_) Expr(genExprList(
			inPredList, plan, MODE_NORMAL_SELECT, NULL).front());

	Expr outExpr(alloc_);
	outExpr.op_ = SQLType::EXPR_RANGE_GROUP_ID;
	outExpr.next_ = ALLOC_NEW(alloc_) ExprList(alloc_);
	outExpr.next_->push_back(outKeyExpr);
	outExpr.next_->push_back(makeRangeGroupConst(interval));
	outExpr.next_->push_back(makeRangeGroupConst(offset));

	return outExpr;
}

SQLCompiler::Expr SQLCompiler::makeRangeGroupPredicate(
		const Expr &keyExpr, Type fillType, const TupleValue &interval,
		const TupleValue &lower, const TupleValue &upper, int64_t limit) {
	Expr expr(alloc_);
	expr.op_ = SQLType::EXPR_RANGE_GROUP;
	expr.next_ = ALLOC_NEW(alloc_) ExprList(alloc_);
	expr.next_->push_back(ALLOC_NEW(alloc_) Expr(keyExpr));
	expr.next_->push_back(Expr::makeExpr(alloc_, fillType));
	expr.next_->push_back(makeRangeGroupConst(interval));
	expr.next_->push_back(makeRangeGroupConst(lower));
	expr.next_->push_back(makeRangeGroupConst(upper));
	expr.next_->push_back(makeRangeGroupConst(TupleValue(limit)));
	return expr;
}

void SQLCompiler::getRangeGroupOptions(
		const Expr &optionExpr, Expr *&inKeyExpr, Type &fillType,
		int64_t &baseInterval, util::DateTime::FieldType &unit,
		int64_t &baseOffset, util::TimeZone &timeZone) {
	assert(optionExpr.op_ == SQLType::EXPR_RANGE_GROUP);
	inKeyExpr = (*optionExpr.next_)[0];
	fillType = (*optionExpr.next_)[1]->op_;
	baseInterval = (*optionExpr.next_)[2]->value_.get<int64_t>();
	unit = static_cast<util::DateTime::FieldType>(
			(*optionExpr.next_)[3]->value_.get<int64_t>());
	baseOffset = (*optionExpr.next_)[4]->value_.get<int64_t>();
	timeZone = resolveRangeGroupTimeZone((*optionExpr.next_)[5]->value_);
}

bool SQLCompiler::findRangeGroupBoundary(
		const Expr &expr, const Expr &keyExpr, TupleValue &value,
		bool forLower) {
	bool bounding = true;
	bool less = false;
	bool inclusive = false;
	switch (expr.op_) {
	case SQLType::OP_LT:
		less = true;
		break;
	case SQLType::OP_GT:
		break;
	case SQLType::OP_LE:
		inclusive = true;
		less = true;
		break;
	case SQLType::OP_GE:
		inclusive = true;
		break;
	case SQLType::EXPR_AND:
		bounding = false;
		break;
	default:
		return false;
	}

	if (bounding) {
		const Expr *lhs = expr.left_;
		const Expr *rhs = expr.right_;
		if (lhs->op_ != SQLType::EXPR_COLUMN) {
			std::swap(lhs, rhs);
			less = !less;
			if (lhs->op_ != SQLType::EXPR_COLUMN) {
				return false;
			}
		}
		if (!(!forLower == less)) {
			return false;
		}
		if (lhs->columnId_ != keyExpr.columnId_) {
			return false;
		}
		if (rhs->op_ != SQLType::EXPR_CONSTANT) {
			if (!isConstantOnExecution(*rhs)) {
				return false;
			}
			return true;
		}
		const ColumnType keyType = keyExpr.columnType_;
		const ColumnType valueType = rhs->value_.getType();
		if (!ProcessorUtils::isRangeGroupSupportedType(keyType) ||
				!ProcessorUtils::isRangeGroupSupportedType(valueType)) {
			return false;
		}
		return ProcessorUtils::findRangeGroupBoundary(
				alloc_, keyType, rhs->value_, forLower, inclusive, value);
	}

	bool found = false;
	TupleValue lastValue;
	for (ExprArgsIterator<> it(expr); it.exists(); it.next()) {
		TupleValue subValue;
		if (!findRangeGroupBoundary(it.get(), keyExpr, subValue, forLower)) {
			continue;
		}

		found = true;
		lastValue = ProcessorUtils::mergeRangeGroupBoundary(
				lastValue, subValue, forLower);
	}

	value = lastValue;
	return found;
}

util::TimeZone SQLCompiler::resolveRangeGroupTimeZone(
		const TupleValue &specified) {
	if (!ColumnTypeUtils::isNull(specified.getType())) {
		return util::TimeZone::ofOffsetMillis(specified.get<int64_t>());
	}

	const util::TimeZone &zone = getCompileOption().getTimeZone();
	if (!zone.isEmpty()) {
		return zone;
	}

	return util::TimeZone();
}

SQLCompiler::Expr* SQLCompiler::makeRangeGroupConst(const TupleValue &value) {
	Expr *expr = ALLOC_NEW(alloc_) Expr(alloc_);
	expr->op_ = SQLType::EXPR_CONSTANT;
	expr->value_ = value;
	expr->autoGenerated_ = true;
	setUpExprAttributes(
			*expr, NULL, TupleList::TYPE_NULL, MODE_GROUP_SELECT,
			SQLType::AGG_PHASE_ALL_PIPE);
	return expr;
}

bool SQLCompiler::findDistinctAggregation(const ExprList *exprList) {
	if (exprList == NULL) {
		return false;
	}

	for (ExprList::const_iterator it = exprList->begin();
			it != exprList->end(); ++it) {
		if (findDistinctAggregation(*it)) {
			return true;
		}
	}
	return false;
}

bool SQLCompiler::findDistinctAggregation(const Expr *expr) {
	if (expr == NULL) {
		return false;
	}

	assert(!(SQLType::START_AGG < expr->op_ && expr->op_ < SQLType::END_AGG));
	return (expr->aggrOpts_ & SyntaxTree::AGGR_OPT_DISTINCT) != 0 ||
			findDistinctAggregation(expr->left_) ||
			findDistinctAggregation(expr->right_) ||
			findDistinctAggregation(expr->next_);
}

void SQLCompiler::genOrderBy(GenRAContext &cxt, ExprRef &innerTopExprRef, Plan &plan) {
	const Select &select = cxt.select_;
	bool inSubQuery = cxt.inSubQuery_;
	PlanNodeId inputId = plan.back().id_;
	SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_SORT);
	node.inputList_.push_back(inputId);
	node.outputList_ = inputListToOutput(plan);
	PlanNodeId origSortNodeId = node.id_;
	size_t origOutputListSize = node.outputList_.size();

	PlanExprList addColumnExprList(alloc_);
	util::Vector<int32_t> replaceColumnIndexList(alloc_);

	PlanExprList sortList(alloc_);
	Mode mode = (select.selectOpt_ == SyntaxTree::AGGR_OPT_DISTINCT) ?
		MODE_WHERE : MODE_ORDERBY;
	if (select.orderByList_) {
		assert(cxt.productedNodeId_ != UNDEF_PLAN_NODEID);
		sortList = genExprList(*select.orderByList_, plan, mode, &cxt.productedNodeId_);
	}
	PlanExprList sortList2(alloc_);
	sortList2.reserve(sortList.size() + 1);
	if (inSubQuery) {
		sortList2.push_back(genColumnExpr(plan, 0, 0));
	}
	int32_t index = inSubQuery ? 1 : 0;
	PlanExprList::const_iterator itr = sortList.begin();
	for (; itr != sortList.end(); ++itr) {
		if (SQLType::EXPR_CONSTANT == itr->op_
			&& TupleList::TYPE_LONG == itr->value_.getType()) {
			int64_t columnPos = size_t(itr->value_.get<int64_t>());
			if (!cxt.select_.selectList_
				|| columnPos > static_cast<int64_t>(cxt.select_.selectList_->size())
				|| columnPos <= 0) {
				SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, &(*itr),
					"Invalid column reference (" << columnPos << ")");
			}
			ColumnId colId = static_cast<ColumnId>(columnPos - 1);
			if (inSubQuery) {
				assert(innerTopExprRef.get() != NULL);
				Expr col(alloc_);
				col = innerTopExprRef.generate();
				col.sortAscending_ = itr->sortAscending_;
				sortList2.push_back(col);
			} else {
				Expr col = genColumnExpr(plan, 0, colId);
				col.sortAscending_ = itr->sortAscending_;
				sortList2.push_back(col);
			}
		}
		else {
			sortList2.push_back(*itr);
			if (itr->op_ != SQLType::EXPR_COLUMN) {
				addColumnExprList.push_back(*itr);
				replaceColumnIndexList.push_back(index);
			}
		}
		index++;
	}
	node = plan.nodeList_[origSortNodeId];
	node.predList_.insert(node.predList_.end(), sortList2.begin(), sortList2.end());

	if (addColumnExprList.size() > 0) {
		PlanNodeId inputId = plan.back().id_;
		SQLPreparedPlan::Node &newSortNode = plan.append(SQLType::EXEC_SORT);
		newSortNode.inputList_.push_back(inputId);
		SQLPreparedPlan::Node &oldSortNode = plan.nodeList_[origSortNodeId];
		oldSortNode.type_ = SQLType::EXEC_SELECT;

		newSortNode.predList_.assign(
			oldSortNode.predList_.begin(), oldSortNode.predList_.end());

		const uint32_t inId = 0;
		oldSortNode.outputList_.clear();
		oldSortNode.outputList_ = inputListToOutput(plan, oldSortNode, &inId);
		newSortNode.outputList_ = inputListToOutput(plan, newSortNode, &inId);

		oldSortNode.predList_.clear();
		oldSortNode.outputList_.insert(
			oldSortNode.outputList_.end(),
			addColumnExprList.begin(), addColumnExprList.end());
		for (size_t addIndex = 0; addIndex < addColumnExprList.size(); addIndex++) {
			ColumnId colId = static_cast<ColumnId>(origOutputListSize + addIndex);
			const Expr& col = genColumnExpr(plan, 0, colId);
			newSortNode.predList_[replaceColumnIndexList[addIndex]] = col;
		}
	}
}

void SQLCompiler::genLimitOffset(
		const Expr *limitExpr, const Expr *offsetExpr, Plan &plan) {
	PlanNodeId inputId = plan.back().id_;
	SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_LIMIT);
	node.inputList_.push_back(inputId);
	node.outputList_ = inputListToOutput(plan);

	node.limit_ = genLimitOffsetValue(limitExpr, plan, MODE_LIMIT);
	node.offset_ = genLimitOffsetValue(offsetExpr, plan, MODE_OFFSET);
}

void SQLCompiler::genSelectProjection(
		GenRAContext &cxt, Plan &plan, bool removeAutoGen) {
	const Select &select = cxt.select_;
	const size_t basePlanSize = cxt.basePlanSize_;
	bool inSubquery = cxt.inSubQuery_;

	PlanNodeId inputId = (basePlanSize != plan.nodeList_.size()) ? plan.back().id_ : SyntaxTree::UNDEF_INPUTID;

	{
		SQLPreparedPlan::Node *node = &plan.append(SQLType::EXEC_SELECT);
		if (inputId != SyntaxTree::UNDEF_INPUTID) {
			node->inputList_.push_back(inputId);
			node->outputList_ = inputListToOutput(plan); 
		}
		if (cxt.selectList_.size() == 0) {
			const Mode mode =
					(select.selectList_ != NULL &&
					findAggrExpr(*select.selectList_, false) ?
					MODE_AGGR_SELECT : MODE_NORMAL_SELECT);
			genSelectList(select, plan, cxt.selectList_, mode, NULL);
			node = &plan.back();
			assert(node->type_ == SQLType::EXEC_SELECT);
		}
		if (removeAutoGen) {
			assert(!inSubquery);
			if (node->outputList_.size() < 2) {
				SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
			}
			node->outputList_.erase(node->outputList_.begin() + 2, node->outputList_.end());
		}
		else {
			if (!inSubquery) {
				node->outputList_.clear();
			}
		}
		node->outputList_.insert(node->outputList_.end(), cxt.selectList_.begin(), cxt.selectList_.end());
		cxt.productedNodeId_ = node->id_;
	}
}

void SQLCompiler::genSelectProjection(
		const Select &select, const size_t basePlanSize, bool inSubquery,
		PlanExprList &selectList,
		ExprList *&windowExprList, size_t &windowExprCount,
		PlanNodeId &productedNodeId, Plan &plan, bool removeAutoGen) {
	static_cast<void>(windowExprList);
	static_cast<void>(windowExprCount);
	PlanNodeId inputId = (basePlanSize != plan.nodeList_.size()) ? plan.back().id_ : SyntaxTree::UNDEF_INPUTID;

	{
		SQLPreparedPlan::Node *node = &plan.append(SQLType::EXEC_SELECT);
		if (inputId != SyntaxTree::UNDEF_INPUTID) {
			node->inputList_.push_back(inputId);
			node->outputList_ = inputListToOutput(plan); 
		}
		if (selectList.size() == 0) {
			const Mode mode =
					(select.selectList_ != NULL &&
					findAggrExpr(*select.selectList_, false) ?
					MODE_AGGR_SELECT : MODE_NORMAL_SELECT);
			genSelectList(select, plan, selectList, mode, NULL);
			node = &plan.back();
			assert(node->type_ == SQLType::EXEC_SELECT);
		}
		if (removeAutoGen) {
			assert(!inSubquery);
			if (node->outputList_.size() < 2) {
				SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
			}
			node->outputList_.erase(node->outputList_.begin() + 2, node->outputList_.end());
		}
		else {
			if (!inSubquery) {
				node->outputList_.clear();
			}
		}
		node->outputList_.insert(node->outputList_.end(), selectList.begin(), selectList.end());
		productedNodeId = node->id_;
	}
}

void SQLCompiler::genSelectList(
		const Select &select, Plan &plan, PlanExprList &selectList,
		Mode mode, const PlanNodeId *productedNodeId) {
	if (select.selectList_) {
		const PlanExprList &list = genColumnExprList(
				*select.selectList_, plan, mode, productedNodeId);
		selectList.insert(selectList.end(), list.begin(), list.end());
	}
}

void SQLCompiler::genPromotedProjection(
		Plan &plan, const util::Vector<ColumnType> &columnTypeList) {
	{
		const PlanNode &node = plan.back();
		const PlanExprList &outputList = node.outputList_;

		if (outputList.size() != columnTypeList.size()) {
			assert(false);
			SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
		}

		bool same = true;
		util::Vector<ColumnType>::const_iterator typeIt =
				columnTypeList.begin();
		for (PlanExprList::const_iterator it = outputList.begin();
				it != outputList.end(); ++it, ++typeIt) {
			if (it->columnType_ != *typeIt) {
				same = false;
				break;
			}
		}

		if (same) {
			return;
		}
	}

	{
		const Mode mode = MODE_NORMAL_SELECT;
		const PlanNodeId lastNodeId = plan.back().id_;
		PlanNode &node = plan.append(SQLType::EXEC_SELECT);
		node.inputList_.push_back(lastNodeId);
		node.outputList_ = inputListToOutput(plan);

		PlanExprList &outputList = node.outputList_;

		util::Vector<ColumnType>::const_iterator typeIt =
				columnTypeList.begin();
		for (PlanExprList::iterator it = outputList.begin();
				it != outputList.end(); ++it, ++typeIt) {
			if (it->columnType_ == *typeIt) {
				continue;
			}

			*it = genCastExpr(
					plan, *typeIt, ALLOC_NEW(alloc_) Expr(*it),
					mode, true, true);
		}
	}
}

void SQLCompiler::genSubquerySet(
		const Set &set, Type type, PlanNodeId outerBaseNodeId,
		PlanExprList &argSelectList, PlanNodeId &productedNodeId,
		ExprRef &innerIdExprRef,
		ExprRef &innerTopExprRef,
		const Select* &lastSelect, bool &needToCompensate, Plan &plan) {

	if (SyntaxTree::SET_OP_UNION_ALL == set.type_) {
		needToCompensate = true;
		genSubqueryUnionAll(
			set, type, outerBaseNodeId, argSelectList, productedNodeId,
			innerIdExprRef, innerTopExprRef,
			lastSelect, needToCompensate, plan);
	}
	else if (set.left_) {
		needToCompensate = true;
		genSubquerySet(
			*set.left_, type, outerBaseNodeId, argSelectList, productedNodeId,
			innerIdExprRef, innerTopExprRef,
			lastSelect, needToCompensate, plan);
		SQLPreparedPlan::Node leftNode = plan.back();
		genSubquerySelect(
			*set.right_, type, outerBaseNodeId, argSelectList, productedNodeId,
			innerIdExprRef, innerTopExprRef,
			needToCompensate, plan);
		lastSelect = set.right_;
		SQLPreparedPlan::Node rightNode = plan.back();

		if ((leftNode.outputList_.size() == 0)
			|| (leftNode.outputList_.size() != rightNode.outputList_.size())) {
			SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
					"UNION selection must have same columns");
		}
		SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_UNION);
		node.inputList_.push_back(leftNode.id_);
		node.inputList_.push_back(rightNode.id_);
		node.outputList_ = mergeOutput(plan);
		innerIdExprRef.update();
		innerTopExprRef.update();

		switch (set.type_) {
			case SyntaxTree::SET_OP_UNION_ALL:
				node.unionType_ = SQLType::UNION_ALL;
				break;
			case SyntaxTree::SET_OP_UNION:
				node.unionType_ = SQLType::UNION_DISTINCT;
				break;
			case SyntaxTree::SET_OP_INTERSECT:
				node.unionType_ = SQLType::UNION_INTERSECT;
				needToCompensate = true;
				break;
			case SyntaxTree::SET_OP_EXCEPT:
				node.unionType_ = SQLType::UNION_EXCEPT;
				needToCompensate = true;
				break;
			default:
				assert(false);
				SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL,
					"unknown set_type=" << static_cast<int32_t>(set.type_));
				break;
		}
	}
	else {
		genSubquerySelect(
			*set.right_, type, outerBaseNodeId, argSelectList, productedNodeId,
			innerIdExprRef, innerTopExprRef,
			needToCompensate, plan);
		lastSelect = set.right_;
	}
}

void SQLCompiler::genSubqueryUnionAll(
		const Set &set, Type type, PlanNodeId outerBaseNodeId,
		PlanExprList &argSelectList, PlanNodeId &productedNodeId,
		ExprRef &innerIdExprRef,
		ExprRef &innerTopExprRef,
		const Select* &lastSelect, bool &needToCompensate, Plan &plan) {

	const SelectList &inSelectList = *set.unionAllList_;
	SQLPreparedPlan::Node::IdList planNodeIdList(alloc_);
	size_t prevNodeOutputSize = 0;
	if (set.left_) {
		genSubquerySet(
			*set.left_, type, outerBaseNodeId, argSelectList, productedNodeId,
			innerIdExprRef, innerTopExprRef,
			lastSelect, needToCompensate, plan);
		planNodeIdList.push_back(plan.back().id_);
	}
	SelectList::const_iterator itr = inSelectList.begin();
	for (; itr != inSelectList.end(); ++itr) {
		genSubquerySelect(
			**itr, type, outerBaseNodeId, argSelectList, productedNodeId,
			innerIdExprRef, innerTopExprRef,
			needToCompensate, plan);
		lastSelect = *itr;
		SQLPreparedPlan::Node &node = plan.back();
		planNodeIdList.push_back(node.id_);

		size_t outputSize = getOutput(plan).size();
		if (0 == outputSize
			|| ((prevNodeOutputSize > 0) && (outputSize != prevNodeOutputSize))) {
			SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
					"UNION do not have the same number of result columns");
		}
		prevNodeOutputSize = getOutput(plan).size();
	}
	SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_UNION);
	node.unionType_ = SQLType::UNION_ALL;
	node.inputList_ = planNodeIdList;
	const PlanExprList outExprList = mergeOutput(plan);
	node.outputList_ = outExprList;
	innerIdExprRef.update();
	innerTopExprRef.update();
}

void SQLCompiler::genWindowOverClauseOption(
		GenRAContext &cxt, GenOverContext &overCxt, Plan &plan) {

	PlanNodeId inputId = plan.back().id_;
	SQLPreparedPlan::Node *node = &plan.append(SQLType::EXEC_SELECT);
	node->inputList_.push_back(inputId);
	node->outputList_ = inputListToOutput(plan);

	PlanNodeId firstSelectNodeId = node->id_;

	const Expr* windowExpr = overCxt.windowExpr_;
	PlanExprList windowPartitionByList1(alloc_);
	PlanExprList windowOrderByList1(alloc_);

	if (windowExpr->windowOpt_) {
		SQLPreparedPlan::Node &inputNode = node->getInput(plan, 0);
		PlanExprList saveOutputList(alloc_);
		saveOutputList = inputNode.outputList_;
		hideSelectListName(cxt, inputNode, plan);

		assert(cxt.productedNodeId_ != UNDEF_PLAN_NODEID);
		if (windowExpr->windowOpt_->partitionByList_) {
			if (findSubqueryExpr(*windowExpr->windowOpt_->partitionByList_)) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
						"Subqueries in the over clause are not supported");
			}
			windowPartitionByList1 = genExprList(
					*windowExpr->windowOpt_->partitionByList_,
					plan, MODE_NORMAL_SELECT, &inputId);
			assert(node->id_ == plan.back().id_);
		}
		if (windowExpr->windowOpt_->orderByList_) {
			const ExprList &inExprList = *windowExpr->windowOpt_->orderByList_;
			if (findSubqueryExpr(inExprList)) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
						"Subqueries in the over clause are not supported");
			}
			windowOrderByList1 = genExprList(
					inExprList, plan, MODE_NORMAL_SELECT, &inputId);
			assert(node->id_ == plan.back().id_);

			PlanExprList::iterator itr = windowOrderByList1.begin();
			ExprList::const_iterator inItr = inExprList.begin();
			for (; inItr != inExprList.end(); ++inItr, ++itr) {
				if (itr == windowOrderByList1.end() ||
						(*inItr)->op_ == SQLType::EXPR_ALL_COLUMN) {
					SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
				}
				if ((*inItr)->op_ == SQLType::EXPR_COLUMN) {
					continue;
				}
				itr->sortAscending_ = (*inItr)->sortAscending_;
			}
		}
		restoreSelectListName(cxt, saveOutputList, plan);
		inputNode.outputList_ = saveOutputList;
	}
	overCxt.windowPartitionByList_.reserve(windowPartitionByList1.size() + 1);
	overCxt.windowOrderByList_.reserve(windowOrderByList1.size() + 1);
	overCxt.addColumnExprList_.reserve(windowPartitionByList1.size() + 1);
	overCxt.replaceColumnIndexList_.reserve(windowPartitionByList1.size() + 1);

	int32_t index = cxt.inSubQuery_ ? 1 : 0;
	PlanExprList::const_iterator itr = windowPartitionByList1.begin();
	for (; itr != windowPartitionByList1.end(); ++itr) {
		overCxt.windowPartitionByList_.push_back(*itr);
		if (itr->op_ != SQLType::EXPR_COLUMN) {
			overCxt.addColumnExprList_.push_back(*itr);
			overCxt.replaceColumnIndexList_.push_back(index);
		}
		index++;
	}
	overCxt.orderByStartPos_ = overCxt.addColumnExprList_.size(); 
	itr = windowOrderByList1.begin();
	for (; itr != windowOrderByList1.end(); ++itr) {
		overCxt.windowOrderByList_.push_back(*itr);
		if (itr->op_ != SQLType::EXPR_COLUMN) {
			overCxt.addColumnExprList_.push_back(*itr);
			overCxt.replaceColumnIndexList_.push_back(index);
		}
		index++;
	}
	const uint32_t inId = 0;
	SQLPreparedPlan::Node &firstSelectNode = plan.nodeList_[firstSelectNodeId];
	firstSelectNode.outputList_.clear();
	firstSelectNode.outputList_ = inputListToOutput(plan, firstSelectNode, &inId);
	overCxt.baseOutputListSize_ = firstSelectNode.outputList_.size();
	firstSelectNode.outputList_.insert(
			firstSelectNode.outputList_.end(),
			overCxt.addColumnExprList_.begin(),
			overCxt.addColumnExprList_.end());
	if (overCxt.addColumnExprList_.size() > 0) {
		PlanNodeId inputId = plan.back().id_;
		SQLPreparedPlan::Node &addColumnNode = plan.append(SQLType::EXEC_SELECT);
		addColumnNode.inputList_.push_back(inputId);
		addColumnNode.outputList_ = inputListToOutput(plan);
		node = &plan.back();
	}
	{
		inputId = plan.back().id_;
		SQLPreparedPlan::Node &inputNode = node->getInput(plan, 0);
		PlanExprList saveOutputList(alloc_);
		saveOutputList = inputNode.outputList_;
		PlanNodeId saveOutputNodeId = inputNode.id_;
		hideSelectListName(cxt, inputNode, plan);
		assert(cxt.windowExprList_.size() > 0);
		assert(cxt.productedNodeId_ != UNDEF_PLAN_NODEID);
		overCxt.windowList_ = genColumnExprList(
				cxt.windowExprList_, plan, MODE_AGGR_SELECT, &inputId);
		node = &plan.back();
		node->outputList_ = inputListToOutput(plan, *node, &inId);

		restoreSelectListName(cxt, saveOutputList, plan);
		plan.nodeList_[saveOutputNodeId].outputList_ = saveOutputList;
	}
}

void SQLCompiler::genWindowSortPlan(
		GenRAContext &cxt, GenOverContext &overCxt, Plan &plan) {

	size_t predListSize =
			overCxt.windowPartitionByList_.size()
			+ overCxt.windowOrderByList_.size()
			+ (cxt.inSubQuery_ ? 1 : 0);

	if (predListSize > 0) {
		PlanNodeId inputId = plan.back().id_;
		SQLPreparedPlan::Node &sortNode = plan.append(SQLType::EXEC_SORT);
		sortNode.inputList_.push_back(inputId);
		sortNode.predList_.reserve(predListSize);
		sortNode.outputList_ = inputListToOutput(plan);

		PlanExprList tmpPredList(alloc_);
		if (cxt.inSubQuery_) {
			tmpPredList.push_back(genColumnExpr(plan, 0, 0));
		}


		tmpPredList.insert(
				tmpPredList.end(),
				overCxt.windowPartitionByList_.begin(),
				overCxt.windowPartitionByList_.end());
		tmpPredList.insert(
				tmpPredList.end(),
				overCxt.windowOrderByList_.begin(),
				overCxt.windowOrderByList_.end());

		for (size_t addIndex = 0;
				addIndex < overCxt.addColumnExprList_.size(); addIndex++) {
			ColumnId colId = static_cast<ColumnId>(overCxt.baseOutputListSize_ + addIndex);
			Expr col = genColumnExpr(plan, 0, colId);
			Expr &destCol = tmpPredList[overCxt.replaceColumnIndexList_[addIndex]];
			col.sortAscending_ = destCol.sortAscending_;
			destCol = col;
		}
		appendUniqueExpr(tmpPredList, sortNode.predList_);
	}
}

void SQLCompiler::appendUniqueExpr(const PlanExprList &appendList, PlanExprList &outList) {
	util::Set<ColumnExprId> checkSet(alloc_);
	PlanExprList::iterator outItr = outList.begin();
	for (; outItr != outList.end(); ++outItr) {
		if (outItr->op_ == SQLType::EXPR_COLUMN) {
			checkSet.insert(ColumnExprId(outItr->srcId_, outItr->subqueryLevel_, outItr->qName_));
		}
	}
	PlanExprList::const_iterator inItr = appendList.begin();
	for (; inItr != appendList.end(); ++inItr) {
		if (inItr->op_ == SQLType::EXPR_COLUMN) {
			ColumnExprId colExprId(inItr->srcId_, inItr->subqueryLevel_, inItr->qName_);
			if (checkSet.find(colExprId) == checkSet.end()) {
				checkSet.insert(colExprId);
				outList.push_back(*inItr);
			}
		}
		else {
			outList.push_back(*inItr);
		}
	}
}

void SQLCompiler::genFrameOptionExpr(
		GenOverContext& overCxt, PlanExprList& tmpFrameOptionList,
		Plan& plan) {
	const Expr *windowExpr = overCxt.windowExpr_;

	const SyntaxTree::WindowOption *windowOption = windowExpr->windowOpt_;
	const SyntaxTree::WindowFrameOption *frameOption =
			windowOption->frameOption_;

	const SyntaxTree::WindowFrameBoundary *frameStartBoundary =
			frameOption->frameStartBoundary_;
	const SyntaxTree::WindowFrameBoundary *frameFinishBoundary =
			frameOption->frameFinishBoundary_;

	assert(windowExpr);
	checkWindowFrameFunctionType(*windowExpr);

	int64_t frameFlags = windowFrameModeToFlag(frameOption->frameMode_);

	if (!frameStartBoundary) {
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, windowExpr,
				"Frame range not specified");
	}
	bool existStartValue = false;
	switch (frameStartBoundary->boundaryType_) {
	case SyntaxTree::WindowFrameBoundary::BOUNDARY_CURRENT_ROW:
		break;
	case SyntaxTree::WindowFrameBoundary::BOUNDARY_UNBOUNDED_PRECEDING:
		frameFlags |= windowFrameTypeToFlag(SQLType::FRAME_START_PRECEDING);
		break;
	case SyntaxTree::WindowFrameBoundary::BOUNDARY_UNBOUNDED_FOLLOWING:
		frameFlags |= windowFrameTypeToFlag(SQLType::FRAME_START_FOLLOWING);
		break;
	case SyntaxTree::WindowFrameBoundary::BOUNDARY_N_PRECEDING:
		frameFlags |= windowFrameTypeToFlag(SQLType::FRAME_START_PRECEDING);
		existStartValue = true;
		break;
	case SyntaxTree::WindowFrameBoundary::BOUNDARY_N_FOLLOWING:
		frameFlags |= windowFrameTypeToFlag(SQLType::FRAME_START_FOLLOWING);
		existStartValue = true;
		break;
	default:
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
		break;
	}

	bool existFinishValue = false;
	if (frameFinishBoundary) {
		switch (frameFinishBoundary->boundaryType_) {
		case SyntaxTree::WindowFrameBoundary::BOUNDARY_CURRENT_ROW:
			break;
		case SyntaxTree::WindowFrameBoundary::BOUNDARY_UNBOUNDED_PRECEDING:
			frameFlags |= windowFrameTypeToFlag(SQLType::FRAME_FINISH_PRECEDING);
			break;
		case SyntaxTree::WindowFrameBoundary::BOUNDARY_UNBOUNDED_FOLLOWING:
			frameFlags |= windowFrameTypeToFlag(SQLType::FRAME_FINISH_FOLLOWING);
			break;
		case SyntaxTree::WindowFrameBoundary::BOUNDARY_N_PRECEDING:
			frameFlags |= windowFrameTypeToFlag(SQLType::FRAME_FINISH_PRECEDING);
			existFinishValue = true;
			break;
		case SyntaxTree::WindowFrameBoundary::BOUNDARY_N_FOLLOWING:
			frameFlags |= windowFrameTypeToFlag(SQLType::FRAME_FINISH_FOLLOWING);
			existFinishValue = true;
			break;
		default:
			SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
			break;
		}
	}

	frameFlags |= getWindowFrameOrderingFlag(overCxt.windowOrderByList_);

	const TupleValue &startValue = (existStartValue ?
			getWindowFrameBoundaryValue(*frameStartBoundary, plan) :
			TupleValue());
	const TupleValue &finishValue = (existFinishValue ?
			getWindowFrameBoundaryValue(*frameFinishBoundary, plan) :
			TupleValue());

	checkWindowFrameValues(startValue, finishValue, frameFlags, windowExpr);

	checkWindowFrameBoundaryOptions(
			frameFlags, startValue, finishValue, windowExpr);
	checkWindowFrameFlags(frameFlags, windowExpr);

	checkWindowFrameOrdering(
			overCxt.windowOrderByList_, frameFlags, startValue, finishValue,
			windowExpr);

	const Mode mode = MODE_NORMAL_SELECT;
	const Expr frameFlagsExpr = genConstExpr(plan, TupleValue(frameFlags), mode);
	const Expr startValueExpr = genConstExpr(plan, startValue, mode);
	const Expr finishValueExpr = genConstExpr(plan, finishValue, mode);

	tmpFrameOptionList.push_back(frameFlagsExpr);
	tmpFrameOptionList.push_back(startValueExpr);
	tmpFrameOptionList.push_back(finishValueExpr);
}

TupleValue SQLCompiler::getWindowFrameBoundaryValue(
		const SyntaxTree::WindowFrameBoundary &boundary, Plan &plan) {
	const Expr *expr = boundary.boundaryValueExpr_;
	if (expr) {
		const int64_t intRange = genFrameRangeValue(expr, plan);
		if (intRange < 0) {
			SQL_COMPILER_THROW_ERROR(
					GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, expr,
					"Frame range expression must be positive integral value");
		}
		return TupleValue(intRange);
	}
	else {
		const int64_t timeRange = boundary.boundaryLongValue_;
		if (timeRange < 0) {
			SQL_COMPILER_THROW_ERROR(
					GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, expr,
					"Frame range expression must be positive integral value");
		}

		const util::DateTime::FieldType timeUnit =
				static_cast<util::DateTime::FieldType>(
						boundary.boundaryTimeUnit_);
		util::PreciseDateTime time;
		try {
			time.addField(timeRange, timeUnit, util::DateTime::ZonedOption());
		}
		catch (util::UtilityException &e) {
			SQL_COMPILER_RETHROW_ERROR(
					e, expr, "Failed to check time range (reason=" <<
					GS_EXCEPTION_MESSAGE(e) << ")");
		}

		return ProcessorUtils::makeTimestampValue(alloc_, time);
	}
}

int64_t SQLCompiler::genFrameRangeValue(const Expr *inExpr, Plan &plan) {
	const Mode mode = MODE_NORMAL_SELECT;

	if (inExpr == NULL) {
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	const Expr targetExpr = genScalarExpr(*inExpr, plan, mode, false, NULL);
	if (targetExpr.op_ != SQLType::EXPR_CONSTANT) {
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, inExpr,
				"Frame range expression must be constant");
	}

	const TupleValue& targetValue = targetExpr.value_;
	if (!ColumnTypeUtils::isIntegral(targetValue.getType())) {
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, inExpr,
				"Frame range expression must be evaluated as integral value");
	}

	const bool implicit = true;
	const TupleValue& value = ProcessorUtils::convertType(
		getVarContext(), targetValue, TupleList::TYPE_LONG, implicit);

	assert(value.getType() == TupleList::TYPE_LONG);

	return value.get<int64_t>();
}

void SQLCompiler::checkWindowFrameFunctionType(const Expr &windowExpr) {
	Type opType = windowExpr.op_;
	if (opType == SQLType::EXPR_FUNCTION) {
		opType = resolveFunction(windowExpr);
	}

	bool windowOnly;
	bool pseudoWindow;
	const bool forWindow = ProcessorUtils::isWindowExprType(
			opType, windowOnly, pseudoWindow);
	if (!forWindow || windowOnly || pseudoWindow) {
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, &windowExpr,
				"Frame is not allowed for the speficied window function");
	}
}

void SQLCompiler::checkWindowFrameValues(
		const TupleValue &startValue, const TupleValue &finishValue,
		int64_t flags, const Expr *inExpr) {
	const bool withStartValue = !ColumnTypeUtils::isNull(startValue.getType());
	const bool withFinishValue = !ColumnTypeUtils::isNull(finishValue.getType());
	if (!withStartValue || !withFinishValue) {
		return;
	}

	if ((flags & windowFrameTypeToFlag(SQLType::FRAME_ROWS)) != 0) {
		const ColumnType promoType = ProcessorUtils::findPromotionType(
				startValue.getType(), finishValue.getType());
		if (ColumnTypeUtils::isNull(promoType)) {
			SQL_COMPILER_THROW_ERROR(
					GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, inExpr,
					"Incompatible type between window frame range values");
		}
	}

	const bool startPreceding = ((flags & windowFrameTypeToFlag(
			SQLType::FRAME_START_PRECEDING)) != 0);
	const bool startFollowing = ((flags & windowFrameTypeToFlag(
			SQLType::FRAME_START_FOLLOWING)) != 0);

	const bool finishPreceding = ((flags & windowFrameTypeToFlag(
			SQLType::FRAME_FINISH_PRECEDING)) != 0);
	const bool finishFollowing = ((flags & windowFrameTypeToFlag(
			SQLType::FRAME_FINISH_FOLLOWING)) != 0);

	do {
		int32_t direction;
		if (startPreceding && finishPreceding) {
			direction = -1;
		}
		else if (startFollowing && finishFollowing) {
			direction = 1;
		}
		else {
			break;
		}

		if (SQLProcessor::ValueUtils::orderValue(
				startValue, finishValue) * direction > 0) {
			SQL_COMPILER_THROW_ERROR(
					GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, inExpr,
					"Empty window frame range");
		}
	}
	while (false);
}

void SQLCompiler::checkWindowFrameBoundaryOptions(
		int64_t flags, const TupleValue &startValue,
		const TupleValue &finishValue, const Expr *inExpr) {
	const bool startFollowing = ((flags & windowFrameTypeToFlag(
			SQLType::FRAME_START_FOLLOWING)) != 0);
	const bool finishPreceding = ((flags & windowFrameTypeToFlag(
			SQLType::FRAME_FINISH_PRECEDING)) != 0);

	const bool withStartValue = !ColumnTypeUtils::isNull(startValue.getType());
	const bool withFinishValue = !ColumnTypeUtils::isNull(finishValue.getType());

	if (startFollowing && !withStartValue) {
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, inExpr,
				"Unacceptable frame directions (following unbounded, ...)");
	}

	if (finishPreceding && !withFinishValue) {
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, inExpr,
				"Unacceptable frame directions (..., preceding unbounded)");
	}
}

void SQLCompiler::checkWindowFrameFlags(int64_t flags, const Expr *inExpr) {
	const bool startPreceding = ((flags & windowFrameTypeToFlag(
			SQLType::FRAME_START_PRECEDING)) != 0);
	const bool startFollowing = ((flags & windowFrameTypeToFlag(
			SQLType::FRAME_START_FOLLOWING)) != 0);

	const bool finishPreceding = ((flags & windowFrameTypeToFlag(
			SQLType::FRAME_FINISH_PRECEDING)) != 0);
	const bool finishFollowing = ((flags & windowFrameTypeToFlag(
			SQLType::FRAME_FINISH_FOLLOWING)) != 0);

	if (startFollowing && finishPreceding) {
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, inExpr,
				"Unacceptable frame directions (following, preceding)");
	}

	if ((!startPreceding && !startFollowing) && finishPreceding) {
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, inExpr,
				"Unacceptable frame directions (current, preceding)");
	}

	if (startFollowing && (!finishPreceding && !finishFollowing)) {
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, inExpr,
				"Unacceptable frame directions (following, current)");
	}
}

void SQLCompiler::checkWindowFrameOrdering(
		const PlanExprList &orderByList, int64_t flags,
		const TupleValue &startValue, const TupleValue &finishValue,
		const Expr *inExpr) {
	if (orderByList.empty()) {
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, inExpr,
				"ORDER BY not specified in OVER clause with window frame");
	}

	if (orderByList.size() != 1) {
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, inExpr,
				"Multiple ORDER BY columns are not allowed "
				"in OVER clause with window frame");
	}

	const ColumnType columnType = orderByList.front().columnType_;
	for (size_t i = 0; i < 2; i++) {
		const TupleValue &value = (i == 0 ? startValue : finishValue);
		const ColumnType valueType = value.getType();
		if (ColumnTypeUtils::isNull(valueType)) {
			continue;
		}

		if ((flags & windowFrameTypeToFlag(SQLType::FRAME_ROWS)) == 0) {
			const ColumnType promoType =
					ProcessorUtils::findPromotionType(columnType, valueType);
			if (ColumnTypeUtils::isNull(promoType)) {
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, inExpr,
						"Incompatible range value type with ORDER BY "
						"column specified in OVER clause with window frame");
			}
		}
		else {
			if (valueType != TupleList::TYPE_LONG) {
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, inExpr,
						"Unacceptable range value type specified "
						"in ROWS mode of window frame");
			}
		}
	}
}

int64_t SQLCompiler::windowFrameModeToFlag(
		SyntaxTree::WindowFrameOption::FrameMode mode) {
	switch (mode) {
	case SyntaxTree::WindowFrameOption::FRAME_MODE_ROW:
		return windowFrameTypeToFlag(SQLType::FRAME_ROWS);
	case SyntaxTree::WindowFrameOption::FRAME_MODE_RANGE:
		return 0;
	default:
		assert(false);
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}
}

int64_t SQLCompiler::getWindowFrameOrderingFlag(
		const PlanExprList &orderByList) {
	if (!orderByList.empty() && orderByList.front().sortAscending_) {
		return windowFrameTypeToFlag(SQLType::FRAME_ASCENDING);
	}
	return 0;
}

int64_t SQLCompiler::windowFrameTypeToFlag(SQLType::WindowFrameType type) {
	return static_cast<int64_t>(1) << type;
}

void SQLCompiler::genWindowMainPlan(
		GenRAContext &cxt, GenOverContext &overCxt, Plan &plan) {

	PlanNodeId inputId = plan.back().id_;
	SQLPreparedPlan::Node &windowNode = plan.append(SQLType::EXEC_SORT);
	windowNode.cmdOptionFlag_ |= PlanNode::Config::CMD_OPT_WINDOW_SORTED;
	size_t predSize = (cxt.inSubQuery_ ? 1 : 0)
			+ overCxt.windowPartitionByList_.size();

	const bool hasFrame = (overCxt.windowExpr_->windowOpt_ && overCxt.windowExpr_->windowOpt_->frameOption_);
	predSize += (hasFrame ? 3 : 0);

	windowNode.inputList_.push_back(inputId);
	windowNode.predList_.reserve(predSize);
	windowNode.outputList_ = inputListToOutput(plan);

	PlanExprList tmpPredList(alloc_);
	if (cxt.inSubQuery_) {
		tmpPredList.push_back(genColumnExpr(plan, 0, 0));
	}
	PlanExprList tmpFrameOptionList(alloc_);
	if (hasFrame) {
		genFrameOptionExpr(overCxt, tmpFrameOptionList, plan);
		assert(tmpFrameOptionList.size() == 3);
	}
	tmpPredList.insert(
		tmpPredList.end(),
		overCxt.windowPartitionByList_.begin(),
		overCxt.windowPartitionByList_.end());
	const size_t orderByStartPos = tmpPredList.size();
	const bool ranking = isRankingWindowExpr(*overCxt.windowExpr_);
	if (ranking) {
		tmpPredList.insert(
			tmpPredList.end(),
			overCxt.windowOrderByList_.begin(),
			overCxt.windowOrderByList_.end());
	}
	for (size_t addIndex = 0;
			addIndex < overCxt.replaceColumnIndexList_.size(); addIndex++) {
		if (!ranking && addIndex >= overCxt.orderByStartPos_) {
			break;
		}
		ColumnId colId = static_cast<ColumnId>(overCxt.baseOutputListSize_ + addIndex);
		const Expr &col = genColumnExpr(plan, 0, colId);
		tmpPredList[overCxt.replaceColumnIndexList_[addIndex]] = col;
	}
	for (size_t i = 0; i < tmpPredList.size(); i++) {
		tmpPredList[i].sortAscending_ = (i < orderByStartPos);
	}
	if (hasFrame) {
		tmpPredList.insert(
				tmpPredList.begin(), tmpFrameOptionList.begin(), tmpFrameOptionList.end());
	}
	appendUniqueExpr(tmpPredList, windowNode.predList_);
	assert(cxt.selectTopColumnId_ != UNDEF_COLUMNID);
	size_t pos = overCxt.windowExprPos_;
	if (cxt.selectTopColumnId_ > 0 && overCxt.windowList_.size() > 0) {
		PlanExprList::const_iterator inItr = overCxt.windowList_.begin() + pos;
		PlanExprList::iterator outItr = windowNode.outputList_.begin() + cxt.selectTopColumnId_ + pos;

		cxt.selectExprRefList_[pos].update();
		Expr *selectExpr = cxt.selectExprRefList_[pos].get();
		*selectExpr = *inItr;
		*outItr = *inItr;
		assert(pos < cxt.selectExprRefList_.size());
		cxt.selectExprRefList_[pos] = toRef(plan, *selectExpr);
	}
	else {
		PlanExprList::const_iterator inItr = overCxt.windowList_.begin() + pos;
		PlanExprList::iterator outItr = windowNode.outputList_.begin() + cxt.selectTopColumnId_ + pos;
		*outItr = *inItr;
		assert(pos < cxt.selectList_.size());
		cxt.selectList_[pos] = *inItr;
	}
}

void SQLCompiler::genOver(GenRAContext &cxt, Plan &plan) {
	assert(cxt.windowExprList_.size() > 0);

	size_t windowExprPos = 0;
	ExprList::const_iterator it = cxt.windowExprList_.begin();
	for (; it != cxt.windowExprList_.end(); ++it, ++windowExprPos) {
		Expr *inExpr = *it;
		const Expr *windowExpr = NULL;
		if (findWindowExpr(*inExpr, false, windowExpr)) {
			assert(windowExpr);
			uint32_t funcCategory = calcFunctionCategory(*windowExpr, false);
			if (funcCategory != FUNCTION_WINDOW) {
				continue;
			}
			GenOverContext overCxt(alloc_);

			overCxt.windowExprPos_ = windowExprPos;
			overCxt.windowExpr_ = windowExpr;
			genWindowOverClauseOption(cxt, overCxt, plan);

			overCxt.sortNodePredSize_ = (cxt.inSubQuery_ ? 1 : 0)
					+ overCxt.windowPartitionByList_.size()
					+ overCxt.windowOrderByList_.size();
			if (overCxt.sortNodePredSize_ > 0) {

				genWindowSortPlan(cxt, overCxt, plan);
			}
			genWindowMainPlan(cxt, overCxt, plan);
		}
	}
}

void SQLCompiler::genPseudoWindowFunc(GenRAContext &cxt, Plan &plan) {
	bool inSubQuery = cxt.inSubQuery_;

	assert(cxt.pseudoWindowExprCount_ == 1); 

	GenOverContext overCxt(alloc_);

	overCxt.windowExprPos_ = 0;
	overCxt.windowExpr_ = NULL;

	ExprList::const_iterator it = cxt.windowExprList_.begin();
	for (; it != cxt.windowExprList_.end(); ++it, ++overCxt.windowExprPos_) {
		Expr *inExpr = *it;
		const Expr *windowExpr = NULL;
		if (findWindowExpr(*inExpr, false, windowExpr)) {
			assert(windowExpr);
			uint32_t funcCategory = calcFunctionCategory(*windowExpr, false);
			if (funcCategory != FUNCTION_PSEUDO_WINDOW) {
				continue;
			}
			overCxt.windowExpr_ = windowExpr;
			preparePseudoWindowOption(cxt, overCxt, plan);

			overCxt.sortNodePredSize_ = (inSubQuery ? 1 : 0)
					+ overCxt.windowPartitionByList_.size()
					+ overCxt.windowOrderByList_.size();
			if (overCxt.sortNodePredSize_ > 0) {

				genWindowSortPlan(cxt, overCxt, plan);
			}

			genWindowMainPlan(cxt, overCxt, plan);

			genPseudoWindowResult(cxt, overCxt, plan);
		}
	}
}


void SQLCompiler::preparePseudoWindowOption(
		GenRAContext &cxt, GenOverContext &overCxt, Plan &plan) {

	const Select &select = cxt.select_;
	const bool inSubQuery = cxt.inSubQuery_;
	PlanNodeId inputId = plan.back().id_;
	SQLPreparedPlan::Node *node = &plan.append(SQLType::EXEC_SELECT);
	node->inputList_.push_back(inputId);
	node->outputList_ = inputListToOutput(plan);

	int32_t rangeGroupIdPos;
	if (genRangeGroupIdNode(plan, select, rangeGroupIdPos)) {
		node = &plan.back();
		inputId = node->inputList_.front();
	}

	PlanNodeId firstSelectNodeId = node->id_;

	PlanExprList windowPartitionByList1(alloc_);
	PlanExprList windowOrderByList1(alloc_);

	const Expr* windowExpr = overCxt.windowExpr_;
	assert(windowExpr->windowOpt_ == NULL);
	Mode mode = MODE_ORDERBY;
	const uint32_t inId = 0;
	{
		assert(windowPartitionByList1.size() == 0);

		PlanExprList groupByList0(alloc_);
		if (select.groupByList_) {
			size_t orgSize = node->outputList_.size();
			if (rangeGroupIdPos >= 0) {
				groupByList0.push_back(genColumnExpr(
						plan, 0, static_cast<ColumnId>(rangeGroupIdPos)));
			}
			else {
				groupByList0 = genExprList(
						*select.groupByList_, plan, mode, &inputId);
			}
			node = &plan.back();
			node->outputList_ = inputListToOutput(plan, *node, &inId);
			trimExprList(orgSize, node->outputList_);
		}
		windowPartitionByList1.reserve(groupByList0.size());
		PlanExprList::const_iterator itr = groupByList0.begin();
		size_t pos = 0;
		for (; itr != groupByList0.end(); ++itr, ++pos) {
			if (SQLType::EXPR_CONSTANT == itr->op_
					&& TupleList::TYPE_LONG == itr->value_.getType()) {
				int64_t columnPos = size_t(itr->value_.get<int64_t>());
				if (columnPos > static_cast<int64_t>(cxt.selectList_.size())
					|| columnPos <= 0) {
					SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, &(*itr),
							"Invalid column reference (" << columnPos << ")");
				}
				ColumnId colId = static_cast<ColumnId>(columnPos - 1);
				if (findAggrExpr(*cxt.aggrExprList_[colId], false)) {
					SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, &(*itr),
							"Aggregate functions are not allowed in GROUP BY");
				}
				if (colId < cxt.selectExprRefList_.size()) {
					windowPartitionByList1.push_back(
							cxt.selectExprRefList_[colId].generate());
				}
				else {
					SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
				}
			}
			else {
				windowPartitionByList1.push_back(*itr);
			}
		}
	}
	SQLPreparedPlan::Node &inputNode = node->getInput(plan, 0);
	PlanExprList saveOutputList(alloc_);
	saveOutputList = inputNode.outputList_;
	PlanNodeId saveOutputNodeId = inputNode.id_;
	hideSelectListName(cxt, inputNode, plan);
	if (windowExpr->next_) {
		assert(windowOrderByList1.size() == 0);
		if (windowExpr->next_->size() > 0) {
			ExprList inExprList(alloc_);
			inExprList.push_back(windowExpr->next_->at(0));
			PlanExprList outExprList = genExprList(
					inExprList, plan, mode, &inputId);
			windowOrderByList1.push_back(outExprList.at(0));
		}
	}
	overCxt.windowPartitionByList_.reserve(windowPartitionByList1.size() + 1);
	overCxt.windowOrderByList_.reserve(windowOrderByList1.size() + 1);

	int32_t index = inSubQuery ? 1 : 0;
	overCxt.orderByStartPos_ = 0;
	PlanExprList::const_iterator itr = windowPartitionByList1.begin();
	for (; itr != windowPartitionByList1.end(); ++itr) {
		overCxt.windowPartitionByList_.push_back(*itr);
		if (itr->op_ != SQLType::EXPR_COLUMN) {
			overCxt.addColumnExprList_.push_back(*itr);
			overCxt.replaceColumnIndexList_.push_back(index);
		}
		index++;
	}
	overCxt.orderByStartPos_ = overCxt.addColumnExprList_.size(); 
	itr = windowOrderByList1.begin();
	for (; itr != windowOrderByList1.end(); ++itr) {
		overCxt.windowOrderByList_.push_back(*itr);
		if (itr->op_ != SQLType::EXPR_COLUMN) {
			overCxt.addColumnExprList_.push_back(*itr);
			overCxt.replaceColumnIndexList_.push_back(index);
		}
		index++;
	}
	SQLPreparedPlan::Node &firstSelectNode = plan.nodeList_[firstSelectNodeId];
	firstSelectNode.outputList_.clear();
	firstSelectNode.outputList_ = inputListToOutput(plan, firstSelectNode, &inId);
	overCxt.baseOutputListSize_ = firstSelectNode.outputList_.size();
	firstSelectNode.outputList_.insert(
			firstSelectNode.outputList_.end(),
			overCxt.addColumnExprList_.begin(), overCxt.addColumnExprList_.end());
	if (overCxt.addColumnExprList_.size() > 0) {
		PlanNodeId inputId = plan.back().id_;
		SQLPreparedPlan::Node &addColumnNode = plan.append(SQLType::EXEC_SELECT);
		addColumnNode.inputList_.push_back(inputId);
		addColumnNode.outputList_ = inputListToOutput(plan);
		node = &plan.back();
	}

	{
		assert(cxt.windowExprList_.size() > 0);
		overCxt.windowList_ = genColumnExprList(
				cxt.windowExprList_, plan, MODE_AGGR_SELECT, &inputId);
		node = &plan.back();
		node->outputList_ = inputListToOutput(plan, *node, &inId);
	}
	restoreSelectListName(cxt, saveOutputList, plan);
	plan.nodeList_[saveOutputNodeId].outputList_ = saveOutputList;
}

void SQLCompiler::genPseudoWindowResult(
		GenRAContext &cxt, GenOverContext &overCxt, Plan &plan) {
	assert(cxt.selectTopColumnId_ != UNDEF_COLUMNID);

	ColumnId colId = static_cast<ColumnId>(cxt.selectTopColumnId_ + overCxt.windowExprPos_);
	const Expr& col = genColumnExpr(plan, 0, colId);

	ExprList argList(alloc_);
	argList.push_back(ALLOC_NEW(alloc_) Expr(col));

	Expr lastFuncExpr(alloc_);
	lastFuncExpr = genExprWithArgs(
			plan, SQLType::AGG_LAST, &argList, MODE_GROUP_SELECT, true);
	lastFuncExpr.autoGenerated_ = false;
	if (cxt.outExprList_[overCxt.windowExprPos_]->aliasName_) {
		lastFuncExpr.aliasName_ = cxt.outExprList_[overCxt.windowExprPos_]->aliasName_;
	}
	assert(overCxt.windowExprPos_ < cxt.aggrExprList_.size());
	if (cxt.selectExprRefList_.size() > 0) {
		cxt.aggrExprList_[overCxt.windowExprPos_] = ALLOC_NEW(alloc_) Expr(lastFuncExpr);
	}
	++cxt.aggrExprCount_;
}


void SQLCompiler::genSubquerySelect(
		const Select &select, Type type, PlanNodeId outerBaseNodeId,
		PlanExprList &inSelectList, PlanNodeId &productedNodeId,
		ExprRef &innerIdExprRef,
		ExprRef &innerTopExprRef,
		bool &needToCompensate, Plan &plan) {

	GenRAContext cxt(alloc_, select);
	cxt.basePlanSize_ = plan.nodeList_.size();
	cxt.productedNodeId_ = UNDEF_PLAN_NODEID;
	cxt.inSubQuery_ = true;
	cxt.type_ = type;

	validateWindowFunctionOccurrence(
			select, cxt.genuineWindowExprCount_, cxt.pseudoWindowExprCount_);

	if (!select.selectList_
		|| (type != SQLType::EXPR_EXISTS && select.selectList_->size() > 1)) {
		SQL_COMPILER_THROW_ERROR(
			GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
			"Subquery has too many columns");
	}
	innerTopExprRef = ExprRef();
	cxt.whereExpr_ = NULL;
	size_t basePlanSize = plan.nodeList_.size();
	cxt.basePlanSize_ = basePlanSize;
	cxt.productedNodeId_ = UNDEF_PLAN_NODEID;
	if (select.from_) {
		genFrom(select, cxt.whereExpr_, plan);
	}
	else {
		genEmptyRootNode(plan);
	}
	cxt.whereExpr_ = select.where_;
	{
		SQLPreparedPlan::Node &node = plan.back();
		for (PlanExprList::iterator itr = node.outputList_.begin();
				itr != node.outputList_.end(); ++itr) {
			if (itr->subqueryLevel_ < subqueryLevel_) {
				itr->subqueryLevel_ = subqueryLevel_;
			}
		}
	}
	const bool whereHasSubquery = findSubqueryExpr(cxt.whereExpr_);
	const bool selectHasAggrExpr = checkExistAggrExpr(*select.selectList_, false);
	if (!needToCompensate) {
		if (whereHasSubquery || selectHasAggrExpr
			|| select.selectOpt_ == SyntaxTree::AGGR_OPT_DISTINCT
			|| select.having_ != NULL || select.limitList_ != NULL) {
			needToCompensate = true;
		}
	}
	{
		const PlanNodeId inputId = plan.back().id_;
		SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_SELECT);
		node.inputList_.push_back(inputId);
		node.outputList_ = inputListToOutput(plan, true);

		const Expr tupleIdExpr = genTupleIdExpr(plan, 0, MODE_NORMAL_SELECT);
		const Expr constExpr =
				genConstExpr(plan, TupleValue(INT64_C(-1)), MODE_NORMAL_SELECT);
		if (needToCompensate) {
			node.outputList_.push_back(constExpr);
		}
		else {
			node.outputList_.push_back(tupleIdExpr);
		}
	}
	{
		PlanNodeId lastNodeId = plan.back().id_;

		SQLPreparedPlan::Node* joinNode = &plan.append(SQLType::EXEC_JOIN);
		joinNode->joinType_ = SQLType::JOIN_INNER;
		if (!needToCompensate || (!select.groupByList_ && selectHasAggrExpr)) {
			joinNode->joinType_ = SQLType::JOIN_LEFT_OUTER;
		}
		joinNode->inputList_.push_back(outerBaseNodeId);
		joinNode->inputList_.push_back(lastNodeId);
		joinNode->outputList_ = inputListToOutput(plan, true);
		innerIdExprRef = toRef(plan, joinNode->outputList_.back());
		cxt.innerIdExprRef_ = innerIdExprRef;

		Expr joinWhereExpr(alloc_);
		const Expr &exprTrue = genConstExpr(plan, makeBoolValue(true), MODE_WHERE);
		joinWhereExpr = exprTrue;
		if (cxt.whereExpr_ != NULL && !whereHasSubquery) {
			joinWhereExpr = genExpr(*cxt.whereExpr_, plan, MODE_WHERE);
			cxt.whereExpr_ = NULL;
			assert(joinNode->id_ == plan.back().id_);
		}
		{
			Expr* primaryCond;
			Expr* destWhere;
			Expr* destOn;
			splitJoinPrimaryCond(
				plan, exprTrue, joinWhereExpr, primaryCond, destWhere, destOn,
				joinNode->joinType_);
			joinNode->predList_.push_back(*primaryCond);
			joinNode->predList_.push_back(*destWhere);
			if (destOn) {
				joinNode->predList_.push_back(*destOn);
			}
		}
	}
	PlanNodeId prevJoinNodeId = plan.back().id_;
	if (cxt.whereExpr_) {
		genWhere(cxt, plan);

		cxt.innerIdExprRef_.update();
		bool havingHasAggrExpr = false;
		if (select.having_ != NULL) {
			havingHasAggrExpr = findAggrExpr(*select.having_, false);
		}
		if (!select.groupByList_ && (selectHasAggrExpr || havingHasAggrExpr)) {
			PlanNodeId lastNodeId = plan.back().id_;
#ifndef NDEBUG
			size_t lastNodeOutputCount = plan.back().outputList_.size();
#endif
			SQLPreparedPlan::Node* joinNode = &plan.append(SQLType::EXEC_JOIN);
			joinNode->joinType_ = SQLType::JOIN_LEFT_OUTER;

			joinNode->inputList_.push_back(outerBaseNodeId);
			joinNode->inputList_.push_back(lastNodeId);
			const PlanExprList &baseOutputList = inputListToOutput(plan, true);
			size_t leftCount = 0;
			size_t rightCount = 0;
			for (PlanExprList::const_iterator itr = baseOutputList.begin();
					 itr != baseOutputList.end(); ++itr) {
				if (itr->inputId_ == 0) {
					joinNode->outputList_.push_back(*itr);
					++leftCount;
				}
				if (itr->inputId_ == 1) {
					if (rightCount >= leftCount) {
						joinNode->outputList_.push_back(*itr);
					}
					++rightCount;
				}
			}
			cxt.innerIdExprRef_.update();
			assert(joinNode->outputList_.size() == lastNodeOutputCount);

			Expr srcOn = genOpExpr(
				plan, SQLType::OP_EQ,
				ALLOC_NEW(alloc_) Expr(genColumnExpr(plan, 0, 0)),
				ALLOC_NEW(alloc_) Expr(genColumnExpr(plan, 1, 0)),
				MODE_WHERE, true);

			const Expr &exprTrue = genConstExpr(plan, makeBoolValue(true), MODE_WHERE);

			Expr* primaryCond;
			Expr* destWhere;
			Expr* destOn;

			splitJoinPrimaryCond(
				plan, exprTrue, srcOn, primaryCond, destWhere, destOn,
				joinNode->joinType_);
			joinNode->predList_.push_back(*primaryCond);
			joinNode->predList_.push_back(*destWhere);
			if (destOn) {
				joinNode->predList_.push_back(*destOn);
			}
		}
	}
	if (select.having_ && !select.groupByList_) {
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
								 "GROUP BY clause is required before HAVING");
	}
	assert(cxt.innerIdExprRef_.get() != NULL);
	{
		if (select.groupByList_ || selectHasAggrExpr) {
			ExprRef havingExprRef;

			genGroupBy(cxt, havingExprRef, plan);

			if (select.groupByList_ && select.having_) {
				genHaving(cxt, havingExprRef, plan);
			}
			else {
				const PlanNodeId inputId = plan.back().id_;
				SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_SELECT);
				node.inputList_.push_back(inputId);
				node.outputList_ = inputListToOutput(plan);
			}
		}
		else {
			genSplittedSelectProjection(cxt, *select.selectList_, plan);
			if (cxt.pseudoWindowExprCount_ > 0) {
				genPseudoWindowFunc(cxt, plan);
			}
			const PlanNodeId inputId = plan.back().id_;
			SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_SELECT);
			node.inputList_.push_back(inputId);
			node.outputList_ = inputListToOutput(plan);
		}
		if (cxt.genuineWindowExprCount_ > 0) {
			genOver(cxt, plan);
			const PlanNodeId inputId = plan.back().id_;
			SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_SELECT);
			node.inputList_.push_back(inputId);
			node.outputList_ = inputListToOutput(plan);
		}
		SQLPreparedPlan::Node *node = &plan.back();
		node->outputList_.clear();
		PlanExprList &join1OutputList = plan.nodeList_[prevJoinNodeId].outputList_;
		PlanExprList::iterator itr = join1OutputList.begin();
		uint32_t leftId = itr->inputId_;
		ColumnId colId = 0;
		for (; itr != join1OutputList.end(); ++itr) {
			if (itr->inputId_ == leftId) {
				node->outputList_.push_back(genColumnExpr(plan, 0, colId));
			}
			else {
				break;
			}
			++colId;
		}
		node->outputList_.push_back(cxt.innerIdExprRef_.generate());
		cxt.innerIdExprRef_.update();

		for (size_t pos = 0; pos < cxt.selectExprRefList_.size(); ++pos) {
			Expr selectExpr(alloc_);
			colId = cxt.selectTopColumnId_ + static_cast<ColumnId>(pos);
			selectExpr = genColumnExpr(plan, 0, colId);
			if (selectExpr.subqueryLevel_ < subqueryLevel_) {
				selectExpr.subqueryLevel_ = subqueryLevel_;
			}
			node->outputList_.push_back(selectExpr);
			if (pos == 0) {
				innerTopExprRef = toRef(plan, node->outputList_.back());
			}
			cxt.selectExprRefList_[pos] = toRef(plan, node->outputList_.back());
		}
	}
	if (select.selectOpt_ == SyntaxTree::AGGR_OPT_DISTINCT) {
		PlanNodeId inputId = plan.back().id_;

		SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_UNION);
		node.unionType_ = SQLType::UNION_DISTINCT;
		node.inputList_.push_back(inputId);
		node.outputList_ = inputListToOutput(plan);
	}
	if (inSelectList.size() == 0) {
		inSelectList.assign(cxt.selectList_.begin(), cxt.selectList_.end());
	}
	assert(cxt.productedNodeId_ != UNDEF_PLAN_NODEID);
	cxt.innerIdExprRef_.update();
	innerIdExprRef = cxt.innerIdExprRef_;
	innerTopExprRef.update();
	productedNodeId = cxt.productedNodeId_;
}

void SQLCompiler::genSubquery(
		const Expr &orgInExpr, Plan &plan, Mode mode, Type type) {
	const bool withNotOp = (orgInExpr.op_ == SQLType::OP_NOT);
	const Expr &inExpr = withNotOp ? (*orgInExpr.left_) : orgInExpr;
	PlanNodeId productedNodeId = UNDEF_PLAN_NODEID;

	++subqueryLevel_;
	PlanNodeId rootNodeId = UNDEF_PLAN_NODEID;
	SQLPreparedPlan::Node::Type rootNodeType = SQLType::EXEC_SELECT;
	size_t rootInputCount = 0;
	{
		assert(!plan.nodeList_.empty());
		SQLPreparedPlan::Node &rootNode = plan.nodeList_.back();

		rootInputCount = rootNode.inputList_.size();
		rootNodeType = rootNode.type_;
		if (rootInputCount == 0) {
			rootNode.outputList_.push_back(
					genConstExpr(plan, TupleValue(), MODE_NORMAL_SELECT));
		}
		else {
			const PlanExprList &rootOutputList = inputListToOutput(plan, true);
			rootNode.outputList_.assign(rootOutputList.begin(), rootOutputList.end());
		}

		rootNodeId = rootNode.id_;
	}


	Expr tupleIdExpr(alloc_);
	tupleIdExpr = genTupleIdExpr(plan, 0, mode);

	PlanNodeId outerBaseNodeId = UNDEF_PLAN_NODEID;
	PlanNodeId joinRightNodeId = UNDEF_PLAN_NODEID;
	{
		if (rootNodeType == SQLType::EXEC_JOIN) {
			assert(plan.nodeList_.size() > 2);
			SQLPreparedPlan::Node *addTupleIdNode = &plan.nodeList_.back();
			joinRightNodeId = addTupleIdNode->inputList_.back();
			addTupleIdNode->inputList_.pop_back();
			addTupleIdNode->type_ = SQLType::EXEC_SELECT;
			addTupleIdNode->predList_.clear();
			Expr inLeftExpr(alloc_);
			if (type == SQLType::EXPR_IN && inExpr.left_) {
				inLeftExpr = genExpr(*inExpr.left_, plan, MODE_NORMAL_SELECT);
				inLeftExpr.qName_ = NULL; 
			}
			addTupleIdNode = &plan.back();

			const PlanExprList &baseOutputList = inputListToOutput(plan, true);
			addTupleIdNode->outputList_.clear();
			addTupleIdNode->outputList_.push_back(tupleIdExpr);
			if (type == SQLType::EXPR_IN) {
				addTupleIdNode->outputList_.push_back(inLeftExpr);
			}
			addTupleIdNode->outputList_.insert(
				addTupleIdNode->outputList_.end(), baseOutputList.begin(), baseOutputList.end());

			SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_SELECT);
			node.inputList_.push_back(addTupleIdNode->id_);
			node.inputList_.push_back(joinRightNodeId);
			node.outputList_ = inputListToOutput(plan, true);
			outerBaseNodeId = node.id_;
		}
		else {
			const PlanNodeId inputId = plan.back().id_;
			assert(rootNodeType != SQLType::EXEC_JOIN);
			SQLPreparedPlan::Node *outerBaseNode = &plan.append(SQLType::EXEC_SELECT);
			outerBaseNode->inputList_.push_back(inputId);
			outerBaseNode->outputList_ = inputListToOutput(plan, true);
			Expr inLeftExpr(alloc_);
			const PlanNodeId startId = plan.back().id_;
			if (type == SQLType::EXPR_IN && inExpr.left_) {
				inLeftExpr = genExpr(*inExpr.left_, plan, MODE_NORMAL_SELECT);
				inLeftExpr.qName_ = NULL; 
			}
			outerBaseNode = &plan.back();
			outerBaseNode->outputList_.clear();
			outerBaseNode->outputList_.push_back(tupleIdExpr);
			if (type == SQLType::EXPR_IN) {
				outerBaseNode->outputList_.push_back(inLeftExpr);
			}
			if (rootInputCount != 0) {
				const PlanExprList &baseOutputList = inputListToOutput(plan, true);
				outerBaseNode->outputList_.insert(
					outerBaseNode->outputList_.end(), baseOutputList.begin(), baseOutputList.end());
				if (startId != outerBaseNode->id_) {
					assert(baseOutputList.size() > 1);
					outerBaseNode->outputList_.pop_back();
				}
			}
			outerBaseNodeId = outerBaseNode->id_;
		}
	}

	{
		Set *set = NULL;
		if (inExpr.right_ && inExpr.right_->subQuery_) {
			set = inExpr.right_->subQuery_;
		}
		else if (inExpr.subQuery_) {
			set = inExpr.subQuery_;
		}
		else {
			assert(false);
			SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL, "");
		}
		PlanExprList selectList(alloc_);
		ExprRef innerIdExprRef;
		ExprRef innerTopExprRef;
		const Select *select = NULL; 
		bool needToCompensate = false; 
		genSubquerySet(
				*set, type, outerBaseNodeId, selectList, productedNodeId,
				innerIdExprRef, innerTopExprRef,
				select, needToCompensate, plan);

		GenRAContext cxt(alloc_, *select);
		cxt.type_ = type;
		cxt.selectList_ = selectList;
		cxt.whereExpr_ = select->where_;
		cxt.basePlanSize_ = plan.nodeList_.size();
		cxt.productedNodeId_ = productedNodeId;
		cxt.inSubQuery_ = true;
		cxt.innerIdExprRef_ = innerIdExprRef;

		if (select->orderByList_) {
			genOrderBy(cxt, innerTopExprRef, plan);
		}
		if (select->limitList_) {
			const Expr* limitExpr = NULL;
			const Expr* offsetExpr = NULL;

			if (select->limitList_->size() > 0) {
				limitExpr = (*select->limitList_)[0];
			}
			if (select->limitList_->size() > 1) {
				offsetExpr = (*select->limitList_)[1];
			}
			if (!select->orderByList_) {
				genOrderBy(cxt, innerTopExprRef, plan);
			}
			SQLPreparedPlan::Node &node = plan.back(); 
			assert(node.type_ == SQLType::EXEC_SORT);
			node.subLimit_ = genLimitOffsetValue(limitExpr, plan, MODE_LIMIT);
			node.subOffset_ = genLimitOffsetValue(offsetExpr, plan, MODE_OFFSET);
		}
		assert(innerIdExprRef.get() != NULL);
		if (needToCompensate) {
			const PlanNodeId inputId = plan.back().id_;
			SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_SELECT);
			node.inputList_.push_back(inputId);
			node.outputList_ = inputListToOutput(plan, true);

			innerIdExprRef.update();
			Expr *innerIdExpr = innerIdExprRef.get();
			*innerIdExpr = genTupleIdExpr(plan, 0, mode);
			innerIdExprRef = toRef(plan, *innerIdExpr);
			cxt.innerIdExprRef_ = innerIdExprRef;
		}
		else {
			const PlanNodeId inputId = plan.back().id_;
			SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_SELECT);
			node.inputList_.push_back(inputId);
			node.outputList_ = inputListToOutput(plan, true);
		}
		innerIdExprRef.update();
		innerTopExprRef.update();
		cxt.innerIdExprRef_ = innerIdExprRef;
		if (needToCompensate || select->limitList_) {
			{
				PlanNodeId lastNodeId = plan.back().id_;
				SQLPreparedPlan::Node* joinNode = &plan.append(SQLType::EXEC_JOIN);
				joinNode->joinType_ = SQLType::JOIN_LEFT_OUTER;
				joinNode->inputList_.push_back(outerBaseNodeId);
				joinNode->inputList_.push_back(lastNodeId);
				const PlanExprList &baseOutputList = inputListToOutput(plan, true);
				for (PlanExprList::const_iterator itr = baseOutputList.begin();
					 itr != baseOutputList.end(); ++itr) {
					if (itr->inputId_ == 0) {
						joinNode->outputList_.push_back(*itr);
					}
					if (itr->inputId_ == 1) {
						break;
					}
				}
				joinNode->outputList_.push_back(innerIdExprRef.generate());
				innerIdExprRef.update();
				joinNode->outputList_.push_back(innerTopExprRef.generate());
				innerTopExprRef.update();
				cxt.innerIdExprRef_ = innerIdExprRef;

				Expr* primaryCond;
				Expr* destWhere;
				Expr* destOn;
				Expr srcOn = genOpExpr(
					plan, SQLType::OP_EQ,
					ALLOC_NEW(alloc_) Expr(genColumnExpr(plan, 0, 0)),
					ALLOC_NEW(alloc_) Expr(genColumnExpr(plan, 1, 0)),
					MODE_WHERE, true);
				const Expr &exprTrue = genConstExpr(plan, makeBoolValue(true), MODE_WHERE);
				splitJoinPrimaryCond(
					plan, exprTrue, srcOn, primaryCond, destWhere, destOn,
					joinNode->joinType_);
				joinNode->predList_.push_back(*primaryCond);
				joinNode->predList_.push_back(*destWhere);
				if (destOn) {
					joinNode->predList_.push_back(*destOn);
				}
			}
		}
		PlanNodeId aggrSelectNodeId = UNDEF_PLAN_NODEID;
		{
			PlanNodeId inputId = plan.back().id_;
			SQLPreparedPlan::Node *aggrSelectNode = &plan.append(SQLType::EXEC_SELECT);
			aggrSelectNode->inputList_.push_back(inputId);
			aggrSelectNode->outputList_ = inputListToOutput(plan, true);
			innerIdExprRef.update();
			innerTopExprRef.update();
			cxt.innerIdExprRef_ = innerIdExprRef;

			Expr replacedAggrExpr = genReplacedAggrExpr(plan, NULL);
			aggrSelectNode->outputList_.push_back(replacedAggrExpr);
			aggrSelectNodeId = plan.back().id_;
		}
		SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_GROUP);
		node.inputList_.push_back(aggrSelectNodeId);

		Expr mainqueryIdExpr = genColumnExpr(plan, 0, 0);
		Expr inLeftExpr(alloc_);
		if (type == SQLType::EXPR_IN) {
			inLeftExpr = genColumnExpr(plan, 0, 1);
		}
		assert(innerIdExprRef.get() != NULL);
		Expr innerIdColumnExpr(alloc_);
		innerIdColumnExpr = innerIdExprRef.generate();

		assert(innerTopExprRef.get() != NULL);
		Expr innerTopExpr(alloc_);
		innerTopExpr = innerTopExprRef.generate();

		node.predList_.push_back(mainqueryIdExpr);
		PlanExprList baseOutputList = inputListToOutput(plan, true);
		node.outputList_.push_back(baseOutputList.front());

		ExprList argList(alloc_);
		Expr condExpr(alloc_);

		PlanNodeId orig = node.inputList_[0];
		node.inputList_[0] = UNDEF_PLAN_NODEID;
		if (type == SQLType::EXPR_IN) {
			assert(innerTopExpr.op_ != SQLType::Id());
			argList.push_back(ALLOC_NEW(alloc_) Expr(innerIdColumnExpr));
			argList.push_back(ALLOC_NEW(alloc_) Expr(inLeftExpr));
			argList.push_back(ALLOC_NEW(alloc_) Expr(innerTopExpr));
			Type opType = withNotOp ? SQLType::AGG_FOLD_NOT_IN : SQLType::AGG_FOLD_IN;
			condExpr = genExprWithArgs(
					plan, opType, &argList, MODE_GROUP_SELECT, true);
		}
		else if (type == SQLType::EXPR_EXISTS) {
			argList.push_back(ALLOC_NEW(alloc_) Expr(innerIdColumnExpr));
			Type opType = withNotOp ? SQLType::AGG_FOLD_NOT_EXISTS : SQLType::AGG_FOLD_EXISTS;
			condExpr = genExprWithArgs(
					plan, opType, &argList, MODE_GROUP_SELECT, true);
		}
		else if (type == SQLType::EXPR_SELECTION) {
			assert(innerTopExpr.op_ != SQLType::Id());
			argList.push_back(ALLOC_NEW(alloc_) Expr(innerIdColumnExpr));
			argList.push_back(ALLOC_NEW(alloc_) Expr(innerTopExpr));
			condExpr = genExprWithArgs(
					plan, SQLType::AGG_FOLD_UPTO_ONE, &argList,
					MODE_GROUP_SELECT, true);
			if (innerTopExpr.qName_ != NULL
					&& innerTopExpr.qName_->name_ != NULL
					&& (mode == MODE_NORMAL_SELECT
							|| mode == MODE_AGGR_SELECT
							|| mode == MODE_GROUP_SELECT)) {
				condExpr.qName_ = ALLOC_NEW(alloc_) QualifiedName(alloc_);
				condExpr.qName_->name_ = innerTopExpr.qName_->name_;
			}
		}
		else {
			SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
		}
		node.inputList_[0] = orig;
		node.outputList_.push_back(condExpr);
	}
	PlanNodeId lastNodeId = plan.back().id_;
	SQLPreparedPlan::Node &join2Node = plan.append(SQLType::EXEC_JOIN);
	join2Node.joinType_ = SQLType::JOIN_INNER;
	join2Node.inputList_.push_back(outerBaseNodeId);
	join2Node.inputList_.push_back(lastNodeId);
	PlanExprList baseOutputList = inputListToOutput(plan, true); 

	Expr leftIdExpr(alloc_);
	Expr rightIdExpr(alloc_);
	Expr rightLastExpr(alloc_);
	{
		PlanExprList::iterator itr = baseOutputList.begin();
		uint32_t leftId = itr->inputId_;
		for (; itr != baseOutputList.end(); ++itr) {
			if (itr == baseOutputList.begin()) {
				assert(itr->autoGenerated_);
				assert(itr->op_ == SQLType::EXPR_COLUMN);
				leftIdExpr = *itr;
				continue;
			}
			else if (itr->inputId_ == leftId) {
				join2Node.outputList_.push_back(*itr); 
			}
			else {
				if (rightIdExpr.op_ == SQLType::Id()
					&& itr->op_ == SQLType::EXPR_COLUMN
					&& itr->autoGenerated_) {
					rightIdExpr = *itr;
				}
				else {
					rightLastExpr = *itr;
				}
			}
		}
		if (type == SQLType::EXPR_IN) {
			join2Node.outputList_.erase(join2Node.outputList_.begin());
		}
		join2Node.outputList_.push_back(rightLastExpr);
	}

	const Expr &exprTrue = genConstExpr(plan, makeBoolValue(true), MODE_WHERE);
	Expr srcOn = genOpExpr(
		plan, SQLType::OP_EQ,
		ALLOC_NEW(alloc_) Expr(leftIdExpr),
		ALLOC_NEW(alloc_) Expr(rightIdExpr),
		MODE_WHERE, true);

	Expr* primaryCond;
	Expr* destWhere;
	Expr* destOn;
	splitJoinPrimaryCond(
			plan, exprTrue, srcOn, primaryCond, destWhere, destOn,
			join2Node.joinType_);
	join2Node.predList_.push_back(*primaryCond);
	join2Node.predList_.push_back(*destWhere);
	if (destOn) {
		join2Node.predList_.push_back(*destOn);
	}

	{
		const PlanNodeId inputId = plan.back().id_;
		SQLPreparedPlan::Node &rootNode = plan.nodeList_[rootNodeId];
		rootNode.type_ = SQLType::EXEC_SELECT;
		SQLPreparedPlan::Node &newNode = plan.append(rootNodeType);
		newNode.inputList_.push_back(inputId);
	}
	--subqueryLevel_;
}

void SQLCompiler::genEmptyRootNode(Plan &plan) {
	SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_SELECT);
	node.outputList_.push_back(
		genConstExpr(plan, TupleValue(), MODE_NORMAL_SELECT));
}

void SQLCompiler::genTable(const Expr &table, const Expr* &whereExpr, Plan &plan) {
	assert(table.qName_);

	if (table.qName_->table_ && Meta::isSystemTable(*table.qName_)) {
		Meta meta(*this);
		meta.genSystemTable(table, whereExpr, plan);

		if (table.aliasName_ != NULL) {
			SQLPreparedPlan::Node &node = plan.back();
			int64_t nsId = 0;
			if (table.qName_) {
				nsId = table.qName_->nsId_;
			}
			node.setName(*table.aliasName_, NULL, nsId);
		}
	}
	else {
		const SQLTableInfo& tableInfo = resolveTable(*table.qName_);

		SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_SCAN);

		if (table.aliasName_ != NULL) {
			int64_t nsId = 0;
			if (table.qName_) {
				nsId = table.qName_->nsId_;
			}
			node.setName(*table.aliasName_, NULL, nsId);
		}
		node.tableIdInfo_ = tableInfo.idInfo_;
		node.indexInfoList_ = tableInfo.basicIndexInfoList_;
		node.outputList_ =
				tableInfoToOutput(plan, tableInfo, MODE_NORMAL_SELECT);
		if (table.next_ != NULL) {
			PlanExprList tableOptionList(alloc_);
#ifndef NDEBUG
			PlanNodeId currentId = node.id_;
#endif
			PlanNodeId productedNodeId = UNDEF_PLAN_NODEID;
			tableOptionList = genExprList(
				*table.next_, plan, MODE_DEFAULT, &productedNodeId);
			assert(plan.back().id_ == currentId);
			ExprList *paramList = ALLOC_NEW(alloc_) ExprList(alloc_);
			PlanExprList::iterator itr = tableOptionList.begin();
			for (; itr != tableOptionList.end(); ++itr) {
				if (itr->op_ == SQLType::EXPR_COLUMN) {
					const int64_t sqlColumnId =
							static_cast<int64_t>(itr->columnId_) + 1;
					const Expr &sqlColumnPosExpr = genConstExpr(
							plan, TupleValue(sqlColumnId), MODE_NORMAL_SELECT);
					paramList->push_back(
							ALLOC_NEW(alloc_) Expr(sqlColumnPosExpr));
				}
				else {
					paramList->push_back(
							ALLOC_NEW(alloc_) Expr(*itr));
				}
			}

			const Expr &exprTrue = genConstExpr(
					plan, makeBoolValue(true), MODE_WHERE);
			node.predList_.push_back(exprTrue); 

			const Expr &paramsExpr = genExprWithArgs(
					plan, SQLType::EXPR_LIST, paramList, MODE_NORMAL_SELECT,
					true); 
			node.predList_.push_back(paramsExpr);
		}
	}
}

void SQLCompiler::Meta::genDriverTables(
		const Expr* &whereExpr, Plan &plan, const SystemTableInfo &info) {
	typedef MetaType::ContainerMeta MetaType;

	const PlanNode &scanNode = genMetaScan(plan, info);
	PlanNode &projNode = genMetaSelect(plan);
	const PlanExprList &scanOutputList = base_.inputListToOutput(plan, false);


	projNode.outputList_.push_back(genEmptyColumn(
			plan, TupleList::TYPE_STRING, STR_TABLE_CAT));
	projNode.outputList_.push_back(genEmptyColumn(
			plan, TupleList::TYPE_STRING, STR_TABLE_SCHEM));
	projNode.outputList_.push_back(genRefColumn<MetaType>(
			plan, scanOutputList, MetaType::CONTAINER_NAME,
					STR_TABLE_NAME));
	{
		const Mode mode = MODE_NORMAL_SELECT;
		const Expr scanContainerAttribute = genRefColumn<MetaType>(
				plan, scanOutputList, MetaType::CONTAINER_ATTRIBUTE, END_STR);


		const Expr &constAttributeViewExpr = base_.genConstExpr(
				plan, TupleValue(static_cast<int32_t>(CONTAINER_ATTR_VIEW)), mode);
		const Expr &constStrTableExpr = base_.genConstExpr(
				plan, genStr(STR_TABLE), mode);
		const Expr &constStrViewExpr = base_.genConstExpr(
				plan, genStr(STR_VIEW), mode);

		Expr *ifExpr = ALLOC_NEW(alloc_) Expr(base_.genOpExpr(
				plan, SQLType::OP_EQ,
				ALLOC_NEW(alloc_) Expr(scanContainerAttribute),
				ALLOC_NEW(alloc_) Expr(constAttributeViewExpr), mode, true));

		ExprList argList(alloc_);
		argList.push_back(ifExpr);
		argList.push_back(ALLOC_NEW(alloc_) Expr(constStrViewExpr));
		argList.push_back(ALLOC_NEW(alloc_) Expr(constStrTableExpr));

		Expr exprResult(alloc_);
		exprResult = base_.genExprWithArgs(
				plan, SQLType::EXPR_CASE, &argList, mode, true);
		exprResult.qName_ = genColumnName(STR_TABLE_TYPE);
		projNode.outputList_.push_back(exprResult);
	}
	projNode.outputList_.push_back(genEmptyColumn(
			plan, TupleList::TYPE_STRING, STR_REMARKS));
	projNode.outputList_.push_back(genEmptyColumn(
			plan, TupleList::TYPE_STRING, STR_TYPE_CAT));
	projNode.outputList_.push_back(genEmptyColumn(
			plan, TupleList::TYPE_STRING, STR_TYPE_SCHEM));
	projNode.outputList_.push_back(genEmptyColumn(
			plan, TupleList::TYPE_STRING, STR_TYPE_NAME));
	projNode.outputList_.push_back(genEmptyColumn(
			plan, TupleList::TYPE_STRING, STR_SELF_REFERENCING_COL_NAME));
	projNode.outputList_.push_back(genEmptyColumn(
			plan, TupleList::TYPE_STRING, STR_REF_GENERATION));

	genMetaWhere(plan, whereExpr, scanNode);
}

void SQLCompiler::Meta::genDriverColumns(
		const Expr* &whereExpr, Plan &plan, const SystemTableInfo &info) {
	typedef MetaType::ColumnMeta MetaType;

	const PlanNode &scanNode = genMetaScan(plan, info);
	PlanNode &projNode = genMetaSelect(plan);
	const PlanExprList &scanOutputList = base_.inputListToOutput(plan, false);

	addDBNamePred(plan, info, scanOutputList, scanNode, projNode);

	const Mode mode = MODE_NORMAL_SELECT;


	projNode.outputList_.push_back(genEmptyColumn(
			plan, TupleList::TYPE_STRING, STR_TABLE_CAT));
	projNode.outputList_.push_back(genEmptyColumn(
			plan, TupleList::TYPE_STRING, STR_TABLE_SCHEM));
	projNode.outputList_.push_back(genRefColumn<MetaType>(
			plan, scanOutputList, MetaType::COLUMN_CONTAINER_NAME,
					STR_TABLE_NAME));
	projNode.outputList_.push_back(genRefColumn<MetaType>(
			plan, scanOutputList, MetaType::COLUMN_NAME,
					STR_COLUMN_NAME));
	projNode.outputList_.push_back(genRefColumn<MetaType>(
			plan, scanOutputList, MetaType::COLUMN_SQL_TYPE,
					STR_DATA_TYPE));
	projNode.outputList_.push_back(genRefColumn<MetaType>(
			plan, scanOutputList, MetaType::COLUMN_TYPE_NAME,
					STR_TYPE_NAME));
	projNode.outputList_.push_back(genConstColumn(
			plan, TupleValue(static_cast<int32_t>(128 * 1024)),
					STR_COLUMN_SIZE));
	projNode.outputList_.push_back(genConstColumn(
			plan, TupleValue(static_cast<int32_t>(2000000000)),
					STR_BUFFER_LENGTH));
	projNode.outputList_.push_back(genRefColumn<MetaType>(
			plan, scanOutputList, MetaType::COLUMN_DECIMAL_DIGITS,
					STR_DECIMAL_DIGITS));
	projNode.outputList_.push_back(genConstColumn(
			plan, TupleValue(static_cast<int32_t>(10)), STR_NUM_PREC_RADIX));
	{
		const Expr scanColumnNullable = genRefColumn<MetaType>(
				plan, scanOutputList, MetaType::COLUMN_NULLABLE, END_STR);


		const Expr &constTrueExpr = base_.genConstExpr(
				plan, makeBoolValue(true), mode);
		const Expr &constNullableExpr = base_.genConstExpr(
				plan, TupleValue(JDBCDatabaseMetadata::TYPE_NULLABLE), mode);
		const Expr &constNoNullsExpr = base_.genConstExpr(
				plan, TupleValue(JDBCDatabaseMetadata::TYPE_NO_NULLS), mode);

		Expr *ifExpr = ALLOC_NEW(alloc_) Expr(base_.genOpExpr(
				plan, SQLType::OP_EQ,
				ALLOC_NEW(alloc_) Expr(scanColumnNullable),
				ALLOC_NEW(alloc_) Expr(constTrueExpr), mode, true));

		ExprList argList(alloc_);
		argList.push_back(ifExpr);
		argList.push_back(ALLOC_NEW(alloc_) Expr(constNullableExpr));
		argList.push_back(ALLOC_NEW(alloc_) Expr(constNoNullsExpr));

		Expr exprResult(alloc_);
		exprResult = base_.genExprWithArgs(
				plan, SQLType::EXPR_CASE, &argList, mode, true);
		exprResult.qName_ = genColumnName(STR_NULLABLE);
		projNode.outputList_.push_back(exprResult);
	}
	projNode.outputList_.push_back(genEmptyColumn(
			plan, TupleList::TYPE_STRING, STR_REMARKS));
	projNode.outputList_.push_back(genEmptyColumn(
			plan, TupleList::TYPE_STRING, STR_COLUMN_DEF));
	projNode.outputList_.push_back(genConstColumn(
			plan, TupleValue(static_cast<int32_t>(0)),
					STR_SQL_DATA_TYPE));
	projNode.outputList_.push_back(genConstColumn(
			plan, TupleValue(static_cast<int32_t>(0)),
					STR_SQL_DATETIME_SUB));
	projNode.outputList_.push_back(genRefColumn<MetaType>(
			plan, scanOutputList, MetaType::COLUMN_CHAR_OCTET_LENGTH,
					STR_CHAR_OCTET_LENGTH));
	projNode.outputList_.push_back(genRefColumn<MetaType>(
			plan, scanOutputList, MetaType::COLUMN_ORDINAL,
					STR_ORDINAL_POSITION));
	{
		const Expr scanColumnNullable = genRefColumn<MetaType>(
				plan, scanOutputList, MetaType::COLUMN_NULLABLE, END_STR);


		const Expr &constTrueExpr = base_.genConstExpr(plan, TupleValue(true), mode);
		const Expr &constYesExpr = base_.genConstExpr(plan, genStr(STR_YES), mode);
		const Expr &constNoExpr = base_.genConstExpr(plan, genStr(STR_NO), mode);

		Expr *ifExpr = ALLOC_NEW(alloc_) Expr(base_.genOpExpr(
				plan, SQLType::OP_EQ,
				ALLOC_NEW(alloc_) Expr(scanColumnNullable),
				ALLOC_NEW(alloc_) Expr(constTrueExpr), mode, true));

		ExprList argList(alloc_);
		argList.push_back(ifExpr);
		argList.push_back(ALLOC_NEW(alloc_) Expr(constYesExpr));
		argList.push_back(ALLOC_NEW(alloc_) Expr(constNoExpr));

		Expr exprResult(alloc_);
		exprResult = base_.genExprWithArgs(
				plan, SQLType::EXPR_CASE, &argList, mode, true);
		exprResult.qName_ = genColumnName(STR_IS_NULLABLE);
		projNode.outputList_.push_back(exprResult);
	}
	projNode.outputList_.push_back(genEmptyColumn(
			plan, TupleList::TYPE_STRING, STR_SCOPE_CATALOG));
	projNode.outputList_.push_back(genEmptyColumn(
			plan, TupleList::TYPE_STRING, STR_SCOPE_SCHEMA));
	projNode.outputList_.push_back(genEmptyColumn(
			plan, TupleList::TYPE_STRING, STR_SCOPE_TABLE));
	projNode.outputList_.push_back(genEmptyColumn(
			plan, TupleList::TYPE_SHORT, STR_SOURCE_DATA_TYPE));
	projNode.outputList_.push_back(genConstColumn(
			plan, genStr(STR_NO), STR_IS_AUTOINCREMENT));
	projNode.outputList_.push_back(genConstColumn(
			plan, genStr(STR_NO), STR_IS_GENERATEDCOLUMN));

	genMetaWhere(plan, whereExpr, scanNode);
}

void SQLCompiler::Meta::genDriverPrimaryKeys(
		const Expr* &whereExpr, Plan &plan, const SystemTableInfo &info) {
	typedef MetaType::ColumnMeta MetaType;


	const PlanNode &scanNode = genMetaScan(plan, info);
	PlanNode &projNode = genMetaSelect(plan);
	const PlanExprList &scanOutputList = base_.inputListToOutput(plan, false);

	addDBNamePred(plan, info, scanOutputList, scanNode, projNode);
	addPrimaryKeyPred<MetaType>(
			plan, MetaType::COLUMN_KEY, scanOutputList, projNode);


	projNode.outputList_.push_back(genEmptyColumn(
			plan, TupleList::TYPE_STRING, STR_TABLE_CAT));
	projNode.outputList_.push_back(genEmptyColumn(
			plan, TupleList::TYPE_STRING, STR_TABLE_SCHEM));
	projNode.outputList_.push_back(genRefColumn<MetaType>(
			plan, scanOutputList, MetaType::COLUMN_CONTAINER_NAME,
					STR_TABLE_NAME));
	projNode.outputList_.push_back(genRefColumn<MetaType>(
			plan, scanOutputList, MetaType::COLUMN_NAME, STR_COLUMN_NAME));
	projNode.outputList_.push_back(genRefColumn<MetaType>(
			plan, scanOutputList, MetaType::COLUMN_KEY_SEQUENCE,
					STR_KEY_SEQ));
	projNode.outputList_.push_back(genEmptyColumn(
			plan, TupleList::TYPE_STRING, STR_PK_NAME));

	genMetaWhere(plan, whereExpr, scanNode);
}

void SQLCompiler::Meta::genDriverIndexInfo(
		const Expr* &whereExpr, Plan &plan, const SystemTableInfo &info) {
	typedef MetaType::IndexMeta MetaType;



	const PlanNode &scanNode = genMetaScan(plan, info);
	PlanNode &projNode = genMetaSelect(plan);
	const PlanExprList &scanOutputList = base_.inputListToOutput(plan, false);

	addDBNamePred(plan, info, scanOutputList, scanNode, projNode);

	const Mode mode = MODE_NORMAL_SELECT;


	projNode.outputList_.push_back(genEmptyColumn(
			plan, TupleList::TYPE_STRING, STR_TABLE_CAT));
	projNode.outputList_.push_back(genEmptyColumn(
			plan, TupleList::TYPE_STRING, STR_TABLE_SCHEM));
	projNode.outputList_.push_back(genRefColumn<MetaType>(
			plan, scanOutputList, MetaType::INDEX_CONTAINER_NAME,
					STR_TABLE_NAME));
	projNode.outputList_.push_back(genConstColumn(
			plan, makeBoolValue(true), STR_NON_UNIQUE));
	projNode.outputList_.push_back(genEmptyColumn(
			plan, TupleList::TYPE_STRING, STR_INDEX_QUALIFIER));
	projNode.outputList_.push_back(genRefColumn<MetaType>(
			plan, scanOutputList, MetaType::INDEX_NAME, STR_INDEX_NAME));
	{
		const Expr &exprColumn = genRefColumn<MetaType>(
				plan, scanOutputList, MetaType::INDEX_TYPE, END_STR);
		const int8_t indexTypeDefault =
				static_cast<int8_t>(MAP_TYPE_HASH);
		const Expr &indexTypeDefaultExpr = base_.genConstExpr(
				plan, TupleValue(indexTypeDefault), mode);
		const Expr &sqlIndexTypeHashExpr = base_.genConstExpr(
				plan, TupleValue(JDBCDatabaseMetadata::TABLE_INDEX_HASHED), mode);
		const Expr &sqlIndexTypeOtherExpr = base_.genConstExpr(
				plan, TupleValue(JDBCDatabaseMetadata::TABLE_INDEX_OTHER), mode);

		Expr *ifExpr = ALLOC_NEW(alloc_) Expr(base_.genOpExpr(
				plan, SQLType::OP_EQ,
				ALLOC_NEW(alloc_) Expr(exprColumn),
				ALLOC_NEW(alloc_) Expr(indexTypeDefaultExpr), mode, true));

		ExprList argList(alloc_);
		argList.push_back(ifExpr);
		argList.push_back(ALLOC_NEW(alloc_) Expr(sqlIndexTypeHashExpr));
		argList.push_back(ALLOC_NEW(alloc_) Expr(sqlIndexTypeOtherExpr));

		Expr exprResult(alloc_);
		exprResult = base_.genExprWithArgs(
				plan, SQLType::EXPR_CASE, &argList, mode, true);
		exprResult.qName_ = genColumnName(STR_TYPE);
		projNode.outputList_.push_back(exprResult);
	}
	projNode.outputList_.push_back(genRefColumn<MetaType>(
			plan, scanOutputList, MetaType::INDEX_ORDINAL,
				STR_ORDINAL_POSITION));
	projNode.outputList_.push_back(genRefColumn<MetaType>(
			plan, scanOutputList, MetaType::INDEX_COLUMN_NAME,
					STR_COLUMN_NAME));
	projNode.outputList_.push_back(genEmptyColumn(
			plan, TupleList::TYPE_STRING, STR_ASC_OR_DESC));
	projNode.outputList_.push_back(genConstColumn(
			plan, TupleValue(static_cast<int64_t>(0)), STR_CARDINALITY));
	projNode.outputList_.push_back(genConstColumn(
			plan, TupleValue(static_cast<int64_t>(0)), STR_PAGES));
	projNode.outputList_.push_back(genEmptyColumn(
			plan, TupleList::TYPE_STRING, STR_FILTER_CONDITION));

	genMetaWhere(plan, whereExpr, scanNode);
}

SQLCompiler::Expr SQLCompiler::Meta::genDBNameCondExpr(
		const Plan &plan, const util::String &dbName,
		const Expr &dbColumnExpr) {

	Expr *leftExpr = ALLOC_NEW(alloc_) Expr(dbColumnExpr);

	const TupleValue &constDbNameValue = SyntaxTree::makeStringValue(
			alloc_, dbName.c_str(), dbName.size());
	Expr *rightExpr = ALLOC_NEW(alloc_) Expr(base_.genConstExpr(
			plan, constDbNameValue, MODE_WHERE));

	return base_.genOpExpr(
			plan, SQLType::OP_EQ, leftExpr, rightExpr, MODE_WHERE, true);
}

void SQLCompiler::Meta::appendMetaTableInfoId(
		const Plan &plan, const PlanNode &node, const Expr& exprWhere,
		SQLTableInfo::Id tableInfoId, const Plan::ValueList *parameterList,
		bool &placeholderAffected) {
	placeholderAffected = false;

	const SQLTableInfo &tableInfo = base_.tableInfoList_->resolve(tableInfoId);
	const SQLTableInfo::IdInfo &baseIdInfo = tableInfo.idInfo_;

	if (tableInfo.partitioning_ == NULL ||
			baseIdInfo.type_ != SQLType::TABLE_META) {
		assert(false);
		return;
	}

	const bool forCore = true;
	const MetaType::InfoTable &infoTable = MetaType::InfoTable::getInstance();
	const MetaContainerInfo &metaInfo =
			infoTable.resolveInfo(baseIdInfo.containerId_, forCore);

	uint32_t partitionColumn;
	uint32_t tableNameColumn;
	uint32_t tableIdColumn;
	uint32_t partitionNameColumn;
	if (!findCommonColumnId(
			plan, node, tableInfoId, metaInfo, partitionColumn, tableNameColumn,
			tableIdColumn, partitionNameColumn)) {
		return;
	}

	PartitionId targetPId;
	try {
		if (!ProcessorUtils::predicateToMetaTarget(
				base_.getVarContext(), exprWhere, partitionColumn,
				tableNameColumn, tableIdColumn, partitionNameColumn,
				tableInfo.partitioning_->clusterPartitionCount_,
				targetPId, parameterList, placeholderAffected)) {
			return;
		}
	}
	catch (UserException&) {
		placeholderAffected = true;
		return;
	}

	{
		const TableInfo::SubInfoList &subInfoList = tableInfo.partitioning_->subInfoList_;

		TableInfo::SubInfoList::const_iterator subItr = subInfoList.begin();
		for (; subItr != subInfoList.end(); ++subItr) {
			if (subItr->partitionId_ == targetPId) {
				SQLTableInfo::IdInfo subIdInfo = baseIdInfo;
				subIdInfo.partitionId_ = subItr->partitionId_;
				assert(static_cast<int64_t>(subItr - subInfoList.begin()) ==
						static_cast<int64_t>(targetPId)); 
				subIdInfo.containerId_ = subItr->containerId_;
				subIdInfo.schemaVersionId_ = subItr->schemaVersionId_;
				base_.metaTableInfoIdList_.push_back(subIdInfo);
				break;
			}
		}
	}
}

void SQLCompiler::Meta::genDriverTypeInfo(
		const Expr* &whereExpr, Plan &plan, const SystemTableInfo &info) {
	UNUSED_VARIABLE(whereExpr);
	UNUSED_VARIABLE(info);


	util::Vector<PlanNodeId> planNodeIdList(alloc_);
	const ColumnType list[] = {
		TupleList::TYPE_BYTE,
		TupleList::TYPE_SHORT,
		TupleList::TYPE_INTEGER,
		TupleList::TYPE_LONG,
		TupleList::TYPE_FLOAT,
		TupleList::TYPE_DOUBLE,
		TupleList::TYPE_NANO_TIMESTAMP, 
		TupleList::TYPE_BOOL,
		TupleList::TYPE_ANY,
		TupleList::TYPE_STRING,
		TupleList::TYPE_BLOB
	};
	const ColumnType *listEnd = list + sizeof(list) / sizeof(*list);
	for (const ColumnType *it = list; it != listEnd; ++it) {
		SQLPreparedPlan::Node &projNode = plan.append(SQLType::EXEC_SELECT);
		planNodeIdList.push_back(projNode.id_);

		const bool precisionIgnorable = true;
		const char8_t *typeStr = ValueProcessor::getTypeNameChars(
				convertTupleTypeToNoSQLType(*it), precisionIgnorable);
		if (*it == TupleList::TYPE_ANY) {
			typeStr = str(STR_UNKNOWN);
		}
		const TupleValue &typeNameValue = SyntaxTree::makeStringValue(
				alloc_, typeStr, strlen(typeStr));
		projNode.outputList_.push_back(genConstColumn(
				plan, typeNameValue, STR_TYPE_NAME));
		const int32_t dataType = ProcessorUtils::toSQLColumnType(*it);
		projNode.outputList_.push_back(genConstColumn(
				plan, TupleValue(dataType), STR_DATA_TYPE));
		int32_t typePrecision = ValueProcessor::getValuePrecision(
				convertTupleTypeToNoSQLType(*it));
		if (typePrecision < 0) {
			typePrecision = 0;
		}
		projNode.outputList_.push_back(genConstColumn(
				plan, TupleValue(typePrecision), STR_PRECISION));
		projNode.outputList_.push_back(genEmptyColumn(
				plan, TupleList::TYPE_STRING, STR_LITERAL_PREFIX));
		projNode.outputList_.push_back(genEmptyColumn(
				plan, TupleList::TYPE_STRING, STR_LITERAL_SUFFIX));
		projNode.outputList_.push_back(genEmptyColumn(
				plan, TupleList::TYPE_STRING, STR_CREATE_PARAMS));
		projNode.outputList_.push_back(genConstColumn(
				plan, TupleValue(JDBCDatabaseMetadata::TYPE_NULLABLE),
						STR_NULLABLE));
		projNode.outputList_.push_back(genConstColumn(
				plan, makeBoolValue(true), STR_CASE_SENSITIVE));
		projNode.outputList_.push_back(genConstColumn(
				plan, TupleValue(static_cast<int16_t>(JDBCDatabaseMetadata::TYPE_SEARCHABLE)),
						STR_SEARCHABLE));
		projNode.outputList_.push_back(genConstColumn(
				plan, makeBoolValue(false), STR_UNSIGNED_ATTRIBUTE));
		projNode.outputList_.push_back(genConstColumn(
				plan, makeBoolValue(false), STR_FIXED_PREC_SCALE));
		projNode.outputList_.push_back(genConstColumn(
				plan, makeBoolValue(false), STR_AUTO_INCREMENT));
		projNode.outputList_.push_back(genEmptyColumn(
				plan, TupleList::TYPE_STRING, STR_LOCAL_TYPE_NAME));
		projNode.outputList_.push_back(genConstColumn(
				plan, TupleValue(static_cast<int16_t>(0)), STR_MINIMUM_SCALE));
		projNode.outputList_.push_back(genConstColumn(
				plan, TupleValue(static_cast<int16_t>(0)), STR_MAXIMUM_SCALE));
		projNode.outputList_.push_back(genConstColumn(
				plan, TupleValue(static_cast<int32_t>(0)), STR_SQL_DATA_TYPE));
		projNode.outputList_.push_back(genConstColumn(
				plan, TupleValue(static_cast<int32_t>(0)), STR_SQL_DATETIME_SUB));
		projNode.outputList_.push_back(genConstColumn(
				plan, TupleValue(static_cast<int32_t>(10)), STR_NUM_PREC_RADIX));
	}
	if (planNodeIdList.size() > 1) {
		SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_UNION);
		node.unionType_ = SQLType::UNION_ALL;
		node.inputList_.assign(planNodeIdList.begin(), planNodeIdList.end());
		const uint32_t inputId = 0;
		node.outputList_ = base_.inputListToOutput(plan, plan.back(), &inputId);
	}
}

void SQLCompiler::Meta::genDriverTableTypes(
		const Expr* &whereExpr, Plan &plan, const SystemTableInfo &info) {
	UNUSED_VARIABLE(whereExpr);
	UNUSED_VARIABLE(info);


	util::Vector<PlanNodeId> planNodeIdList(alloc_);
	const StringConstant list[] = {
		SQLCompiler::Meta::STR_TABLE,
		SQLCompiler::Meta::STR_VIEW
	};
	const StringConstant *listEnd = list + sizeof(list) / sizeof(*list);
	for (const StringConstant *it = list; it != listEnd; ++it) {
		SQLPreparedPlan::Node &projNode = plan.append(SQLType::EXEC_SELECT);
		planNodeIdList.push_back(projNode.id_);

		const char8_t *typeStr = str(*it);
		const TupleValue &tableTypeValue = SyntaxTree::makeStringValue(
				alloc_, typeStr, strlen(typeStr));
		projNode.outputList_.push_back(genConstColumn(
				plan, tableTypeValue, STR_TABLE_TYPE));
	}
	if (planNodeIdList.size() > 1) {
		SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_UNION);
		node.unionType_ = SQLType::UNION_ALL;
		node.inputList_.assign(planNodeIdList.begin(), planNodeIdList.end());
		const uint32_t inputId = 0;
		node.outputList_ = base_.inputListToOutput(plan, plan.back(), &inputId);
	}
}

void SQLCompiler::unescape(util::String &str, char escape) {
	std::string::size_type pos = 0;
	while (pos = str.find(escape, pos), pos != std::string::npos) {
		str.erase(str.begin() + pos);
		++pos; 
	}
}

void SQLCompiler::genUnionAll(const Set &set, const util::String *tableName, Plan &plan) {
	const SelectList &inSelectList = *set.unionAllList_;
	SQLPreparedPlan::Node::IdList planNodeIdList(alloc_);
	size_t prevNodeOutputSize = 0;
	if (set.left_) {
		genSet(*set.left_, tableName, plan);
		planNodeIdList.push_back(plan.back().id_);
	}
	SelectList::const_iterator itr = inSelectList.begin();
	const Select *lastSelect = NULL;
	PlanNodeId productedNodeId = UNDEF_PLAN_NODEID;
	for (; itr != inSelectList.end(); ++itr) {
		genSelect(**itr, true, productedNodeId, plan);
		lastSelect = *itr;
		SQLPreparedPlan::Node &node = plan.back();
		planNodeIdList.push_back(node.id_);

		size_t outputSize = getOutput(plan).size();
		if (0 == outputSize
			|| ((prevNodeOutputSize > 0) && (outputSize != prevNodeOutputSize))) {
			SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
					"UNION do not have the same number of result columns");
		}
		prevNodeOutputSize = getOutput(plan).size();
	}
	SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_UNION);
	node.unionType_ = SQLType::UNION_ALL;
	node.inputList_ = planNodeIdList;
	const PlanExprList outExprList = mergeOutput(plan);
	node.outputList_ = outExprList;

	const Select &select = *lastSelect;
	if (select.orderByList_) {
		GenRAContext cxt(alloc_, select);
		cxt.whereExpr_ = select.where_;
		cxt.basePlanSize_ = plan.nodeList_.size();
		cxt.productedNodeId_ = productedNodeId;

		ExprRef innerTopExprRef;
		genOrderBy(cxt, innerTopExprRef, plan);
	}
	if (select.limitList_) {
		const Expr* limitExpr = NULL;
		const Expr* offsetExpr = NULL;

		if (select.limitList_->size() > 0) {
			limitExpr = (*select.limitList_)[0];
		}
		if (select.limitList_->size() > 1) {
			offsetExpr = (*select.limitList_)[1];
		}
		genLimitOffset(limitExpr, offsetExpr, plan);
	}
}


void SQLCompiler::genSet(const Set &set, const util::String *tableName, Plan &plan) {
	PlanNodeId productedNodeId = UNDEF_PLAN_NODEID;
	if (SyntaxTree::SET_OP_UNION_ALL == set.type_) {
		genUnionAll(set, tableName, plan);
	}
	else if (set.left_) {
		genSet(*set.left_, tableName, plan);
		SQLPreparedPlan::Node leftNode = plan.back();
		genSelect(*set.right_, true, productedNodeId, plan);
		SQLPreparedPlan::Node rightNode = plan.back();

		if ((leftNode.outputList_.size() == 0)
			|| (leftNode.outputList_.size() != rightNode.outputList_.size())) {
			SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
					"UNION selection must have same columns");
		}
		SQLPreparedPlan::Node &node = plan.append(SQLType::EXEC_UNION);
		node.inputList_.push_back(leftNode.id_);
		node.inputList_.push_back(rightNode.id_);
		node.outputList_ = mergeOutput(plan);
		switch (set.type_) {
			case SyntaxTree::SET_OP_UNION_ALL:
				node.unionType_ = SQLType::UNION_ALL;
				break;
			case SyntaxTree::SET_OP_UNION:
				node.unionType_ = SQLType::UNION_DISTINCT;
				break;
			case SyntaxTree::SET_OP_INTERSECT:
				node.unionType_ = SQLType::UNION_INTERSECT;
				break;
			case SyntaxTree::SET_OP_EXCEPT:
				node.unionType_ = SQLType::UNION_EXCEPT;
				break;
			default:
				assert(false);
				SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL,
					"unknown set_type=" << static_cast<int32_t>(set.type_));
				break;
		}
		const Select &select = *set.right_;
		const PlanExprList outputList = mergeOutput(plan);
		if (select.orderByList_) {
			GenRAContext cxt(alloc_, select);
			cxt.whereExpr_ = select.where_;
			cxt.basePlanSize_ = plan.nodeList_.size();
			cxt.productedNodeId_ = productedNodeId;

			ExprRef innerTopExprRef;
			genOrderBy(cxt, innerTopExprRef, plan);
		}
		if (select.limitList_) {
			const Expr* limitExpr = NULL;
			const Expr* offsetExpr = NULL;

			if (select.limitList_->size() > 0) {
				limitExpr = (*select.limitList_)[0];
			}
			if (select.limitList_->size() > 1) {
				offsetExpr = (*select.limitList_)[1];
			}
			genLimitOffset(limitExpr, offsetExpr, plan);
		}
	}
	else {
		genSelect(*set.right_, false, productedNodeId, plan);
	}
	if (tableName) {
		plan.back().setName(*tableName, NULL, 0);
	}
}

void SQLCompiler::genJoin(
		const Expr &joinExpr, const util::String *tableName,
		const Expr* &selectWhereExpr, const bool isTop,
		StringSet &aliases, Plan &plan) {

	Expr left(alloc_);
	left = *joinExpr.left_;
	const Expr* nullWhereExpr = NULL;
	genTableOrSubquery(left, nullWhereExpr, false, aliases, plan);
	PlanNodeId leftNodeId = plan.back().id_;

	Expr right(alloc_);
	right = *joinExpr.right_;
	genTableOrSubquery(right, nullWhereExpr, false, aliases, plan);
	PlanNodeId rightNodeId = plan.back().id_;

	SQLPreparedPlan::Node *joinNode = &plan.append(SQLType::EXEC_JOIN); 
	joinNode->inputList_.push_back(leftNodeId);
	joinNode->inputList_.push_back(rightNodeId);
	joinNode->outputList_ = inputListToOutput(plan); 
	joinNode->joinType_ = SQLType::removeNatural(joinExpr.joinOp_);
	if (joinExpr.aliasName_ != NULL) {
		int64_t nsId = 0;
		if (joinExpr.qName_) {
			nsId = joinExpr.qName_->nsId_;
		}
		joinNode->setName(*joinExpr.aliasName_, NULL, nsId);
	}

	bool baseLeft = (SQLType::JOIN_RIGHT_OUTER != SQLType::removeNatural(joinExpr.joinOp_));
	PlanExprList baseOutputList = inputListToOutput(plan);
	PlanExprList joinOutputList(alloc_); 
	TempColumnExprList leftColList(alloc_);
	TempColumnExprList rightColList(alloc_);

	PlanExprList::iterator itr = baseOutputList.begin();
	uint32_t leftId = itr->inputId_;
	for (; itr != baseOutputList.end(); ++itr) {
		if (itr->inputId_ == leftId) {
			leftColList.push_back(*itr);
		}
		else {
			rightColList.push_back(*itr);
		}
	}
	TempColumnExprList* baseColList = &leftColList;
	TempColumnExprList* anotherColList = &rightColList;
	if (!baseLeft) {
		baseColList = &rightColList;
		anotherColList = &leftColList;
	}

	const Expr &exprTrue = genConstExpr(plan, makeBoolValue(true), MODE_WHERE);

	Expr onExpr(alloc_);
	onExpr = exprTrue;

	Expr whereExpr(alloc_);
	whereExpr = exprTrue;

	if (SQLType::isNatural(joinExpr.joinOp_)) {
		if ((*joinExpr.next_)[0] || (*joinExpr.next_)[1]) {
			SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
					"A NATURAL join may not have an ON or USING clause");
		}
		PlanExprList dummyExprList(alloc_);
		PlanExprList outColList(alloc_);
		genJoinCondition(plan, *baseColList, *anotherColList, dummyExprList,
						 onExpr, outColList);
		joinOutputList = outColList;
	}
	else if ((*joinExpr.next_)[1]) {
		if ((*joinExpr.next_)[0]) {
			SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
					"Cannot have both ON and USING clauses in the same join");
		}
		PlanExprList usingExprList(alloc_);
		ExprList &exprList = *joinExpr.next_;
		ExprList::iterator itr = exprList.begin();
		++itr; 
		for (; itr != exprList.end(); ++itr) {
			usingExprList.push_back(**itr);
		}
		PlanExprList outColList(alloc_);
		genJoinCondition(plan, *baseColList, *anotherColList, usingExprList,
						 onExpr, outColList);
		joinOutputList = outColList;
	}
	else if ((*joinExpr.next_)[0]) {
		PlanNodeId origJoinNodeId = plan.back().id_;
		if (findSubqueryExpr((*joinExpr.next_)[0])) {
			SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, (*joinExpr.next_)[0],
					"Subqueries in the ON clause are not supported");
		}
		onExpr = genExpr(*(*joinExpr.next_)[0], plan, MODE_WHERE);
		joinNode = &plan.back();
		if (origJoinNodeId != joinNode->id_) {
			joinNode->inputList_.push_back(rightNodeId);
			joinNode->joinType_ = SQLType::removeNatural(joinExpr.joinOp_);
		}
		joinOutputList = baseOutputList;
	}
	else {
		joinOutputList = baseOutputList;
	}
	joinNode->outputList_.clear();
	joinNode->outputList_.insert(
		joinNode->outputList_.end(), joinOutputList.begin(), joinOutputList.end());
	if (isTop && selectWhereExpr) {
		whereExpr = genExpr(*selectWhereExpr, plan, MODE_WHERE);
		selectWhereExpr = NULL;
		joinNode = &plan.back();
	}
	Expr* primaryCond;
	Expr* destWhere;
	Expr* destOn;
	splitJoinPrimaryCond(
			plan, whereExpr, onExpr, primaryCond, destWhere, destOn,
			joinNode->joinType_);
	joinNode->predList_.push_back(*primaryCond);
	joinNode->predList_.push_back(*destWhere);
	if (destOn) {
		joinNode->predList_.push_back(*destOn);
	}
	if (tableName) {
		int64_t nsId = 0;
		if (joinExpr.qName_) {
			nsId = joinExpr.qName_->nsId_;
		}
		joinNode->setName(*tableName, NULL, nsId);
	}

	if (joinNode->joinType_ != SQLType::JOIN_INNER &&
			joinNode->joinType_ != SQLType::JOIN_LEFT_OUTER) {
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
				"Unsupported join operation (type=" <<
				SQLType::Coder()(joinNode->joinType_, "") << ")");
	}
}

void SQLCompiler::genJoinCondition(const Plan &plan,
		TempColumnExprList& baseColList, TempColumnExprList& anotherColList,
		PlanExprList &usingExprList,
		Expr &onExpr, PlanExprList &outColList) {
	bool checkUsing = (usingExprList.size() > 0);
	PlanExprList commonColList(alloc_);
	TempColumnExprList::iterator itr = baseColList.begin();
	Expr tempOnExpr(alloc_);

	while (itr != baseColList.end()) {
		bool found = false;
		if (itr->qName_ && itr->qName_->name_) {
			TempColumnExprList::iterator cur = anotherColList.begin();
			for (; cur != anotherColList.end(); ++cur) {
				if (cur->qName_ && cur->qName_->name_ &&
						isNameOnlyMatched(*itr->qName_, *cur->qName_)) {
					if (checkUsing) {
						PlanExprList::iterator uItr = usingExprList.begin();
						bool inUsing = false;
						for (; uItr != usingExprList.end(); ++uItr) {
							assert(uItr->op_ == SQLType::EXPR_COLUMN);
							if (isNameOnlyMatched(*itr->qName_, *uItr->qName_)) {
								inUsing = true;
								usingExprList.erase(uItr);
								break;
							}
						}
						if (!inUsing) {
							break;
						}
					}
					Expr eqExpr = genOpExpr(
						plan, SQLType::OP_EQ,
						ALLOC_NEW(alloc_) Expr(*cur),
						ALLOC_NEW(alloc_) Expr(*itr), MODE_DEFAULT, true);
					if (tempOnExpr.op_ != SQLType::Id()) {
						tempOnExpr = genOpExpr(
							plan, SQLType::EXPR_AND,
							ALLOC_NEW(alloc_) Expr(tempOnExpr),
							ALLOC_NEW(alloc_) Expr(eqExpr), MODE_DEFAULT, false);
					}
					else {
						tempOnExpr = eqExpr;
					}
					commonColList.push_back(*itr);
					baseColList.erase(itr++);
					anotherColList.erase(cur);
					found = true;
					break;
				}
			}
			if (found) {
				continue;
			}
		}
		++itr;
	}
	if (checkUsing && (usingExprList.size() > 0)) {
		const char8_t *name = "";
		const Expr &frontExpr = usingExprList.front();
		if (frontExpr.qName_ != NULL && frontExpr.qName_->name_ != NULL) {
			name = frontExpr.qName_->name_->c_str();
		}
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
				"Column \"" << name <<
				"\" specified in USING clause does not exist in left or right table");
	}
	if (tempOnExpr.op_ != SQLType::Id()) {
		onExpr = tempOnExpr;
	}
	else {
	}
	PlanExprList::iterator pItr = commonColList.begin();
	for (; pItr != commonColList.end(); ++pItr) {
		outColList.push_back(*pItr);
	}
	TempColumnExprList::iterator cItr = baseColList.begin();
	for (; cItr != baseColList.end(); ++cItr) {
		outColList.push_back(*cItr);
	}
	cItr = anotherColList.begin();
	for (; cItr != anotherColList.end(); ++cItr) {
		outColList.push_back(*cItr);
	}
}

void SQLCompiler::genTableOrSubquery(
		const Expr &table, const Expr* &whereExpr, const bool isTop,
		StringSet &aliases, Plan &plan) {

	if (table.aliasName_) {
		std::pair<util::Set<util::String>::iterator, bool> insertResult;
		insertResult = aliases.insert(*table.aliasName_);
		if (!insertResult.second) {
			SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, &table,
				"Duplicate alias: name=\""
				<< table.aliasName_->c_str() << "\"");
		}
	}
	if (table.subQuery_) {
		if (table.subQuery_->type_ == SyntaxTree::SET_OP_NONE) {
			assert(table.subQuery_->right_);
			PlanNodeId productedNodeId = UNDEF_PLAN_NODEID;
			genSelect(*table.subQuery_->right_, false, productedNodeId, plan);
		}
		else {
			genSet(*table.subQuery_, table.aliasName_, plan);
		}
		if (table.aliasName_) {
			int64_t nsId = 0;
			if (table.qName_) {
				nsId = table.qName_->nsId_;
			}
			plan.back().setName(*table.aliasName_, NULL, nsId);
		}
	}
	else if (table.op_ == SQLType::EXPR_TABLE) {
		if (table.qName_) {
			if (!table.aliasName_) {
				std::pair<util::Set<util::String>::iterator, bool> insertResult;
				if (table.qName_->table_) {
					insertResult = aliases.insert(*table.qName_->table_);
					if (!insertResult.second) {
						SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, &table,
							"Table name \"" << table.qName_->table_->c_str()
							<< "\" specified more than once");
					}
				}
				else {
					SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, &table,
						"Table name is empty");
				}
			}
			genTable(table, whereExpr, plan);
		}
		else {
			assert(false);
			SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
		}
	}
	else if (table.op_ == SQLType::EXPR_JOIN) {
		genJoin(table, table.aliasName_, whereExpr, isTop, aliases, plan);
	}
	else {
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, &table,
			"Invalid from expression");
	}
}

void SQLCompiler::prepareInnerJoin(
		const Plan &plan, const Expr *whereExpr, SQLPreparedPlan::Node &node) {
	Expr exprTrue(alloc_);
	exprTrue.op_ = SQLType::EXPR_CONSTANT;
	exprTrue.value_ = makeBoolValue(true);
	Expr* primaryCond;
	Expr* destWhere;
	Expr* destOn;
	if (!whereExpr) {
		whereExpr = &exprTrue;
	}
	splitJoinPrimaryCond(
			plan, *whereExpr, exprTrue, primaryCond, destWhere, destOn,
			node.joinType_);
	node.predList_.push_back(*primaryCond);
	node.predList_.push_back(*destWhere);
	if (destOn) {
		node.predList_.push_back(*destOn);
	}
}

void SQLCompiler::validateGroupBy(
		const PlanExprList &groupByList, const PlanExprList &selectList) {
	PlanExprList::const_iterator itr = selectList.begin();
	for (; itr != selectList.end(); ++itr) {
		validateColumnExpr(*itr, groupByList);
	}
}

bool SQLCompiler::validateColumnExpr(
		const Expr &expr, const PlanExprList &groupByList) {
	if (SQLType::START_AGG < expr.op_ && expr.op_ < SQLType::END_AGG) {
		return true;
	}
	if (expr.qName_ && expr.op_ == SQLType::EXPR_COLUMN) {
		bool nullOnly = true; 
		PlanExprList::const_iterator groupItr = groupByList.begin();
		for (; groupItr != groupByList.end(); ++groupItr) {
			const QualifiedName *inName = groupItr->qName_;
			if (inName == NULL) {
				continue;
			}
			nullOnly = false;
			if (expr.qName_ && expr.qName_->name_) {
				if (inName->name_ != NULL &&
						isNameOnlyMatched(*inName, *expr.qName_)) {
					return true;
				}
			}
		}
		UNUSED_VARIABLE(nullOnly);
	}
	if (expr.left_) {
		if (!validateColumnExpr(*expr.left_, groupByList)) {
			return false;
		}
	}
	if (expr.right_) {
		if (!validateColumnExpr(*expr.right_, groupByList)) {
			return false;
		}
	}
	if (expr.next_) {
		ExprList::const_iterator listItr = expr.next_->begin();
		for (; listItr != expr.next_->end(); ++listItr) {
			if (!validateColumnExpr(**listItr, groupByList)) {
				return false;
			}
		}
	}
	if (expr.op_ == SQLType::EXPR_COLUMN) {
		if (!expr.qName_) {
			return true;
		}
		bool nullOnly = true; 
		PlanExprList::const_iterator groupItr = groupByList.begin();
		for (; groupItr != groupByList.end(); ++groupItr) {
			const QualifiedName *inName = groupItr->qName_;
			if (inName == NULL) {
				continue;
			}
			nullOnly = false;
			if (expr.qName_ && expr.qName_->name_) {
				if (inName->name_ != NULL &&
						isNameOnlyMatched(*inName, *expr.qName_)) {
					return true;
				}
			}
		}
		UNUSED_VARIABLE(nullOnly);
	}
	return true;
}

void SQLCompiler::validateSelectList(const PlanExprList &selectList) {
	bool hasAggrExpr = false;
	bool hasNonAggrExpr = false;
	PlanExprList::const_iterator itr = selectList.begin();
	for (; itr != selectList.end(); ++itr) {
		bool found = findAggrExpr(*itr, true);
		if ((hasNonAggrExpr && found) || (hasAggrExpr && !found)) {
			const Expr *column = NULL;
			if (hasAggrExpr && !found) {
				column = findFirstColumnExpr(*itr);
			}
			else {
				PlanExprList::const_iterator itr2 = selectList.begin();
				for (; itr2 != selectList.end(); ++itr2) {
					column = findFirstColumnExpr(*itr2);
					if (column) {
						break;
					}
				}
			}
		}
		if (found) {
			hasAggrExpr = true;
		}
		else {
			hasNonAggrExpr = true;
		}
	}
}

bool SQLCompiler::checkExistAggrExpr(const ExprList &exprList, bool resolved) {
	for (ExprList::const_iterator it = exprList.begin();
			it != exprList.end(); ++it) {
		if (findAggrExpr(**it, resolved, true)) {
			return true;
		}
	}
	return false;
}

bool SQLCompiler::findAggrExpr(const ExprList &exprList, bool resolved, bool exceptWindowFunc) {
	for (ExprList::const_iterator it = exprList.begin();
			it != exprList.end(); ++it) {
		if (findAggrExpr(**it, resolved, exceptWindowFunc)) {
			return true;
		}
	}

	return false;
}

bool SQLCompiler::findAggrExpr(const Expr &expr, bool resolved, bool exceptWindowFunc) {
	uint32_t funcCategory = calcFunctionCategory(expr, resolved);
	if (exceptWindowFunc) {
		if (funcCategory == FUNCTION_AGGR
			|| funcCategory == FUNCTION_PSEUDO_WINDOW) {
			return true;
		}
	} else {
		if (funcCategory == FUNCTION_AGGR
			|| funcCategory == FUNCTION_WINDOW
			|| funcCategory == FUNCTION_PSEUDO_WINDOW) {
			return true;
		}
	}
	if (expr.left_) {
		if (findAggrExpr(*expr.left_, resolved, exceptWindowFunc)) {
			return true;
		}
	}
	if (expr.right_) {
		if (findAggrExpr(*expr.right_, resolved, exceptWindowFunc)) {
			return true;
		}
	}
	if (expr.next_) {
		ExprList::const_iterator listItr = expr.next_->begin();
		for (; listItr != expr.next_->end(); ++listItr) {
			if (findAggrExpr(**listItr, resolved, exceptWindowFunc)) {
				return true;
			}
		}
	}
	return false;
}

bool SQLCompiler::findSubqueryExpr(const ExprList &exprList) {
	for (ExprList::const_iterator it = exprList.begin();
			it != exprList.end(); ++it) {
		if (findSubqueryExpr(*it)) {
			return true;
		}
	}
	return false;
}

bool SQLCompiler::findSubqueryExpr(const Expr *expr) {
	if (!expr) {
		return false;
	}
	if (expr->subQuery_) {
		return true;
	}
	if (expr->left_) {
		if (findSubqueryExpr(expr->left_)) {
			return true;
		}
	}
	if (expr->right_) {
		if (findSubqueryExpr(expr->right_)) {
			return true;
		}
	}
	if (expr->next_) {
		ExprList::const_iterator listItr = expr->next_->begin();
		for (; listItr != expr->next_->end(); ++listItr) {
			if (findSubqueryExpr(*listItr)) {
				return true;
			}
		}
	}
	return false;
}

const SQLCompiler::Expr* SQLCompiler::findFirstColumnExpr(const Expr &expr) {
	const Expr *columnExpr = NULL;
	if (expr.op_ == SQLType::EXPR_COLUMN) {
		return &expr;
	}
	if (expr.left_) {
		if ((columnExpr = findFirstColumnExpr(*expr.left_)) != NULL) {
			return columnExpr;
		}
	}
	if (expr.right_) {

		if ((columnExpr = findFirstColumnExpr(*expr.right_)) != NULL) {
			return columnExpr;
		}
	}
	if (expr.next_) {
		ExprList::const_iterator listItr = expr.next_->begin();
		for (; listItr != expr.next_->end(); ++listItr) {
			if ((columnExpr = findFirstColumnExpr(**listItr)) != NULL) {
				return columnExpr;
			}
		}
	}
	return NULL;
}

void SQLCompiler::checkLimit(Plan &plan) {
	for (PlanNodeList::iterator nodeIt = plan.nodeList_.begin();
			nodeIt != plan.nodeList_.end(); ++nodeIt) {
		PlanNode &node = *nodeIt;
		if (!TupleList::Info::checkColumnCount(node.outputList_.size())) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_LIMIT_EXCEEDED,
					"Column count exceeds maximum size : " << node.outputList_.size());
		}
	}
}

void SQLCompiler::trimExprList(size_t len, PlanExprList &list) {
	if (len < list.size()) {
		size_t count = list.size() - len;
		for (size_t i = 0; i < count; ++i) {
			list.pop_back();
		}
	}
	assert(len == list.size());
}

size_t SQLCompiler::validateWindowFunctionOccurrence(
		const Select &select,
		size_t &genuineWindowExprCount, size_t &pseudoWindowExprCount) {
	genuineWindowExprCount = 0;
	pseudoWindowExprCount = 0;

	size_t occurrenceCount = 0;
	bool resolved = false;
	bool found = false;
	const Expr* foundExpr = NULL;
	if (select.from_) {
		found = findWindowExpr(*select.from_, resolved, foundExpr);
		if (found) {
			SQL_COMPILER_THROW_ERROR(
					GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, select.from_,
					"Window functions are permitted only in the SELECT list.");
		}
	}
	if (select.where_) {
		found = findWindowExpr(*select.where_, resolved, foundExpr);
		if (found) {
			SQL_COMPILER_THROW_ERROR(
					GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, select.where_,
					"Window functions are permitted only in the SELECT list.");
		}
	}
	if (select.having_) {
		found = findWindowExpr(*select.having_, resolved, foundExpr);
		if (found) {
			SQL_COMPILER_THROW_ERROR(
					GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, select.having_,
					"Window functions are permitted only in the SELECT list.");
		}
	}
	if (select.groupByList_) {
		ExprList::const_iterator it = select.groupByList_->begin();
		for (; it != select.groupByList_->end(); ++it) {
			assert(*it);
			found = findWindowExpr(**it, resolved, foundExpr);
			if (found) {
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, *it,
						"Window functions are permitted only in the SELECT list.");
			}
		}
	}
	if (select.orderByList_) {
		ExprList::const_iterator it = select.orderByList_->begin();
		for (; it != select.orderByList_->end(); ++it) {
			assert(*it);
			found = findWindowExpr(**it, resolved, foundExpr);
			if (found) {
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, *it,
						"Window functions are permitted only in the SELECT list.");
			}
		}
	}
	if (select.limitList_) {
		ExprList::const_iterator it = select.limitList_->begin();
		for (; it != select.limitList_->end(); ++it) {
			assert(*it);
			found = findWindowExpr(**it, resolved, foundExpr);
			if (found) {
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, *it,
						"Window functions are permitted only in the SELECT list.");
			}
		}
	}

	if (select.selectList_) {
		size_t totalWindowCount = 0;
		size_t totalAggrCount = 0;

		ExprList::const_iterator it = select.selectList_->begin();
		for (; it != select.selectList_->end(); ++it) {
			assert(*it);

			size_t aggrCount = 0;
			size_t genuineWindowCount = 0;
			size_t pseudoWindowCount = 0;
			checkNestedAggrWindowExpr(**it, resolved, genuineWindowCount, pseudoWindowCount, aggrCount);
			totalWindowCount += genuineWindowCount + pseudoWindowCount;
			genuineWindowExprCount += genuineWindowCount;
			pseudoWindowExprCount += pseudoWindowCount;
			totalAggrCount += aggrCount;
			if (totalWindowCount > 1) {
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, *it,
						"Too many window functions are found.");
			}
			if ((genuineWindowCount + pseudoWindowCount) > 0) {
				assert((genuineWindowCount + pseudoWindowCount) == 1);
				assert(aggrCount == 0);
				found = findWindowExpr(**it, resolved, foundExpr);
				if (found) {
					found = findWindowExpr(**it, resolved, foundExpr);
					if (foundExpr) {
						if (foundExpr->next_ && foundExpr->next_->size() > 0) {
							checkNestedAggrWindowExpr(*foundExpr->next_, resolved);
						}
						if (foundExpr->windowOpt_) {
							if (foundExpr->windowOpt_->partitionByList_
									&& foundExpr->windowOpt_->partitionByList_->size() > 0) {
								checkNestedAggrWindowExpr(
									*foundExpr->windowOpt_->partitionByList_,
									resolved);
							}
							if (foundExpr->windowOpt_->orderByList_
									&& foundExpr->windowOpt_->orderByList_->size() > 0) {
								checkNestedAggrWindowExpr(
									*foundExpr->windowOpt_->orderByList_,
									resolved);
							}
						}
					}
				}
			}
		}
		occurrenceCount = totalWindowCount;
	}
	return occurrenceCount;
}

bool SQLCompiler::findWindowExpr(const ExprList &exprList, bool resolved, const Expr* &foundExpr) {
	for (ExprList::const_iterator it = exprList.begin();
			it != exprList.end(); ++it) {
		if ((*it != NULL) && findWindowExpr(**it, resolved, foundExpr)) {
			return true;
		}
	}
	foundExpr = NULL;
	return false;
}

bool SQLCompiler::findWindowExpr(const Expr &expr, bool resolved, const Expr* &foundExpr) {
	uint32_t funcCategory = calcFunctionCategory(expr, resolved);
	if (funcCategory == FUNCTION_WINDOW
			|| funcCategory == FUNCTION_PSEUDO_WINDOW) {
		foundExpr = &expr;
		return true;
	}
	if (expr.left_) {
		if (findWindowExpr(*expr.left_, resolved, foundExpr)) {
			return true;
		}
	}
	if (expr.right_) {
		if (findWindowExpr(*expr.right_, resolved, foundExpr)) {
			return true;
		}
	}
	if (expr.next_ && expr.next_->size() > 0) {
		ExprList::const_iterator listItr = expr.next_->begin();
		for (; listItr != expr.next_->end(); ++listItr) {
			if ((*listItr != NULL) && findWindowExpr(**listItr, resolved, foundExpr)) {
				return true;
			}
		}
	}
	foundExpr = NULL;
	return false;
}

bool SQLCompiler::isRankingWindowExpr(const Expr &expr) {
	bool ranking = false;

	Type opType = expr.op_;
	if (opType == SQLType::EXPR_FUNCTION) {
		opType = resolveFunction(expr);
	}

	bool windowOnly;
	bool pseudoWindow;
	if (ProcessorUtils::isWindowExprType(opType, windowOnly, pseudoWindow)) {
		ranking = !(windowOnly || pseudoWindow);
	}

	return ranking;
}

uint32_t SQLCompiler::calcFunctionCategory(const Expr &expr, bool resolved) {
	SQLType::Id opType = expr.op_;
	if (!resolved && opType == SQLType::EXPR_FUNCTION) {
		opType = resolveFunction(expr);
	}
	if (SQLType::START_AGG < opType && opType < SQLType::END_AGG) {
		bool windowOnly;
		bool pseudoWindow;
		bool isWindow = ProcessorUtils::isWindowExprType(opType, windowOnly, pseudoWindow);
		if (isWindow) {
			if (windowOnly) {
				if (!expr.windowOpt_) {
					SQL_COMPILER_THROW_ERROR(
							GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, &expr,
							"The function must have an OVER clause");
				}
				return FUNCTION_WINDOW;
			}
			if (expr.windowOpt_) {
				return FUNCTION_WINDOW;
			}
			else {
				return FUNCTION_AGGR;
			}
		}
		else if (pseudoWindow) {
			if (expr.windowOpt_) {
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, &expr,
						"The OVER clause cannot be used with the function");
			}
			return FUNCTION_PSEUDO_WINDOW;
		}
		else {
			if (expr.windowOpt_) {
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, &expr,
						"The OVER clause cannot be used with the function");
			}
		}
		return FUNCTION_AGGR;
	}
	return FUNCTION_SCALAR;
}

bool SQLCompiler::isRangeGroupNode(const PlanNode &node) {
	if ((node.cmdOptionFlag_ &
			PlanNode::Config::CMD_OPT_GROUP_RANGE) != 0) {
		assert(node.type_ == SQLType::EXEC_GROUP);
		return true;
	}
	return false;
}

bool SQLCompiler::isWindowNode(const PlanNode &node) {
	if ((node.cmdOptionFlag_ &
			PlanNode::Config::CMD_OPT_WINDOW_SORTED) != 0) {
		assert(node.type_ == SQLType::EXEC_SORT);
		return true;
	}
	return false;
}

void SQLCompiler::checkNestedAggrWindowExpr(
		const Expr &expr, bool resolved, size_t &genuineWindowCount,
		size_t &pseudoWindowCount, size_t &aggrCount) {

	uint32_t funcCategory = calcFunctionCategory(expr, resolved);
	switch(funcCategory) {
	case FUNCTION_WINDOW:
		++genuineWindowCount;
		break;
	case FUNCTION_PSEUDO_WINDOW:
		++pseudoWindowCount;
		break;
	case FUNCTION_AGGR:
		++aggrCount;
		break;
	default:
		break;
	}

	if ((genuineWindowCount + pseudoWindowCount) > 1) {
		SQL_COMPILER_THROW_ERROR(
			GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, &expr,
			"Too many window functions are found");
	}
	if (aggrCount > 0 && (genuineWindowCount + pseudoWindowCount) > 0) {
		SQL_COMPILER_THROW_ERROR(
			GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, &expr,
			"Illegal window or aggregation function occurrence");
	}
	if (expr.left_) {
		checkNestedAggrWindowExpr(*expr.left_, resolved, genuineWindowCount, pseudoWindowCount, aggrCount);
	}
	if (expr.right_) {
		checkNestedAggrWindowExpr(*expr.right_, resolved, genuineWindowCount, pseudoWindowCount, aggrCount);
	}
	if (expr.next_ && expr.next_->size() > 0) {
		ExprList::const_iterator listItr = expr.next_->begin();
		for (; listItr != expr.next_->end(); ++listItr) {
			if (*listItr != NULL) {
				checkNestedAggrWindowExpr(**listItr, resolved, genuineWindowCount, pseudoWindowCount, aggrCount);
			}
		}
	}
}

void SQLCompiler::checkNestedAggrWindowExpr(const ExprList &exprList, bool resolved) {
	ExprList::const_iterator it = exprList.begin();
	for (; it != exprList.end(); ++it) {
		if (*it != NULL) {
			size_t genuineWindowCount = 0;
			size_t pseudoWindowCount = 0;
			size_t aggrCount = 0;
			checkNestedAggrWindowExpr(**it, resolved, genuineWindowCount, pseudoWindowCount, aggrCount);
			if ((genuineWindowCount + pseudoWindowCount) > 0) {
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, *it,
						"Illegal window or aggregation function occurrence");
			}
			if (aggrCount > 0) {
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, *it,
						"Illegal aggregation function occurrence");
			}
		}
	}
}



SQLCompiler::Expr SQLCompiler::genExpr(
		const Expr &inExpr, Plan &plan, Mode mode) {
	SubqueryStack::Scope scope(*this);

	const bool aggregated =
			(mode == MODE_AGGR_SELECT || mode == MODE_GROUP_SELECT);

	Expr outExpr = genScalarExpr(inExpr, plan, mode, aggregated, NULL);

	if (scope.hasSubquery()) {
		scope.applyBefore(plan, outExpr);
		for (const Expr *subExpr; (subExpr = scope.takeExpr()) != NULL;) {
			expandSubquery(*subExpr, plan, mode);
		}
		scope.applyAfter(plan, outExpr, mode, aggregated);
	}

	return outExpr;
}

SQLCompiler::PlanExprList SQLCompiler::genExprList(
		const ExprList &inExprList, Plan &plan, Mode mode,
		const PlanNodeId *productedNodeId, bool wildcardEnabled) {
	SubqueryStack::Scope scope(*this);

	const bool aggregated = (mode == MODE_AGGR_SELECT);

	PlanExprList outExprList(alloc_);
	uint32_t maxAggrLevel = 0;
	uint32_t minAggrLevel = std::numeric_limits<uint32_t>::max();
	for (ExprList::const_iterator it = inExprList.begin();
			it != inExprList.end(); ++it) {
		const Expr &inExpr = **it;
		if (wildcardEnabled && inExpr.op_ == SQLType::EXPR_ALL_COLUMN) {
			if (inExpr.autoGenerated_) {
				outExprList.push_back(genColumnExpr(
						plan, inExpr.inputId_, inExpr.columnId_));
				outExprList.back().autoGenerated_ = false;
			}
			else {
				const PlanExprList &allColumnExpr =
						genAllColumnExpr(plan, mode, inExpr.qName_);
				outExprList.insert(
						outExprList.end(),
						allColumnExpr.begin(), allColumnExpr.end());
			}
		}
		else {
			outExprList.push_back(genScalarExpr(
					inExpr, plan, mode, aggregated, productedNodeId));
		}

		assert(!outExprList.empty());
		const Expr &outExpr = outExprList.back();
		maxAggrLevel = static_cast<uint32_t>(
				std::max(maxAggrLevel, outExpr.aggrNestLevel_));
		minAggrLevel = static_cast<uint32_t>(
				std::min(minAggrLevel, outExpr.aggrNestLevel_));
	}

	if (!pendingColumnList_.empty()) {
		applyPendingColumnList(plan);
	}

	if (scope.hasSubquery()) {
		for (PlanExprList::iterator it = outExprList.begin();
				it != outExprList.end(); ++it) {
			scope.applyBefore(plan, *it);
		}

		for (const Expr *subExpr; (subExpr = scope.takeExpr()) != NULL;) {
			expandSubquery(*subExpr, plan, mode);
		}

		for (PlanExprList::iterator it = outExprList.begin();
				it != outExprList.end(); ++it) {
			scope.applyAfter(plan, *it, mode, aggregated);
		}
	}

	return outExprList;
}

SQLCompiler::PlanExprList SQLCompiler::genColumnExprList(
		const ExprList &inExprList, Plan &plan, Mode mode,
		const PlanNodeId *productedNodeId) {
	return genExprList(inExprList, plan, mode, productedNodeId, true);
}

SQLCompiler::PlanNodeRef SQLCompiler::toRef(Plan &plan, const PlanNode *node) {
	return PlanNodeRef(plan, node);
}

SQLCompiler::ExprRef SQLCompiler::toRef(
		Plan &plan, const Expr &expr, const PlanNode *node) {
	return ExprRef(*this, plan, expr, node);
}

void SQLCompiler::expandSubquery(const Expr &inExpr, Plan &plan, Mode mode) {
	const Type orgExprType = inExpr.op_;
	const Type exprType =
			(orgExprType == SQLType::OP_NOT ? inExpr.left_->op_ : orgExprType);

	if (exprType == SQLType::EXPR_IN) {
		genSubquery(inExpr, plan, mode, exprType);
	}
	else if (exprType == SQLType::EXPR_EXISTS) {
		genSubquery(inExpr, plan, mode, exprType);
	}
	else if (orgExprType == SQLType::EXPR_SELECTION) {
		genSubquery(inExpr, plan, mode, orgExprType);
	}
	else {
		assert(false);
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}
}

SQLCompiler::Expr SQLCompiler::genScalarExpr(
		const Expr &inExpr, const Plan &plan, Mode mode, bool aggregated,
		const PlanNodeId *productedNodeId) {
	ExprHint exprHint;
	return genScalarExpr(
			inExpr, plan, mode, aggregated, productedNodeId, exprHint,
			SQLType::START_EXPR);
}

SQLCompiler::Expr SQLCompiler::genScalarExpr(
		const Expr &inExpr, const Plan &plan, Mode mode, bool aggregated,
		const PlanNodeId *productedNodeId, ExprHint &exprHint,
		Type parentExprType) {

	{
		const Expr *subExpr =
				(inExpr.op_ == SQLType::OP_NOT ? inExpr.left_ : &inExpr);
		assert(subExpr != NULL);

		if (subExpr->op_ == SQLType::EXPR_IN) {
			subExpr = subExpr->right_;
			assert(subExpr != NULL);
		}
		else if (subExpr->op_ == SQLType::EXPR_SELECTION &&
				inExpr.op_ == SQLType::OP_NOT) {
			subExpr = &inExpr;
		}

		if (subExpr->subQuery_ != NULL) {
			return genSubqueryRef(inExpr, mode);
		}
	}

	Type exprType = inExpr.op_;

	const Expr *exprLeft = inExpr.left_;
	const Expr *exprRight = inExpr.right_;
	const ExprList *exprNext = inExpr.next_;

	if (inExpr.op_ == SQLType::EXPR_FUNCTION) {
		exprType = resolveFunction(inExpr);

		if (exprType == SQLType::AGG_COUNT_ALL) {
			exprNext = NULL;
		}
	}
	else if (inExpr.op_ == SQLType::EXPR_IN) {
		assert(exprRight != NULL);
		assert(exprRight->op_ == SQLType::EXPR_LIST);
		exprNext = exprRight->next_;
		exprRight = NULL;
	}

	Expr outExpr(alloc_);
	ColumnType columnType = TupleList::TYPE_NULL;

	bool subAggregated = aggregated;
	if (SQLType::START_AGG < exprType && exprType <= SQLType::END_AGG) {
		subAggregated = false;
	}

	if (exprLeft != NULL) {
		outExpr.left_ = ALLOC_NEW(alloc_) Expr(genScalarExpr(
				*exprLeft, plan, mode, subAggregated, productedNodeId,
				exprHint, exprType));
	}

	ExprHint rightExprHint;
	if (exprRight != NULL) {
		outExpr.right_ = ALLOC_NEW(alloc_) Expr(genScalarExpr(
				*exprRight, plan, mode, subAggregated, productedNodeId,
				rightExprHint, exprType));
		exprHint = rightExprHint;
	}

	if (exprNext != NULL) {
		ExprList *outNext = ALLOC_NEW(alloc_) ExprList(alloc_);

		ExprList::const_iterator it = exprNext->begin();
		for (; it != exprNext->end(); ++it) {
			outNext->push_back(ALLOC_NEW(alloc_) Expr(genScalarExpr(
					**it, plan, mode, subAggregated, productedNodeId,
					exprHint, exprType)));
		}

		outExpr.next_ = outNext;
	}

	if (inExpr.op_ == SQLType::EXPR_FUNCTION) {
		arrangeFunctionArgs(exprType, outExpr);
	}

	if (SQLType::START_FUNC < exprType && exprType < SQLType::END_FUNC) {
		outExpr.op_ = exprType;

		if (exprType == SQLType::FUNC_NOW) {
			if (queryStartTime_.getUnixTime() !=
					util::DateTime::INITIAL_UNIX_TIME) {
				const int64_t unixTime = queryStartTime_.getUnixTime();
				outExpr.value_ =
						TupleValue(&unixTime, TupleList::TYPE_TIMESTAMP);
				outExpr.op_ = SQLType::EXPR_CONSTANT;
			}
			else {
				currentTimeRequired_ = true;
			}
		}
	}
	else if (SQLType::START_AGG < exprType && exprType < SQLType::END_AGG) {
		outExpr.op_ = exprType;

		if ((exprType == SQLType::AGG_GROUP_CONCAT ||
				exprType == SQLType::AGG_DISTINCT_GROUP_CONCAT) &&
				exprNext != NULL && exprNext->size() == 1) {
			const char8_t *concatSeparator = ",";
			outExpr.next_->push_back(ALLOC_NEW(alloc_) Expr(genConstExpr(
					plan, SyntaxTree::makeStringValue(
							alloc_, concatSeparator, strlen(concatSeparator)),
					mode)));
		}

		validateAggrExpr(outExpr, mode);
	}
	else if (exprType == SQLType::EXPR_COLUMN) {
		if (inExpr.autoGenerated_) {
			outExpr = genColumnExprBase(
					&plan, &plan.back(), inExpr.inputId_, inExpr.columnId_,
					NULL);
		}
		else {
			if (inExpr.qName_ == NULL) {
				assert(false);
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
			}
			resolveColumn(
					plan, plan.back(), *inExpr.qName_, outExpr, mode,
					productedNodeId, NULL, NULL);
			outExpr.sortAscending_ = inExpr.sortAscending_;
		}
		setUpColumnExprAttributes(outExpr, aggregated);
	}
	else if (exprType == SQLType::EXPR_ALL_COLUMN) {
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, &inExpr,
				"All column can not be occurred at this position");
	}
	else if ((SQLType::START_OP < exprType && exprType < SQLType::END_OP) ||
			(SQLType::START_EXPR < exprType && exprType < SQLType::END_EXPR)) {
		outExpr.op_ = exprType;

		if (exprType == SQLType::EXPR_TYPE) {
			if (!inExpr.autoGenerated_ &&
					ColumnTypeUtils::isDeclarable(inExpr.columnType_)) {
				columnType = setColumnTypeNullable(inExpr.columnType_, true);
			}
		}
		else if (exprType == SQLType::EXPR_CONSTANT) {
			outExpr.value_ = inExpr.value_;

			if (inExpr.columnId_ == REPLACED_AGGR_COLUMN_ID) {
				outExpr.columnId_ = inExpr.columnId_;
			}
		}
		else if (exprType == SQLType::EXPR_ID) {
			outExpr.inputId_ = inExpr.inputId_;
		}
		else if (exprType == SQLType::EXPR_PLACEHOLDER) {
			if (parameterTypeList_ == NULL) {
				SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
						&inExpr, "Placeholder can be used for prepared statements");
			}

			assert(inExpr.placeHolderCount_ > 0);
			const ColumnId pos =
					static_cast<ColumnId>(inExpr.placeHolderCount_ - 1);
			if (pos < plan.parameterList_.size()) {
				outExpr.value_ = plan.parameterList_[pos];
				outExpr.op_ = SQLType::EXPR_CONSTANT;
			}
			else {
				outExpr.columnId_ = pos;
			}
		}
		else if (exprType == SQLType::EXPR_AGG_ORDERING) {
			assert(outExpr.next_ != NULL);
			if (outExpr.next_->size() != 1) {
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, &inExpr,
						"Only one sort key can be used for WITHIN GROUP clause");
			}
			return *outExpr.next_->front();
		}
		else if (outExpr.next_ != NULL &&
				(exprType == SQLType::EXPR_AND ||
				exprType == SQLType::EXPR_OR)) {
			Expr *treeExpr;
			exprListToTree(&plan, exprType, *outExpr.next_, mode, treeExpr);
			assert(treeExpr != NULL);

			outExpr = *treeExpr;
			outExpr.autoGenerated_ = false;
		}
		else if (exprType == SQLType::EXPR_OR) {
			if (inExpr.right_ != NULL &&
					inExpr.right_->op_ == SQLType::EXPR_CONSTANT &&
					inExpr.right_->value_.getType() == TupleList::TYPE_BOOL &&
					!SQLProcessor::ValueUtils::isTrueValue(inExpr.right_->value_)) {
				exprHint.setNoIndex(true);
			}
			else {
				exprHint.setNoIndex(rightExprHint.isNoIndex());
			}
		}
		else {
			if (exprType == SQLType::OP_PLUS) {
				Expr *left = outExpr.left_;
				assert(left != NULL);
				outExpr = *left;
			}
			else if (exprType == SQLType::OP_MINUS) {
				minusOpToBinary(plan, outExpr, mode);
			}
			else if (exprType == SQLType::EXPR_IN) {
				inListToOrTree(plan, outExpr, mode);
			}
			else if (exprType == SQLType::EXPR_BETWEEN) {
				betweenToAndTree(plan, outExpr, mode);
			}
		}
	}
	else {
		assert(false);
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	setUpExprAttributes(
			outExpr, &inExpr, columnType, mode, SQLType::AGG_PHASE_ALL_PIPE);

	if (exprHint.isNoIndex() && parentExprType != SQLType::EXPR_OR) {
		exprHint.setNoIndex(false);

		Expr *subExpr = ALLOC_NEW(alloc_) Expr(outExpr);

		outExpr = Expr(alloc_);
		outExpr.op_ = SQLType::EXPR_HINT_NO_INDEX;
		outExpr.left_ = subExpr;

		setUpExprAttributes(
				outExpr, &inExpr, TupleList::TYPE_NULL, mode,
				SQLType::AGG_PHASE_ALL_PIPE);
	}

	return outExpr;
}

void SQLCompiler::setUpExprAttributes(
		Expr &outExpr, const Expr *inExpr, ColumnType columnType,
		Mode mode, AggregationPhase phase) {
	const size_t argCount = getArgCount(outExpr);
	if (!ProcessorUtils::checkArgCount(outExpr.op_, argCount, phase)) {
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_INVALID_ARG_COUNT, inExpr,
				"Illegal argument count (count=" << argCount << ")");
	}

	if (inExpr != NULL) {
		outExpr.autoGenerated_ = inExpr->autoGenerated_;

		if (inExpr->aliasName_ != NULL) {
			outExpr.qName_ = ALLOC_NEW(alloc_) QualifiedName(alloc_);
			outExpr.qName_->name_ = inExpr->aliasName_;
		}

		if (mode == MODE_ORDERBY) {
			outExpr.sortAscending_ = inExpr->sortAscending_;
		}
	}

	if (outExpr.srcId_ < 0 && outExpr.op_ != SQLType::EXPR_COLUMN) {
		outExpr.columnType_ =
				getResultType(outExpr, inExpr, columnType, mode, 0, phase);
		outExpr.constantEvaluable_ = isConstantEvaluable(outExpr);
		outExpr.aggrNestLevel_ = getAggregationLevel(outExpr);

		if (!outExpr.autoGenerated_) {
			outExpr.srcId_ = genSourceId();
		}
	}

	{
		const bool strict = true;
		reduceExpr(outExpr, strict);
	}

	if (outExpr.op_ == SQLType::EXPR_PLACEHOLDER) {
		while (outExpr.columnId_ >= parameterTypeList_->size()) {
			const ColumnType nullType = TupleList::TYPE_NULL;
			parameterTypeList_->push_back(nullType);
		}

		(*parameterTypeList_)[outExpr.columnId_] = outExpr.columnType_;
	}
}

void SQLCompiler::setUpColumnExprAttributes(Expr &outExpr, bool aggregated) {
	if (aggregated) {
		outExpr.columnType_ = setColumnTypeNullable(
				outExpr.columnType_, aggregated);
	}
}

SQLCompiler::PlanExprList SQLCompiler::genAllColumnExpr(
		const Plan &plan, Mode mode, const QualifiedName *name) {
	assert(name == NULL || name->name_ == NULL);

	const bool forAllTable = (name == NULL || name->table_ == NULL);

	PlanExprList list(alloc_);

	const PlanNode &node = plan.back();

	uint32_t inputId;
	ColumnId startColumn;
	ColumnId endColumn;
	if (node.inputList_.empty()) {
		inputId = 0;
		startColumn = 0;
		endColumn = 0;
	}
	else if (forAllTable) {
		inputId = 0;
		startColumn = 0;

		if (node.tableIdInfo_.isEmpty()) {
			list = inputListToOutput(plan, false);
			endColumn = 0;
		}
		else {
			endColumn = static_cast<ColumnId>(getTableInfoList().resolve(
					node.tableIdInfo_.id_).columnInfoList_.size());
		}
	}
	else {
		Expr expr(alloc_);
		resolveColumn(plan, plan.back(), *name, expr, mode, NULL, NULL, NULL);
		assert(expr.op_ == SQLType::EXPR_ALL_COLUMN);

		inputId = expr.inputId_;
		startColumn = expr.columnId_;
		endColumn = expr.left_->columnId_;
	}

	for (ColumnId id = startColumn; id < endColumn; id++) {
		const Expr &expr = genColumnExpr(plan, inputId, id);
		if (!expr.autoGenerated_) {
			list.push_back(expr);
		}
	}

	uint32_t maxSubqueryLevel = 0;
	if (forAllTable) {
		maxSubqueryLevel = subqueryLevel_;
	}
	for (PlanExprList::iterator it = list.begin(); it != list.end();) {
		const uint32_t level = it->subqueryLevel_;
		if (level < maxSubqueryLevel) {
			it = list.erase(it);
		}
		else {
			if (level > maxSubqueryLevel) {
				maxSubqueryLevel = level;
				it = list.erase(list.begin(), it);
			}
			++it;
		}
	}

	if (mode == MODE_AGGR_SELECT) {
		for (PlanExprList::iterator it = list.begin(); it != list.end(); ++it) {
			Expr &expr = *it;
			expr.columnType_ = setColumnTypeNullable(
					expr.columnType_, true);
		}
	}

	if (list.empty()) {
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
				"Wildcard specified for no tables");
	}

	return list;
}

SQLCompiler::Expr SQLCompiler::genColumnExpr(
		const Plan &plan, uint32_t inputId, ColumnId columnId) {
	return genColumnExprBase(&plan, &plan.back(), inputId, columnId, NULL);
}

SQLCompiler::Expr SQLCompiler::genColumnExprBase(
		const Plan *plan, const PlanNode *node, uint32_t inputId,
		ColumnId columnId, const Expr *baseExpr) {
	assert(baseExpr != NULL || (plan != NULL && node != NULL));
	Expr outExpr(alloc_);

	if (baseExpr != NULL || node->tableIdInfo_.isEmpty()) {
		const PlanNode *inNode =
				(node == NULL ? NULL : &node->getInput(*plan, inputId));
		const Expr &inExpr = (baseExpr == NULL ?
				inNode->outputList_[columnId] : *baseExpr);

		outExpr.srcId_ = inExpr.srcId_;
		outExpr.columnType_ = inExpr.columnType_;
		if (inNode != NULL &&
				inNode->qName_ != NULL &&
				inNode->qName_->table_ != NULL) {
			outExpr.qName_ = ALLOC_NEW(alloc_) QualifiedName(alloc_);
			outExpr.qName_->table_ = inNode->qName_->table_;

			if (inExpr.qName_ != NULL) {
				outExpr.qName_->name_ = inExpr.qName_->name_;
			}
		}
		else {
			outExpr.qName_ = inExpr.qName_;
		}

		outExpr.subqueryLevel_ = inExpr.subqueryLevel_;
		outExpr.autoGenerated_ = inExpr.autoGenerated_;
	}
	else {
		assert(node != NULL);
		const TableInfo &orgInfo =
				getTableInfoList().resolve(node->tableIdInfo_.id_);
		const TableInfo::SQLColumnInfo &columnInfo =
				orgInfo.columnInfoList_[columnId];

		outExpr.srcId_ = genSourceId();
		outExpr.columnType_ = ProcessorUtils::filterColumnType(
				ProcessorUtils::getTableColumnType(orgInfo, columnId, NULL));

		outExpr.qName_ = ALLOC_NEW(alloc_) QualifiedName(alloc_);

		if (node->qName_ != NULL && node->qName_->table_ != NULL) {
			outExpr.qName_->table_ = node->qName_->table_;
		}
		else {
			outExpr.qName_->db_ =
					ALLOC_NEW(alloc_) util::String(orgInfo.dbName_);
			outExpr.qName_->table_ =
					ALLOC_NEW(alloc_) util::String(orgInfo.tableName_);
		}

		outExpr.qName_->name_ =
				ALLOC_NEW(alloc_) util::String(columnInfo.second);
	}

	if (node != NULL && isInputNullable(*node, inputId)) {
		outExpr.columnType_ =
				setColumnTypeNullable(outExpr.columnType_, true);
	}

	outExpr.op_ = SQLType::EXPR_COLUMN;
	outExpr.inputId_ = inputId;
	outExpr.columnId_ = columnId;

	return outExpr;
}

SQLCompiler::Expr SQLCompiler::genSubqueryRef(
		const Expr &inExpr, Mode mode) {
	Expr outExpr(alloc_);

	outExpr.srcId_ = inExpr.srcId_;

	outExpr.columnType_ = TupleList::TYPE_NULL;

	if (inExpr.aliasName_ != NULL) {
		outExpr.qName_ = ALLOC_NEW(alloc_) QualifiedName(alloc_);
		outExpr.qName_->name_ =
				ALLOC_NEW(alloc_) util::String(*inExpr.aliasName_);
	}

	outExpr.autoGenerated_ = inExpr.autoGenerated_;

	outExpr.op_ = SQLType::EXPR_COLUMN;
	outExpr.inputId_ = SyntaxTree::UNDEF_INPUTID;
	outExpr.columnId_ = getSubqueryStack().addExpr(inExpr, mode);

	return outExpr;
}

SQLCompiler::Expr SQLCompiler::genConstExpr(
		const Plan &plan, const TupleValue &value, Mode mode,
		const char8_t *name) {
	return genConstExprBase(&plan, value, mode, name);
}

SQLCompiler::Expr SQLCompiler::genConstExprBase(
		const Plan *plan, const TupleValue &value, Mode mode,
		const char8_t *name) {
	Expr expr(alloc_);
	expr.op_ = SQLType::EXPR_CONSTANT;
	expr.value_ = value;
	expr.autoGenerated_ = true;
	if (name != NULL) {
		expr.aliasName_ = ALLOC_NEW(alloc_) util::String(name, alloc_);
	}

	if (plan == NULL) {
		setUpExprAttributes(
				expr, NULL, TupleList::TYPE_NULL, mode,
				SQLType::AGG_PHASE_ALL_PIPE);
		return expr;
	}
	else {
		return genScalarExpr(expr, *plan, mode, false, NULL);
	}
}

SQLCompiler::Expr SQLCompiler::genTupleIdExpr(
		const Plan &plan, uint32_t inputId, Mode mode) {
	Expr expr(alloc_);
	expr.op_ = SQLType::EXPR_ID;
	expr.inputId_ = inputId;
	expr.autoGenerated_ = true;

	return genScalarExpr(expr, plan, mode, false, NULL);
}

SQLCompiler::Expr SQLCompiler::genCastExpr(
		const Plan &plan, ColumnType columnType, Expr *inExpr,
		Mode mode, bool shallow, bool withAttributes) {
	assert(inExpr != NULL);

	Expr typeExpr(alloc_);
	typeExpr.op_ = SQLType::EXPR_TYPE;
	typeExpr.columnType_ = columnType;
	typeExpr.autoGenerated_ = true;
	typeExpr = genScalarExpr(typeExpr, plan, mode, false, NULL);

	Expr outExpr = genOpExpr(
			plan, SQLType::OP_CAST, inExpr,
			ALLOC_NEW(alloc_) Expr(typeExpr), mode, shallow);

	if (withAttributes) {
		outExpr.autoGenerated_ = inExpr->autoGenerated_;
		outExpr.qName_ = inExpr->qName_;
		outExpr.srcId_ = inExpr->srcId_;
	}

	return outExpr;
}

SQLCompiler::Expr SQLCompiler::genOpExpr(
		const Plan &plan, Type exprType, Expr *left, Expr *right, Mode mode,
		bool shallow, AggregationPhase phase) {
	return genOpExprBase(
			&plan, exprType, left, right, mode, shallow, phase);
}

SQLCompiler::Expr SQLCompiler::genOpExprBase(
		const Plan *plan, Type exprType, Expr *left, Expr *right, Mode mode,
		bool shallow, AggregationPhase phase) {
	Expr expr(alloc_);
	expr.op_ = exprType;
	expr.left_ = left;
	expr.right_ = right;
	expr.autoGenerated_ = true;

	if (shallow) {
		setUpExprAttributes(expr, NULL, TupleList::TYPE_NULL, mode, phase);
		return expr;
	}
	else {
		assert(plan != NULL);
		return genScalarExpr(expr, *plan, mode, false, NULL);
	}
}

SQLCompiler::Expr SQLCompiler::genExprWithArgs(
		const Plan &plan, Type exprType, const ExprList *argList,
		Mode mode, bool shallow, AggregationPhase phase) {
	return genExprWithArgs(&plan, exprType, argList, mode, shallow, phase);
}

SQLCompiler::Expr SQLCompiler::genExprWithArgs(
		const Plan *plan, Type exprType, const ExprList *argList,
		Mode mode, bool shallow, AggregationPhase phase) {
	Expr expr(alloc_);
	expr.op_ = exprType;
	expr.autoGenerated_ = true;

	if (argList != NULL) {
		expr.next_ = ALLOC_NEW(alloc_) ExprList(
				argList->begin(), argList->end(), alloc_);
	}

	if (shallow) {
		setUpExprAttributes(expr, NULL, TupleList::TYPE_NULL, mode, phase);
		return expr;
	}
	else {
		assert(plan != NULL);
		return genScalarExpr(expr, *plan, mode, false, NULL);
	}
}

int64_t SQLCompiler::genLimitOffsetValue(
		const Expr *inExpr, Plan &plan, Mode mode) {
	assert(mode == MODE_LIMIT || mode == MODE_OFFSET);

	if (inExpr == NULL) {
		return (mode == MODE_LIMIT ? -1 : 0);
	}

	const Expr targetExpr = genScalarExpr(*inExpr, plan, mode, false, NULL);
	if (targetExpr.op_ != SQLType::EXPR_CONSTANT) {
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, inExpr,
				"Limit or offset expression must be constant");
	}

	const TupleValue &targetValue = targetExpr.value_;
	if (!ColumnTypeUtils::isIntegral(targetValue.getType())) {
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, inExpr,
				"Limit or offset expression must be evaluated as integral value");
	}

	const bool implicit = true;
	const TupleValue &value = ProcessorUtils::convertType(
			getVarContext(), targetValue, TupleList::TYPE_LONG, implicit);

	assert(value.getType() == TupleList::TYPE_LONG);

	return value.get<int64_t>();
}

void SQLCompiler::reduceExpr(Expr &expr, bool strict) {
	if (expr.op_ == SQLType::EXPR_CONSTANT ||
			expr.op_ == SQLType::EXPR_TYPE ||
			!expr.constantEvaluable_ || neverReduceExpr_) {
		return;
	}

	const ColumnType columnType = expr.columnType_;
	if (!strict) {
		expr.columnType_ = TupleList::TYPE_NULL;
	}

	TupleValue value;
	try {
		value = ProcessorUtils::evalConstExpr(
				getVarContext(), expr, getCompileOption());
	}
	catch (std::exception &e) {
		SQL_COMPILER_RETHROW_ERROR(
				e, &expr, "Failed to evaluate constant expression (reason=" <<
				GS_EXCEPTION_MESSAGE(e) << ")");
	}

	ColumnType outType;
	do {
		ColumnType baseType = value.getType();
		if (baseType == TupleList::TYPE_NULL) {
			baseType = TupleList::TYPE_ANY;
		}

		if (columnType == TupleList::TYPE_NULL) {
			outType = baseType;
			break;
		}

		const bool implicit = true;
		const bool evaluating = true;
		const ColumnType convType = ProcessorUtils::findConversionType(
				baseType, columnType, implicit, evaluating);
		if (convType == TupleList::TYPE_NULL) {
			SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
		}
		outType = columnType;

		if (outType != TupleList::TYPE_ANY) {
			value = ProcessorUtils::convertType(
					getVarContext(), value, outType, implicit);
		}
	}
	while (false);

	Expr outExpr(alloc_);
	outExpr.op_ = SQLType::EXPR_CONSTANT;
	outExpr.value_ = value;
	outExpr.columnType_ = outType;

	outExpr.srcId_ = expr.srcId_;
	outExpr.qName_ = expr.qName_;
	outExpr.aggrNestLevel_ = expr.aggrNestLevel_;
	outExpr.constantEvaluable_ = expr.constantEvaluable_;
	outExpr.autoGenerated_ = expr.autoGenerated_;

	expr = outExpr;
}

void SQLCompiler::validateAggrExpr(const Expr &inExpr, Mode mode) {
	switch (mode) {
	case MODE_AGGR_SELECT:
	case MODE_GROUP_SELECT:
	case MODE_HAVING:
		break;
	default:
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, &inExpr,
				"Illegal aggregation function occurrence");
	}

	if (inExpr.next_ == NULL) {
		return;
	}

	for (ExprList::const_iterator it = inExpr.next_->begin();
			it != inExpr.next_->end(); ++it) {
		if ((*it)->aggrNestLevel_ != 0) {
			SQL_COMPILER_THROW_ERROR(
					GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, &inExpr,
					"Aggregation function can not be nested");
		}
	}
}

void SQLCompiler::minusOpToBinary(const Plan &plan, Expr &expr, Mode mode) {
	assert(expr.left_ != NULL && expr.right_ == NULL && expr.next_ == NULL);

	const TupleValue zero(int64_t(0));
	expr.right_ = ALLOC_NEW(alloc_) Expr(genConstExpr(plan, zero, mode));

	std::swap(expr.left_, expr.right_);
	expr.op_ = SQLType::OP_SUBTRACT;
}

void SQLCompiler::inListToOrTree(const Plan &plan, Expr &expr, Mode mode) {
	assert(expr.left_ != NULL && expr.right_ == NULL);

	if (expr.next_ == NULL || expr.next_->empty()) {
		expr = genConstExpr(plan, makeBoolValue(false), mode);
		expr.autoGenerated_ = false;
		return;
	}

	ExprList eqOrList(alloc_);
	eqOrList.reserve(expr.next_->size());

	for (ExprList::iterator it = expr.next_->begin();
			it != expr.next_->end(); ++it) {

		Expr *leftDup = ALLOC_NEW(alloc_) Expr(alloc_);
		copyExpr(*expr.left_, *leftDup);

		eqOrList.push_back(ALLOC_NEW(alloc_) Expr(genOpExpr(
				plan, SQLType::OP_EQ, leftDup, *it, mode, true)));
	}

	Expr *treeExpr;
	exprListToTree(&plan, SQLType::EXPR_OR, eqOrList, mode, treeExpr);
	assert(treeExpr != NULL);

	expr = *treeExpr;
	expr.autoGenerated_ = false;
}

void SQLCompiler::exprListToTree(
		const Plan *plan, Type type, ExprList &list, Mode mode,
		Expr *&outExpr) {
	outExpr = NULL;

	if (list.size() > 1) {
		ExprList destList(alloc_);
		destList.reserve(list.size() / 2 + 1);

		while (list.size() > 1) {
			for (ExprList::iterator it = list.begin(); it != list.end(); ++it) {
				Expr *leftExpr = *it;
				if (++it == list.end()) {
					destList.push_back(leftExpr);
					break;
				}
				else {
					destList.push_back(ALLOC_NEW(alloc_) Expr(genOpExprBase(
							plan, type, leftExpr, *it, mode, true)));
				}
			}

			list.clear();
			list.swap(destList);
		}
	}

	if (!list.empty()) {
		outExpr = list.front();
	}
}

void SQLCompiler::betweenToAndTree(const Plan &plan, Expr &expr, Mode mode) {
	assert(expr.next_ != NULL && expr.left_ == NULL && expr.right_ == NULL);

	ExprList &next = *expr.next_;

	Expr *leftDup1 = ALLOC_NEW(alloc_) Expr(alloc_);
	copyExpr(*next[0], *leftDup1);
	Expr *leftDup2 = ALLOC_NEW(alloc_) Expr(alloc_);
	copyExpr(*next[0], *leftDup2);

	expr.op_ = SQLType::EXPR_AND;
	expr.left_ = ALLOC_NEW(alloc_) Expr(genOpExpr(
			plan, SQLType::OP_LE, next[1], leftDup1, mode, true));
	expr.right_ = ALLOC_NEW(alloc_) Expr(genOpExpr(
			plan, SQLType::OP_LE, leftDup2, next[2], mode, true));
	expr.next_ = NULL;
}

void SQLCompiler::splitAggrWindowExprList(
		GenRAContext &cxt, const ExprList &inExprList, const Plan &plan) {
	const size_t size = inExprList.size();

	cxt.outExprList_.reserve(size);
	cxt.aggrExprList_.reserve(size);
	cxt.windowExprList_.reserve(size);

	size_t windowExprCount = 0;
	cxt.aggrExprCount_ = 0;
	for (ExprList::const_iterator it = inExprList.begin();
			it != inExprList.end(); ++it) {
		Expr *inExpr = *it;
		assert(inExpr->srcId_ < 0);
		const Expr* foundExpr = NULL;
		if (findWindowExpr(*inExpr, false, foundExpr)) {
			cxt.outExprList_.push_back(ALLOC_NEW(alloc_) Expr(
					genReplacedAggrExpr(plan, inExpr)));

			cxt.windowExprList_.push_back(inExpr);
			++windowExprCount;

			if (windowExprCount > 1) {
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
						"Too many window functions are found.");
			}
			cxt.aggrExprList_.push_back(ALLOC_NEW(alloc_) Expr(
					genReplacedAggrExpr(plan, inExpr)));
		}
		else if (findAggrExpr(*inExpr, false)) {
			cxt.outExprList_.push_back(ALLOC_NEW(alloc_) Expr(
					genReplacedAggrExpr(plan, inExpr)));

			cxt.aggrExprList_.push_back(inExpr);
			++cxt.aggrExprCount_;
			cxt.windowExprList_.push_back(ALLOC_NEW(alloc_) Expr(
					genReplacedAggrExpr(plan, inExpr)));
		}
		else if (inExpr->op_ == SQLType::EXPR_ALL_COLUMN) {
			const PlanExprList &allExprList = genAllColumnExpr(
					plan, MODE_NORMAL_SELECT, inExpr->qName_);

			for (PlanExprList::const_iterator allIt = allExprList.begin();
					allIt != allExprList.end(); ++allIt) {

				Expr *outExpr = ALLOC_NEW(alloc_) Expr(alloc_);
				outExpr->op_ = SQLType::EXPR_ALL_COLUMN;
				outExpr->inputId_ = allIt->inputId_;
				outExpr->columnId_ = allIt->columnId_;
				outExpr->autoGenerated_ = true;
				cxt.outExprList_.push_back(outExpr);

				Expr *aggrExpr = ALLOC_NEW(alloc_) Expr(alloc_);
				aggrExpr->op_ = SQLType::EXPR_ALL_COLUMN;
				aggrExpr->inputId_ = 0;
				aggrExpr->columnId_ =
						static_cast<ColumnId>(it - inExprList.begin());
				aggrExpr->autoGenerated_ = true;
				cxt.aggrExprList_.push_back(aggrExpr);

				Expr *windowExpr = ALLOC_NEW(alloc_) Expr(*aggrExpr);
				cxt.windowExprList_.push_back(windowExpr);
			}
		}
		else {
			cxt.outExprList_.push_back(inExpr);

			Expr *columnExpr = ALLOC_NEW(alloc_) Expr(alloc_);
			columnExpr->op_ = SQLType::EXPR_COLUMN;
			columnExpr->inputId_ = 0;
			columnExpr->columnId_ =
					static_cast<ColumnId>(it - inExprList.begin());
			columnExpr->autoGenerated_ = true;
			cxt.aggrExprList_.push_back(columnExpr);
			Expr *windowExpr = ALLOC_NEW(alloc_) Expr(*columnExpr);
			cxt.windowExprList_.push_back(windowExpr);
		}
	}
	assert(cxt.outExprList_.size() == cxt.aggrExprList_.size());
}

SQLCompiler::Expr SQLCompiler::genReplacedAggrExpr(
		const Plan &plan, const Expr *inExpr) {
	Expr outExpr = genConstExpr(plan, TupleValue(), MODE_NORMAL_SELECT);

	if (inExpr != NULL) {
		outExpr.aliasName_ = inExpr->aliasName_;
	}

	outExpr.columnId_ = REPLACED_AGGR_COLUMN_ID;

	return outExpr;
}

bool SQLCompiler::splitSubqueryCond(
		const Expr &inExpr, Expr *&scalarExpr, Expr *&subExpr) {
	assert(inExpr.srcId_ < 0);

	scalarExpr = NULL;
	subExpr = NULL;

	if (inExpr.op_ == SQLType::EXPR_AND) {
		assert(inExpr.left_ != NULL);
		assert(inExpr.right_ != NULL);

		Expr *scalarList[2];
		Expr *subList[2];

		bool found = false;
		found |= splitSubqueryCond(*inExpr.left_, scalarList[0], subList[0]);
		found |= splitSubqueryCond(*inExpr.right_, scalarList[1], subList[1]);

		mergeAndOpBase(
				scalarList[0], scalarList[1], NULL, MODE_DEFAULT, scalarExpr);
		mergeAndOpBase(
				subList[0], subList[1], NULL, MODE_DEFAULT, subExpr);

		return found;
	}

	if (findSubqueryExpr(&inExpr)) {
		subExpr = ALLOC_NEW(alloc_) Expr(inExpr);
		return true;
	}

	scalarExpr = ALLOC_NEW(alloc_) Expr(inExpr);
	return false;
}

bool SQLCompiler::splitJoinPrimaryCond(
		const Plan &plan, const Expr &srcWhere, const Expr &srcOn,
		Expr *&primaryCond, Expr *&destWhere, Expr *&destOn,
		SQLType::JoinType joinType, const Expr *orgPrimaryCond,
		bool complex) {

	primaryCond = NULL;
	destWhere = ALLOC_NEW(alloc_) Expr(srcWhere);
	destOn = ALLOC_NEW(alloc_) Expr(srcOn);

	const bool whereRemovable = (joinType == SQLType::JOIN_INNER);

	uint32_t maxScore = 0;
	Mode splittedMode = MODE_DEFAULT;
	do {
		if (orgPrimaryCond == NULL || !isTrueExpr(*orgPrimaryCond)) {
			break;
		}

		const uint32_t onScore =
				getJoinPrimaryCondScore(*destOn, NULL, complex);
		const uint32_t whereScore =
				getJoinPrimaryCondScore(*destWhere, NULL, complex);

		maxScore = std::max(onScore, whereScore);
		if (maxScore == 0) {
			break;
		}

		if (onScore == maxScore) {
			splittedMode = MODE_ON;
		}
		else {
			splittedMode = MODE_WHERE;
		}
	}
	while (false);

	bool splitted;
	{
		switch (splittedMode) {
		case MODE_ON:
			splitted = splitJoinPrimaryCondSub(
					plan, *destOn, primaryCond,
					maxScore, complex, MODE_ON, true);
			break;
		case MODE_WHERE:
			splitted = splitJoinPrimaryCondSub(
					plan, *destWhere, primaryCond,
					maxScore, complex, MODE_WHERE, whereRemovable);
			break;
		default:
			primaryCond = ALLOC_NEW(alloc_) Expr(
					genConstExpr(plan, makeBoolValue(true), MODE_ON));
			splitted = false;
			break;
		}
	}

	if (joinType == SQLType::JOIN_INNER) {
		Expr *outExpr;
		mergeAndOp(destOn, destWhere, plan, MODE_WHERE, outExpr);
		if (outExpr != NULL) {
			destWhere = outExpr;
		}
		destOn = NULL;
	}

	return splitted;
}

bool SQLCompiler::splitJoinPrimaryCondSub(
		const Plan &plan, Expr &target, Expr *&primaryCond,
		uint32_t maxScore, bool complex, Mode mode, bool removable) {

	if (target.op_ == SQLType::EXPR_AND) {
		assert(target.left_ != NULL);
		assert(target.right_ != NULL);

		do {
			if (splitJoinPrimaryCondSub(
					plan, *target.left_, primaryCond,
					maxScore, complex, mode, removable)) {
				break;
			}

			if (splitJoinPrimaryCondSub(
					plan, *target.right_, primaryCond,
					maxScore, complex, mode, removable)) {
				break;
			}

			return false;
		}
		while (false);

		Expr *outExpr;
		mergeAndOp(target.left_, target.right_, plan, mode, outExpr);
		if (outExpr == NULL) {
			target = genConstExpr(
					plan, TupleValue(makeBoolValue(true)), mode);
		}
		else {
			target = *outExpr;
		}

		return true;
	}

	const uint32_t score = getJoinPrimaryCondScore(target, &maxScore, complex);
	if (score == 0) {
		return false;
	}

	primaryCond = ALLOC_NEW(alloc_) Expr(genOpExpr(
			plan, target.op_, target.left_, target.right_, mode, true));

	if (removable) {
		target = genConstExpr(
				plan, TupleValue(makeBoolValue(true)), mode);
	}

	return true;
}

uint32_t SQLCompiler::getJoinPrimaryCondScore(
		const Expr &expr, const uint32_t *maxScore, bool complex) {
	uint32_t score;

	switch (expr.op_) {
	case SQLType::EXPR_AND:
		return std::max(
				getJoinPrimaryCondScore(*expr.left_, maxScore, complex),
				getJoinPrimaryCondScore(*expr.right_, maxScore, complex));
	case SQLType::OP_EQ:
		score = 1;
		break;
	case SQLType::OP_LT:
	case SQLType::OP_GT:
	case SQLType::OP_LE:
	case SQLType::OP_GE:
		score = 1;
		break;
	default:
		return 0;
	}

	for (size_t i = 0; i < 2; i++) {
		const Expr &opExpr = *(i == 0 ? expr.left_ : expr.right_);
		score *= 2;
		if (opExpr.op_ == SQLType::EXPR_COLUMN) {
			score++;
		}
		else if (!complex) {
			return 0;
		}
	}

	if (maxScore != NULL && score < *maxScore) {
		return 0;
	}

	uint32_t leftId;
	uint32_t rightId;
	if (!findSingleInputExpr(*expr.left_, leftId) ||
			!findSingleInputExpr(*expr.right_, rightId) || leftId == rightId) {
		return 0;
	}

	return score;
}

bool SQLCompiler::getJoinCondColumns(
		const PlanNode &node,
		util::Vector< std::pair<ColumnId, ColumnId> > &columnIdPairList) {
	assert(node.type_ == SQLType::EXEC_JOIN);

	for (PlanExprList::const_iterator predIt = node.predList_.begin();
			predIt != node.predList_.end(); ++predIt) {
		getJoinCondColumns(*predIt, columnIdPairList);

		if (predIt - node.predList_.begin() >= 2) {
			break;
		}
	}

	return !columnIdPairList.empty();
}

void SQLCompiler::getJoinCondColumns(
		const Expr &expr,
		util::Vector< std::pair<ColumnId, ColumnId> > &columnIdPairList) {
	if (expr.op_ == SQLType::EXPR_AND) {
		getJoinCondColumns(*expr.left_, columnIdPairList);
		getJoinCondColumns(*expr.right_, columnIdPairList);
	}
	else if (expr.op_ == SQLType::OP_EQ &&
			getJoinPrimaryCondScore(expr, NULL, false) > 0) {
		assert(expr.left_->op_ == SQLType::EXPR_COLUMN);
		assert(expr.right_->op_ == SQLType::EXPR_COLUMN);
		assert(expr.left_->inputId_ != expr.right_->inputId_);

		std::pair<ColumnId, ColumnId> columnIdPair(
				expr.left_->columnId_, expr.right_->columnId_);
		if (expr.left_->inputId_ != 0) {
			std::swap(columnIdPair.first, columnIdPair.second);
		}

		if (std::find(
				columnIdPairList.begin(), columnIdPairList.end(),
				columnIdPair) == columnIdPairList.end()) {
			columnIdPairList.push_back(columnIdPair);
		}
	}
}

bool SQLCompiler::splitExprList(
		const PlanExprList &inExprList, AggregationPhase inPhase,
		PlanExprList &outExprList1, AggregationPhase &outPhase1,
		PlanExprList &outExprList2, AggregationPhase &outPhase2,
		const PlanExprList *groupExprList1, PlanExprList *groupExprList2) {
	bool splitted = false;

	PlanExprList *sameOutList = NULL;
	if (inPhase == SQLType::AGG_PHASE_ADVANCE_PIPE) {
		outExprList1 = inExprList;
		sameOutList = &outExprList2;
		outPhase1 = SQLType::AGG_PHASE_ADVANCE_PIPE;
		outPhase2 = SQLType::AGG_PHASE_ALL_PIPE;
	}
	else if (inPhase == SQLType::AGG_PHASE_MERGE_PIPE) {
		assert(false);
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}
	else {
		outExprList1.clear();
		outExprList2.clear();

		size_t groupCount = 0;
		if (groupExprList1 != NULL) {
			groupCount = groupExprList1->size();
		}

		if (groupExprList2 != NULL) {
			groupExprList2->clear();
		}

		util::Vector<uint32_t> *inputOffsetList = NULL;
		util::Vector<uint32_t> inputOffsetListBase(alloc_);
		bool fullSplittable = true;
		if (groupCount > 0) {
			splitted = true;
		}
		else if (!isDereferenceableExprList(inExprList)) {
			fullSplittable = false;
			makeSplittingInputOffsetList(inExprList, inputOffsetListBase);
			inputOffsetList = &inputOffsetListBase;
		}

		uint32_t aggLevel = getNodeAggregationLevel(inExprList);
		const bool aggregating = (aggLevel > 0);
		if (!aggregating && splitted) {
			aggLevel += 1;
		}

		ColumnId nonDupGroupCount = 0;
		typedef util::Map<uint32_t, uint32_t> DupMap;
		DupMap dupMap(alloc_);
		const uint32_t initialDupColumn = std::numeric_limits<uint32_t>::max();
		if (splitted) {
			for (PlanExprList::const_iterator it = inExprList.begin();
					it != inExprList.end(); ++it) {
				if (it->op_ == SQLType::EXPR_COLUMN) {
					dupMap.insert(std::pair<uint32_t, uint32_t>(
							it->columnId_, initialDupColumn));
				}
			}

			if (groupExprList1 != NULL) {
				for (PlanExprList::const_iterator it = groupExprList1->begin();
						it != groupExprList1->end(); ++it) {
					DupMap::const_iterator dupIt;
					if (it->op_ != SQLType::EXPR_COLUMN ||
							(dupIt = dupMap.find(it->columnId_)) == dupMap.end()) {
						++nonDupGroupCount;
					}
				}
			}
		}

		for (PlanExprList::const_iterator it = inExprList.begin();
				it != inExprList.end(); ++it) {
			DupMap::iterator dupIt;
			if (it->op_ == SQLType::EXPR_COLUMN &&
					(dupIt = dupMap.find(it->columnId_)) != dupMap.end() &&
					dupIt->second == initialDupColumn) {
				dupIt->second = static_cast<uint32_t>(outExprList1.size());
			}

			const Expr *outExpr;
			const bool lastTop = (it + 1 == inExprList.end());
			splitted |= splitExprListSub(
					*it, outExprList1, outExpr, aggLevel, nonDupGroupCount,
					aggregating, lastTop, inputOffsetList);
			outExprList2.push_back(*outExpr);
		}

		if (splitted) {
			outPhase1 = SQLType::AGG_PHASE_ADVANCE_PIPE;
			outPhase2 = SQLType::AGG_PHASE_MERGE_PIPE;

			if (groupCount > 0) {
				assert(groupExprList2 != NULL);

				const uint32_t inputId = 0;
				ColumnId nonDupColumnId = 0;
				for (PlanExprList::const_iterator it = groupExprList1->begin();
						it != groupExprList1->end(); ++it) {
					ColumnId columnId;
					DupMap::const_iterator dupIt;
					if (it->op_ != SQLType::EXPR_COLUMN ||
							(dupIt = dupMap.find(it->columnId_)) == dupMap.end()) {
						columnId = nonDupColumnId;
						outExprList1.insert(outExprList1.begin() + columnId, *it);
						++nonDupColumnId;
					}
					else {
						assert(dupIt->second != initialDupColumn);
						columnId = nonDupGroupCount + dupIt->second;
					}
					groupExprList2->push_back(Expr(genColumnExprBase(
							NULL, NULL, inputId, columnId, &(*it))));
				}
			}

			if (outExprList1.empty()) {
				outExprList1.push_back(genConstExpr(
						*static_cast<Plan*>(NULL),
						TupleValue(static_cast<int64_t>(0)),
						MODE_NORMAL_SELECT));
			}
			setSplittedExprListType(outExprList1, outPhase1);
		}
		else {
			if (fullSplittable) {
				outExprList1.swap(outExprList2);
				sameOutList = &outExprList2;
				assert(sameOutList->empty());
			}
			else {
				splitted = true;
			}

			outPhase1 = SQLType::AGG_PHASE_ALL_PIPE;
			outPhase2 = SQLType::AGG_PHASE_ALL_PIPE;
		}
	}

	if (sameOutList != NULL) {
		sameOutList->clear();
		const uint32_t inputId = 0;

		for (PlanExprList::const_iterator it = inExprList.begin();
				it != inExprList.end(); ++it) {
			const ColumnId columnId = static_cast<ColumnId>(
					it - inExprList.begin());
			sameOutList->push_back(genColumnExprBase(
					NULL, NULL, inputId, columnId, &(*it)));
		}
	}

	return splitted;
}

bool SQLCompiler::splitExprListSub(
		const Expr &inExpr, PlanExprList &outExprList,
		const Expr *&outExpr, uint32_t parentLevel, size_t groupCount,
		bool aggregating, bool lastTop,
		util::Vector<uint32_t> *inputOffsetList) {
	outExpr = NULL;

	const bool fullSplittable = (inputOffsetList == NULL);
	assert(fullSplittable || groupCount <= 0);

	if ((SQLType::START_AGG < inExpr.op_ && inExpr.op_ < SQLType::END_AGG) &&
			fullSplittable) {
		const AggregationPhase phase = SQLType::AGG_PHASE_ADVANCE_PIPE;

		const Mode mode =
				(groupCount > 0 ? MODE_GROUP_SELECT : MODE_NORMAL_SELECT);
		const ColumnType advanceType = getResultType(
				inExpr, NULL, TupleList::TYPE_NULL, mode, 0, phase);

		const uint32_t inputId = 0;
		const ColumnId columnId =
				static_cast<ColumnId>(groupCount + outExprList.size());

		Expr *target = ALLOC_NEW(alloc_) Expr(inExpr);
		outExpr = target;
		target->next_ = ALLOC_NEW(alloc_) ExprList(alloc_);

		{
			Expr advanceExpr = inExpr;
			advanceExpr.columnType_ = advanceType;
			outExprList.push_back(advanceExpr);

			target->next_->push_back(ALLOC_NEW(alloc_) Expr(genColumnExprBase(
					NULL, NULL, inputId, columnId, &advanceExpr)));
		}

		const size_t resultCount =
				ProcessorUtils::getResultCount(inExpr.op_, phase);
		for (size_t i = 1; i < resultCount; i++) {
			Expr followingExpr(alloc_);
			followingExpr.op_ = SQLType::EXPR_AGG_FOLLOWING;
			followingExpr.columnType_ = getResultType(
				inExpr, NULL, TupleList::TYPE_NULL, mode, i, phase);
			outExprList.push_back(followingExpr);

			target->next_->push_back(ALLOC_NEW(alloc_) Expr(genColumnExprBase(
					NULL, NULL, inputId, static_cast<ColumnId>(columnId + i),
					&followingExpr)));
		}

		return true;
	}
	else {
		bool splitted = false;

		if (inExpr.op_ == SQLType::START_EXPR) {
			splitted = (parentLevel > 0);

			Expr expr(alloc_);
			expr.op_ = SQLType::START_EXPR;

			if (splitted) {
				outExprList.push_back(expr);
			}

			outExpr = ALLOC_NEW(alloc_) Expr(expr);

			return splitted;
		}

		const bool needSplitting = (fullSplittable &&
				inExpr.aggrNestLevel_ != parentLevel &&
				isDereferenceableExprType(inExpr.op_) &&
				inExpr.op_ != SQLType::EXPR_TYPE &&
				inExpr.op_ != SQLType::EXPR_CONSTANT);

		Expr *left = NULL;
		if (inExpr.left_ != NULL) {
			const Expr *subExpr;
			splitted |= splitExprListSub(
					*inExpr.left_, outExprList, subExpr, inExpr.aggrNestLevel_,
					groupCount, aggregating, false, inputOffsetList);
			left = ALLOC_NEW(alloc_) Expr(*subExpr);
		}

		Expr *right = NULL;
		if (inExpr.right_ != NULL) {
			const Expr *subExpr;
			splitted |= splitExprListSub(
					*inExpr.right_, outExprList, subExpr, inExpr.aggrNestLevel_,
					groupCount, aggregating, false, inputOffsetList);
			right = ALLOC_NEW(alloc_) Expr(*subExpr);
		}

		ExprList *next = NULL;
		if (inExpr.next_ != NULL) {
			next = ALLOC_NEW(alloc_) ExprList(alloc_);
			for (ExprList::iterator it = inExpr.next_->begin();
					it != inExpr.next_->end(); ++it) {
				const Expr *subExpr;
				splitted |= splitExprListSub(
						**it, outExprList, subExpr, inExpr.aggrNestLevel_,
						groupCount, aggregating, false, inputOffsetList);
				next->push_back(ALLOC_NEW(alloc_) Expr(*subExpr));
			}
		}

		Expr *targetExpr;
		ColumnId columnId;
		if (needSplitting) {
			columnId = static_cast<ColumnId>(groupCount + outExprList.size());
			outExprList.push_back(inExpr);
			targetExpr = &outExprList.back();
		}
		else if (fullSplittable || inExpr.op_ != SQLType::EXPR_COLUMN) {
			columnId = std::numeric_limits<ColumnId>::max();
			targetExpr = ALLOC_NEW(alloc_) Expr(inExpr);
			outExpr = targetExpr;
		}
		else {
			columnId = (*inputOffsetList)[inExpr.inputId_] + inExpr.columnId_;
			while (columnId >= outExprList.size()) {
				outExprList.push_back(genConstExprBase(
						NULL, TupleValue(static_cast<int64_t>(0)),
						MODE_NORMAL_SELECT));
			}
			targetExpr = &outExprList[columnId];
			*targetExpr = inExpr;
		}

		if (!fullSplittable && lastTop && outExprList.empty()) {
			outExprList.push_back(genConstExprBase(
					NULL, TupleValue(static_cast<int64_t>(0)),
					MODE_NORMAL_SELECT));
		}

		targetExpr->left_ = left;
		targetExpr->right_ = right;
		targetExpr->next_ = next;

		if (outExpr == NULL) {
			const uint32_t inputId = 0;
			outExpr = ALLOC_NEW(alloc_) Expr(genColumnExprBase(
					NULL, NULL, inputId, columnId, targetExpr));
			if (aggregating) {
				targetExpr->columnType_ =
						setColumnTypeNullable(targetExpr->columnType_, true);
			}
		}

		return splitted;
	}
}

void SQLCompiler::makeSplittingInputOffsetList(
		const PlanExprList &exprList, util::Vector<uint32_t> &offsetList) {
	for (PlanExprList::const_iterator it = exprList.begin();
			it != exprList.end(); ++it) {
		makeSplittingInputSizeList(*it, offsetList);
	}

	uint32_t lastOffset = 0;
	for (util::Vector<uint32_t>::iterator it = offsetList.begin();
			it != offsetList.end(); ++it) {
		const uint32_t size = *it;
		*it = lastOffset;
		lastOffset += size;
	}
}

void SQLCompiler::makeSplittingInputSizeList(
		const Expr &expr, util::Vector<uint32_t> &sizeList) {
	if (expr.op_ == SQLType::EXPR_COLUMN) {
		while (expr.inputId_ >= sizeList.size()) {
			sizeList.push_back(0);
		}
		uint32_t &destSize = sizeList[expr.inputId_];
		destSize = std::max(destSize, expr.columnId_ + 1);
	}
	for (ExprArgsIterator<> it(expr); it.exists(); it.next()) {
		makeSplittingInputSizeList(it.get(), sizeList);
	}
}

void SQLCompiler::setSplittedExprListType(
		PlanExprList &exprList, AggregationPhase &phase) {
	if (phase != SQLType::AGG_PHASE_ADVANCE_PIPE) {
		return;
	}

	for (PlanExprList::iterator it = exprList.begin();
			it != exprList.end(); ++it) {
		if (!(SQLType::START_AGG < it->op_ &&
				it->op_ < SQLType::END_AGG) &&
				it->op_ != SQLType::START_EXPR &&
				it->op_ != SQLType::EXPR_AGG_FOLLOWING &&
				it->aggrNestLevel_ == 0) {
			it->columnType_ = setColumnTypeNullable(it->columnType_, true);
		}
	}
}

void SQLCompiler::applySubqueryId(const Expr &idRefExpr, Expr &expr) {
	if (SQLType::START_AGG < expr.op_ && expr.op_ < SQLType::END_AGG) {
		assert(idRefExpr.op_ == SQLType::EXPR_COLUMN);
		assert(setColumnTypeNullable(idRefExpr.columnType_, false) ==
				TupleList::TYPE_LONG);

		if (expr.op_ == SQLType::AGG_COUNT_ALL) {
			expr.op_ = SQLType::AGG_COUNT_COLUMN;
			expr.next_ = ALLOC_NEW(alloc_) ExprList(alloc_);
			expr.next_->push_back(ALLOC_NEW(alloc_) Expr(alloc_));
			copyExpr(idRefExpr, *expr.next_->back());
		}
		else if (expr.next_ != NULL && !expr.next_->empty()) {
			Expr **aggArgExpr = &expr.next_->front();

			Expr *checkTargetExpr = ALLOC_NEW(alloc_) Expr(alloc_);
			copyExpr(idRefExpr, *checkTargetExpr);

			Expr *checkExpr = ALLOC_NEW(alloc_) Expr(genOpExprBase(
					NULL, SQLType::OP_IS_NOT_NULL, checkTargetExpr,
					NULL, MODE_GROUP_SELECT, true));

			ExprList *caseArgs = ALLOC_NEW(alloc_) ExprList(alloc_);
			caseArgs->push_back(checkExpr);
			caseArgs->push_back(*aggArgExpr);
			Expr *caseExpr = ALLOC_NEW(alloc_) Expr(genExprWithArgs(
					NULL, SQLType::EXPR_CASE, caseArgs,
					MODE_GROUP_SELECT, true));

			*aggArgExpr = caseExpr;
		}
		else {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
		}
	}

	for (ExprArgsIterator<false> it(expr); it.exists(); it.next()) {
		applySubqueryId(idRefExpr, it.get());
	}
}

SQLCompiler::PlanExprList SQLCompiler::tableInfoToOutput(
		const Plan &plan, const TableInfo &info, Mode mode,
		int32_t subContainerId) {
	assert(plan.back().inputList_.empty() && !info.idInfo_.isEmpty());

	PlanExprList list(alloc_);

	list.push_back(genConstExpr(plan, TupleValue(
			static_cast<int64_t>(subContainerId)), mode));

	const uint32_t inputId = 0;
	list.push_back(genTupleIdExpr(plan, inputId, mode));

	const size_t count = info.columnInfoList_.size();
	for (size_t i = 0; i < count; i++) {
		list.push_back(genColumnExpr(plan, inputId, static_cast<ColumnId>(i)));
	}

	return list;
}

SQLCompiler::PlanExprList SQLCompiler::inputListToOutput(
		const Plan &plan, bool withAutoGenerated) {
	return inputListToOutput(plan, plan.back(), NULL, withAutoGenerated);
}

SQLCompiler::PlanExprList SQLCompiler::inputListToOutput(
		const Plan &plan, const PlanNode &node,
		const uint32_t *inputId, bool withAutoGenerated) {
	assert(!node.inputList_.empty());
	assert(inputId == NULL || *inputId < node.inputList_.size());

	PlanExprList list(alloc_);

	const PlanNode::IdList &inputList = node.inputList_;
	for (PlanNode::IdList::const_iterator inIt = inputList.begin();
			inIt != inputList.end(); ++inIt) {
		const uint32_t curInputId = static_cast<uint32_t>(inIt - inputList.begin());
		if (inputId != NULL && curInputId != *inputId) {
			continue;
		}

		const PlanNode &inNode = plan.nodeList_[*inIt];
		const size_t count = inNode.outputList_.size();
		for (size_t i = 0; i < count; i++) {
			if (inNode.outputList_[i].op_ == SQLType::START_EXPR) {
				list.push_back(inNode.outputList_[i]);
				continue;
			}

			list.push_back(genColumnExprBase(
					&plan, &node, curInputId, static_cast<ColumnId>(i), NULL));

			if (!withAutoGenerated && list.back().autoGenerated_) {
				list.pop_back();
			}
		}
	}

	return list;
}

SQLCompiler::PlanExprList SQLCompiler::mergeOutput(
		const Plan &plan, const PlanNode *node,
		bool unionMismatchIgnorable) {
	const PlanNode &curNode = (node == NULL ? plan.back() : *node);
	assert(!curNode.inputList_.empty());

	PlanExprList list(alloc_);

	util::Vector<ColumnType> promotionList(alloc_);

	const PlanNode::IdList &inputList = curNode.inputList_;
	for (PlanNode::IdList::const_iterator inIt = inputList.begin();
			inIt != inputList.end(); ++inIt) {
		const uint32_t inputId = static_cast<uint32_t>(inIt - inputList.begin());

		const PlanNode &inNode = plan.nodeList_[*inIt];
		size_t count = inNode.outputList_.size();

		if (inputId == 0) {
			for (size_t i = 0; i < count; i++) {
				const bool reduced = isDisabledExpr(inNode.outputList_[i]);
				list.push_back(reduced ? genDisabledExpr() : genColumnExprBase(
						&plan, &curNode, inputId, static_cast<ColumnId>(i),
						NULL));

				Expr &expr = list.back();

				if (reduced) {
					expr.columnType_ = TupleList::TYPE_NULL;
				}

				promotionList.push_back(expr.columnType_);
			}
		}
		else {
			if (count != list.size()) {
				if (!unionMismatchIgnorable) {
					SQL_COMPILER_THROW_ERROR(
							GS_ERROR_SQL_COMPILE_COLUMN_LIST_UNMATCH, NULL,
							"Different column count between set operands");
				}
				count = std::min(count, list.size());
				while (list.size() > count) {
					list.pop_back();
				}
			}

			for (size_t i = 0; i < count; i++) {
				Expr &expr1 = list[i];
				const Expr &expr2 = inNode.outputList_[i];
				if (isDisabledExpr(expr1) || isDisabledExpr(expr2)) {
					expr1 = genDisabledExpr();
					continue;
				}

				ColumnType &type1 = expr1.columnType_;
				const ColumnType type2 = expr2.columnType_;

				if (type1 == TupleList::TYPE_NULL ||
						type2 == TupleList::TYPE_NULL) {
					assert(parameterTypeList_ != NULL);
					type1 = TupleList::TYPE_NULL;
					continue;
				}

				type1 = ProcessorUtils::findPromotionType(type1, type2);
				if (type1 == TupleList::TYPE_NULL) {
					SQL_COMPILER_THROW_ERROR(
							GS_ERROR_SQL_COMPILE_COLUMN_LIST_UNMATCH, NULL,
							"Incompatible column type between set operands");
				}
			}
		}
	}

	return list;
}

void SQLCompiler::refreshAllColumnTypes(
		Plan &plan, bool binding, bool *insertOrUpdateFound) {
	util::StackAllocator::Scope scope(alloc_);

	util::Vector<bool> visitedList(plan.nodeList_.size(), false, alloc_);
	bool insertOrUpdateFoundLocal = false;

	for (size_t lastCount = 0;;) {
		size_t curCount = lastCount;

		for (PlanNodeList::iterator nodeIt = plan.nodeList_.begin();
				nodeIt != plan.nodeList_.end(); ++nodeIt) {
			PlanNode &node = *nodeIt;
			const PlanNodeId nodeId =
					static_cast<PlanNodeId>(nodeIt - plan.nodeList_.begin());

			if (visitedList[nodeId]) {
				continue;
			}

			insertOrUpdateFoundLocal |= isForInsertOrUpdate(node);

			bool visitable = true;
			for (PlanNode::IdList::iterator it = node.inputList_.begin();
					it != node.inputList_.end(); ++it) {
				visitable &= visitedList[*it];
			}

			if (!visitable) {
				continue;
			}

			refreshColumnTypes(plan, node, node.predList_, false, binding);
			refreshColumnTypes(plan, node, node.outputList_, true, binding);

			visitedList[nodeId] = true;
			curCount++;
		}

		if (curCount == lastCount || curCount >= plan.nodeList_.size()) {
			break;
		}
		lastCount = curCount;
	}

	if (insertOrUpdateFound != NULL) {
		*insertOrUpdateFound = insertOrUpdateFoundLocal;
	}
}

void SQLCompiler::refreshColumnTypes(
		Plan &plan, PlanNode &node, PlanExprList &exprList, bool forOutput,
		bool binding, bool unionMismatchIgnorable) {

	if (forOutput && node.type_ == SQLType::EXEC_UNION) {
		const PlanExprList &list =
				mergeOutput(plan, &node, unionMismatchIgnorable);
		assert(exprList.size() == list.size());

		PlanExprList::iterator it = exprList.begin();

		for (PlanExprList::const_iterator listIt = list.begin();
				listIt != list.end(); ++listIt, ++it) {
			if (isDisabledExpr(*listIt)) {
				continue;
			}
			it->columnType_ = listIt->columnType_;
		}
		return;
	}

	const bool aggregated = isNodeAggregated(
			exprList, node.type_, isWindowNode(node));

	for (PlanExprList::iterator it = exprList.begin();
			it != exprList.end(); ++it) {
		const ColumnId outputColumnId =
				static_cast<ColumnId>(it - exprList.begin());
		refreshColumnTypes(
				plan, node, *it, forOutput, aggregated, binding,
				node.aggPhase_,
				(forOutput ? &exprList : NULL),
				(forOutput ? &outputColumnId: NULL));
	}
}

void SQLCompiler::refreshColumnTypes(
		Plan &plan, PlanNode &node, Expr &expr, bool forOutput,
		bool aggregated, bool binding, AggregationPhase phase,
		const PlanExprList *outputList, const ColumnId *outputColumnId) {
	if (isDisabledExpr(expr)) {
		return;
	}

	do {
		if (phase != SQLType::AGG_PHASE_MERGE_PIPE) {
			break;
		}

		Expr *advExpr;
		if (expr.op_ == SQLType::EXPR_COLUMN) {
			const PlanNode &inNode = node.getInput(plan, expr.inputId_);
			const Expr &inExpr = inNode.outputList_[expr.columnId_];
			if (isDisabledExpr(inExpr)) {
				return;
			}

			advExpr = &expr;
		}
		else if (SQLType::START_AGG < expr.op_ &&
				expr.op_ < SQLType::END_AGG) {
			for (ExprArgsIterator<false> it(expr); it.exists(); it.next()) {
				refreshColumnTypes(
						plan, node, it.get(), forOutput, false, binding,
						SQLType::AGG_PHASE_ALL_PIPE, NULL, NULL);
			}

			if (expr.next_ != NULL && !expr.next_->empty()) {
				advExpr = expr.next_->front();
				if (advExpr->op_ == SQLType::EXPR_CONSTANT &&
						expr.op_ == SQLType::AGG_COUNT_ALL &&
						advExpr->columnType_ == TupleList::TYPE_LONG &&
						expr.columnType_ == TupleList::TYPE_LONG) {
					return;
				}
				else if (advExpr->op_ != SQLType::EXPR_COLUMN) {
					advExpr = NULL;
				}
			}
			else {
				advExpr = NULL;
			}

			if (advExpr == NULL) {
				assert(false);
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
			}
		}
		else {
			break;
		}

		expr.columnType_ = followAdvanceColumnType(
				plan, node, *advExpr, forOutput, binding, NULL, NULL);
		return;
	}
	while (false);

	if (expr.op_ == SQLType::EXPR_AGG_FOLLOWING) {
		if (outputList == NULL || outputColumnId == NULL) {
			assert(false);
			SQL_COMPILER_THROW_ERROR(
					GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
		}
		expr.columnType_ = getAggregationFollowingColumnType(
				node.type_, *outputList, *outputColumnId, forOutput, binding,
				node.aggPhase_, isWindowNode(node));
		return;
	}

	if (expr.op_ == SQLType::EXPR_COLUMN) {
		const uint32_t tableCount = node.getInputTableCount();
		if (expr.inputId_ < tableCount) {
			const SQLTableInfo::Id id = node.tableIdInfo_.id_;
			expr.columnType_ = ProcessorUtils::getTableColumnType(
					getTableInfoList().resolve(id), expr.columnId_, NULL);
		}
		else {
			PlanNode::IdList::const_iterator inBegin = node.inputList_.begin();
			PlanNode::IdList::const_iterator inEnd = node.inputList_.end();
			if (node.type_ != SQLType::EXEC_UNION) {
				inBegin += (expr.inputId_ - tableCount);
				inEnd = inBegin + 1;
			}

			ColumnType outType = TupleList::TYPE_NULL;
			for (PlanNode::IdList::const_iterator it = inBegin; it != inEnd; ++it) {
				const PlanNode &inNode = plan.nodeList_[*it];
				const Expr &inExpr = inNode.outputList_[expr.columnId_];
				if (isDisabledExpr(inExpr)) {
					return;
				}

				const ColumnType inType = inExpr.columnType_;
				if (ColumnTypeUtils::isNull(inType)) {
					outType = TupleList::TYPE_NULL;
					break;
				}
				else if (it == inBegin) {
					outType = inType;
					continue;
				}

				outType = ProcessorUtils::findPromotionType(outType, inType);
				if (ColumnTypeUtils::isNull(inType)) {
					assert(false);
					SQL_COMPILER_THROW_ERROR(
							GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
				}
			}
			expr.columnType_ = outType;
		}

		if (isInputNullable(node, expr.inputId_) || aggregated) {
			expr.columnType_ =
					setColumnTypeNullable(expr.columnType_, true);
		}
	}
	else if (expr.op_ == SQLType::EXPR_PLACEHOLDER) {
		if (binding) {
			if (expr.columnId_ >= plan.parameterList_.size()) {
				assert(false);
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
			}
			expr.columnType_ = plan.parameterList_[expr.columnId_].getType();

			if (expr.columnType_ == TupleList::TYPE_NULL) {
				expr.columnType_ = TupleList::TYPE_ANY;
			}
		}
		else {
			expr.columnType_ = TupleList::TYPE_NULL;
		}
	}
	else if (expr.op_ != SQLType::EXPR_CONSTANT) {
		bool subAggregated = aggregated;
		if (SQLType::START_AGG < expr.op_ && expr.op_ < SQLType::END_AGG) {
			subAggregated = false;
		}

		for (ExprArgsIterator<false> it(expr); it.exists(); it.next()) {
			refreshColumnTypes(
					plan, node, it.get(), forOutput, subAggregated, binding,
					phase, NULL, NULL);
		}

		const Mode mode = getModeForColumnTypeRefresh(
				node.type_, isWindowNode(node));
		expr.columnType_ = getResultType(
				expr, NULL, TupleList::TYPE_NULL, mode, 0, phase);
	}

	if (forOutput && outputColumnId != NULL &&
			node.aggPhase_ == SQLType::AGG_PHASE_ADVANCE_PIPE &&
			!(SQLType::START_AGG < expr.op_ &&
					expr.op_ < SQLType::END_AGG) &&
					expr.op_ != SQLType::EXPR_AGG_FOLLOWING) {
		expr.columnType_ =
				setColumnTypeNullable(expr.columnType_, true);
	}
}

TupleList::TupleColumnType SQLCompiler::followAdvanceColumnType(
		Plan &plan, PlanNode &node, const Expr &expr,
		bool forOutput, bool binding, const PlanExprList *outputList,
		const ColumnId *outputColumnId) {
	UNUSED_VARIABLE(outputList);
	UNUSED_VARIABLE(outputColumnId);

	if (node.aggPhase_ == SQLType::AGG_PHASE_ADVANCE_PIPE) {
		Expr localExpr = expr;
		refreshColumnTypes(
				plan, node, localExpr, forOutput,
				isNodeAggregated(node), binding,
				SQLType::AGG_PHASE_ALL_PIPE, NULL, NULL);
		return localExpr.columnType_;
	}
	else if (node.type_ == SQLType::EXEC_UNION) {
		ColumnType type = TupleList::TYPE_NULL;
		for (PlanNode::IdList::iterator it = node.inputList_.begin();
				it != node.inputList_.end(); ++it) {
			PlanNode &inNode = plan.nodeList_[*it];
			const ColumnType curType = followAdvanceColumnType(
					plan, inNode, inNode.outputList_[expr.columnId_],
					forOutput, binding, &inNode.outputList_, &expr.columnId_);
			if (curType == TupleList::TYPE_NULL) {
				type = TupleList::TYPE_NULL;
				break;
			}

			if (it == node.inputList_.begin()) {
				type = curType;
			}
			else {
				type = ProcessorUtils::findPromotionType(type, curType);
				assert(type != TupleList::TYPE_NULL);
			}
		}
		return type;
	}
	else {
		PlanNode &inNode = node.getInput(plan, expr.inputId_);
		const bool forOutput = true;
		return followAdvanceColumnType(
				plan, inNode, inNode.outputList_[expr.columnId_],
				forOutput, binding, &inNode.outputList_, &expr.columnId_);
	}
}

TupleList::TupleColumnType SQLCompiler::getAggregationFollowingColumnType(
		Type nodeType, const PlanExprList &outputList, ColumnId columnId,
		bool forOutput, bool binding, AggregationPhase phase, bool forWindow) {
	UNUSED_VARIABLE(forOutput);

	if (phase != SQLType::AGG_PHASE_ADVANCE_PIPE ||
			columnId <= 0 || columnId >= outputList.size() ||
			outputList[columnId].op_ != SQLType::EXPR_AGG_FOLLOWING) {
		assert(false);
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	const PlanExprList::const_iterator columnIt = outputList.begin() + columnId;

	PlanExprList::const_iterator aggIt = columnIt;
	for (; aggIt != outputList.begin(); --aggIt) {
		if (aggIt->op_ != SQLType::EXPR_AGG_FOLLOWING) {
			break;
		}
	}

	const size_t resultCount =
			ProcessorUtils::getResultCount(aggIt->op_, phase);
	const size_t resultIndex = static_cast<size_t>(columnIt - aggIt);

	if (!(SQLType::START_AGG < aggIt->op_ &&
			aggIt->op_ < SQLType::END_AGG) ||
			aggIt->op_ == SQLType::EXPR_AGG_FOLLOWING ||
			resultIndex <= 0 || resultIndex >= resultCount) {
		assert(false);
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	const Mode mode = getModeForColumnTypeRefresh(nodeType, forWindow);
	const ColumnType columnType = getResultType(
			*aggIt, NULL, TupleList::TYPE_NULL, mode, resultIndex, phase);
	if (binding && columnType == TupleList::TYPE_NULL) {
		assert(false);
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	return columnType;
}

SQLCompiler::Mode SQLCompiler::getModeForColumnTypeRefresh(
		Type type, bool forWindow) {
	return (type == SQLType::EXEC_GROUP || forWindow ?
			MODE_GROUP_SELECT : MODE_NORMAL_SELECT);
}

void SQLCompiler::checkModificationColumnConversions(
		const Plan &plan, bool insertOrUpdateFound) {
	if (!insertOrUpdateFound) {
		return;
	}

	for (PlanNodeList::const_iterator nodeIt = plan.nodeList_.begin();
			nodeIt != plan.nodeList_.end(); ++nodeIt) {
		const PlanNode &node = *nodeIt;
		if (!isForInsertOrUpdate(node)) {
			continue;
		}

		for (PlanNode::IdList::const_iterator idIt = node.inputList_.begin();
				idIt != node.inputList_.end(); ++idIt) {
			const PlanNode &inNode = plan.nodeList_[*idIt];

			for (PlanExprList::const_iterator it =
					inNode.outputList_.begin();
					it != inNode.outputList_.end(); ++it) {
				const Expr &expr = *it;
				if (expr.op_ != SQLType::OP_CAST) {
					continue;
				}

				if (expr.left_ == NULL || expr.right_ == NULL) {
					assert(false);
					continue;
				}

				if (!expr.right_->autoGenerated_) {
					continue;
				}

				const ColumnType srcType = expr.left_->columnType_;
				const ColumnType desiredType = expr.right_->columnType_;
				const bool implicit = true;
				const bool evaluating = false;

				const ColumnType retType =
						ProcessorUtils::findConversionType(
								srcType, desiredType, implicit, evaluating);

				if (retType == TupleList::TYPE_NULL) {
					SQL_COMPILER_THROW_ERROR(
							GS_ERROR_SQL_PROC_UNSUPPORTED_TYPE_CONVERSION,
							NULL,
							"Unacceptable implicit type conversion for column "
							"of modifying table ("
							"from=" << TupleList::TupleColumnTypeCoder()(srcType) <<
							", to=" << TupleList::TupleColumnTypeCoder()(
									desiredType) <<
							")");
				}
			}
		}
	}
}

bool SQLCompiler::isForInsertOrUpdate(const PlanNode &node) {
	return (node.type_ == SQLType::EXEC_INSERT ||
			node.type_ == SQLType::EXEC_UPDATE);
}

void SQLCompiler::finalizeScanCostHints(Plan &plan, bool tableCostAffected) {
	for (PlanNodeList::iterator nodeIt = plan.nodeList_.begin();
			nodeIt != plan.nodeList_.end(); ++nodeIt) {
		SQLTableInfo::IdInfo &idInfo = nodeIt->tableIdInfo_;
		int64_t &approxSize = idInfo.approxSize_;

		if (tableCostAffected && nodeIt->type_ == SQLType::EXEC_SCAN) {
			const SQLTableInfo &info = getTableInfoList().resolve(idInfo.id_);
			if (info.partitioning_ == NULL) {
				approxSize = info.idInfo_.approxSize_;
			}
			else {
				assert(idInfo.subContainerId_ >= 0);
				approxSize = info.partitioning_->subInfoList_[
						idInfo.subContainerId_].approxSize_;
			}

			if (approxSize < 0) {
				approxSize = 0;
			}
		}
		else {
			approxSize = -1;
		}
	}
}

void SQLCompiler::applyPlanningOption(Plan &plan) {
	const CompileOption &option = getCompileOption();

	applyPlanningVersion(option.getPlanningVersion(), plan.planningVersion_);

	const SQLHintInfo *hintInfo = plan.hintInfo_;
	applyPlanningVersion(
			getPlanningVersionHint(hintInfo), plan.planningVersion_);
}

void SQLCompiler::applyPlanningVersion(
		const SQLPlanningVersion &src, SQLPlanningVersion &dest) {
	if (!src.isEmpty()) {
		dest = src;
	}
}

void SQLCompiler::applyScanOption(Plan &plan) {
	const SQLHintInfo *hint = plan.hintInfo_;
	if (hint == NULL && !indexScanEnabled_) {
		return;
	}

	for (PlanNodeList::iterator it = plan.nodeList_.begin();
			it != plan.nodeList_.end(); ++it) {
		if (it->type_ == SQLType::EXEC_SCAN) {
			if (indexScanEnabled_) {
				it->cmdOptionFlag_ |= PlanNode::Config::CMD_OPT_SCAN_INDEX;
			}

			if (multiIndexScanEnabled_) {
				it->cmdOptionFlag_ |=
						PlanNode::Config::CMD_OPT_SCAN_MULTI_INDEX;
			}

			if (hint != NULL) {
				applyScanOption(it->cmdOptionFlag_, *it, *hint);
			}
		}
	}
}


void SQLCompiler::applyScanOption(
		PlanNode::CommandOptionFlag &optionFlag,
		const PlanNode &node, const SQLHintInfo &hintInfo) {
	assert(node.type_ == SQLType::EXEC_SCAN);

	util::Vector<const Expr*> hintList(alloc_);
	const SQLHintInfo::SimpleHint *scanHint;
	bool exist = hintInfo.findScanHint(node.tableIdInfo_.id_, scanHint);

	if (exist) {
		if (scanHint->hintId_ == SQLHint::INDEX_SCAN) {
			optionFlag |= PlanNode::Config::CMD_OPT_SCAN_INDEX; 
		}
		else if (scanHint->hintId_ == SQLHint::NO_INDEX_SCAN) {
			optionFlag &= ~PlanNode::Config::CMD_OPT_SCAN_INDEX; 
		}
		else if (scanHint->hintId_ == SQLHint::START_ID) {
		}
		else {
			const util::String &targetTableName = getTableInfoList().resolve(node.tableIdInfo_.id_).tableName_;
			const Expr* hintExpr = scanHint->hintExpr_;
			util::NormalOStringStream strstrm;
			assert(hintExpr->qName_ && hintExpr->qName_->name_);
			strstrm << "Table (" << targetTableName.c_str() <<
					") has already scan hint(" <<
					hintExpr->qName_->name_->c_str() << ").";
			hintInfo.reportWarning(strstrm.str().c_str());
		}
	}
	else {
	}
}

void SQLCompiler::applyJoinOption(Plan &plan) {
	const SQLHintInfo *hint = plan.hintInfo_;
	if (hint == NULL) {
		return;
	}

	for (PlanNodeList::iterator it = plan.nodeList_.begin();
			it != plan.nodeList_.end(); ++it) {
		if (it->type_ == SQLType::EXEC_JOIN) {
			applyJoinOption(plan, *it, it->cmdOptionFlag_, *hint);
		}
	}
}

void SQLCompiler::applyJoinOption(
		const Plan &plan, const PlanNode &node,
		PlanNode::CommandOptionFlag &optionFlag, const SQLHintInfo &hintInfo) {
	assert(node.type_ == SQLType::EXEC_JOIN);
	util::Vector<const util::String*> nodeNameList(alloc_);
	for (PlanNode::IdList::const_iterator itr = node.inputList_.begin();
			itr != node.inputList_.end(); ++itr) {
		const PlanNode &inputNode = plan.nodeList_[*itr];
		if (inputNode.qName_ != NULL && inputNode.qName_->table_ != NULL
				&& inputNode.qName_->nsId_ == SyntaxTree::QualifiedName::TOP_NS_ID) {
			const SyntaxTree::QualifiedName &qName = *inputNode.qName_;
			util::String *normName = ALLOC_NEW(alloc_) util::String(
					normalizeName(alloc_, qName.table_->c_str()));
			nodeNameList.push_back(normName);
		}
		else if (inputNode.type_ == SQLType::EXEC_SCAN) {
			if (inputNode.qName_ != NULL
					&& inputNode.qName_->nsId_ != SyntaxTree::QualifiedName::TOP_NS_ID) {
				return;
			}
			const SQLTableInfo& tableInfo = getTableInfoList().resolve(inputNode.tableIdInfo_.id_);
			util::String *normName = ALLOC_NEW(alloc_) util::String(
					normalizeName(alloc_, tableInfo.tableName_.c_str()));
			nodeNameList.push_back(normName);
		}
		else {
			return;
		}
	}
	typedef util::Vector<SQLHintInfo::ParsedHint> ParsedHintList;
	ParsedHintList hintList(alloc_);
	hintInfo.findJoinHintList(nodeNameList, SQLHint::LEADING, hintList);
	if (hintList.size() > 0 
		&& (optionFlag & PlanNode::Config::CMD_OPT_JOIN_LEADING_LEFT) == 0
		&& (optionFlag & PlanNode::Config::CMD_OPT_JOIN_LEADING_RIGHT) == 0) {
		for (ParsedHintList::iterator itr = hintList.begin();
			 itr != hintList.end(); ++itr) {
			if (itr->isTree_) {
				assert(itr->hintTableIdList_.size() > 0);
				optionFlag |= (itr->hintTableIdList_[0] == 0)
						? PlanNode::Config::CMD_OPT_JOIN_LEADING_LEFT
						: PlanNode::Config::CMD_OPT_JOIN_LEADING_RIGHT;
				break; 
			}
		}
	}
	const SQLHintInfo::SimpleHint* hint = NULL;
	bool found = hintInfo.findJoinMethodHint(nodeNameList, SQLHint::NO_INDEX_JOIN, hint);
	if (found) {
		optionFlag |= PlanNode::Config::CMD_OPT_JOIN_NO_INDEX;
	}
	found = hintInfo.findJoinMethodHint(nodeNameList, SQLHint::INDEX_JOIN, hint);
	if (found) {
		optionFlag |= PlanNode::Config::CMD_OPT_JOIN_USE_INDEX;
	}
}

void SQLCompiler::relocateOptimizationProfile(Plan &plan) {
	if (nextProfileKey_ < 0) {
		return;
	}
	KeyToNodeIdList keyToNodeIdList(alloc_);

	SQLPreparedPlan::TotalProfile **totalProfile = NULL;
	for (PlanNodeList::iterator nodeIt = plan.nodeList_.begin();
			nodeIt != plan.nodeList_.end(); ++nodeIt) {
		const KeyToNodeId entry(
				nodeIt->profileKey_,
				static_cast<PlanNodeId>(nodeIt - plan.nodeList_.begin()));

		if (entry.first >= 0) {
			keyToNodeIdList.push_back(entry);
		}
		if (nodeIt->type_ == SQLType::EXEC_RESULT) {
			totalProfile = &nodeIt->profile_;
			if (*totalProfile == NULL) {
				break;
			}
		}
	}

	if (keyToNodeIdList.empty() ||
			totalProfile == NULL ||
			*totalProfile == NULL ||
			(*totalProfile)->plan_ == NULL ||
			(*totalProfile)->plan_->optimization_ == NULL) {
		return;
	}

	std::sort(keyToNodeIdList.begin(), keyToNodeIdList.end());

	SQLPreparedPlan::OptProfile &optProfile =
			*(*totalProfile)->plan_->optimization_;

	bool relocatedFull = true;
	relocatedFull &= relocateOptimizationProfile(
			plan, keyToNodeIdList, optProfile.joinReordering_);
	relocatedFull &= relocateOptimizationProfile(
			plan, keyToNodeIdList, optProfile.joinDriving_);

	if (relocatedFull) {
		SQLPreparedPlan::TotalProfile::clearIfEmpty(*totalProfile);
	}
}

template<typename T>
bool SQLCompiler::relocateOptimizationProfile(
		Plan &plan, const KeyToNodeIdList &keyToNodeIdList,
		util::Vector<T> *&entryList) {
	if (entryList == NULL) {
		return true;
	}

	for (typename util::Vector<T>::iterator it = entryList->begin();
			it != entryList->end();) {
		KeyToNodeIdList::const_iterator nodeIt = std::lower_bound(
				keyToNodeIdList.begin(), keyToNodeIdList.end(),
				KeyToNodeId(it->profileKey_, 0));

		if (nodeIt != keyToNodeIdList.end() &&
				nodeIt->first == it->profileKey_) {
			addOptimizationProfile(
					NULL, &plan.nodeList_[nodeIt->second], &(*it));
			it = entryList->erase(it);
		}
		else {
			++it;
		}
	}

	if (entryList->empty()) {
		entryList = NULL;
		return true;
	}
	return false;
}

template<typename T>
void SQLCompiler::addOptimizationProfile(
		Plan *plan, PlanNode *node, T *profile) {
	if (profile == NULL) {
		return;
	}

	PlanNode *nodeForAdd = (plan == NULL ? node : NULL);
	SQLPreparedPlan::OptProfile &optProfile =
			prepareOptimizationProfile(plan, nodeForAdd);

	util::Vector<T> *&entryList = optProfile.getEntryList<T>();
	if (entryList == NULL) {
		entryList = ALLOC_NEW(alloc_) util::Vector<T>(alloc_);
	}

	profile->profileKey_ = prepareProfileKey(*node);
	entryList->push_back(*profile);
}

template
void SQLCompiler::addOptimizationProfile(
		Plan *plan, PlanNode *node, Plan::OptProfile::JoinDriving *profile);

SQLPreparedPlan::OptProfile&
SQLCompiler::prepareOptimizationProfile(Plan *plan, PlanNode *node) {
	if (node == NULL) {
		assert(plan != NULL);
		for (PlanNodeList::iterator nodeIt = plan->nodeList_.begin();
				nodeIt != plan->nodeList_.end(); ++nodeIt) {
			if (nodeIt->type_ == SQLType::EXEC_RESULT) {
				return prepareOptimizationProfile(NULL, &(*nodeIt));
			}
		}
		assert(false);
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	SQLPreparedPlan::TotalProfile *&profile = node->profile_;
	if (profile == NULL) {
		profile = ALLOC_NEW(alloc_) SQLPreparedPlan::TotalProfile();
	}

	SQLPreparedPlan::Profile *&planProfile = profile->plan_;
	if (planProfile == NULL) {
		planProfile = ALLOC_NEW(alloc_) SQLPreparedPlan::Profile();
	}

	SQLPreparedPlan::OptProfile *&optProfile = planProfile->optimization_;
	if (optProfile == NULL) {
		optProfile = ALLOC_NEW(alloc_) SQLPreparedPlan::OptProfile();
	}

	return *optProfile;
}

SQLPreparedPlan::ProfileKey SQLCompiler::prepareProfileKey(PlanNode &node) {
	if (node.profileKey_ < 0) {
		node.profileKey_ = ++nextProfileKey_;
	}
	return node.profileKey_;
}

void SQLCompiler::checkOptimizationUnitSupport(OptimizationUnitType type) {
	if (!optimizationOption_->isSupported(type) &&
			!optimizationOption_->isCheckOnly(type)) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}
}

bool SQLCompiler::isOptimizationUnitEnabled(OptimizationUnitType type) {
	return optimizationOption_->isEnabled(type);
}

bool SQLCompiler::isOptimizationUnitCheckOnly(OptimizationUnitType type) {
	return optimizationOption_->isCheckOnly(type);
}

SQLCompiler::OptimizationFlags SQLCompiler::getDefaultOptimizationFlags() {
	OptimizationOption option(NULL);
	option.supportFull();




	return option.getEnablementFlags();
}

SQLCompiler::OptimizationOption SQLCompiler::makeJoinPushDownHintsOption() {
	OptimizationOption option(NULL);
	option.support(OPT_SIMPLIFY_SUBQUERY, false);
	option.support(OPT_MERGE_SUB_PLAN, false);
	option.support(OPT_MINIMIZE_PROJECTION, false);
	option.support(OPT_PUSH_DOWN_PREDICATE, false);
	option.support(OPT_PUSH_DOWN_AGGREGATE, false);
	option.support(OPT_MAKE_JOIN_PUSH_DOWN_HINTS, false);
	option.support(OPT_PUSH_DOWN_JOIN, false);
	option.support(OPT_PUSH_DOWN_LIMIT, true);
	option.support(OPT_REMOVE_REDUNDANCY, true);
	option.support(OPT_REMOVE_INPUT_REDUNDANCY, true);
	option.support(OPT_REMOVE_PARTITION_COND_REDUNDANCY, true);
	option.support(OPT_MAKE_INDEX_JOIN, true);
	option.setCheckOnly(OPT_MAKE_INDEX_JOIN);
	return option;
}

void SQLCompiler::optimize(Plan &plan) {
	if (!optimizationOption_->isSomeEnabled()) {
		return;
	}

	OptimizationContext cxt(*this, plan);

	bool minimizedLast = false;
	if (isOptimizationUnitEnabled(OPT_SIMPLIFY_SUBQUERY)) {
		{
			OptimizationUnit unit(cxt, OPT_MINIMIZE_PROJECTION);
			if (unit(unit() && minimizeProjection(plan, true))) {
				removeEmptyColumns(cxt, plan);
			}
		}

		{
			OptimizationUnit unit(cxt, OPT_SIMPLIFY_SUBQUERY);
			if (unit(unit() && simplifySubquery(plan))) {
				removeEmptyColumns(cxt, plan);
			}
			else {
				minimizedLast = true;
			}
		}
	}

	for (bool first = true;; first = false) {
		bool found = false;

		if (!first || !minimizedLast) {
			OptimizationUnit unit(cxt, OPT_MINIMIZE_PROJECTION);
			found |= unit(unit() && minimizeProjection(plan, false));
		}

		{
			OptimizationUnit unit(cxt, OPT_PUSH_DOWN_PREDICATE);
			found |= unit(unit() && pushDownPredicate(plan, false));
		}

		{
			OptimizationUnit unit(cxt, OPT_PUSH_DOWN_AGGREGATE);
			found |= unit(unit() && pushDownAggregate(plan));
		}

		if (first) {
			OptimizationUnit unit(cxt, OPT_MAKE_JOIN_PUSH_DOWN_HINTS);
			found |= unit(unit() && makeJoinPushDownHints(plan));
		}

		{
			OptimizationUnit unit(cxt, OPT_PUSH_DOWN_JOIN);
			found |= unit(unit() && pushDownJoin(plan));
		}

		{
			OptimizationUnit unit(cxt, OPT_PUSH_DOWN_LIMIT);
			found |= unit(unit() && pushDownLimit(plan));
		}

		{
			OptimizationUnit unit(cxt, OPT_REMOVE_REDUNDANCY);
			found |= unit(unit() && removeRedundancy(plan));
		}

		{
			OptimizationUnit unit(cxt, OPT_REMOVE_INPUT_REDUNDANCY);
			found |= unit(unit() && removeInputRedundancy(plan));
		}

		if (!found) {
			break;
		}

		removeEmptyNodes(cxt, plan);
		removeEmptyColumns(cxt, plan);
	}

	do {
		OptimizationUnit unit(cxt, OPT_MERGE_SUB_PLAN);
		if (!unit(unit() && mergeSubPlan(plan))) {
			break;
		}

		removeEmptyNodes(cxt, plan);
		removeEmptyColumns(cxt, plan);
	}
	while (false);

	{
		OptimizationUnit unit(cxt, OPT_MAKE_INDEX_JOIN);
		if (unit(unit() && selectJoinDriving(plan, unit.isCheckOnly()))) {
			removeEmptyNodes(cxt, plan);
		}
		if (unit.isCheckOnly()) {
			return;
		}
	}

	do {
		{
			bool complexFound;

			OptimizationUnit unit(cxt, OPT_ASSIGN_JOIN_PRIMARY_COND);
			unit(unit() && assignJoinPrimaryCond(plan, complexFound));

			if (!complexFound) {
				break;
			}
		}

		for (;;) {
			bool found = false;

			{
				OptimizationUnit unit(cxt, OPT_MINIMIZE_PROJECTION);
				found |= unit(unit() && minimizeProjection(plan, false));
			}

			{
				OptimizationUnit unit(cxt, OPT_REMOVE_REDUNDANCY);
				found |= unit(unit() && removeRedundancy(plan));
			}

			if (!found) {
				break;
			}

			removeEmptyNodes(cxt, plan);
			removeEmptyColumns(cxt, plan);
		}
	}
	while (false);


	{
		OptimizationUnit unit(cxt, OPT_REMOVE_PARTITION_COND_REDUNDANCY);
		unit(unit() && removePartitionCondRedundancy(plan));
	}
}

bool SQLCompiler::mergeSubPlan(Plan &plan) {
	util::StackAllocator::Scope scope(alloc_);

	typedef util::MultiMap<uint32_t, PlanNode*> NodeMap;

	typedef std::pair<PlanNodeId, uint32_t> InputRevEntry;
	typedef util::Set<InputRevEntry> InputRevSet;
	typedef util::Vector<InputRevSet> InputRevList;

	typedef std::pair<PlanNodeId, PlanNodeId> TargetEntry;
	typedef util::Vector<TargetEntry> TargetList;

	SubPlanHasher hasher;
	SubPlanEq eq;

	NodeMap nodeMap(alloc_);
	util::Vector<uint32_t> hashList(alloc_);

	TargetList targetList(alloc_);
	InputRevList inputRevList(alloc_);

	util::Vector<bool> updatedSet(alloc_);
	PlanNode::IdList updatedList(alloc_);

	bool found = false;
	for (;;) {
		{
			const bool forUpdate = !updatedList.empty();
			hashList.resize(plan.nodeList_.size());

			PlanNode::IdList::iterator updateIt = updatedList.begin();
			PlanNodeList::iterator nodeIt = plan.nodeList_.begin();
			for (;;) {
				PlanNodeId nodeId;
				PlanNode *node;
				if (forUpdate) {
					if (updateIt == updatedList.end()) {
						break;
					}
					nodeId = *updateIt;
					node = &plan.nodeList_[nodeId];
					updatedSet[nodeId] = false;
					++updateIt;
				}
				else {
					if (nodeIt == plan.nodeList_.end()) {
						break;
					}
					nodeId = static_cast<PlanNodeId>(
							nodeIt - plan.nodeList_.begin());
					node = &(*nodeIt);
					++nodeIt;
				}

				if (isDisabledNode(*node)) {
					continue;
				}

				const uint32_t key = hasher(*node);
				for (NodeMap::iterator it = nodeMap.lower_bound(key);; ++it) {
					if (it == nodeMap.end() || it->first != key) {
						nodeMap.insert(it, std::make_pair(key, node));
						break;
					}
					PlanNode *eqNode = it->second;
					if (eq(*eqNode, *node)) {
						const PlanNodeId orgId = nodeId;
						const PlanNodeId newId = static_cast<PlanNodeId>(
								eqNode - &plan.nodeList_[0]);
						targetList.push_back(std::make_pair(orgId, newId));
						break;
					}
				}
				hashList[nodeId] = key;
			}
		}

		if (targetList.empty()) {
			break;
		}

		assert(std::count(updatedSet.begin(), updatedSet.end(), true) == 0);

		updatedSet.resize(plan.nodeList_.size(), false);
		updatedList.clear();

		if (inputRevList.empty()) {
			inputRevList.assign(plan.nodeList_.size(), InputRevSet(alloc_));

			for (PlanNodeList::iterator nodeIt = plan.nodeList_.begin();
					nodeIt != plan.nodeList_.end(); ++nodeIt) {
				PlanNode &node = *nodeIt;
				if (isDisabledNode(node)) {
					continue;
				}

				const PlanNodeId destId = static_cast<PlanNodeId>(
						nodeIt - plan.nodeList_.begin());
				for (PlanNode::IdList::iterator it = node.inputList_.begin();
						it != node.inputList_.end(); ++it) {
					const uint32_t inputId = static_cast<uint32_t>(
							it - node.inputList_.begin());
					inputRevList[*it].insert(InputRevEntry(destId, inputId));
				}
			}
		}

		for (TargetList::iterator targetIt = targetList.begin();
				targetIt != targetList.end(); ++targetIt) {
			const PlanNodeId orgId = targetIt->first;
			const PlanNodeId newId = targetIt->second;

			if (isDisabledNode(plan.nodeList_[newId])) {
				assert(false);
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
			}

			InputRevSet &orgRevSet = inputRevList[orgId];
			for (InputRevSet::iterator revIt = orgRevSet.begin();
					revIt != orgRevSet.end(); ++revIt) {
				const PlanNodeId destId = revIt->first;
				const uint32_t destInput = revIt->second;

				PlanNode &destNode = plan.nodeList_[destId];

				if (isDisabledNode(destNode) ||
						destNode.inputList_[destInput] != orgId) {
					assert(false);
					SQL_COMPILER_THROW_ERROR(
							GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
				}

				destNode.inputList_[destInput] = newId;

				if (updatedSet[destId]) {
					continue;
				}

				updatedSet[destId] = true;
				updatedList.push_back(destId);
			}

			PlanNode &orgNode = plan.nodeList_[orgId];
			if (isDisabledNode(orgNode)) {
				assert(false);
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
			}

			setDisabledNodeType(orgNode);

			if (orgNode.inputList_ != plan.nodeList_[newId].inputList_) {
				assert(false);
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
			}

			for (PlanNode::IdList::iterator inIt = orgNode.inputList_.begin();
					inIt != orgNode.inputList_.end(); ++inIt) {
				InputRevSet &inRevSet = inputRevList[*inIt];
				const uint32_t inputId = static_cast<uint32_t>(
						inIt - orgNode.inputList_.begin());

				InputRevSet::iterator it =
						inRevSet.find(InputRevEntry(orgId, inputId));
				if (it == inRevSet.end()) {
					assert(false);
					SQL_COMPILER_THROW_ERROR(
							GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
				}
				inRevSet.erase(it);
				inRevSet.insert(InputRevEntry(newId, inputId));
			}

			inputRevList[newId].insert(orgRevSet.begin(), orgRevSet.end());
		}

		for (PlanNode::IdList::iterator it = updatedList.begin();
				it != updatedList.end(); ++it) {
			const uint32_t key = hashList[*it];
			PlanNode &node = plan.nodeList_[*it];

			typedef NodeMap::iterator MapIt;
			const std::pair<MapIt, MapIt> &range = nodeMap.equal_range(key);
			const NodeMap::value_type value(key, &node);
			MapIt mapIt = std::find(range.first, range.second, value);
			if (mapIt == range.second) {
				if (!isDisabledNode(plan.nodeList_[*it])) {
					assert(false);
					SQL_COMPILER_THROW_ERROR(
							GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
				}
				continue;
			}
			nodeMap.erase(mapIt);
		}

		assert(std::count(updatedSet.begin(), updatedSet.end(), true) ==
				static_cast<ptrdiff_t>(updatedList.size()));
		assert(!updatedList.empty());

		targetList.clear();
		found = true;
	}
	return found;
}

bool SQLCompiler::minimizeProjection(Plan &plan, bool subqueryAware) {
	typedef util::Vector<bool> CheckEntry;
	typedef util::Vector<CheckEntry*> CheckList;

	PlanNodeId resultId;
	bool unionFound;
	CheckList retainList(alloc_);
	CheckList modList(alloc_);

	initializeMinimizingTargets(plan, resultId, unionFound, retainList, modList);

	retainMinimizingTargetsFromResult(plan, resultId, retainList);

	fixMinimizingTargetNodes(plan, retainList, modList, subqueryAware);

	balanceMinimizingTargets(plan, unionFound, retainList, modList);

	return rewriteMinimizingTargets(plan, retainList, modList, subqueryAware);
}

bool SQLCompiler::pushDownPredicate(Plan &plan, bool subqueryAware) {
	bool found = false;

	const bool first = !predicatePushDownApplied_;
	if (first) {
		predicatePushDownApplied_ = true;
		found |= pushDownPredicate(plan, subqueryAware);
	}

	util::Vector<bool> localVisitedList(alloc_);
	util::Vector<bool> *visitedList = (first ? &localVisitedList : NULL);

	while (pushDownPredicateSub(plan, visitedList, subqueryAware)) {
		found = true;
	}

	return found;
}

bool SQLCompiler::pushDownPredicateSub(
		Plan &plan, util::Vector<bool> *visitedList, bool subqueryAware) {
	typedef std::pair<PlanNodeId, Expr*> Target;
	typedef util::Vector<Target> TargetList;

	util::Vector<uint32_t> nodeRefList(alloc_);
	const bool complex = (visitedList != NULL);

	TargetList targetList(alloc_);
	TargetList subTargetList(alloc_);
	bool found = false;

	for (PlanNodeList::iterator nodeIt = plan.nodeList_.begin();
			nodeIt != plan.nodeList_.end(); ++nodeIt) {
		PlanNode &node = *nodeIt;

		size_t wherePos;
		SQLType::JoinType joinType;
		if (node.type_ == SQLType::EXEC_JOIN) {
			if (node.joinType_ == SQLType::JOIN_FULL_OUTER ||
					node.predList_.size() < 2 ||
					(node.predList_.size() == 2 &&
							isTrueExpr(node.predList_[1])) ||
					(node.predList_.size() >= 3 &&
							isTrueExpr(node.predList_[1]) &&
							isTrueExpr(node.predList_[2]))) {
				continue;
			}
			wherePos = 1;
			joinType = node.joinType_;
		}
		else if (node.type_ == SQLType::EXEC_SELECT) {
			if (node.predList_.empty() ||
					node.predList_.size() < 1 ||
					isTrueExpr(node.predList_[0])) {
				continue;
			}
			wherePos = 0;
			joinType = SQLType::JOIN_INNER;
		}
		else {
			continue;
		}

		if (subqueryAware && isSubqueryResultPredicateNode(plan, node)) {
			continue;
		}

		const PlanNodeId nodeId = static_cast<PlanNodeId>(
				nodeIt - plan.nodeList_.begin());
		if (visitedList != NULL) {
			if (visitedList->empty()) {
				visitedList->resize(plan.nodeList_.size());
			}
			else if ((*visitedList)[nodeId]) {
				continue;
			}
			(*visitedList)[nodeId] = true;
		}

		for (PlanNode::IdList::iterator inIt = node.inputList_.begin();
				inIt != node.inputList_.end(); ++inIt) {
			const uint32_t inputId = static_cast<uint32_t>(
					inIt - node.inputList_.begin());

			Expr *predList[] = { NULL, NULL };
			Expr *condExprList[] = { NULL, NULL };
			for (size_t i = 0; i < 2; i++) {
				if (wherePos + i >= node.predList_.size()) {
					break;
				}

				const Mode mode = (i == 0 ? MODE_WHERE : MODE_ON);
				Expr &inExpr = node.predList_[wherePos + i];
				splitJoinPushDownCond(
						plan, mode, inExpr, predList[i], condExprList[i],
						inputId, joinType, complex);
			}

			Expr *condExpr;
			mergeAndOp(
					condExprList[0], condExprList[1], plan, MODE_WHERE,
					condExpr);
			if (condExpr == NULL) {
				continue;
			}

			targetList.clear();
			subTargetList.clear();

			{
				Expr *outExpr;
				dereferenceExpr(plan, node, inputId, *condExpr, outExpr, NULL);

				PlanNode &inNode = node.getInput(plan, inputId);
				refreshColumnTypes(
						plan, inNode, *outExpr, false, false, false,
						inNode.aggPhase_, NULL, NULL);
				refreshColumnTypes(
						plan, inNode, inNode.outputList_, true, false);

				targetList.push_back(Target(
						node.inputList_[inputId], outExpr));
			}

			if (nodeRefList.empty()) {
				makeNodeRefList(plan, nodeRefList);
			}

			bool resolvable;
			bool resolved;
			do {
				resolvable = true;
				resolved = true;
				for (TargetList::iterator it = targetList.begin();
						it != targetList.end(); ++it) {
					PlanNode &inNode = plan.nodeList_[it->first];
					if (getNodeAggregationLevel(inNode.outputList_) > 0 ||
							inNode.limit_ >= 0 || inNode.offset_ > 0 ||
							inNode.subLimit_ >= 0 ||
							nodeRefList[it->first] > 1) {
						resolvable = false;
						break;
					}

					if (inNode.type_ == SQLType::EXEC_JOIN ||
							inNode.type_ == SQLType::EXEC_SCAN ||
							inNode.type_ == SQLType::EXEC_SELECT) {
						subTargetList.push_back(*it);
					}
					else if (inNode.type_ == SQLType::EXEC_SORT ||
							inNode.type_ == SQLType::EXEC_UNION) {

						for (PlanNode::IdList::iterator subIt =
								inNode.inputList_.begin();
								subIt != inNode.inputList_.end(); ++subIt) {
							const uint32_t subInputId = static_cast<uint32_t>(
									subIt - inNode.inputList_.begin());
							Expr *outExpr;
							dereferenceExpr(
									plan, inNode, subInputId, *it->second,
									outExpr, NULL);

							PlanNode &subNode = inNode.getInput(plan, subInputId);
							refreshColumnTypes(
									plan, subNode, *outExpr, false, false, false,
									subNode.aggPhase_, NULL, NULL);

							subTargetList.push_back(Target(*subIt, outExpr));
						}
						resolved = false;
					}
					else {
						resolvable = false;
						break;
					}
				}
				targetList.swap(subTargetList);
				subTargetList.clear();
			}
			while (resolvable && !resolved);

			if (!resolvable) {
				continue;
			}

			for (size_t i = 0; i < 2; i++) {
				if (predList[i] != NULL) {
					node.predList_[wherePos + i] = *predList[i];
				}
			}

			for (TargetList::iterator it = targetList.begin();
					it != targetList.end(); ++it) {
				PlanNode &inNode = plan.nodeList_[it->first];

				const size_t condIndex =
						(inNode.type_ == SQLType::EXEC_JOIN ? 1 : 0);
				while (inNode.predList_.size() <= condIndex) {
					inNode.predList_.push_back(genConstExpr(
							plan, makeBoolValue(true), MODE_WHERE));
				}

				Expr &predExpr = inNode.predList_[condIndex];
				Expr *outExpr;
				mergeAndOp(
						ALLOC_NEW(alloc_) Expr(predExpr), it->second,
						plan, MODE_WHERE, outExpr);
				if (outExpr != NULL) {
					predExpr = *outExpr;
				}

				if (visitedList != NULL) {
					(*visitedList)[it->first] = false;
				}
			}
			found = true;
		}
	}

	return found;
}

bool SQLCompiler::pushDownAggregate(Plan &plan) {
	const size_t nodeCount = plan.nodeList_.size();

	util::Vector<uint32_t> nodeRefList(alloc_);
	PlanNode::IdList inList(alloc_);

	PlanExprList advanceOutList(alloc_);
	PlanExprList mergeOutList(alloc_);
	PlanExprList derefOutList(alloc_);

	PlanExprList advancePredList(alloc_);

	bool totalFound = false;
	for (PlanNodeId nodeId = 0; nodeId < nodeCount; nodeId++) {
		Type nodeType;
		bool predicateFound;
		AggregationPhase advancePhase;
		PlanNodeId unionNodeId;
		PlanNode *unionNode;
		{
			PlanNode &node = plan.nodeList_[nodeId];
			if (node.aggPhase_ != SQLType::AGG_PHASE_ALL_PIPE ||
					!(node.type_ == SQLType::EXEC_GROUP ?
							!node.predList_.empty() :
					node.type_ == SQLType::EXEC_SELECT ?
							getNodeAggregationLevel(node.outputList_) > 0 :
							false) ||
					node.inputList_.size() != 1) {
				continue;
			}

			if (isRangeGroupNode(node)) {
				continue;
			}

			unionNodeId = node.inputList_.front();
			unionNode = &plan.nodeList_[unionNodeId];
			if (unionNode->type_ != SQLType::EXEC_UNION ||
					unionNode->unionType_ != SQLType::UNION_ALL ||
					unionNode->limit_ >= 0 || unionNode->offset_ > 0) {
				continue;
			}

			if (nodeRefList.empty()) {
				makeNodeRefList(plan, nodeRefList);
			}

			if (nodeRefList[unionNodeId] != 1) {
				continue;
			}

			nodeType = node.type_;
			advancePredList = node.predList_;

			predicateFound =
					(nodeType == SQLType::EXEC_GROUP ||
					(nodeType == SQLType::EXEC_SELECT &&
							!(node.predList_.empty() ||
							isTrueExpr(node.predList_[0]))));

			advanceOutList.clear();
			mergeOutList.clear();
			node.predList_.clear();

			AggregationPhase mergePhase;
			splitExprList(
					node.outputList_, node.aggPhase_,
					advanceOutList, advancePhase, mergeOutList, mergePhase,
					(nodeType == SQLType::EXEC_GROUP ?
							&advancePredList : NULL),
					&node.predList_);
			if (mergePhase == SQLType::AGG_PHASE_ALL_PIPE) {
				continue;
			}

			totalFound = true;

			node.outputList_ = mergeOutList;
			node.aggPhase_ = mergePhase;

			inList = unionNode->inputList_;
		}

		for (PlanNode::IdList::const_iterator it = inList.begin();
				it != inList.end(); ++it) {
			const uint32_t inputId = static_cast<uint32_t>(it - inList.begin());
			const PlanNodeId inNodeId = *it;
			PlanNode &inNode = plan.nodeList_[inNodeId];

			PlanNode *splittedNode;
			if (!predicateFound && nodeRefList[inNodeId] == 1 &&
					(inNode.type_ == SQLType::EXEC_JOIN ||
							inNode.type_ == SQLType::EXEC_SCAN ||
							inNode.type_ == SQLType::EXEC_SELECT ||
							inNode.type_ == SQLType::EXEC_SORT) &&
					inNode.subLimit_ < 0 &&
					inNode.limit_ < 0 && inNode.offset_ == 0 &&
					inNode.aggPhase_ == SQLType::AGG_PHASE_ALL_PIPE &&
					getNodeAggregationLevel(inNode.outputList_) == 0) {

				derefOutList.clear();
				dereferenceExprList(
						plan, *unionNode, inputId, advanceOutList, derefOutList,
						advancePhase, NULL);

				inNode.outputList_ = derefOutList;
				inNode.aggPhase_ = advancePhase;
				splittedNode = &inNode;
			}
			else {
				PlanNode &advanceNode = plan.append(nodeType);
				advanceNode.inputList_.push_back(inNodeId);

				const bool shallow = (it == inList.begin());
				copyExprList(advancePredList, advanceNode.predList_, shallow);
				copyExprList(advanceOutList, advanceNode.outputList_, shallow);

				advanceNode.aggPhase_ = advancePhase;
				splittedNode = &advanceNode;

				unionNode = &plan.nodeList_[unionNodeId];
				unionNode->inputList_[inputId] = advanceNode.id_;
			}

			refreshColumnTypes(
					plan, *splittedNode, splittedNode->predList_, false,
					false);
			refreshColumnTypes(
					plan, *splittedNode, splittedNode->outputList_, true,
					false);

			if (inputId == 0) {
				unionNode->outputList_ =
						inputListToOutput(plan, *unionNode, &inputId);
			}
		}
		refreshColumnTypes(
				plan, *unionNode, unionNode->outputList_, true, false);
	}

	return totalFound;
}

bool SQLCompiler::pushDownJoin(Plan &plan) {
	JoinExpansionScoreList scoreList(alloc_);
	JoinExpansionScoreIdList scoreIdList(alloc_);
	bool scoreMade = false;

	util::Vector<uint32_t> nodeRefList(alloc_);
	util::Vector< std::pair<int64_t, bool> > nodeLimitList(alloc_);

	const PlanNode::IdList initalIdList(alloc_);
	const util::Vector<uint32_t> initalUnitList(alloc_);

	JoinNodeIdList joinInputList = { initalIdList, initalIdList };
	JoinNodeIdList metaInputList = { initalIdList, initalIdList };
	PlanNode::IdList unifiedIdList = initalIdList;

	PlanNode::IdList unificationList(alloc_);
	util::Vector<uint32_t> unifyingUnitList(alloc_);
	JoinExpansionList expansionList(alloc_);

	JoinExpansionInfo expansionInfo;
	expansionInfo.unificationList_ = &unificationList;
	expansionInfo.unifyingUnitList_ = &unifyingUnitList;
	expansionInfo.expansionList_ = &expansionList;

	bool totalFound = false;
	for (;;) {
		if (!scoreMade) {
			makeJoinExpansionScoreIdList(plan, scoreList, scoreIdList);
			scoreMade = true;
		}

		if (scoreIdList.empty()) {
			break;
		}

		if (nodeLimitList.empty()) {
			makeNodeLimitList(plan, nodeLimitList);
		}

		const PlanNodeId nodeId = scoreIdList.back().second;
		scoreIdList.pop_back();

		PlanNode &node = plan.nodeList_[nodeId];

		JoinBoolList unionList;
		if (!checkAndAcceptJoinExpansion(
				plan, node, nodeLimitList, joinInputList, unionList,
				expansionInfo)) {
			continue;
		}

		totalFound = true;
		scoreMade = false;

		if (nodeRefList.empty()) {
			makeNodeRefList(plan, nodeRefList);
		}

		const SQLType::JoinType joinType = node.joinType_;
		const PlanNode srcNode = node;

		cleanUpJoinExpansionInput(
				plan, srcNode, nodeId, nodeRefList, joinInputList, unionList,
				expansionInfo);

		makeJoinExpansionMetaInput(
				plan, joinType, nodeRefList, joinInputList, metaInputList,
				expansionInfo);

		makeJoinExpansionInputUnification(
				plan, joinType, joinInputList, metaInputList, unifiedIdList,
				expansionInfo);

		makeJoinExpansionEntries(
				plan, srcNode, nodeId, joinInputList, metaInputList,
				unifiedIdList, expansionInfo);

		nodeRefList.clear();
		nodeLimitList.clear();
	}

	return totalFound;
}

bool SQLCompiler::pushDownLimit(Plan &plan) {
	util::Vector<uint32_t> nodeRefList(alloc_);
	util::Vector< std::pair<int64_t, bool> > nodeLimitList(alloc_);

	bool found = false;
	bool foundLocal;
	do {
		foundLocal = false;
		for (PlanNodeList::iterator nodeIt = plan.nodeList_.begin();
				nodeIt != plan.nodeList_.end(); ++nodeIt) {
			PlanNode &node = *nodeIt;
			const PlanNodeId nodeId =
					static_cast<PlanNodeId>(nodeIt - plan.nodeList_.begin());

			if (node.limit_ < 0) {
				continue;
			}

			if (nodeLimitList.empty()) {
				makeNodeLimitList(plan, nodeLimitList);
			}

			bool limitChanged = false;
			do {
				if (nodeLimitList[nodeId].second) {
					node.limit_ = -1;
					limitChanged = true;
					break;
				}

				if (!isNodeLimitUnchangeable(node)) {
					break;
				}

				if (nodeRefList.empty()) {
					makeNodeRefList(plan, nodeRefList);
				}

				bool acceptable = true;
				for (PlanNode::IdList::iterator it = node.inputList_.begin();
						it != node.inputList_.end(); ++it) {
					PlanNode &inNode = plan.nodeList_[*it];

					if (nodeRefList[*it] > 1 || inNode.subLimit_ >= 0) {
						acceptable = false;
						break;
					}
				}

				int64_t total;
				if (!acceptable || !mergeLimit(node.limit_, -1, node.offset_, &total)) {
					break;
				}

				for (PlanNode::IdList::iterator it = node.inputList_.begin();
						it != node.inputList_.end(); ++it) {
					PlanNode &inNode = plan.nodeList_[*it];
					const int64_t inLimit = nodeLimitList[*it].first;

					int64_t limit = total;

					if (inLimit >= 0) {
						limit = std::min(inLimit, limit);
					}

					if (inLimit != limit) {
						inNode.limit_ = limit;
						limitChanged = true;
					}
				}

				if (node.inputList_.size() <= 1 && node.offset_ <= 0) {
					node.limit_ = -1;
				}
			}
			while (false);

			if (node.type_ == SQLType::EXEC_LIMIT &&
					node.limit_ < 0 && node.offset_ <= 0) {
				disableNode(plan, nodeId);

				nodeRefList.clear();
				limitChanged = true;
			}

			if (limitChanged) {
				nodeLimitList.clear();
				foundLocal = true;
			}
		}
		found |= foundLocal;
	}
	while (foundLocal);

	return found;
}

bool SQLCompiler::removeRedundancy(Plan &plan) {
	util::Vector<PlanNode*> inNodeList(alloc_);
	util::Vector<int64_t> inLimitList(alloc_);
	util::Vector<uint32_t> nodeRefList(alloc_);
	bool found = false;
	for (PlanNodeList::iterator nodeIt = plan.nodeList_.begin();
			nodeIt != plan.nodeList_.end(); ++nodeIt) {
		PlanNode &node = *nodeIt;

		if (node.type_ != SQLType::EXEC_SELECT ||
				node.inputList_.size() != 1 ||
				!(node.predList_.empty() || isTrueExpr(node.predList_[0])) ||
				node.offset_ > 0) {
			continue;
		}

		inNodeList.clear();
		PlanNode &baseInNode = node.getInput(plan, 0);

		const bool forUnionAll = (baseInNode.type_ == SQLType::EXEC_UNION &&
				baseInNode.unionType_ == SQLType::UNION_ALL);
		if (forUnionAll) {
			for (PlanNode::IdList::iterator inIt = baseInNode.inputList_.begin();
					inIt != baseInNode.inputList_.end(); ++inIt) {
				inNodeList.push_back(&plan.nodeList_[*inIt]);
			}
		}
		else {
			inNodeList.push_back(&baseInNode);
		}

		bool acceptable = true;

		bool sameOutput = true;
		for (util::Vector<PlanNode*>::iterator inNodeIt = inNodeList.begin();
				inNodeIt != inNodeList.end(); ++inNodeIt) {
			PlanNode &inNode = **inNodeIt;

			if (inNode.subLimit_ >= 0) {
				acceptable = false;
				break;
			}

			sameOutput &=
					(node.outputList_.size() == inNode.outputList_.size() &&
					node.aggPhase_ == SQLType::AGG_PHASE_ALL_PIPE);
			if (!sameOutput) {
				break;
			}

			for (PlanExprList::iterator it = node.outputList_.begin();
					it != node.outputList_.end(); ++it) {
				const ColumnId columnId =
						static_cast<ColumnId>(it - node.outputList_.begin());
				if (it->op_ == SQLType::START_EXPR ||
						!(it->op_ == SQLType::EXPR_COLUMN &&
						it->inputId_ == 0 && it->columnId_ == columnId)) {
					sameOutput = false;
					break;
				}
			}
		}

		if (!acceptable) {
			continue;
		}

		const bool multiInput = (inNodeList.size() > 1);
		if (inNodeList.size() > 1 &&
				node.aggPhase_ != SQLType::AGG_PHASE_MERGE_PIPE &&
				getNodeAggregationLevel(node.outputList_) > 0) {
			continue;
		}

		for (util::Vector<PlanNode*>::iterator inNodeIt = inNodeList.begin();
				inNodeIt != inNodeList.end(); ++inNodeIt) {
			PlanNode &inNode = **inNodeIt;

			if (!sameOutput && !isDereferenceableNode(
					inNode, &node.outputList_, node.aggPhase_, multiInput)) {
				acceptable = false;
				break;
			}
		}

		if (!acceptable) {
			continue;
		}

		if (nodeRefList.empty()) {
			makeNodeRefList(plan, nodeRefList);
		}

		inLimitList.clear();
		for (util::Vector<PlanNode*>::iterator inNodeIt = inNodeList.begin();;
				++inNodeIt) {
			const bool forBase = (inNodeIt == inNodeList.end());
			if (forBase && !forUnionAll) {
				break;
			}

			PlanNode &inNode = (forBase ? baseInNode : **inNodeIt);

			int64_t inLimit;
			if (nodeRefList[&inNode - &plan.nodeList_[0]] > 1) {
				acceptable = false;
				break;
			}

			if (forBase) {
				if (inNode.limit_ >= 0 || inNode.offset_ > 0) {
					acceptable = false;
				}
				break;
			}
			else if (!mergeLimit(node.limit_, inNode.limit_, node.offset_, &inLimit)) {
				acceptable = false;
				break;
			}
			inLimitList.push_back(inLimit);
		}

		if (!acceptable) {
			continue;
		}

		if (!sameOutput) {
			util::StackAllocator::Scope scope(alloc_);
			for (util::Vector<PlanNode*>::iterator inNodeIt = inNodeList.begin();
					inNodeIt != inNodeList.end(); ++inNodeIt) {
				PlanNode &inNode = **inNodeIt;

				const uint32_t inputId = 0;
				PlanExprList outputList(alloc_);
				dereferenceExprList(
						plan, node, inputId, node.outputList_, outputList,
						node.aggPhase_, NULL, &inNode);

				if (getNodeAggregationLevel(inNode.outputList_) > 0 &&
						getNodeAggregationLevel(outputList) == 0) {
					acceptable = false;
					break;
				}
			}
		}

		if (!acceptable) {
			continue;
		}

		if (!sameOutput) {
			for (util::Vector<PlanNode*>::iterator inNodeIt = inNodeList.begin();
					inNodeIt != inNodeList.end(); ++inNodeIt) {
				PlanNode &inNode = **inNodeIt;

				const uint32_t inputId = 0;
				PlanExprList outputList(alloc_);
				dereferenceExprList(
						plan, node, inputId, node.outputList_, outputList,
						node.aggPhase_, NULL, &inNode);

				if (node.aggPhase_ != SQLType::AGG_PHASE_ALL_PIPE) {
					assert(inNode.aggPhase_ == SQLType::AGG_PHASE_ALL_PIPE);
					inNode.aggPhase_ = node.aggPhase_;
				}

				refreshColumnTypes(plan, inNode, outputList, true, false);

				inNode.outputList_.swap(outputList);
			}
		}

		if (!sameOutput && forUnionAll) {
			const PlanExprList &outputList = mergeOutput(plan, &baseInNode);

			for (PlanNode::IdList::iterator inIt = node.inputList_.begin();
					inIt != node.inputList_.end(); ++inIt) {
				plan.nodeList_[*inIt].outputList_ = outputList;
			}
		}

		for (util::Vector<PlanNode*>::iterator inNodeIt = inNodeList.begin();
				inNodeIt != inNodeList.end(); ++inNodeIt) {
			PlanNode &inNode = **inNodeIt;

			inNode.limit_ = inLimitList[inNodeIt - inNodeList.begin()];
		}

		found = true;
		const PlanNodeId nodeId =
				static_cast<PlanNodeId>(nodeIt - plan.nodeList_.begin());
		disableNode(plan, nodeId);
		makeNodeRefList(plan, nodeRefList);
	}

	return found;
}

bool SQLCompiler::removeInputRedundancy(Plan &plan) {
	util::Vector<uint32_t> nodeRefList(alloc_);
	bool found = false;
	for (PlanNodeList::iterator nodeIt = plan.nodeList_.begin();
			nodeIt != plan.nodeList_.end(); ++nodeIt) {
		PlanNode &node = *nodeIt;

		if (node.type_ != SQLType::EXEC_GROUP &&
				node.type_ != SQLType::EXEC_JOIN &&
				node.type_ != SQLType::EXEC_SORT) {
			continue;
		}

		for (PlanNode::IdList::iterator inIt = node.inputList_.begin();
				inIt != node.inputList_.end(); ++inIt) {
			PlanNode &inNode = plan.nodeList_[*inIt];
			if (inNode.type_ != SQLType::EXEC_SELECT ||
					inNode.inputList_.size() != 1 ||
					!(inNode.predList_.empty() ||
							(inNode.predList_.size() == 1 &&
							isTrueExpr(inNode.predList_.front()))) ||
					inNode.aggPhase_ != SQLType::AGG_PHASE_ALL_PIPE ||
					getNodeAggregationLevel(inNode.outputList_) > 0 ||
					inNode.limit_ >= 0 || inNode.offset_ > 0) {
				continue;
			}

			const uint32_t inputId =
					static_cast<uint32_t>(inIt - node.inputList_.begin());

			if (!isInputDereferenceable(
					plan, node, inputId, node.predList_, false)) {
				continue;
			}

			if (!isInputDereferenceable(
					plan, node, inputId, node.outputList_, true)) {
				continue;
			}

			found = true;

			util::Map<uint32_t, uint32_t> inputIdMap(alloc_);
			const uint32_t inCount =
					static_cast<uint32_t>(node.inputList_.size());
			for (uint32_t i = 0; i < inCount; i++) {
				const uint32_t mappedIn = (i == inputId ?
						std::numeric_limits<uint32_t>::max() : i);
				inputIdMap.insert(std::make_pair(i, mappedIn));
			}

			for (size_t i = 0; i < 2; i++) {
				PlanExprList &inExprList =
						(i > 0 ? node.outputList_ : node.predList_);
				PlanExprList exprList(alloc_);

				dereferenceExprList(
						plan, node, inputId, inExprList, exprList,
						node.aggPhase_, &inputIdMap);
				inExprList.swap(exprList);
			}

			if (nodeRefList.empty()) {
				makeNodeRefList(plan, nodeRefList);
			}

			const PlanNodeId inNodeId =
					static_cast<PlanNodeId>(&inNode - &plan.nodeList_[0]);
			*inIt = inNode.inputList_.front();

			if (nodeRefList[inNodeId] <= 1) {
				setDisabledNodeType(inNode);
			}

			for (size_t i = 0; i < 2; i++) {
				const bool forOutput = (i > 0);
				PlanExprList &inExprList =
						(forOutput ? node.outputList_ : node.predList_);
				refreshColumnTypes(plan, node, inExprList, forOutput, false);
			}

			makeNodeRefList(plan, nodeRefList);
		}
	}

	return found;
}

bool SQLCompiler::removeAggregationRedundancy(Plan &plan) {
	util::Vector<uint32_t> nodeRefList(alloc_);
	bool found = false;
	for (PlanNodeList::iterator nodeIt = plan.nodeList_.begin();
			nodeIt != plan.nodeList_.end(); ++nodeIt) {
		PlanNode &node = *nodeIt;

		if (node.type_ != SQLType::EXEC_GROUP ||
				node.aggPhase_ != SQLType::AGG_PHASE_MERGE_PIPE ||
				node.inputList_.size() != 1) {
			continue;
		}

		PlanNode &inNode = node.getInput(plan, 0);
		if (inNode.type_ != SQLType::EXEC_UNION ||
				inNode.unionType_ != SQLType::UNION_ALL ||
				inNode.inputList_.size() <= 1) {
			continue;
		}

		bool acceptable = true;
		for (PlanNode::IdList::iterator inIt = inNode.inputList_.begin();
				acceptable && inIt != inNode.inputList_.end(); ++inIt) {
			const PlanNode &advNode = plan.nodeList_[*inIt];
			if (advNode.type_ != SQLType::EXEC_GROUP ||
					advNode.aggPhase_ != SQLType::AGG_PHASE_ADVANCE_PIPE ||
					advNode.outputList_.size() != inNode.outputList_.size()) {
				acceptable = false;
				break;
			}

			PlanExprList::const_iterator baseIt = inNode.outputList_.begin();
			for (PlanExprList::const_iterator it = advNode.outputList_.begin();
					acceptable && it != advNode.outputList_.end();
					++baseIt, ++it) {
				if (ColumnTypeUtils::isNull(it->columnType_)) {
					acceptable = false;
					break;
				}
				if (it->columnType_ != baseIt->columnType_) {
					acceptable = false;
					break;
				}
			}
		}

		if (!acceptable) {
			continue;
		}

		makeNodeRefList(plan, nodeRefList);
		const PlanNodeId inNodeId =
				static_cast<PlanNodeId>(&inNode - &plan.nodeList_[0]);
		if (nodeRefList[inNodeId] != 1) {
			continue;
		}

		node.inputList_ = inNode.inputList_;
		disableNode(plan, inNodeId);
		makeNodeRefList(plan, nodeRefList);

		found = true;
	}

	return found;
}

bool SQLCompiler::removePartitionCondRedundancy(Plan &plan) {
	util::Vector<uint32_t> affinityRevList(alloc_);
	bool found = false;

	for (PlanNodeList::iterator nodeIt = plan.nodeList_.begin();
			nodeIt != plan.nodeList_.end(); ++nodeIt) {
		PlanNode &node = *nodeIt;
		if (node.type_ != SQLType::EXEC_SCAN) {
			continue;
		}

		const SQLTableInfo::IdInfo &idInfo = node.tableIdInfo_;
		const SQLTableInfo &tableInfo = tableInfoList_->resolve(idInfo.id_);
		if (tableInfo.partitioning_ == NULL ||
				idInfo.type_ != SQLType::TABLE_CONTAINER) {
			continue;
		}

		PlanExprList &predList = node.predList_;
		if (predList.empty()) {
			continue;
		}

		const int32_t subContainerId = idInfo.subContainerId_;
		ProcessorUtils::getTablePartitionAffinityRevList(tableInfo, affinityRevList);

		Expr &pred = predList.front();
		reduceTablePartitionCond(
				plan, pred, tableInfo, affinityRevList, subContainerId);

		found = true;
	}

	return found;
}

bool SQLCompiler::assignJoinPrimaryCond(Plan &plan, bool &complexFound) {
	const Expr trueExpr(genConstExpr(plan, makeBoolValue(true), MODE_ON));

	const bool complex = true;
	complexFound = false;

	util::Vector<uint32_t> nodeRefList(alloc_);

	bool found = false;
	const size_t nodeCount = plan.nodeList_.size();
	for (PlanNodeId nodeId = 0; nodeId < nodeCount; nodeId++) {
		PlanNode &node = plan.nodeList_[nodeId];

		if (node.type_ != SQLType::EXEC_JOIN) {
			continue;
		}

		PlanExprList *predList = &node.predList_;
		Expr *primaryCond;
		Expr *whereExpr;
		Expr *onExpr;
		if (splitJoinPrimaryCond(
				plan,
				(predList->size() > 1 ? (*predList)[1] : trueExpr),
				(predList->size() > 2 ? (*predList)[2] : trueExpr),
				primaryCond, whereExpr, onExpr, node.joinType_,
				(predList->size() > 0 ? &(*predList)[0] : &trueExpr),
				complex)) {

			PlanNodeRef nodeRef = toRef(plan, &node);

			for (size_t i = 0; i < 2; i++) {
				Expr *&opExpr =
						(i == 0 ? primaryCond->left_ : primaryCond->right_);

				if (opExpr->op_ == SQLType::EXPR_COLUMN) {
					continue;
				}
				uint32_t inputId;
				const bool single = findSingleInputExpr(*opExpr, inputId);
				if (!single) {
					SQL_COMPILER_THROW_ERROR(
							GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
				}

				const Expr &inExpr = *opExpr;
				dereferenceAndAppendExpr(
						plan, nodeRef, inputId, inExpr, opExpr, nodeRefList);
				predList = &nodeRef->predList_;
				if (profiler_ != NULL) {
					profiler_->stats_.complexJoinPrimaryCond_++;
				}
				complexFound = true;
			}

			predList->clear();
			predList->push_back(*primaryCond);

			const bool whereFound =
					(whereExpr != NULL && !isTrueExpr(*whereExpr));
			const bool onFound = (onExpr != NULL && !isTrueExpr(*onExpr));

			if (whereFound || onFound) {
				predList->push_back(*whereExpr);

				if (onFound) {
					predList->push_back(*onExpr);
				}
			}

			found = true;
		}
	}

	return found;
}

bool SQLCompiler::selectJoinDrivingLegacy(Plan &plan, bool checkOnly) {
	const size_t joinCount = JOIN_INPUT_COUNT;
	util::Vector<uint32_t> nodeRefList(alloc_);

	const PlanNode::CommandOptionFlag drivingMask = getJoinDrivingOptionMask();

	bool found = false;
	bool foundLocal = false;
	for (PlanNodeList::iterator nodeIt = plan.nodeList_.begin();; ++nodeIt) {
		if (nodeIt == plan.nodeList_.end()) {
			if (!foundLocal) {
				break;
			}
			found = true;
			foundLocal = false;
			nodeIt = plan.nodeList_.begin();
		}

		PlanNode &node = *nodeIt;

		if (node.type_ != SQLType::EXEC_JOIN ||
				node.joinType_ != SQLType::JOIN_INNER ||
				node.inputList_.size() != joinCount) {
			continue;
		}

		if (checkOnly && (node.cmdOptionFlag_ & drivingMask) != 0) {
			continue;
		}

		if (plan.hintInfo_) {
			if ((node.cmdOptionFlag_ & PlanNode::Config::CMD_OPT_JOIN_NO_INDEX) != 0) {
				node.cmdOptionFlag_ |=
						PlanNode::Config::CMD_OPT_JOIN_DRIVING_NONE_LEFT;
				continue;
			}
		}

		bool leadingList[] = {
				(node.cmdOptionFlag_ &
						PlanNode::Config::CMD_OPT_JOIN_LEADING_LEFT) != 0,
				(node.cmdOptionFlag_ &
						PlanNode::Config::CMD_OPT_JOIN_LEADING_RIGHT) != 0
		};

		const bool aggressiveList[] = { leadingList[0], leadingList[1] };

		const uint32_t start = (!leadingList[0] && leadingList[1] ? 1 : 0);
		PlanNode::CommandOptionFlag drivingFlags = 0;

		for (uint32_t i = 0; i < joinCount; i++) {
			const uint32_t smaller = (i + start) % joinCount;

			const uint32_t larger = (smaller == 0 ? 1 : 0);
			if (!findIndexedPredicate(plan, node, larger, checkOnly, NULL)) {
				continue;
			}

			if (!aggressiveList[smaller] &&
					!isNarrowingJoinInput(plan, node, smaller)) {
				continue;
			}

			if (checkOnly) {
				drivingFlags = PlanNode::Config::CMD_OPT_JOIN_DRIVING_SOME;
				if (i == 0) {
					drivingFlags |=
							PlanNode::Config::CMD_OPT_JOIN_DRIVING_NONE_LEFT;
				}
				foundLocal = true;
				break;
			}

			util::Map<uint32_t, uint32_t> inputIdMap(alloc_);
			inputIdMap[smaller] = 1;

			makeNodeRefList(plan, nodeRefList);
			const PlanNodeId largerId = node.inputList_[larger];
			PlanNode &largerNode = node.getInput(plan, larger);

			PlanExprList predList(alloc_);
			dereferenceExprList(
					plan, node, larger, node.predList_, predList,
					node.aggPhase_, &inputIdMap);
			if (predList.size() >= 2) {
				if (!isTrueExpr(predList[1])) {
					Expr *predExpr;
					mergeAndOp(
							ALLOC_NEW(alloc_) Expr(predList[0]),
							ALLOC_NEW(alloc_) Expr(predList[1]),
							plan, MODE_WHERE, predExpr);
					predList[0] = (predExpr == NULL ? genConstExpr(
							plan, makeBoolValue(true), MODE_WHERE) : *predExpr);
				}
				while (predList.size() > 1) {
					predList.pop_back();
				}
			}

			if (!largerNode.predList_.empty() &&
					!isTrueExpr(largerNode.predList_[0])) {
				Expr *predExpr;
				mergeAndOp(
						ALLOC_NEW(alloc_) Expr(predList[0]),
						ALLOC_NEW(alloc_) Expr(largerNode.predList_[0]),
						plan, MODE_WHERE, predExpr);
				if (predExpr != NULL) {
					predList[0] = *predExpr;
				}
			}

			PlanExprList outList(alloc_);
			dereferenceExprList(
					plan, node, larger, node.outputList_, outList,
					node.aggPhase_, &inputIdMap);

			node.type_ = SQLType::EXEC_SCAN;
			node.inputList_.erase(node.inputList_.begin() + larger);
			node.predList_.swap(predList);
			node.outputList_.swap(outList);
			node.tableIdInfo_ = largerNode.tableIdInfo_;
			node.indexInfoList_ = largerNode.indexInfoList_;
			node.cmdOptionFlag_ |= largerNode.cmdOptionFlag_;

			refreshColumnTypes(plan, node, node.predList_, false, false);
			refreshColumnTypes(plan, node, node.outputList_, true, false);

			if (nodeRefList[largerId] <= 1) {
				setDisabledNodeType(largerNode);
			}

			foundLocal = true;
			break;
		}

		if (checkOnly) {
			if (drivingFlags == 0) {
				drivingFlags |=
						PlanNode::Config::CMD_OPT_JOIN_DRIVING_NONE_LEFT;
			}
			node.cmdOptionFlag_ |= drivingFlags;
		}
	}

	return found;
}

bool SQLCompiler::makeScanCostHints(Plan &plan) {
	util::StackAllocator::Scope scope(alloc_);
	Plan modPlan(alloc_);
	copyPlan(plan, modPlan);

	CompilerSource src(this);
	CompileOption option(&src);
	SQLCompiler subCompiler(getVarContext(), &option);
	subCompiler.assignReducedScanCostHints(modPlan);

	return applyScanCostHints(modPlan, plan);
}

bool SQLCompiler::makeJoinPushDownHints(Plan &plan) {
	if (!findJoinPushDownHintingTarget(plan)) {
		return false;
	}

	util::StackAllocator::Scope scope(alloc_);
	Plan modPlan(alloc_);
	copyPlan(plan, modPlan);

	CompilerSource src(this);
	CompileOption option(&src);
	SQLCompiler subCompiler(getVarContext(), &option);

	*subCompiler.optimizationOption_ = makeJoinPushDownHintsOption();
	subCompiler.optimize(modPlan);

	return applyJoinPushDownHints(modPlan, plan);
}

bool SQLCompiler::factorizePredicate(Plan &plan) {
	bool found = false;
	for (PlanNodeList::iterator nodeIt = plan.nodeList_.begin();
			nodeIt != plan.nodeList_.end(); ++nodeIt) {
		if (isDisabledNode(*nodeIt)) {
			continue;
		}

		PlanExprList &predList = nodeIt->predList_;
		for (PlanExprList::iterator it = predList.begin();
				it != predList.end(); ++it) {
			Expr *outExpr;
			found |= factorizeExpr(*it, outExpr, NULL);

			if (outExpr != NULL) {
				*it = *outExpr;
			}
		}
	}
	return found;
}

void SQLCompiler::makeNodeNameList(
		const Plan &plan, const util::Vector<PlanNodeId> &nodeIdList,
		util::Vector<const util::String*> &nodeNameList) const {
	typedef util::Vector<PlanNodeId> PlanNodeIdList;
	PlanNodeIdList hintTableIdList(alloc_);
	for (PlanNodeIdList::const_iterator itr = nodeIdList.begin();
			itr != nodeIdList.end(); ++itr) {
		const PlanNode &node = plan.nodeList_[*itr];
		if (node.qName_ != NULL && node.qName_->table_ != NULL
				&& node.qName_->nsId_ == SyntaxTree::QualifiedName::TOP_NS_ID) {

			const SyntaxTree::QualifiedName &qName = *node.qName_;
			util::String *normName = ALLOC_NEW(alloc_) util::String(
					normalizeName(alloc_, qName.table_->c_str()));
			nodeNameList.push_back(normName);

		}
		else if (node.type_ == SQLType::EXEC_SCAN) {
			if (node.qName_ != NULL
					&& node.qName_->nsId_ != SyntaxTree::QualifiedName::TOP_NS_ID) {
				continue;
			}
			const SQLTableInfo& tableInfo = getTableInfoList().resolve(node.tableIdInfo_.id_);
			util::String *normName = ALLOC_NEW(alloc_) util::String(
					normalizeName(alloc_, tableInfo.tableName_.c_str()));
			nodeNameList.push_back(normName);

		}
	}
}

bool SQLCompiler::reorderJoin(Plan &plan, bool &tableCostAffected) {
	tableCostAffected = false;

	util::Vector<bool> acceptedRefList(plan.nodeList_.size(), false, alloc_);
	util::Vector<PlanNodeId> nodeIdList(alloc_);
	util::Vector<PlanNodeId> joinIdList(alloc_);
	util::Vector<JoinNode> joinNodeList(alloc_);
	util::Vector<JoinEdge> joinEdgeList(alloc_);
	util::Vector<JoinOperation> srcJoinOpList(alloc_);
	util::Vector<JoinOperation> destJoinOpList(alloc_);

	const bool legacy = isLegacyJoinReordering(plan);
	const bool profiling = (explainType_ != SyntaxTree::EXPLAIN_NONE);

	bool found = false;
	PlanNodeId tailNodeId = static_cast<PlanNodeId>(plan.nodeList_.size());
	while (makeJoinGraph(
			plan, tailNodeId, acceptedRefList, nodeIdList, joinIdList,
			joinNodeList, joinEdgeList, srcJoinOpList, legacy)) {

		util::Vector<SQLHintInfo::ParsedHint> hintList(alloc_);
		if (plan.hintInfo_) {
			util::Vector<const util::String*> nodeNameList(alloc_);
			makeNodeNameList(plan, nodeIdList, nodeNameList);
			plan.hintInfo_->findJoinHintList(nodeNameList, SQLHint::LEADING, hintList);
			if (!legacy) {
				const util::Vector<SQLHintInfo::ParsedHint> &symbolHints =
						plan.hintInfo_->getSymbolHintList();
				hintList.insert(
						hintList.end(), symbolHints.begin(), symbolHints.end());
			}
		}

		if (reorderJoinSub(
				alloc_, joinNodeList, joinEdgeList,
				srcJoinOpList, hintList, destJoinOpList, legacy, profiling)) {
			applyJoinOrder(
					plan, nodeIdList, joinIdList, joinNodeList, destJoinOpList);
			found = true;
		}

		addJoinReorderingProfile(
				plan, nodeIdList, joinIdList, joinNodeList, destJoinOpList);

		if (!destJoinOpList.empty()) {
			tableCostAffected |= destJoinOpList.back().tableCostAffected_;
		}


	}

	if (found) {
		plan.removeEmptyNodes();
	}

	return found;
}

void SQLCompiler::addJoinReorderingProfile(
		Plan &plan, const util::Vector<PlanNodeId> &nodeIdList,
		const util::Vector<PlanNodeId> &joinIdList,
		const util::Vector<JoinNode> &joinNodeList,
		const util::Vector<JoinOperation> &joinOpList) {
	if (joinOpList.empty() || joinIdList.empty()) {
		return;
	}

	SQLPreparedPlan::OptProfile::Join *profile = joinOpList.back().profile_;
	if (profile == NULL) {
		return;
	}

	profileJoinTables(plan, nodeIdList, joinNodeList, *profile);
	addOptimizationProfile(
			&plan, &plan.nodeList_[joinIdList.back()], profile);
}

void SQLCompiler::profileJoinTables(
		const Plan &plan, const util::Vector<PlanNodeId> &nodeIdList,
		const util::Vector<JoinNode> &joinNodeList,
		SQLPreparedPlan::OptProfile::Join &profile) const {
	SQLPreparedPlan::OptProfile::JoinOrdinal ordinal = 0;
	util::Vector<SQLPreparedPlan::OptProfile::JoinNode> *nodeList =
			ALLOC_NEW(alloc_) util::Vector<
					SQLPreparedPlan::OptProfile::JoinNode>(alloc_);

	util::Vector<JoinNode>::const_iterator noinNodeIt = joinNodeList.begin();
	for (util::Vector<PlanNodeId>::const_iterator it = nodeIdList.begin();
			it != nodeIdList.end(); ++it) {
		const PlanNode &node = plan.nodeList_[*it];

		nodeList->push_back(SQLPreparedPlan::OptProfile::JoinNode());
		SQLPreparedPlan::OptProfile::JoinNode &nodeProfile = nodeList->back();

		QualifiedName *qName = NULL;
		const char8_t *tableName = getJoinTableName(node);
		if (tableName != NULL) {
			qName = ALLOC_NEW(alloc_) QualifiedName(alloc_);
			qName->table_ = ALLOC_NEW(alloc_) util::String(tableName, alloc_);
		}

		nodeProfile.ordinal_ = ordinal;
		nodeProfile.qName_ = qName;

		if (noinNodeIt != joinNodeList.end()) {
			nodeProfile.approxSize_ = noinNodeIt->approxSize_;
			++noinNodeIt;
		}
		++ordinal;
	}

	profile.nodes_ = nodeList;
}

void SQLCompiler::dumpJoinTables(
		std::ostream &os, const Plan &plan,
		const util::Vector<PlanNodeId> &nodeIdList) const {
	for (util::Vector<PlanNodeId>::const_iterator it = nodeIdList.begin();
			it != nodeIdList.end(); ++it) {
		const PlanNode &node = plan.nodeList_[*it];

		if (it != nodeIdList.begin()) {
			os << ", ";
		}
		os << it - nodeIdList.begin() << ":";

		const char8_t *tableName = getJoinTableName(node);
		if (tableName != NULL) {
			os << tableName;
		}
	}
	os << std::endl;
}

void SQLCompiler::dumpJoinGraph(
		std::ostream &os,
		const util::Vector<JoinEdge> &joinEdgeList,
		const util::Vector<JoinOperation> &joinOpList) const {
	for (util::Vector<JoinEdge>::const_iterator it = joinEdgeList.begin();
			it != joinEdgeList.end(); ++it) {
		it->dump(os);
		os << std::endl;
	}

	for (util::Vector<JoinOperation>::const_iterator it = joinOpList.begin();
			it != joinOpList.end(); ++it) {
		it->dump(os);
		os << std::endl;
	}
}

const char8_t* SQLCompiler::getJoinTableName(const PlanNode &node) const {
	if (node.qName_ != NULL && node.qName_->table_ != NULL) {
		return node.qName_->table_->c_str();
	}
	else if (node.type_ == SQLType::EXEC_SCAN) {
		return getTableInfoList().resolve(
				node.tableIdInfo_.id_).tableName_.c_str();
	}
	else {
		return NULL;
	}
}

bool SQLCompiler::makeJoinGraph(
		const Plan &plan, PlanNodeId &tailNodeId,
		util::Vector<bool> &acceptedRefList,
		util::Vector<PlanNodeId> &nodeIdList,
		util::Vector<PlanNodeId> &joinIdList,
		util::Vector<JoinNode> &joinNodeList,
		util::Vector<JoinEdge> &joinEdgeList,
		util::Vector<JoinOperation> &joinOpList, bool legacy) {
	assert(acceptedRefList.size() == plan.nodeList_.size());

	nodeIdList.clear();
	joinIdList.clear();
	joinNodeList.clear();
	joinEdgeList.clear();
	joinOpList.clear();

	util::Vector<uint32_t> nodeRefList(alloc_);
	makeNodeRefList(plan, nodeRefList);

	util::Vector<PlanNodeId> predIdList(alloc_);

	typedef util::Map<PlanNodeId, JoinNodeId> NodeIdMap;
	NodeIdMap nodeIdMap(alloc_);

	util::Vector<int64_t> approxSizeList(alloc_);

	if (tailNodeId <= 0) {
		return false;
	}

	for (PlanNodeList::const_iterator nodeIt =
			plan.nodeList_.begin() + tailNodeId;
			nodeIt != plan.nodeList_.begin(); --nodeIt) {

		const PlanNodeId nodeId = --tailNodeId;
		const PlanNode &node = *(nodeIt - 1);

		const bool accepted = acceptedRefList[nodeId];
		acceptedRefList[nodeId] = true;

		if (accepted || node.type_ != SQLType::EXEC_JOIN) {
			continue;
		}

		do {
			if (!makeJoinNodes(
					plan, node, nodeRefList, acceptedRefList,
					nodeIdMap, nodeIdList, joinIdList, joinNodeList,
					joinOpList, true)) {
				break;
			}

			if (joinNodeList.size() <= 2) {
				break;
			}

			if (!legacy) {
				makeJoinNodesCost(plan, node, approxSizeList);
			}

			for (util::Vector<PlanNodeId>::const_iterator it = nodeIdList.begin();
					it != nodeIdList.end(); ++it) {

				 const JoinNodeId joinNodeId =
						static_cast<JoinNodeId>(it - nodeIdList.begin());
				 if (!approxSizeList.empty()) {
					joinNodeList[joinNodeId].approxSize_ = approxSizeList[*it];
				 }

			 }
			for (util::Vector<PlanNodeId>::iterator joinIt = joinIdList.begin();
					joinIt != joinIdList.end(); ++joinIt) {
				const PlanNode joinPlanNode = plan.nodeList_[*joinIt];
				const PlanExprList &joinPredList = joinPlanNode.predList_;

				for (PlanExprList::const_iterator it = joinPredList.begin();
						it != joinPredList.end(); ++it) {
					if (!isTrueExpr(*it)) {
						makeJoinEdges(
								plan, joinPlanNode, *it, nodeIdMap,
								joinNodeList, joinEdgeList);
					}
				}
			}
			{
				const PlanNode *predNode =
						findJoinPredNode(plan, nodeId, predIdList);
				if (predNode != NULL) {
					makeJoinEdges(
							plan, *predNode, predNode->predList_.front(),
							nodeIdMap, joinNodeList, joinEdgeList);
				}
			}
			if (joinEdgeList.empty() && legacy) {
				break;
			}

			const size_t joinNodeCount = joinNodeList.size();
			const JoinNodeId gap = static_cast<JoinNodeId>(
					plan.nodeList_.size() - joinNodeCount);
			for (util::Vector<JoinOperation>::iterator it = joinOpList.begin();
					it != joinOpList.end(); ++it) {
				for (size_t i = 0; i < 2; i++) {
					if (it->nodeIdList_[i] >= joinNodeCount) {
						it->nodeIdList_[i] -= gap;
					}
				}
			}

			return true;
		}
		while (false);

		nodeIdMap.clear();
		nodeIdList.clear();
		joinIdList.clear();
		joinNodeList.clear();
		joinEdgeList.clear();
		joinOpList.clear();
	}

	return false;
}

bool SQLCompiler::makeJoinNodes(
		const Plan &plan, const PlanNode &node,
		const util::Vector<uint32_t> &nodeRefList,
		util::Vector<bool> &acceptedRefList,
		util::Map<PlanNodeId, JoinNodeId> &nodeIdMap,
		util::Vector<PlanNodeId> &nodeIdList,
		util::Vector<PlanNodeId> &joinIdList,
		util::Vector<JoinNode> &joinNodeList,
		util::Vector<JoinOperation> &joinOpList, bool top) {

	if (node.type_ != SQLType::EXEC_JOIN ||
			node.joinType_ != SQLType::JOIN_INNER ||
			node.inputList_.size() != 2) {
		return false;
	}

	if (!top && node.predList_.size() > 2) {
		for (PlanExprList::const_iterator it = node.predList_.begin() + 2;
				it != node.predList_.end(); ++it) {
			if (!isTrueExpr(*it)) {
				return false;
			}
		}
	}

	const PlanNodeId nodeId =
			static_cast<PlanNodeId>(&node - &plan.nodeList_[0]);
	if (!top) {
		if (nodeRefList[nodeId] != 1) {
			return false;
		}

		if (acceptedRefList[nodeId]) {
			assert(false);
			SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
		}
		acceptedRefList[nodeId] = true;
	}

	JoinOperation joinOp;
	for (PlanNode::IdList::const_iterator it = node.inputList_.begin();
			it != node.inputList_.end(); ++it) {
		const PlanNode &inNode = plan.nodeList_[*it];
		JoinNodeId joinNodeId;
		if (makeJoinNodes(
				plan, inNode, nodeRefList, acceptedRefList,
				nodeIdMap, nodeIdList, joinIdList, joinNodeList,
				joinOpList, false)) {
			joinNodeId =
					static_cast<JoinNodeId>(plan.nodeList_.size()) +
					static_cast<JoinNodeId>(joinOpList.size()) - 1;
		}
		else {
			joinNodeId = static_cast<JoinNodeId>(joinNodeList.size());
			joinNodeList.push_back(JoinNode(alloc_));

			nodeIdMap.insert(std::make_pair(*it, joinNodeId));
			nodeIdList.push_back(*it);
		}
		joinOp.nodeIdList_[it - node.inputList_.begin()] = joinNodeId;
	}

	joinOpList.push_back(joinOp);
	joinIdList.push_back(nodeId);

	return true;
}

void SQLCompiler::makeJoinEdges(
		const Plan &plan, const PlanNode &node, const Expr &expr,
		const util::Map<PlanNodeId, JoinNodeId> &nodeIdMap,
		util::Vector<JoinNode> &joinNodeList,
		util::Vector<JoinEdge> &joinEdgeList) {

	const size_t startPos = joinEdgeList.size();
	makeJoinEdgesSub(plan, node, expr, nodeIdMap, joinEdgeList, false);

	for (util::Vector<JoinEdge>::iterator it = joinEdgeList.begin() + startPos;
			it != joinEdgeList.end();) {
		assert(it->nodeIdList_[0] <= it->nodeIdList_[1]);

		const JoinEdgeId edgeId =
				static_cast<JoinEdgeId>(it - joinEdgeList.begin());

		if (it->nodeIdList_[0] == std::numeric_limits<JoinNodeId>::max()) {
			it = joinEdgeList.erase(it);
			continue;
		}
		joinNodeList[it->nodeIdList_[0]].edgeIdList_.push_back(edgeId);

		if (it->nodeIdList_[1] != std::numeric_limits<JoinNodeId>::max()) {
			joinNodeList[it->nodeIdList_[1]].edgeIdList_.push_back(edgeId);
		}

		 ++it;
	}
}

void SQLCompiler::makeJoinNodesCost(
		const Plan &plan, const PlanNode &node,
		util::Vector<int64_t> &approxSizeList) {
	assert(approxSizeList.empty());
	const PlanNodeId nodeId =
			static_cast<PlanNodeId>(&node - &plan.nodeList_[0]);

	util::Vector< std::pair<int64_t, bool> > nodeLimitList(alloc_);
	makeNodeLimitList(plan, nodeLimitList);

	typedef std::pair<PlanNodeId, uint32_t> NodePathEntry;
	typedef util::Vector<NodePathEntry> NodePath;
	NodePath nodePath(alloc_);
	util::Vector<bool> visitedList(alloc_);
	nodePath.push_back(NodePathEntry(nodeId, 0));

	while (!nodePath.empty()) {
		const NodePathEntry &entry = nodePath.back();
		const uint32_t inputId = entry.second;
		nodePath.pop_back();

		const PlanNodeId nodeId = entry.first;
		const PlanNode &node = plan.nodeList_[nodeId];

		if (inputId < node.inputList_.size()) {
			nodePath.push_back(NodePathEntry(nodeId, inputId + 1));

			const PlanNodeId inId = node.inputList_[inputId];
			nodePath.push_back(NodePathEntry(inId, 0));
			continue;
		}

		if (nodeId >= visitedList.size()) {
			visitedList.resize(nodeId + 1, false);
		}
		if (visitedList[nodeId]) {
			continue;
		}
		visitedList[nodeId] = true;

		if (nodeId >= approxSizeList.size()) {
			approxSizeList.resize(nodeId + 1, -1);
		}
		int64_t &approxSize = approxSizeList[nodeId];

		approxSize = resolveJoinNodeApproxSize(node, plan.hintInfo_);

		if (approxSize < 0) {
			for (PlanNode::IdList::const_iterator it =
					node.inputList_.begin();
					it != node.inputList_.end(); ++it) {
				const int64_t sub = approxSizeList[*it];
				if (approxSize < 0) {
					approxSize = sub;
				}
				else if (sub >= 0) {
					if (node.type_ == SQLType::EXEC_UNION) {
						approxSize += sub;
					}
					else {
						approxSize = std::max(approxSize, sub);
					}
				}
			}
		}

		const int64_t limit = nodeLimitList[nodeId].first;
		if (limit >= 0 && (approxSize < 0 || approxSize >= limit)) {
			approxSize = limit;
		}
	}
}

int64_t SQLCompiler::resolveJoinNodeApproxSize(
		const PlanNode &node, const SQLHintInfo *hintInfo) {
	do {
		if (hintInfo == NULL) {
			break;
		}

		const char8_t *tableName = getJoinTableName(node);
		if (tableName == NULL) {
			break;
		}

		const SQLHintValue *value = hintInfo->findStatisticalHint(
				SQLHint::TABLE_ROW_COUNT, tableName);
		if (value == NULL) {
			break;
		}

		return value->getInt64();
	}
	while (false);

	if (node.type_ == SQLType::EXEC_SCAN) {
		return node.tableIdInfo_.approxSize_;
	}

	return -1;
}

void SQLCompiler::makeJoinEdgesSub(
		const Plan &plan, const PlanNode &node, const Expr &expr,
		const util::Map<PlanNodeId, JoinNodeId> &nodeIdMap,
		util::Vector<JoinEdge> &joinEdgeList, bool negative) {

	if (expr.op_ == SQLType::OP_NOT) {
		makeJoinEdgesSub(
				plan, node, *expr.left_, nodeIdMap, joinEdgeList, !negative);
		return;
	}

	const Type type = ProcessorUtils::getLogicalOp(expr.op_, negative);
	if (type == SQLType::EXPR_AND) {
		makeJoinEdgesSub(
				plan, node, *expr.left_, nodeIdMap, joinEdgeList, negative);
		makeJoinEdgesSub(
				plan, node, *expr.right_, nodeIdMap, joinEdgeList, negative);
	}
	else if (type == SQLType::EXPR_OR) {
		const size_t orStartPos = joinEdgeList.size();
		makeJoinEdgesSub(
				plan, node, *expr.left_, nodeIdMap, joinEdgeList, negative);
		const size_t orEndPos = joinEdgeList.size();

		makeJoinEdgesSub(
				plan, node, *expr.right_, nodeIdMap, joinEdgeList, negative);

		mergeJoinEdges(joinEdgeList, orStartPos, orEndPos);
	}
	else {
		makeJoinEdgeElement(
				plan, node, expr, nodeIdMap, joinEdgeList, negative);
	}
}

void SQLCompiler::mergeJoinEdges(
		util::Vector<JoinEdge> &joinEdgeList,
		size_t orStartPos, size_t orEndPos) {
	typedef util::Vector<JoinEdge>::iterator JoinEdgeIterator;

	JoinEdgeIterator totalBegin = joinEdgeList.begin();
	JoinEdgeIterator totalEnd = joinEdgeList.end();

	JoinEdgeIterator destBegin = totalBegin + orStartPos;
	JoinEdgeIterator destEnd = totalBegin + orEndPos;
	JoinEdgeIterator destIt = destBegin;

	for (JoinEdgeIterator srcIt = destEnd; srcIt != totalEnd; ++srcIt) {
		if (destIt == destEnd) {
			break;
		}

		bool typeMatched = false;
		bool columnMatched = false;
		JoinEdgeIterator matchedIt = destEnd;
		for (JoinEdgeIterator it = destIt; it != destEnd; ++it) {
			const bool typeMatchedSub = (it->type_ == srcIt->type_);

			bool matched = true;
			bool columnMatchedSub = typeMatchedSub;
			for (size_t i = 0; i < 2; i++) {
				if (it->nodeIdList_[i] != srcIt->nodeIdList_[i]) {
					matched = false;
					break;
				}
				if (it->columnIdList_[i] != srcIt->columnIdList_[i]) {
					columnMatchedSub = false;
				}
			}

			if (matched) {
				if (matchedIt != destEnd) {
					if (typeMatched && !typeMatchedSub) {
						continue;
					}
					if (columnMatched && !columnMatchedSub) {
						continue;
					}
				}

				matchedIt = it;
				typeMatched = typeMatchedSub;
				columnMatched = columnMatchedSub;

				if (columnMatched && !it->simple_ == !srcIt->simple_) {
					break;
				}
			}
		}

		if (matchedIt != destEnd) {
			if (!columnMatched || !matchedIt->simple_ || !srcIt->simple_) {
				if (!typeMatched) {
					matchedIt->type_ = SQLType::START_EXPR;
				}

				for (size_t i = 0; i < 2; i++) {
					if (!columnMatched) {
						matchedIt->columnIdList_[i] = UNDEF_COLUMNID;
					}
				}

				matchedIt->simple_ = false;
				matchedIt->weakness_ += srcIt->weakness_;
				++matchedIt->weakness_;

				normalizeJoinEdge(*matchedIt);
			}

			if (matchedIt != destIt) {
				std::swap(*matchedIt, *destIt);
			}

			++destIt;
		}
	}

	joinEdgeList.erase(destIt, totalEnd);
}

void SQLCompiler::makeJoinEdgeElement(
		const Plan &plan, const PlanNode &node, const Expr &expr,
		const util::Map<PlanNodeId, JoinNodeId> &nodeIdMap,
		util::Vector<JoinEdge> &joinEdgeList, bool negative) {

	Type type = expr.op_;
	const size_t lastPos = joinEdgeList.size();
	do {
		const Type negativeType = ProcessorUtils::negateCompOp(type);
		if (negativeType == type) {
			if (type != SQLType::FUNC_LIKE) {
				type = SQLType::START_EXPR;
			}
			listJoinEdgeColumn(plan, node, expr, nodeIdMap, joinEdgeList);
			break;
		}

		if (negative) {
			type = negativeType;
		}

		JoinEdge opList[2];
		size_t countList[2];

		listJoinEdgeColumn(plan, node, *expr.left_, nodeIdMap, joinEdgeList);
		countList[0] = mergeJoinEdgeInput(joinEdgeList, lastPos, &opList[0]);
		const size_t middlePos = joinEdgeList.size();

		listJoinEdgeColumn(plan, node, *expr.right_, nodeIdMap, joinEdgeList);
		countList[1] = mergeJoinEdgeInput(joinEdgeList, middlePos, &opList[1]);

		if (countList[0] != 1 || countList[1] != 1) {
			if (lastPos == middlePos) {
				type = ProcessorUtils::swapCompOp(type);
			}
			break;
		}

		if (opList[0].nodeIdList_[0] == opList[1].nodeIdList_[0]) {
			break;
		}

		JoinEdge destEdge = opList[0];

		destEdge.type_ = type;
		destEdge.nodeIdList_[1] = opList[1].nodeIdList_[0];
		destEdge.columnIdList_[1] = opList[1].columnIdList_[0];

		if (expr.left_->op_ == SQLType::EXPR_COLUMN &&
				expr.right_->op_ == SQLType::EXPR_COLUMN) {
			destEdge.simple_ = true;
		}

		joinEdgeList.resize(lastPos);
		normalizeJoinEdge(destEdge);
		joinEdgeList.push_back(destEdge);

		return;
	}
	while (false);

	if (joinEdgeList.size() > lastPos) {
		JoinEdge destEdge;

		const size_t inCount =
				mergeJoinEdgeInput(joinEdgeList, lastPos, &destEdge);
		do {
			if (inCount != 1) {
				break;
			}

			if (type == SQLType::FUNC_LIKE) {
				uint32_t inputId;
				if (findSingleInputExpr(*(*expr.next_)[0], inputId)) {
					break;
				}
				if (expr.next_->size() >= 3 &&
						findSingleInputExpr(*(*expr.next_)[2], inputId)) {
					break;
				}
			}

			destEdge.type_ = type;
		}
		while (false);

		joinEdgeList.resize(lastPos);
		normalizeJoinEdge(destEdge);
		joinEdgeList.push_back(destEdge);
	}
}

void SQLCompiler::normalizeJoinEdge(JoinEdge &edge) {
	if (edge.nodeIdList_[0] > edge.nodeIdList_[1] ||
			(edge.nodeIdList_[0] == edge.nodeIdList_[1] &&
					edge.columnIdList_[0] > edge.columnIdList_[1])) {
		edge.type_ = ProcessorUtils::swapCompOp(edge.type_);
		std::swap(edge.nodeIdList_[0], edge.nodeIdList_[1]);
		std::swap(edge.columnIdList_[0], edge.columnIdList_[1]);
	}
}

size_t SQLCompiler::mergeJoinEdgeInput(
		const util::Vector<JoinEdge> &joinEdgeList, size_t startPos,
		JoinEdge *destEdge) {
	const size_t maxInCount = 2;
	size_t inCount = 0;

	JoinEdge localEdge;
	for (util::Vector<JoinEdge>::const_iterator it =
			joinEdgeList.begin() + startPos; it != joinEdgeList.end(); ++it) {
		const JoinNodeId nodeId = it->nodeIdList_[0];

		JoinNodeId *nodeBegin = localEdge.nodeIdList_;
		JoinNodeId *nodeEnd = nodeBegin + inCount;
		JoinNodeId *nodeIt = std::find(nodeBegin, nodeEnd, nodeId);
		if (nodeIt == nodeEnd) {
			if (++inCount > maxInCount) {
				localEdge = JoinEdge();
				break;
			}
			assert(nodeIt < nodeBegin + maxInCount);
			*nodeIt = nodeId;
		}

		const ColumnId srcColumnId = it->columnIdList_[0];
		ColumnId &destColumnId = localEdge.columnIdList_[nodeIt - nodeBegin];
		if (destColumnId == UNDEF_COLUMNID) {
			destColumnId = srcColumnId;
		}
		else if (destColumnId != srcColumnId) {
			destColumnId = UNDEF_COLUMNID;
		}
	}

	if (destEdge != NULL) {
		*destEdge = localEdge;
	}

	return inCount;
}

void SQLCompiler::listJoinEdgeColumn(
		const Plan &plan, const PlanNode &node, const Expr &expr,
		const util::Map<PlanNodeId, JoinNodeId> &nodeIdMap,
		util::Vector<JoinEdge> &joinEdgeList) {

	if (expr.op_ == SQLType::EXPR_COLUMN) {
		PlanNodeId nodeId;
		ColumnId columnId;
		if (!followJoinColumnInput(
				plan, node, expr, nodeIdMap, nodeId, &columnId)) {
			return;
		}

		JoinEdge edge;
		edge.nodeIdList_[0] = nodeIdMap.find(nodeId)->second;
		edge.columnIdList_[0] = columnId;
		joinEdgeList.push_back(edge);
	}
	else {
		for (ExprArgsIterator<> it(expr); it.exists(); it.next()) {
			listJoinEdgeColumn(plan, node, it.get(), nodeIdMap, joinEdgeList);
		}
	}
}

bool SQLCompiler::followJoinColumnInput(
		const Plan &plan, const PlanNode &node, const Expr &expr,
		const util::Map<PlanNodeId, JoinNodeId> &nodeIdMap,
		PlanNodeId &resultNodeId, ColumnId *resultColumnId) {
	assert(node.type_ == SQLType::EXEC_JOIN ||
			node.type_ == SQLType::EXEC_SELECT);

	if (expr.op_ != SQLType::EXPR_COLUMN) {
		return false;
	}

	const PlanNodeId nodeId =
			static_cast<PlanNodeId>(&node - &plan.nodeList_[0]);
	assert(nodeIdMap.find(nodeId) == nodeIdMap.end());
	static_cast<void>(nodeId);

	const PlanNodeId inNodeId = node.inputList_[expr.inputId_];
	const PlanNode &inNode = plan.nodeList_[inNodeId];

	if (nodeIdMap.find(inNodeId) == nodeIdMap.end()) {
		const Expr &inExpr = inNode.outputList_[expr.columnId_];
		assert(inNode.type_ == SQLType::EXEC_JOIN);

		return followJoinColumnInput(
				plan, inNode, inExpr, nodeIdMap, resultNodeId, resultColumnId);
	}
	else {
		resultNodeId = inNodeId;

		if (resultColumnId != NULL) {
			assert(expr.columnId_ < inNode.outputList_.size());
			*resultColumnId = expr.columnId_;
		}

		return true;
	}
}

const SQLCompiler::PlanNode* SQLCompiler::findJoinPredNode(
		const Plan &plan, PlanNodeId joinNodeId,
		util::Vector<PlanNodeId> &predIdList) {
	if (predIdList.empty()) {
		predIdList.assign(
				plan.nodeList_.size(), std::numeric_limits<PlanNodeId>::max());

		for (PlanNodeList::const_iterator nodeIt = plan.nodeList_.begin();
				nodeIt != plan.nodeList_.end(); ++nodeIt) {
			const PlanNode &node = *nodeIt;

			if (node.type_ != SQLType::EXEC_SELECT) {
				continue;
			}

			if (node.predList_.empty() || isTrueExpr(node.predList_.front())) {
				continue;
			}

			if (node.inputList_.size() != 1) {
				continue;
			}

			const PlanNodeId inId = node.inputList_.front();
			if (plan.nodeList_[inId].type_ != SQLType::EXEC_JOIN) {
				continue;
			}

			predIdList[inId] =
					static_cast<PlanNodeId>(nodeIt - plan.nodeList_.begin());
		}
	}

	const PlanNodeId nodeId = predIdList[joinNodeId];
	if (nodeId >= plan.nodeList_.size()) {
		return NULL;
	}
	return &plan.nodeList_[nodeId];
}

void SQLCompiler::applyJoinOrder(
		Plan &plan, const util::Vector<PlanNodeId> &nodeIdList,
		const util::Vector<PlanNodeId> &joinIdList,
		const util::Vector<JoinNode> &joinNodeList,
		const util::Vector<JoinOperation> &joinOpList) {
	assert(!joinIdList.empty());
	const size_t joinCount = 2;

	util::Vector<PlanNodeId> restJoinIdList = joinIdList;

	const JoinNodeId joinNodeId = restJoinIdList.back();

	util::Map<PlanNodeId, JoinNodeId> nodeIdMap(alloc_);
	for (util::Vector<PlanNodeId>::const_iterator it = nodeIdList.begin();
			it != nodeIdList.end(); ++it) {
		nodeIdMap.insert(std::make_pair(
				*it, static_cast<JoinNodeId>(it - nodeIdList.begin())));
	}

	PlanNode::IdList inputList(alloc_);
	PlanExprList predList(alloc_);
	PlanExprList outList(alloc_);
	{
		PlanNode &joinNode = plan.nodeList_[joinNodeId];
		pullUpJoinPredicate(plan, joinNode, nodeIdMap);

		inputList = joinNode.inputList_;
		predList.swap(joinNode.predList_);
		outList.swap(joinNode.outputList_);
	}

	typedef util::Vector< std::pair<PlanNodeId, ColumnId> > SrcColumnMapEntry;
	typedef util::Vector<const SrcColumnMapEntry*> SrcColumnMap;
	SrcColumnMap srcColumnMap(alloc_);

	for (PlanNode::IdList::iterator inIt = inputList.begin();
			inIt != inputList.end(); ++inIt) {
		const PlanNodeId inNodeId = *inIt;
		SrcColumnMapEntry *entry =
				ALLOC_NEW(alloc_) SrcColumnMapEntry(alloc_);
		const PlanNode &inNode = plan.nodeList_[inNodeId];
		const bool atInNode = (nodeIdMap.find(inNodeId) != nodeIdMap.end());
		for (PlanExprList::const_iterator it = inNode.outputList_.begin();
				it != inNode.outputList_.end(); ++it) {
			PlanNodeId nodeId;
			ColumnId columnId;
			if (atInNode) {
				nodeId = inNodeId;
				columnId =
						static_cast<ColumnId>(it - inNode.outputList_.begin());
			}
			else if (!followJoinColumnInput(
					plan, inNode, *it, nodeIdMap, nodeId, &columnId)) {
				assert(false);
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
			}
			entry->push_back(std::make_pair(nodeId, columnId));
		}
		srcColumnMap.push_back(entry);
	}

	typedef util::Vector< std::pair<PlanNodeId, uint32_t> > WholeColumnList;
	WholeColumnList wholeColumnList(alloc_);

	const size_t joinOpIndex = resolveTopJoinOpIndex(joinNodeList, joinOpList);
	size_t midPos;
	if (applyJoinOrderSub(
			plan, nodeIdList, joinNodeList, joinOpList, wholeColumnList,
			restJoinIdList, joinOpIndex, midPos) != joinNodeId) {
		assert(false);
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	typedef util::Map<
			PlanNodeId, std::pair<uint32_t, ColumnId> > WholeColumnMap;
	WholeColumnMap wholeColumnMap(alloc_);

	ColumnId nextColumnList[joinCount] = { 0 };
	for (WholeColumnList::iterator it = wholeColumnList.begin();
			it != wholeColumnList.end(); ++it) {
		const uint32_t inputId = (it < wholeColumnList.begin() + midPos ? 0 : 1);
		wholeColumnMap[it->first] =
				std::make_pair(inputId, nextColumnList[inputId]);
		nextColumnList[inputId] += it->second;
	}

	typedef util::Vector< std::pair<uint32_t, ColumnId> > ColumnMapEntry;
	typedef util::Vector<const ColumnMapEntry*> ColumnMap;
	ColumnMap columnMap(alloc_);

	for (SrcColumnMap::const_iterator mapIt = srcColumnMap.begin();
			mapIt != srcColumnMap.end(); ++mapIt) {
		ColumnMapEntry *destEntry = ALLOC_NEW(alloc_) ColumnMapEntry(alloc_);
		for (SrcColumnMapEntry::const_iterator it = (*mapIt)->begin();
				it != (*mapIt)->end(); ++it) {
			const WholeColumnMap::mapped_type &wholeEntry =
					wholeColumnMap.find(it->first)->second;
			const uint32_t inputId = wholeEntry.first;
			const ColumnId columnId = wholeEntry.second + it->second;
			destEntry->push_back(std::make_pair(inputId, columnId));
		}
		columnMap.push_back(destEntry);
	}

	for (util::Vector<PlanNodeId>::iterator it = restJoinIdList.begin();
			it != restJoinIdList.end(); ++it) {
		setDisabledNodeType(plan.nodeList_[*it]);
	}

	{
		PlanNode &joinNode = plan.nodeList_[joinNodeId];
		for (PlanExprList::iterator it = predList.begin();
				it != predList.end(); ++it) {
			joinNode.predList_.push_back(remapJoinColumn(columnMap, *it));
		}

		joinNode.outputList_.clear();
		for (PlanExprList::iterator it = outList.begin();
				it != outList.end(); ++it) {
			joinNode.outputList_.push_back(remapJoinColumn(columnMap, *it));
		}
	}
}

SQLCompiler::PlanNodeId SQLCompiler::applyJoinOrderSub(
		Plan &plan, const util::Vector<PlanNodeId> &nodeIdList,
		const util::Vector<JoinNode> &joinNodeList,
		const util::Vector<JoinOperation> &joinOpList,
		util::Vector< std::pair<PlanNodeId, uint32_t> > &wholeColumnList,
		util::Vector<PlanNodeId> &restJoinIdList, size_t joinOpIndex,
		size_t &midPos) {
	const size_t joinCount = 2;
	const JoinOperation &joinOp = joinOpList[joinOpIndex];

	PlanNode *outNode;
	PlanNodeId outNodeId;
	if (restJoinIdList.empty()) {
		outNode = &plan.append(SQLType::EXEC_JOIN);
		outNodeId = outNode->id_;
	}
	else {
		outNodeId = restJoinIdList.back();
		outNode = &plan.nodeList_[outNodeId];
		restJoinIdList.pop_back();
	}

	PlanNodeId inList[joinCount];
	size_t localMidPos = 0;
	for (size_t i = 0; i < joinCount; i++) {
		const JoinNodeId id = joinOp.nodeIdList_[i];
		if (id < joinNodeList.size()) {
			const PlanNodeId nodeId = nodeIdList[id];
			const PlanNode &inNode = plan.nodeList_[nodeId];
			wholeColumnList.push_back(std::make_pair(
					nodeId, static_cast<uint32_t>(inNode.outputList_.size())));
			inList[i] = nodeId;
		}
		else {
			const size_t index = id - joinNodeList.size();
			inList[i] = applyJoinOrderSub(
					plan, nodeIdList, joinNodeList, joinOpList,
					wholeColumnList, restJoinIdList, index, midPos);
		}
		if (i == 0) {
			localMidPos = wholeColumnList.size();
		}
	}
	midPos = localMidPos;

	*outNode = PlanNode(alloc_);
	outNode->id_ = outNodeId;
	outNode->type_ = SQLType::EXEC_JOIN;

	outNode->inputList_.assign(inList, inList + joinCount);
	outNode->joinType_ = joinOp.type_;

	if (joinOp.hintType_ == JoinOperation::HINT_LEADING_TREE) {
		outNode->cmdOptionFlag_ |= PlanNode::Config::CMD_OPT_JOIN_LEADING_LEFT;
	}
	outNode->outputList_ = inputListToOutput(plan, *outNode, NULL);

	return outNodeId;
}

size_t SQLCompiler::resolveTopJoinOpIndex(
		const util::Vector<JoinNode> &joinNodeList,
		const util::Vector<JoinOperation> &joinOpList) {

	const size_t joinCount = 2;
	util::Vector<bool> joinNodeRefList(joinNodeList.size(), false, alloc_);
	util::Vector<bool> joinOpRefList(joinOpList.size(), false, alloc_);

	for (util::Vector<JoinOperation>::const_iterator it = joinOpList.begin();
			it != joinOpList.end(); ++it) {
		for (size_t i = 0; i < joinCount; i++) {
			const JoinNodeId joinNodeId = it->nodeIdList_[i];

			size_t pos;
			util::Vector<bool> *refList;
			if (joinNodeId < joinNodeList.size()) {
				pos = joinNodeId;
				refList = &joinNodeRefList;
			}
			else {
				pos = static_cast<size_t>(joinNodeId - joinNodeList.size());
				refList = &joinOpRefList;
			}

			if ((*refList)[pos]) {
				assert(false);
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
			}
			(*refList)[pos] = true;
		}
	}

	if (std::find(joinNodeRefList.begin(), joinNodeRefList.end(), false) !=
			joinNodeRefList.end()) {
		assert(false);
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	util::Vector<bool>::iterator joinOpRefIt =
			std::find(joinOpRefList.begin(), joinOpRefList.end(), false);
	if (joinOpRefIt == joinOpRefList.end() ||
			std::find(joinOpRefIt + 1, joinOpRefList.end(), false) !=
			joinOpRefList.end()) {
		assert(false);
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	return static_cast<size_t>(joinOpRefIt - joinOpRefList.begin());
}

SQLCompiler::Expr SQLCompiler::remapJoinColumn(
		const util::Vector<const util::Vector<
				std::pair<uint32_t, ColumnId> >*> &columnMap,
		const Expr &expr) {
	assert(expr.op_ != SQLType::START_EXPR);

	if (expr.op_ != SQLType::EXPR_COLUMN &&
			expr.left_ == NULL &&
			expr.right_ == NULL &&
			expr.next_ == NULL) {
		return expr;
	}

	Expr outExpr(expr);
	if (expr.op_ == SQLType::EXPR_COLUMN) {
		const std::pair<uint32_t, ColumnId> &entry =
				(*columnMap[expr.inputId_])[expr.columnId_];
		outExpr.inputId_ = entry.first;
		outExpr.columnId_ = entry.second;
	}

	if (expr.left_ != NULL) {
		outExpr.left_ = ALLOC_NEW(alloc_) Expr(
				remapJoinColumn(columnMap, *expr.left_));
	}

	if (expr.right_ != NULL) {
		outExpr.right_ = ALLOC_NEW(alloc_) Expr(
				remapJoinColumn(columnMap, *expr.right_));
	}

	if (expr.next_ != NULL) {
		outExpr.next_ = ALLOC_NEW(alloc_) SyntaxTree::ExprList(alloc_);
		outExpr.next_->reserve(expr.next_->size());

		for (SyntaxTree::ExprList::iterator it = expr.next_->begin();
				it != expr.next_->end(); ++it) {
			outExpr.next_->push_back(ALLOC_NEW(alloc_) Expr(
					remapJoinColumn(columnMap, **it)));
		}
	}

	return outExpr;
}

bool SQLCompiler::pullUpJoinPredicate(
		Plan &plan, PlanNode &node,
		const util::Map<PlanNodeId, JoinNodeId> &nodeIdMap) {

	const PlanNodeId nodeId =
			static_cast<PlanNodeId>(&node - &plan.nodeList_[0]);
	if (nodeIdMap.find(nodeId) != nodeIdMap.end()) {
		return false;
	}

	if (node.type_ != SQLType::EXEC_JOIN ||
			node.joinType_ != SQLType::JOIN_INNER) {
		assert(false);
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	Expr *whereExpr = (node.predList_.size() <= 1 ||
			isTrueExpr(node.predList_[1]) ? NULL : &node.predList_[1]);

	for (PlanNode::IdList::iterator inIt = node.inputList_.begin();
			inIt != node.inputList_.end(); ++inIt) {
		PlanNode &inNode = plan.nodeList_[*inIt];

		if (!pullUpJoinPredicate(plan, inNode, nodeIdMap)) {
			continue;
		}

		PlanExprList &inPredList = inNode.predList_;
		if (inPredList.size() <= 1) {
			continue;
		}

		Expr &inExpr = inPredList[1];
		if (isTrueExpr(inExpr)) {
			continue;
		}

		const uint32_t inputId =
				static_cast<uint32_t>(inIt - node.inputList_.begin());

		Expr *pulledExpr = ALLOC_NEW(alloc_) Expr(
				pullUpJoinColumn(plan, inNode, inputId, inExpr));

		Expr *orgWhereExpr = (whereExpr == NULL ?
				NULL : ALLOC_NEW(alloc_) Expr(*whereExpr));

		mergeAndOp(
				pulledExpr, orgWhereExpr,
				plan, MODE_WHERE, whereExpr);

		inExpr = genConstExpr(plan, makeBoolValue(true), MODE_WHERE);
	}

	do {
		if (node.predList_.empty()) {
			break;
		}

		Expr &inExpr = node.predList_[0];
		if (isTrueExpr(inExpr)) {
			break;
		}

		Expr *orgWhereExpr = (whereExpr == NULL ?
				NULL : ALLOC_NEW(alloc_) Expr(*whereExpr));

		mergeAndOp(
				ALLOC_NEW(alloc_) Expr(inExpr), orgWhereExpr,
				plan, MODE_WHERE, whereExpr);

		inExpr = genConstExpr(plan, makeBoolValue(true), MODE_WHERE);
	}
	while (false);

	if (whereExpr != NULL && whereExpr != &node.predList_[1]) {
		while (node.predList_.size() <= 1) {
			node.predList_.push_back(
					genConstExpr(plan, makeBoolValue(true), MODE_WHERE));
		}

		node.predList_[1] = *whereExpr;
	}

	return true;
}

SQLCompiler::Expr SQLCompiler::pullUpJoinColumn(
		const Plan &plan, const PlanNode &node, uint32_t inputId,
		const Expr &expr) {
	assert(expr.op_ != SQLType::START_EXPR);

	if (expr.op_ != SQLType::EXPR_COLUMN &&
			expr.left_ == NULL &&
			expr.right_ == NULL &&
			expr.next_ == NULL) {
		return expr;
	}

	Expr outExpr(expr);
	if (expr.op_ == SQLType::EXPR_COLUMN) {
		size_t columnId = 0;
		for (uint32_t i = 0; i < expr.inputId_; i++) {
			columnId += node.getInput(plan, i).outputList_.size();
		}
		columnId += expr.columnId_;

		const PlanExprList::const_iterator beginIt = node.outputList_.begin();
		const PlanExprList::const_iterator endIt = node.outputList_.end();

		PlanExprList::const_iterator it = endIt;
		do {
			if (columnId >= node.outputList_.size()) {
				break;
			}

			const PlanExprList::const_iterator middleIt = beginIt + columnId;
			for (it = middleIt;;) {
				if (it == endIt) {
					it = beginIt;
				}

				if (it->op_ == SQLType::EXPR_COLUMN &&
						it->inputId_ == expr.inputId_ &&
						it->columnId_ == expr.columnId_) {
					break;
				}

				if (++it == middleIt) {
					it = endIt;
					break;
				}
			}
		}
		while (false);

		if (it == endIt) {
			assert(false);
			SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
		}

		outExpr.inputId_ = inputId;
		outExpr.columnId_ = static_cast<ColumnId>(it - beginIt);
	}

	if (expr.left_ != NULL) {
		outExpr.left_ = ALLOC_NEW(alloc_) Expr(
				pullUpJoinColumn(plan, node, inputId, *expr.left_));
	}

	if (expr.right_ != NULL) {
		outExpr.right_ = ALLOC_NEW(alloc_) Expr(
				pullUpJoinColumn(plan, node, inputId, *expr.right_));
	}

	if (expr.next_ != NULL) {
		outExpr.next_ = ALLOC_NEW(alloc_) SyntaxTree::ExprList(alloc_);
		outExpr.next_->reserve(expr.next_->size());

		for (SyntaxTree::ExprList::iterator it = expr.next_->begin();
				it != expr.next_->end(); ++it) {
			outExpr.next_->push_back(ALLOC_NEW(alloc_) Expr(
					pullUpJoinColumn(plan, node, inputId, **it)));
		}
	}

	return outExpr;
}

uint64_t SQLCompiler::getMaxInputCount(const Plan &plan) {
	uint64_t count;
	getSingleHintValue(
			plan.hintInfo_, SQLHint::MAX_DEGREE_OF_TASK_INPUT,
			DEFAULT_MAX_INPUT_COUNT, count);
	return count;
}

uint64_t SQLCompiler::getMaxExpansionCount(const Plan &plan) {
	uint64_t count;
	getSingleHintValue(
			plan.hintInfo_, SQLHint::MAX_DEGREE_OF_EXPANSION,
			DEFAULT_MAX_EXPANSION_COUNT, count);
	return count;
}

int64_t SQLCompiler::getMaxGeneratedRows(const Plan &plan) {
	uint64_t count;
	const bool found = getSingleHintValue(
			plan.hintInfo_, SQLHint::MAX_GENERATED_ROWS,
			0, count);
	if (!found) {
		return -1;
	}
	return static_cast<int64_t>(count);
}

bool SQLCompiler::isLegacyJoinReordering(const Plan &plan) {
	if (plan.hintInfo_ != NULL) {
		if (plan.hintInfo_->hasHint(SQLHint::COST_BASED_JOIN)) {
			return false;
		}
		if (plan.hintInfo_->hasHint(SQLHint::NO_COST_BASED_JOIN)) {
			return true;
		}
	}

	if (isLegacyPlanning(plan, LEGACY_JOIN_REORDERING_VERSION)) {
		return true;
	}

	if (compileOption_->isCostBasedJoin()) {
		return false;
	}
	return true;
}

bool SQLCompiler::isLegacyJoinDriving(const Plan &plan) {
	if (plan.hintInfo_ != NULL) {
		if (plan.hintInfo_->hasHint(SQLHint::COST_BASED_JOIN_DRIVING)) {
			return false;
		}
		if (plan.hintInfo_->hasHint(SQLHint::NO_COST_BASED_JOIN_DRIVING)) {
			return true;
		}

		if (plan.hintInfo_->hasHint(SQLHint::COST_BASED_JOIN)) {
			return false;
		}
		if (plan.hintInfo_->hasHint(SQLHint::NO_COST_BASED_JOIN)) {
			return true;
		}
	}

	if (isLegacyPlanning(plan, LEGACY_JOIN_DRIVING_VERSION)) {
		return true;
	}

	if (compileOption_->isCostBasedJoinDriving()) {
		return false;
	}
	return true;
}

bool SQLCompiler::isLegacyPlanning(
		const Plan &plan, const SQLPlanningVersion &legacyVersion) {
	return plan.planningVersion_.compareTo(legacyVersion) <= 0;
}

SQLPlanningVersion SQLCompiler::getPlanningVersionHint(
		const SQLHintInfo *hintInfo) {
	const size_t valueCount = 3;
	const size_t minValueCount = 2;

	const SQLHint::Id hintId = SQLHint::LEGACY_PLAN;

	uint64_t valueList[valueCount] = { 0 };
	const bool found = (getMultipleHintValues(
			hintInfo, hintId, valueList, valueCount, minValueCount) > 0);

	SQLPlanningVersion version;
	if (found) {
		version = SQLPlanningVersion(
				static_cast<int32_t>(valueList[0]),
				static_cast<int32_t>(valueList[1]));
	}

	return version;
}

bool SQLCompiler::getSingleHintValue(
		const SQLHintInfo *hintInfo, SQLHint::Id hint, uint64_t defaultValue,
		uint64_t &value) {
	const size_t valueCount = 1;
	const size_t minValueCount = valueCount;

	uint64_t valueList[valueCount] = { defaultValue };
	const bool found = (getMultipleHintValues(
			hintInfo, hint, valueList, valueCount, minValueCount) > 0);
	value = valueList[0];

	return found;
}

size_t SQLCompiler::getMultipleHintValues(
		const SQLHintInfo *hintInfo, SQLHint::Id hint, uint64_t *valueList,
		size_t valueCount, size_t minValueCount) {
	size_t resultValueCount = 0;
	bool valid = true;

	do {
		if (hintInfo == NULL) {
			break;
		}

		util::StackAllocator::Scope scope(alloc_);

		if (hintInfo->getHintList(hint) == NULL) {
			break;
		}

		valid = false;

		util::Vector<TupleValue> hintValueList(alloc_);
		hintInfo->getHintValueList(hint, hintValueList);

		if (hintValueList.size() < minValueCount) {
			break;
		}

		for (size_t i = 0;; i++) {
			if (!(i < valueCount && i < hintValueList.size())) {
				valid = true;
				break;
			}

			const TupleValue &hintValue = hintValueList[i];
			if (hintValue.getType() != TupleList::TYPE_LONG) {
				break;
			}

			const int64_t srcValue = hintValue.get<int64_t>();
			if (srcValue < 0) {
				break;
			}

			valueList[i] = static_cast<uint64_t>(srcValue);
			resultValueCount++;
		}
	}
	while (false);

	if (!valid) {
		const char8_t *hintName = SQLHint::Coder()(hint);
		hintName = (hintName == NULL ? "" : hintName);

		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
				"Invalid value for Hint(" << hintName << ")");
	}

	return resultValueCount;
}


void SQLCompiler::initializeMinimizingTargets(
		const Plan &plan, PlanNodeId &resultId, bool &unionFound,
		util::Vector<util::Vector<bool>*> &retainList,
		util::Vector<util::Vector<bool>*> &modList) {
	typedef util::Vector<bool> CheckEntry;

	resultId = std::numeric_limits<PlanNodeId>::max();
	unionFound = false;

	const size_t nodeCount = plan.nodeList_.size();
	retainList.assign(nodeCount, NULL);
	modList.assign(nodeCount, NULL);

	for (PlanNodeList::const_iterator nodeIt = plan.nodeList_.begin();
			nodeIt != plan.nodeList_.end(); ++nodeIt) {
		const PlanNode &node = *nodeIt;

		if (isDisabledNode(node)) {
			continue;
		}

		if (node.type_ == SQLType::EXEC_UNION ||
				node.type_ == SQLType::EXEC_LIMIT) {
			unionFound = true;
		}

		const PlanNodeId nodeId =
				static_cast<PlanNodeId>(nodeIt - plan.nodeList_.begin());

		bool initial;
		if (node.type_ == SQLType::EXEC_RESULT) {
			resultId = nodeId;
			initial = true;
		}
		else {
			initial = false;
		}

		const size_t columnCount = node.outputList_.size();
		retainList[nodeId] =
				ALLOC_NEW(alloc_) CheckEntry(columnCount, initial, alloc_);
		modList[nodeId] =
				ALLOC_NEW(alloc_) CheckEntry(columnCount, false, alloc_);
	}

	if (resultId == std::numeric_limits<PlanNodeId>::max()) {
		assert(false);
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}
}

void SQLCompiler::retainMinimizingTargetsFromResult(
		const Plan &plan, PlanNodeId resultId,
		util::Vector<util::Vector<bool>*> &retainList) {
	util::StackAllocator::Scope scope(alloc_);

	util::Set<PlanNodeId> inputSet(alloc_);
	util::Set<PlanNodeId> subInputSet(alloc_);

	inputSet.insert(resultId);
	do {
		for (util::Set<PlanNodeId>::iterator it = inputSet.begin();
				it != inputSet.end(); ++it) {
			const PlanNode &node = plan.nodeList_[*it];

			checkReference(plan, node, node.predList_, retainList, false);
			checkReference(plan, node, node.outputList_, retainList, true);

			subInputSet.insert(
					node.inputList_.begin(), node.inputList_.end());
		}

		inputSet.swap(subInputSet);
		subInputSet.clear();
	}
	while (!inputSet.empty());
}

void SQLCompiler::fixMinimizingTargetNodes(
		const Plan &plan,
		util::Vector<util::Vector<bool>*> &retainList,
		util::Vector<util::Vector<bool>*> &modList,
		bool subqueryAware) {
	typedef util::Vector<bool> CheckEntry;

	for (PlanNodeList::const_iterator nodeIt = plan.nodeList_.begin();
			nodeIt != plan.nodeList_.end(); ++nodeIt) {
		const PlanNode &node = *nodeIt;

		const PlanNodeId nodeId =
				static_cast<PlanNodeId>(nodeIt - plan.nodeList_.begin());
		CheckEntry *retainEntry = retainList[nodeId];
		CheckEntry *modEntry = modList[nodeId];

		if (retainEntry == NULL || retainEntry->empty()) {
			continue;
		}

		assert(node.outputList_.size() == retainEntry->size());
		assert(node.outputList_.size() == modEntry->size());
		UNUSED_VARIABLE(modEntry);

		CheckEntry::iterator checkIt = retainEntry->end();
		if (node.type_ != SQLType::EXEC_GROUP || subqueryAware) {
			bool lastAggregated = false;
			bool nextAggregated = false;

			CheckEntry::iterator retainIt = retainEntry->begin();
			for (PlanExprList::const_iterator it = node.outputList_.begin();
					it != node.outputList_.end(); ++it, ++retainIt) {
				if (it->aggrNestLevel_ <= 0) {
					continue;
				}

				lastAggregated = true;
				if (*retainIt) {
					nextAggregated = true;
				}
				else {
					checkIt = retainIt;
				}
			}

			if (lastAggregated && !nextAggregated) {
				if (checkIt == retainEntry->end()) {
					assert(false);
					SQL_COMPILER_THROW_ERROR(
							GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
				}
			}
			else {
				checkIt = retainEntry->end();
			}
		}

		if (checkIt == retainEntry->end() &&
				std::find(retainEntry->begin(), retainEntry->end(), true) ==
						retainEntry->end()) {
			checkIt = retainEntry->end() - 1;
		}

		if (checkIt != retainEntry->end()) {
			CheckEntry *modEntry = modList[nodeId];
			assert(modEntry != NULL);

			*checkIt = true;
			(*modEntry)[checkIt - retainEntry->begin()] = true;
		}
	}
}

void SQLCompiler::balanceMinimizingTargets(
		const Plan &plan, bool unionFound,
		util::Vector<util::Vector<bool>*> &retainList,
		util::Vector<util::Vector<bool>*> &modList) {
	typedef util::Vector<bool> CheckEntry;
	typedef util::Vector<CheckEntry*> CheckList;

	bool unionAffected = unionFound;
	for (PlanNodeList::const_iterator nodeIt = plan.nodeList_.end();;
			++nodeIt) {
		if (nodeIt == plan.nodeList_.end()) {
			if (!unionAffected) {
				break;
			}
			unionAffected = false;
			nodeIt = plan.nodeList_.begin();
		}

		const PlanNode &node = *nodeIt;

		if (node.type_ != SQLType::EXEC_UNION &&
				node.type_ != SQLType::EXEC_LIMIT) {
			continue;
		}

		const PlanNodeId nodeId =
				static_cast<PlanNodeId>(nodeIt - plan.nodeList_.begin());

		bool checked = false;
		do {
			const bool forRetention = true;
			CheckList &list = retainList;
			const bool fixingValue = forRetention;

			CheckEntry *entry = list[nodeId];

			if (entry == NULL || entry->empty()) {
				break;
			}

			CheckEntry::iterator checkIt = entry->begin();
			for (PlanExprList::const_iterator it = node.outputList_.begin();
					it != node.outputList_.end(); ++it, ++checkIt) {
				const ptrdiff_t columnId = checkIt - entry->begin();

				bool diff = false;
				for (PlanNode::IdList::const_iterator inIt =
						node.inputList_.begin();
						inIt != node.inputList_.end(); ++inIt) {
					CheckEntry *inEntry = list[*inIt];
					if (*checkIt ^ (*inEntry)[columnId]) {
						diff = true;
						break;
					}
				}

				if (forRetention) {
					checked |= *checkIt;
				}

				if (!diff) {
					continue;
				}

				*checkIt = fixingValue;

				for (PlanNode::IdList::const_iterator inIt =
						node.inputList_.begin();
						inIt != node.inputList_.end(); ++inIt) {
					CheckEntry::iterator elemIt =
							list[*inIt]->begin() + columnId;
					if (*elemIt == fixingValue) {
						continue;
					}

					*elemIt = fixingValue;
					(*modList[*inIt])[columnId] = fixingValue;
				}

				unionAffected = true;
			}
		}
		while (false);

		if (!checked) {
			assert(false);
			SQL_COMPILER_THROW_ERROR(
					GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
		}
	}
}

bool SQLCompiler::rewriteMinimizingTargets(
		Plan &plan,
		const util::Vector<util::Vector<bool>*> &retainList,
		const util::Vector<util::Vector<bool>*> &modList,
		bool subqueryAware) {
	typedef util::Vector<bool> CheckEntry;

	bool rewrote = false;
	for (PlanNodeList::iterator nodeIt = plan.nodeList_.begin();
			nodeIt != plan.nodeList_.end(); ++nodeIt) {
		PlanNode &node = *nodeIt;

		const PlanNodeId nodeId =
				static_cast<PlanNodeId>(nodeIt - plan.nodeList_.begin());

		CheckEntry *retainEntry = retainList[nodeId];
		CheckEntry *modEntry = modList[nodeId];

		if (retainEntry == NULL || retainEntry->empty()) {
			continue;
		}

		assert(node.outputList_.size() == retainEntry->size());

		bool aggrMod = false;
		if (node.type_ != SQLType::EXEC_GROUP || subqueryAware) {
			bool lastAggregated = false;
			bool nextAggregated = false;

			CheckEntry::iterator retainIt = retainEntry->begin();
			CheckEntry::iterator modIt = modEntry->begin();
			for (PlanExprList::iterator it = node.outputList_.begin();
					it != node.outputList_.end(); ++it, ++retainIt, ++modIt) {
				if (it->aggrNestLevel_ <= 0) {
					continue;
				}

				lastAggregated = true;
				if (*retainIt && !(*modIt)) {
					nextAggregated = true;
				}
			}

			if (lastAggregated && !nextAggregated) {
				aggrMod = true;
			}
		}

		CheckEntry::iterator retainIt = retainEntry->begin();
		CheckEntry::iterator modIt = modEntry->begin();
		for (PlanExprList::iterator it = node.outputList_.begin();
				it != node.outputList_.end(); ++it, ++retainIt, ++modIt) {

			if (!(*retainIt)) {
				it->op_ = SQLType::START_EXPR;
			}
			else if (*modIt) {
				if (aggrMod) {
					const bool asCountAll = (it->op_ == SQLType::AGG_COUNT_ALL);

					ExprList *argList = NULL;
					if (node.aggPhase_ == SQLType::AGG_PHASE_MERGE_PIPE) {
						if (asCountAll && it->next_ != NULL && !it->next_->empty() &&
								it->next_->front()->op_ == SQLType::EXPR_CONSTANT &&
								it->next_->front()->columnType_ == TupleList::TYPE_LONG) {
							continue;
						}
						argList = ALLOC_NEW(alloc_) ExprList(alloc_);
						argList->push_back(ALLOC_NEW(alloc_) Expr(genConstExpr(
								plan, TupleValue(static_cast<int64_t>(0)),
								MODE_AGGR_SELECT)));
					}
					else {
						if (asCountAll) {
							continue;
						}
					}

					*it = genExprWithArgs(
							plan, SQLType::AGG_COUNT_ALL, argList,
							MODE_AGGR_SELECT, true, node.aggPhase_);
				}
				else if (node.type_ == SQLType::EXEC_GROUP ||
						node.type_ == SQLType::EXEC_JOIN ||
						node.type_ == SQLType::EXEC_SCAN ||
						node.type_ == SQLType::EXEC_SELECT ||
						node.type_ == SQLType::EXEC_SORT) {
					if (it->op_ == SQLType::EXPR_CONSTANT &&
							it->columnType_ == TupleList::TYPE_LONG) {
						continue;
					}
					*it = genConstExpr(
							plan, TupleValue(static_cast<int64_t>(0)),
							MODE_NORMAL_SELECT);
				}
				else if (node.type_ == SQLType::EXEC_UNION ||
						node.type_ == SQLType::EXEC_LIMIT) {
					if (it->srcId_ == -1 && it->qName_ == NULL &&
							it->columnType_ == TupleList::TYPE_LONG) {
						continue;
					}
					it->srcId_ = -1;
					it->qName_ = NULL;
					it->columnType_ = TupleList::TYPE_LONG;
				}
				else {
					continue;
				}
			}
			else {
				continue;
			}
			rewrote = true;
		}
	}

	return rewrote;
}

bool SQLCompiler::checkAndAcceptJoinExpansion(
		const Plan &plan, const PlanNode &node,
		const util::Vector< std::pair<int64_t, bool> > &nodeLimitList,
		JoinNodeIdList &joinInputList, JoinBoolList &unionList,
		JoinExpansionInfo &expansionInfo) {
	const uint32_t inCount = JOIN_INPUT_COUNT;
	const uint32_t left = JOIN_LEFT_INPUT;
	const uint32_t right = JOIN_RIGHT_INPUT;

	if (node.type_ != SQLType::EXEC_JOIN ||
			node.inputList_.size() != inCount ||
			node.aggPhase_ == SQLType::AGG_PHASE_MERGE_PIPE) {
		return false;
	}

	JoinBoolList noneList;
	for (uint32_t i = 0; i < inCount; i++) {
		const PlanNode &inNode = node.getInput(plan, i);

		noneList[i] = (nodeLimitList[node.inputList_[i]].first == 0);

		unionList[i] =
				(inNode.type_ == SQLType::EXEC_UNION &&
				inNode.unionType_ == SQLType::UNION_ALL &&
				inNode.limit_ < 0 && inNode.offset_ <= 0);

		if (unionList[i]) {
			joinInputList[i] = inNode.inputList_;
		}
		else {
			joinInputList[i].assign(1U, node.inputList_[i]);
		}
	}

	if (noneList[left] || noneList[right]) {
		JoinBoolList dividableList;
		if (!getJoinDividableList(node.joinType_, dividableList) ||
				(dividableList[left] && noneList[right]) ||
				(dividableList[right] && noneList[left])) {
			return false;
		}
	}

	if (!findJoinExpansion(plan, node, joinInputList, expansionInfo)) {
		return false;
	}

	acceptJoinExpansion(
			expansionInfo.extraCount_, expansionInfo.limitReached_);
	return true;
}

void SQLCompiler::cleanUpJoinExpansionInput(
		Plan &plan, const PlanNode &srcNode, PlanNodeId srcNodeId,
		util::Vector<uint32_t> &nodeRefList,
		JoinNodeIdList &joinInputList, const JoinBoolList &unionList,
		const JoinExpansionInfo &expansionInfo) {
	const uint32_t inCount = JOIN_INPUT_COUNT;

	const SQLType::JoinType joinType = srcNode.joinType_;

	setNodeAndRefDisabled(plan, srcNodeId, nodeRefList);

	for (uint32_t i = 0; i < inCount; i++) {
		if (!unionList[i]) {
			continue;
		}

		const PlanNodeId inNodeId = srcNode.inputList_[i];
		if (nodeRefList[inNodeId] > 0) {
			continue;
		}

		setNodeAndRefDisabled(plan, inNodeId, nodeRefList);
	}

	PlanNodeId lastDisabledList[inCount] = {
		UNDEF_PLAN_NODEID, UNDEF_PLAN_NODEID
	};

	for (uint32_t i = 0; i < inCount; i++) {
		if (isJoinExpansionUnmatchInputRequired(i, joinType)) {
			continue;
		}

		PlanNode::IdList &inList = joinInputList[i];
		for (PlanNode::IdList::iterator it = inList.end();
				it != inList.begin();) {
			if (nodeRefList[*(--it)] <= 0) {
				continue;
			}

			lastDisabledList[i] = *it;
			it = inList.erase(it);
		}
	}

	if (!expansionInfo.expansionList_->empty()) {
		return;
	}

	for (uint32_t i = 0; i < inCount; i++) {
		PlanNode::IdList &inList = joinInputList[i];
		if (!inList.empty()) {
			continue;
		}

		const PlanNodeId lastDisabled = lastDisabledList[i];
		if (lastDisabled == UNDEF_PLAN_NODEID) {
			continue;
		}

		inList.push_back(lastDisabled);
	}
}

void SQLCompiler::makeJoinExpansionMetaInput(
		Plan &plan, SQLType::JoinType joinType,
		const util::Vector<uint32_t> &nodeRefList,
		const JoinNodeIdList &joinInputList, JoinNodeIdList &metaInputList,
		const JoinExpansionInfo &expansionInfo) {
	const uint32_t inCount = JOIN_INPUT_COUNT;

	for (uint32_t i = 0; i < inCount; i++) {
		PlanNode::IdList &metaList = metaInputList[i];
		metaList.clear();

		bool metaOnly;
		if (!isJoinExpansionMetaInputRequired(
				i, joinType, joinInputList, metaOnly)) {
			continue;
		}

		const PlanNode::IdList &inList = joinInputList[i];
		for (PlanNode::IdList::const_iterator it = inList.begin();; ++it) {
			if (it == inList.end() && !metaOnly) {
				break;
			}

			PlanNodeId destNodeId;
			PlanNode *destNode = NULL;
			do {
				if (metaOnly || it == inList.end() || nodeRefList[*it] > 0) {
					break;
				}

				destNode = &plan.nodeList_[*it];
				if (destNode->subLimit_ >= 0) {
					destNode = NULL;
					break;
				}
			}
			while (false);

			if (destNode == NULL) {
				PlanNodeId inNodeId;
				if (it == inList.end()) {
					if (expansionInfo.expansionList_->empty()) {
						assert(false);
						SQL_COMPILER_THROW_ERROR(
								GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
					}
					const JoinExpansionEntry &entry =
							expansionInfo.expansionList_->back();
					const uint32_t keyId = (i == 0 ? entry.first : entry.second);
					inNodeId = (*expansionInfo.unificationList_)[
							(*expansionInfo.unifyingUnitList_)[keyId]];
				}
				else {
					inNodeId = *it;
				}
				destNode = &plan.append(SQLType::EXEC_LIMIT);
				destNode->inputList_.push_back(inNodeId);
				destNode->outputList_ = mergeOutput(plan, destNode);
				destNodeId = destNode->id_;
			}
			else {
				destNodeId = *it;
			}
			destNode->limit_ = 0;
			metaList.push_back(destNodeId);

			if (metaOnly) {
				break;
			}
		}
	}
}

void SQLCompiler::makeJoinExpansionInputUnification(
		Plan &plan, SQLType::JoinType joinType,
		const JoinNodeIdList &joinInputList, JoinNodeIdList &metaInputList,
		PlanNode::IdList &unifiedIdList,
		const JoinExpansionInfo &expansionInfo) {
	const uint32_t inCount = JOIN_INPUT_COUNT;

	unifiedIdList.clear();

	const util::Vector<uint32_t> &unitList = *expansionInfo.unifyingUnitList_;
	const PlanNode::IdList &unificationList = *expansionInfo.unificationList_;

	util::Vector<uint32_t>::const_iterator unitEnd =
			unitList.end() - (unitList.empty() ? 0 : 1);
	for (util::Vector<uint32_t>::const_iterator unitIt = unitList.begin();
			unitIt != unitEnd; ++unitIt) {
		const uint32_t unitCount = *(unitIt + 1) - *unitIt;
		PlanNodeId destNodeId;
		if (unitCount <= 1) {
			if (unitCount != 1) {
				assert(false);
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
			}
			destNodeId = unificationList[*unitIt];
		}
		else {
			PlanNode &destNode = plan.append(SQLType::EXEC_UNION);
			destNode.unionType_ = SQLType::UNION_ALL;
			destNode.inputList_.assign(
					unificationList.begin() + *unitIt,
					unificationList.begin() + *(unitIt + 1));
			destNode.outputList_ = mergeOutput(plan, &destNode);
			destNodeId = destNode.id_;
		}
		unifiedIdList.push_back(destNodeId);
	}

	for (uint32_t i = 0; i < inCount; i++) {
		const size_t unit = getJoinExpansionMetaUnificationCount(
				i, joinType, joinInputList, metaInputList);
		if (unit <= 1) {
			continue;
		}
		else if (unit > metaInputList[i].size()) {
			assert(false);
			SQL_COMPILER_THROW_ERROR(
					GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
		}

		PlanNode::IdList::iterator it = metaInputList[i].begin();

		PlanNode &destNode = plan.append(SQLType::EXEC_UNION);
		destNode.unionType_ = SQLType::UNION_ALL;
		destNode.inputList_.assign(it, it + unit);
		destNode.outputList_ = mergeOutput(plan, &destNode);

		metaInputList[i].push_back(destNode.id_);
	}
}

void SQLCompiler::makeJoinExpansionEntries(
		Plan &plan, const PlanNode &srcNode, PlanNodeId srcNodeId,
		const JoinNodeIdList &joinInputList,
		const JoinNodeIdList &metaInputList,
		const PlanNode::IdList &unifiedIdList,
		const JoinExpansionInfo &expansionInfo) {
	const SQLType::JoinType joinType = srcNode.joinType_;

	do {
		JoinExpansionEntry entry;

		bool single = false;
		for (size_t ordinal = 1;; ordinal--) {
			const bool found = getJoinExpansionTarget(
					joinType, joinInputList, metaInputList, unifiedIdList,
					expansionInfo, ordinal, entry);
			if (ordinal > 0) {
				if (found) {
					break;
				}
				single = true;
			}
			else {
				if (!found) {
					SQL_COMPILER_THROW_ERROR(
							GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
				}
				break;
			}
		}

		if (!single) {
			break;
		}

		assert(entry.first != UNDEF_PLAN_NODEID);
		assert(entry.second != UNDEF_PLAN_NODEID);

		PlanNode &destNode = plan.nodeList_[srcNodeId];
		destNode.inputList_[0] = entry.first;
		destNode.inputList_[1] = entry.second;
		return;
	}
	while (false);

	PlanExprList outExprList1(alloc_);
	PlanExprList outExprList2(alloc_);
	AggregationPhase outPhase1;
	AggregationPhase outPhase2;
	const bool splitted = splitExprList(
			srcNode.outputList_, srcNode.aggPhase_,
			outExprList1, outPhase1, outExprList2, outPhase2,
			NULL, NULL);

	const PlanNodeId beginId =
			static_cast<PlanNodeId>(plan.nodeList_.size());

	for (size_t ordinal = 0;; ordinal++) {
		JoinExpansionEntry entry;
		if (!getJoinExpansionTarget(
				joinType, joinInputList, metaInputList, unifiedIdList,
				expansionInfo, ordinal, entry)) {
			break;
		}
		assert(entry.first != UNDEF_PLAN_NODEID);
		assert(entry.second != UNDEF_PLAN_NODEID);

		PlanNode &destNode = plan.append(SQLType::EXEC_JOIN);
		const PlanNodeId destNodeId = destNode.id_;
		destNode = srcNode;

		destNode.id_ = destNodeId;
		destNode.inputList_[0] = entry.first;
		destNode.inputList_[1] = entry.second;
		copyExprList(srcNode.predList_, destNode.predList_, false);
		copyExprList(outExprList1, destNode.outputList_, false);
		destNode.aggPhase_ = outPhase1;

		refreshColumnTypes(
				plan, destNode, destNode.predList_, false, false);
		refreshColumnTypes(
				plan, destNode, destNode.outputList_, true, false);
	}

	const PlanNodeId endId =
			static_cast<PlanNodeId>(plan.nodeList_.size());

	const uint64_t maxInputCount =
			std::max<uint64_t>(getMaxInputCount(plan), 2);
	PlanNodeId subBeginId = beginId;
	PlanNodeId subEndId = endId;
	PlanNodeId unionNodeId;
	do {
		const bool last = (subEndId - subBeginId) <= maxInputCount;
		PlanNodeId id = subBeginId;
		do {
			PlanNode &destNode = !last || splitted ?
					plan.append(SQLType::EXEC_UNION) :
					plan.nodeList_[srcNodeId];
			const PlanNodeId destNodeId = destNode.id_;

			destNode = PlanNode(alloc_);
			destNode.id_ = destNodeId;
			destNode.type_ = SQLType::EXEC_UNION;
			destNode.unionType_ = SQLType::UNION_ALL;

			for (; id < subEndId; id++) {
				if (!last && destNode.inputList_.size() >= maxInputCount) {
					break;
				}
				destNode.inputList_.push_back(id);
			}
			destNode.outputList_ = mergeOutput(plan, &destNode);

			unionNodeId = destNodeId;
		}
		while (id < subEndId);

		subBeginId = subEndId;
		subEndId = static_cast<PlanNodeId>(plan.nodeList_.size());
	}
	while ((subEndId - subBeginId) > 1);

	if (splitted) {
		PlanNode &destNode = plan.nodeList_[srcNodeId];
		const PlanNodeId destNodeId = destNode.id_;

		destNode = PlanNode(alloc_);
		destNode.id_ = destNodeId;
		destNode.type_ = SQLType::EXEC_SELECT;

		destNode.inputList_.push_back(unionNodeId);
		destNode.outputList_ = outExprList2;
		destNode.aggPhase_ = outPhase2;
	}
}

bool SQLCompiler::findJoinExpansion(
		const Plan &plan, const PlanNode &node,
		JoinNodeIdList &joinInputList, JoinExpansionInfo &expansionInfo) {
	{
		bool neverFound = false;
		if (!expansionInfo.isEstimationOnly()) {
			JoinExpansionInfo subExpansionInfo;
			assert(subExpansionInfo.isEstimationOnly());

			neverFound = !findJoinExpansion(
					plan, node, joinInputList, subExpansionInfo);

			expansionInfo.reserve(subExpansionInfo);
		}

		expansionInfo.clear();

		if (neverFound) {
			return false;
		}
	}

	const TableNarrowingList &tableNarrowingList =
			prepareTableNarrowingList(plan);

	util::StackAllocator::Scope scope(alloc_);

	const NarrowingKeyTable emptyKeyTable(alloc_);
	JoinKeyTableList keyTableList = { emptyKeyTable, emptyKeyTable };
	bool joinCondNarrowable;
	if (!makeJoinNarrowingKeyTables(
			plan, node, joinInputList, tableNarrowingList, keyTableList,
			joinCondNarrowable)) {
		return joinCondNarrowable;
	}

	const util::Vector<uint32_t> uint32List(alloc_);
	const util::Vector<bool> boolList(alloc_);

	JoinKeyIdList joinKeyIdList = { uint32List, uint32List };
	JoinUnifyingUnitList unifyingUnitList = { uint32List, uint32List };
	JoinInputUsageList inputUsageList = { boolList, boolList };
	JoinBoolList singleUnitList;
	const bool splitted = splitJoinByReachableUnit(
			keyTableList, joinKeyIdList, unifyingUnitList, inputUsageList,
			singleUnitList);

	const uint32_t expansionLimit = getAcceptableJoinExpansionCount(plan);

	if (!tryMakeJoinAllUnitDividableExpansion(
			node.joinType_, node.cmdOptionFlag_, joinInputList, keyTableList,
			joinKeyIdList, unifyingUnitList, singleUnitList, expansionLimit,
			&expansionInfo)) {
		if (!splitted) {
			return false;
		}
		makeJoinAllUnitNonDividableExpansion(
				joinInputList, joinKeyIdList, unifyingUnitList, expansionInfo);
	}

	removeJoinExpansionMatchedInput(
			inputUsageList, joinInputList, expansionInfo);

	return true;
}

bool SQLCompiler::makeJoinNarrowingKeyTables(
		const Plan &plan, const PlanNode &node,
		const JoinNodeIdList &joinInputList,
		const TableNarrowingList &tableNarrowingList,
		JoinKeyTableList &keyTableList, bool &joinCondNarrowable) {
	const uint32_t inCount = JOIN_INPUT_COUNT;
	const uint32_t left = JOIN_LEFT_INPUT;
	const uint32_t right = JOIN_RIGHT_INPUT;

	joinCondNarrowable = false;

	typedef util::Vector< std::pair<ColumnId, ColumnId> > ColumnIdPairList;
	ColumnIdPairList columnIdPairList(alloc_);
	if (!getJoinCondColumns(node, columnIdPairList)) {
		const NarrowingKey fullKey;
		assert(fullKey.isFull());

		for (uint32_t i = 0; i < inCount; i++) {
			const PlanNode::IdList &idList = joinInputList[i];
			for (PlanNode::IdList::const_iterator it = idList.begin();
					it != idList.end(); ++it) {
				keyTableList[i].addKey(0, fullKey);
			}
		}
	}

	bool resolved = false;
	const uint32_t firstSide = getJoinSmallerSide(joinInputList);
	for (uint32_t i = firstSide;; i = getJoinAnotherSide(i)) {
		for (ColumnIdPairList::iterator columnIt = columnIdPairList.begin();
				columnIt != columnIdPairList.end(); ++columnIt) {
			const uint32_t columnId =
					(i == left ? columnIt->first : columnIt->second);
			const uint32_t columnIndex = static_cast<uint32_t>(
					columnIt - columnIdPairList.begin());

			const PlanNode::IdList &idList = joinInputList[i];
			for (PlanNode::IdList::const_iterator it = idList.begin();
					it != idList.end(); ++it) {
				const PlanNode &inNode = plan.nodeList_[*it];
				NarrowingKey key;
				resolved |= resolveNarrowingKey(
						plan, inNode, columnId, tableNarrowingList, key);
				keyTableList[i].addKey(columnIndex, key);
			}

			if (keyTableList[i].isNone()) {
				joinCondNarrowable = true;
				return false;
			}
		}
		if (i != firstSide) {
			break;
		}
	}

	const bool single =
			(keyTableList[left].getSize() == 1 &&
			keyTableList[right].getSize() == 1);
	if (!resolved && single) {
		joinCondNarrowable = false;
		return false;
	}

	if (single && keyTableList[left].getMatchCount(
			keyTableList[right], 0) == 1) {
		joinCondNarrowable = false;
		return false;
	}

	joinCondNarrowable = true;
	return true;
}

bool SQLCompiler::splitJoinByReachableUnit(
		JoinKeyTableList &keyTableList, JoinKeyIdList &joinKeyIdList,
		JoinUnifyingUnitList &unifyingUnitList,
		JoinInputUsageList &inputUsageList, JoinBoolList &singleUnitList) {
	const uint32_t inCount = JOIN_INPUT_COUNT;

	typedef NarrowingKeyTable::KeyIdList KeyIdList;

	for (uint32_t i = 0; i < inCount; i++) {
		joinKeyIdList[i].clear();
		unifyingUnitList[i].clear();
		singleUnitList[i] = true;
	}

	for (uint32_t i = 0; i < inCount; i++) {
		unifyingUnitList[i].push_back(0);
		inputUsageList[i].assign(keyTableList[i].getSize(), false);
	}

	const uint32_t smallerSide = getJoinSmallerSide(keyTableList);
	const uint32_t largerSide = getJoinAnotherSide(smallerSide);

	const uint32_t smallerKeyCount = keyTableList[smallerSide].getSize();
	for (uint32_t keyId = 0; keyId < smallerKeyCount; keyId++) {
		if (inputUsageList[smallerSide][keyId]) {
			continue;
		}

		size_t offsetList[inCount];
		for (uint32_t i = 0; i < inCount; i++) {
			offsetList[i] = joinKeyIdList[i].size();
		}

		joinKeyIdList[smallerSide].push_back(keyId);
		const bool shallow = false;
		if (!keyTableList[largerSide].findAll(
				keyTableList[smallerSide], offsetList[smallerSide],
				joinKeyIdList[smallerSide], joinKeyIdList[largerSide],
				shallow)) {
			continue;
		}

		for (uint32_t i = 0; i < inCount; i++) {
			const uint32_t prev = unifyingUnitList[i].back();
			unifyingUnitList[i].push_back(
					static_cast<uint32_t>(joinKeyIdList[i].size()));
			if (unifyingUnitList[i].back() - prev > 1) {
				singleUnitList[i] = false;
			}

			for (KeyIdList::iterator it =
					joinKeyIdList[i].begin() + offsetList[i];
					it != joinKeyIdList[i].end(); ++it) {
				assert(!inputUsageList[i][*it]);
				inputUsageList[i][*it] = true;
			}
		}
	}

	for (uint32_t i = 0; i < inCount; i++) {
		if (joinKeyIdList[i].size() < keyTableList[i].getSize() ||
				unifyingUnitList[i].size() > 2) {
			return true;
		}
	}

	return false;
}

bool SQLCompiler::tryMakeJoinAllUnitDividableExpansion(
		SQLType::JoinType joinType, PlanNode::CommandOptionFlag cmdOptionFlag,
		const JoinNodeIdList &joinInputList, JoinKeyTableList &keyTableList,
		const JoinKeyIdList &joinKeyIdList,
		const JoinUnifyingUnitList &unifyingUnitList,
		const JoinBoolList &singleUnitList, uint32_t expansionLimit,
		JoinExpansionInfo *expansionInfo) {
	const bool checkOnly = (expansionInfo == NULL);
	if (!checkOnly) {
		util::StackAllocator::Scope scope(alloc_);
		JoinExpansionInfo *infoForCheckOnly = NULL;

		const bool dividable = tryMakeJoinAllUnitDividableExpansion(
				joinType, cmdOptionFlag, joinInputList, keyTableList,
				joinKeyIdList, unifyingUnitList, singleUnitList,
				expansionLimit, infoForCheckOnly);
		if (!dividable) {
			return false;
		}
	}

	JoinBoolList dividableList;
	if (!checkJoinUnitDividable(joinType, singleUnitList, dividableList)) {
		return false;
	}

	JoinBoolList baseJoinOptList;
	const JoinBoolList *drivingOptList = getJoinUnitDrivingOptionList(
			cmdOptionFlag, dividableList, baseJoinOptList);

	if (drivingOptList == NULL && expansionLimit <= 0) {
		return false;
	}

	util::Vector<uint32_t> limitList(alloc_);
	util::Vector<bool> fullMatchList(alloc_);
	makeJoinAllUnitLimitList(
			keyTableList, joinKeyIdList, unifyingUnitList, expansionLimit,
			limitList, fullMatchList);

	JoinKeyIdList joinSubKeyIdList = {
		util::Vector<uint32_t>(alloc_), util::Vector<uint32_t>(alloc_)
	};
	JoinMatchCountList matchCountList(alloc_);
	util::Vector<uint32_t> subLimitList(alloc_);

	util::LocalUniquePtr<JoinExpansionBuildInfo> buildInfo;
	if (!checkOnly) {
		buildInfo = UTIL_MAKE_LOCAL_UNIQUE(
				buildInfo, JoinExpansionBuildInfo, alloc_, *expansionInfo);
	}

	bool dividable = false;
	for (util::Vector<uint32_t>::iterator it = limitList.begin();
			it != limitList.end(); ++it) {
		const uint32_t unitIndex =
				static_cast<uint32_t>(it - limitList.begin());
		const uint32_t unitLimit = *it;

		dividable |= makeJoinUnitDividableExpansion(
				dividableList, joinInputList, keyTableList, joinKeyIdList,
				unifyingUnitList, drivingOptList, unitIndex, unitLimit,
				fullMatchList[unitIndex], joinSubKeyIdList, matchCountList,
				subLimitList, buildInfo.get());
	}

	if (!checkOnly) {
		expansionInfo->extraCount_ = static_cast<uint32_t>(
				expansionInfo->expansionCount_ - limitList.size());
		expansionInfo->limitReached_ =
				(expansionInfo->extraCount_ >= expansionLimit);
	}

	return dividable;
}

void SQLCompiler::makeJoinAllUnitNonDividableExpansion(
		const JoinNodeIdList &joinInputList,
		const JoinKeyIdList &joinKeyIdList,
		const JoinUnifyingUnitList &unifyingUnitList,
		JoinExpansionInfo &expansionInfo) {
	const uint32_t inCount = JOIN_INPUT_COUNT;

	JoinExpansionBuildInfo buildInfo(alloc_, expansionInfo);

	util::Vector<uint32_t>::const_iterator unitItList[inCount];
	util::Vector<uint32_t>::const_iterator unitEndList[inCount];
	for (uint32_t i = 0; i < inCount; i++) {
		unitItList[i] = unifyingUnitList[i].begin();
		unitEndList[i] = unifyingUnitList[i].end();

		if (unitItList[i] == unitEndList[i]) {
			assert(false);
			return;
		}
	}

	for (;;) {
		JoinSizeList keyIdStartList;
		JoinSizeList keyIdEndList;
		for (uint32_t i = 0; i < inCount; i++) {
			if (unitItList[i] + 1 == unitEndList[i]) {
				return;
			}
			keyIdStartList[i] = *unitItList[i];
			keyIdEndList[i] = *(unitItList[i] + 1);
			++unitItList[i];
		}
		makeJoinUnitExpansion(
				joinInputList, joinKeyIdList, keyIdStartList, keyIdEndList,
				buildInfo);
	}
}

void SQLCompiler::removeJoinExpansionMatchedInput(
		const JoinInputUsageList &inputUsageList,
		JoinNodeIdList &joinInputList, JoinExpansionInfo &expansionInfo) {
	const uint32_t inCount = JOIN_INPUT_COUNT;

	if (expansionInfo.isEstimationOnly()) {
		return;
	}

	for (uint32_t i = 0; i < inCount; i++) {
		const util::Vector<bool> &inputUsage = inputUsageList[i];
		PlanNode::IdList &idList = joinInputList[i];

		for (PlanNode::IdList::iterator it = idList.end();
				it != idList.begin();) {
			 if (inputUsage[(--it) - idList.begin()]) {
				 it = idList.erase(it);
			 }
		}
	}
}

bool SQLCompiler::makeJoinUnitDividableExpansion(
		const JoinBoolList &dividableList, const JoinNodeIdList &joinInputList,
		JoinKeyTableList &keyTableList, const JoinKeyIdList &joinKeyIdList,
		const JoinUnifyingUnitList &unifyingUnitList,
		const JoinBoolList *drivingOptList, uint32_t unitIndex,
		uint32_t baseUnitLimit, bool fullMatch,
		JoinKeyIdList &joinSubKeyIdList, JoinMatchCountList &matchCountList,
		util::Vector<uint32_t> &subLimitList,
		JoinExpansionBuildInfo *buildInfo) {
	const uint32_t inCount = JOIN_INPUT_COUNT;

	typedef NarrowingKeyTable::KeyIdList KeyIdList;

	KeyIdList::const_iterator unitBeginList[inCount];
	KeyIdList::const_iterator unitEndList[inCount];
	JoinSizeList unitSizeList;
	for (uint32_t i = 0; i < inCount; i++) {
		KeyIdList::const_iterator it = joinKeyIdList[i].begin();
		unitBeginList[i] = it + unifyingUnitList[i][unitIndex];
		unitEndList[i] = it + unifyingUnitList[i][unitIndex + 1];
		unitSizeList[i] =
				static_cast<size_t>(unitEndList[i] - unitBeginList[i]);
	}

	uint32_t dividingSide;
	uint64_t sideLimit;
	uint64_t unitLimit;
	const bool dividable = getJoinUnitDividableExpansionLimit(
			unitSizeList, baseUnitLimit, fullMatch, dividableList,
			drivingOptList, dividingSide, sideLimit, unitLimit);

	const bool checkOnly = (buildInfo == NULL);
	if (checkOnly) {
		return dividable;
	}

	if (!dividable) {
		JoinSizeList keyIdStartList;
		JoinSizeList keyIdEndList;
		for (uint32_t i = 0; i < inCount; i++) {
			keyIdStartList[i] = unifyingUnitList[i][unitIndex];
			keyIdEndList[i] = unifyingUnitList[i][unitIndex + 1];
		}
		makeJoinUnitExpansion(
				joinInputList, joinKeyIdList, keyIdStartList, keyIdEndList,
				*buildInfo);
		return false;
	}

	const uint32_t anotherSide = getJoinAnotherSide(dividingSide);

	NarrowingKeyTable &sideKeyTable = keyTableList[dividingSide];
	NarrowingKeyTable &anotherKeyTable = keyTableList[anotherSide];

	matchCountList.clear();
	subLimitList.clear();
	if (!fullMatch && unitLimit > sideLimit) {
		for (KeyIdList::const_iterator it = unitBeginList[dividingSide];
				it != unitEndList[dividingSide]; ++it) {
			const uint64_t count =
					anotherKeyTable.getMatchCount(sideKeyTable, *it);
			const uint32_t index =
					static_cast<uint32_t>(it - unitBeginList[dividingSide]);

			matchCountList.push_back(std::make_pair(count, index));
		}
		arrangeJoinExpansionLimit(matchCountList, subLimitList, unitLimit);
	}

	IntegralPartitioner sidePartitioner(unitSizeList[dividingSide], sideLimit);
	IntegralPartitioner limitPartitioner(unitLimit, sideLimit);

	KeyIdList &sideSubList = joinSubKeyIdList[dividingSide];
	KeyIdList &anotherSubList = joinSubKeyIdList[anotherSide];

	KeyIdList::const_iterator sideSubIdIt = unitBeginList[dividingSide];
	KeyIdList::const_iterator sideSubIdEnd = unitEndList[dividingSide];
	while (sideSubIdIt != sideSubIdEnd) {
		const size_t sideCount = checkIteratorForwardStep(
				sidePartitioner.popGroup(), (sideSubIdEnd - sideSubIdIt));

		JoinSizeList keyIdStartList;
		JoinSizeList keyIdEndList;

		sideSubList.assign(sideSubIdIt, sideSubIdIt + sideCount);
		keyIdStartList[dividingSide] = 0;
		keyIdEndList[dividingSide] = sideCount;

		if (fullMatch) {
			anotherSubList.assign(
					unitBeginList[anotherSide], unitEndList[anotherSide]);
		}
		else {
			const bool shallow = true;
			anotherSubList.clear();
			anotherKeyTable.findAll(
					sideKeyTable, 0, sideSubList, anotherSubList, shallow);
		}

		const uint64_t subLimit = std::max<uint64_t>(subLimitList.empty() ?
				limitPartitioner.popGroup() :
				subLimitList[sideSubIdIt - unitBeginList[dividingSide]], 1);
		assert(subLimitList.empty() || sideCount == 1);

		IntegralPartitioner anotherPartitioner(
				anotherSubList.size(), subLimit);

		KeyIdList::iterator anotherSubIdIt = anotherSubList.begin();
		while (anotherSubIdIt != anotherSubList.end()) {
			const size_t anotherCount = checkIteratorForwardStep(
					anotherPartitioner.popGroup(),
					(anotherSubList.end() - anotherSubIdIt));

			keyIdStartList[anotherSide] = static_cast<size_t>(
					anotherSubIdIt - anotherSubList.begin());
			keyIdEndList[anotherSide] =
					keyIdStartList[anotherSide] + anotherCount;

			makeJoinUnitExpansion(
					joinInputList, joinSubKeyIdList,
					keyIdStartList, keyIdEndList, *buildInfo);

			anotherSubIdIt += anotherCount;
		}
		sideSubIdIt += sideCount;
	}

	return true;
}

bool SQLCompiler::getJoinUnitDividableExpansionLimit(
		const JoinSizeList &unitSizeList, uint64_t baseUnitLimit,
		bool fullMatch, const JoinBoolList &dividableList,
		const JoinBoolList *drivingOptList, uint32_t &dividingSide,
		uint64_t &sideLimit, uint64_t &unitLimit) {
	unitLimit = baseUnitLimit;

	const uint32_t left = JOIN_LEFT_INPUT;
	const uint32_t right = JOIN_RIGHT_INPUT;

	const size_t leftSize = unitSizeList[left];
	const size_t rightSize = unitSizeList[right];

	const bool leftDividable = dividableList[left];
	const bool rightDividable = dividableList[right];

	if (drivingOptList != NULL) {
		dividingSide = ((*drivingOptList)[left] ? right : left);
		sideLimit = unitSizeList[dividingSide];
		unitLimit = std::max<uint64_t>(baseUnitLimit, 1U);
	}
	else if (baseUnitLimit <= 1 || (!leftDividable && !rightDividable)) {
		dividingSide = (leftSize <= rightSize ? left : right);
		sideLimit = 1;
		if (baseUnitLimit <= 1) {
			return false;
		}
	}
	else if (!leftDividable || !rightDividable) {
		dividingSide = (leftDividable ? left : right);
		sideLimit = baseUnitLimit;
	}
	else if (fullMatch) {
		const uint64_t squareLimit =
				static_cast<uint64_t>(sqrt(static_cast<double>(baseUnitLimit)));
		if (squareLimit > leftSize) {
			dividingSide = left;
			sideLimit = leftSize;
		}
		else if (squareLimit > rightSize) {
			dividingSide = right;
			sideLimit = rightSize;
		}
		else {
			dividingSide = (leftSize <= rightSize ? left : right);
			sideLimit = squareLimit;
		}
	}
	else {
		dividingSide = (leftSize <= rightSize ? left : right);

		const uint64_t minSize = std::min<uint64_t>(leftSize, rightSize);
		if (baseUnitLimit <= minSize) {
			sideLimit = baseUnitLimit;
		}
		else {
			sideLimit = minSize;
		}
	}
	return (sideLimit > 1 || unitLimit > 1);
}

void SQLCompiler::makeJoinUnitExpansion(
		const JoinNodeIdList &joinInputList,
		const JoinKeyIdList &joinKeyIdList,
		const JoinSizeList &keyIdStartList,
		const JoinSizeList &keyIdEndList, JoinExpansionBuildInfo &buildInfo) {
	const uint32_t inCount = JOIN_INPUT_COUNT;
	const uint32_t left = JOIN_LEFT_INPUT;
	const uint32_t right = JOIN_RIGHT_INPUT;

	typedef NarrowingKeyTable::KeyIdList KeyIdList;
	typedef util::Map<PlanNode::IdList, uint32_t> UnifyingMap;

	JoinExpansionInfo &expansionInfo = buildInfo.expansionInfo_;
	PlanNode::IdList &idList = buildInfo.idList_;
	util::Map<PlanNode::IdList, uint32_t> &unifyingMap =
			buildInfo.unifyingMap_;

	uint32_t unifiedIdList[inCount];
	for (uint32_t i = 0; i < inCount; i++) {
		{
			KeyIdList::const_iterator keyStart = joinKeyIdList[i].begin();
			KeyIdList::const_iterator keyIt = keyStart + keyIdStartList[i];
			KeyIdList::const_iterator keyEnd = keyStart + keyIdEndList[i];

			idList.clear();
			for (; keyIt != keyEnd; ++keyIt) {
				idList.push_back(joinInputList[i][*keyIt]);
			}
		}

		uint32_t unifiedId;
		const uint32_t newId = static_cast<uint32_t>(unifyingMap.size());
		const std::pair<UnifyingMap::iterator, bool> &mapRet =
				unifyingMap.insert(std::make_pair(idList, newId));
		if (mapRet.second) {
			if (expansionInfo.unificationCount_ == 0) {
				const uint32_t firstCount = 0;
				JoinExpansionInfo::tryAppend(
						expansionInfo.unifyingUnitList_, firstCount);
				expansionInfo.unifyingUnitCount_++;
			}

			const uint32_t nextCount = static_cast<uint32_t>(
					expansionInfo.unificationCount_ + idList.size());

			JoinExpansionInfo::tryAppend(
					expansionInfo.unificationList_,
					idList.begin(), idList.end());
			JoinExpansionInfo::tryAppend(
					expansionInfo.unifyingUnitList_, nextCount);

			expansionInfo.unificationCount_ = nextCount;
			expansionInfo.unifyingUnitCount_++;

			unifiedId = newId;
		}
		else {
			unifiedId = mapRet.first->second;
		}

		unifiedIdList[i] = unifiedId;
	}

	JoinExpansionInfo::tryAppend(
			expansionInfo.expansionList_,
			std::make_pair(unifiedIdList[left], unifiedIdList[right]));
	expansionInfo.expansionCount_++;
}

void SQLCompiler::makeJoinAllUnitLimitList(
		JoinKeyTableList &keyTableList, const JoinKeyIdList &joinKeyIdList,
		const JoinUnifyingUnitList &unifyingUnitList,
		uint32_t expansionLimit, util::Vector<uint32_t> &limitList,
		util::Vector<bool> &fullMatchList) {
	typedef NarrowingKeyTable::KeyIdList KeyIdList;

	limitList.clear();
	fullMatchList.clear();

	const uint32_t smallerSide = getJoinSmallerSide(keyTableList);
	const uint32_t largerSide = getJoinAnotherSide(smallerSide);

	const util::Vector<uint32_t> &keyIdList = joinKeyIdList[smallerSide];
	const util::Vector<uint32_t> &unitList = unifyingUnitList[smallerSide];

	util::Vector<uint32_t>::const_iterator unitIt = unitList.begin();
	if (unitIt == unitList.end()) {
		assert(false);
		return;
	}

	JoinMatchCountList matchCountList(alloc_);
	KeyIdList::const_iterator keyIdBegin = keyIdList.begin();
	for (; unitIt + 1 != unitList.end(); ++unitIt) {
		const uint32_t unitId =
				static_cast<uint32_t>(unitIt - unitList.begin());
		const uint32_t largerUnitCount =
				unifyingUnitList[largerSide][unitId + 1] -
				unifyingUnitList[largerSide][unitId];

		KeyIdList::const_iterator keyIdIt = keyIdBegin + *unitIt;
		KeyIdList::const_iterator keyIdEnd = keyIdBegin + *(unitIt + 1);

		bool fullMatch = true;
		uint64_t matchCount = 0;
		for (; keyIdIt != keyIdEnd; ++keyIdIt) {
			const uint32_t count = keyTableList[largerSide].getMatchCount(
					keyTableList[smallerSide], *keyIdIt);
			fullMatch &= (count == largerUnitCount);
			matchCount += count;
		}

		matchCountList.push_back(std::make_pair(matchCount, unitId));
		fullMatchList.push_back(fullMatch);
	}

	uint32_t unitCount = static_cast<uint32_t>(unitList.size());
	unitCount -= std::min<uint32_t>(unitCount, 1U);

	uint32_t totalLimit = expansionLimit;
	if (totalLimit <= std::numeric_limits<uint32_t>::max() - unitCount) {
		totalLimit += unitCount;
	}
	else {
		totalLimit = std::numeric_limits<uint32_t>::max();
	}

	arrangeJoinExpansionLimit(matchCountList, limitList, totalLimit);
}

void SQLCompiler::arrangeJoinExpansionLimit(
		JoinMatchCountList &matchCountList,
		util::Vector<uint32_t> &unitLimitList, uint64_t totalLimit) {
	const size_t unitCount = matchCountList.size();

	std::sort(matchCountList.begin(), matchCountList.end());
	unitLimitList.assign(unitCount, 0);

	IntegralPartitioner partitioner(totalLimit, unitCount);
	for (JoinMatchCountList::iterator it = matchCountList.begin();
			it != matchCountList.end(); ++it) {
		assert(it->second < unitCount);
		unitLimitList[it->second] = static_cast<uint32_t>(
				partitioner.popGroupLimited(it->first));
	}
}

bool SQLCompiler::checkJoinUnitDividable(
		SQLType::JoinType joinType, const JoinBoolList &singleUnitList,
		JoinBoolList &dividableList) {
	if (!getJoinDividableList(joinType, dividableList)) {
		return false;
	}

	if (singleUnitList[0] && singleUnitList[1]) {
		return false;
	}

	if (!dividableList[0] || !dividableList[1]) {
		if (!dividableList[0] && singleUnitList[1]) {
			return false;
		}
		if (!dividableList[1] && singleUnitList[0]) {
			return false;
		}
	}

	return true;
}

bool SQLCompiler::getJoinDividableList(
		SQLType::JoinType joinType, JoinBoolList &dividableList) {
	const uint32_t left = JOIN_LEFT_INPUT;
	const uint32_t right = JOIN_RIGHT_INPUT;

	dividableList[left] = (
			joinType == SQLType::JOIN_INNER ||
			joinType == SQLType::JOIN_LEFT_OUTER);
	dividableList[right] = (
			joinType == SQLType::JOIN_INNER ||
			joinType == SQLType::JOIN_RIGHT_OUTER);

	const bool dividable = (dividableList[left] || dividableList[right]);
	return dividable;
}

const SQLCompiler::JoinBoolList* SQLCompiler::getJoinUnitDrivingOptionList(
		PlanNode::CommandOptionFlag cmdOptionFlag,
		const JoinBoolList &dividableList, JoinBoolList &baseJoinOptList) {
	const uint32_t left = JOIN_LEFT_INPUT;
	const uint32_t right = JOIN_RIGHT_INPUT;

	baseJoinOptList[left] = false;
	baseJoinOptList[right] = false;

	if ((cmdOptionFlag & PlanNode::Config::CMD_OPT_JOIN_DRIVING_SOME) == 0) {
		return NULL;
	}

	const uint32_t drivingSide = ((cmdOptionFlag &
			PlanNode::Config::CMD_OPT_JOIN_DRIVING_NONE_LEFT) == 0 ?
					right : left);
	const uint32_t innerSide = getJoinAnotherSide(drivingSide);

	if (!dividableList[innerSide]) {
		return NULL;
	}

	baseJoinOptList[drivingSide] = true;
	return &baseJoinOptList;
}

uint32_t SQLCompiler::getJoinSmallerSide(const JoinNodeIdList &joinInputList) {
	const uint32_t left = JOIN_LEFT_INPUT;
	const uint32_t right = JOIN_RIGHT_INPUT;

	return (joinInputList[left].size() <= joinInputList[right].size() ? left : right);
}

uint32_t SQLCompiler::getJoinSmallerSide(
		const JoinKeyTableList &keyTableList) {
	const uint32_t left = JOIN_LEFT_INPUT;
	const uint32_t right = JOIN_RIGHT_INPUT;

	return (keyTableList[left].getSize() <= keyTableList[right].getSize() ?
			left : right);
}

uint32_t SQLCompiler::getJoinAnotherSide(uint32_t srcSide) {
	const uint32_t inCount = JOIN_INPUT_COUNT;

	assert(srcSide < inCount);
	return (srcSide + 1) % inCount;
}

size_t SQLCompiler::checkIteratorForwardStep(
		uint64_t src, ptrdiff_t remaining) {
	if (remaining < 0 || src > static_cast<size_t>(remaining)) {
		assert(false);
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}
	return static_cast<size_t>(src);
}

bool SQLCompiler::resolveNarrowingKey(
		const Plan &plan, const PlanNode &node, ColumnId columnId,
		const TableNarrowingList &tableNarrowingList, NarrowingKey &key) {

	const PlanNode *curNode = &node;
	uint32_t curInputId = 0;
	ColumnId curColumnId = columnId;
	NarrowingKey lastKey;
	NarrowingKey curKey;

	util::Vector<const PlanNode*> nodePath(alloc_);
	util::Vector<uint32_t> inputIdPath(alloc_);
	util::Vector<ColumnId> columnIdPath(alloc_);
	util::Vector<NarrowingKey> keyPath(alloc_);
	for (;;) {
		const Expr *columnExpr = &curNode->outputList_[curColumnId];
		if (columnExpr->op_ != SQLType::EXPR_COLUMN) {
			columnExpr = NULL;
		}

		const Type type = curNode->type_;
		if (type == SQLType::EXEC_GROUP ||
				type == SQLType::EXEC_LIMIT ||
				type == SQLType::EXEC_SELECT ||
				type == SQLType::EXEC_SORT) {
			curKey = lastKey;
		}
		else if (type == SQLType::EXEC_JOIN) {
			if (curInputId > 0) {
				columnExpr = NULL;
			}
			if (columnExpr != NULL) {
				curInputId = columnExpr->inputId_;
			}
			curKey = lastKey;
		}
		else if (type == SQLType::EXEC_SCAN) {
			if (columnExpr != NULL) {
				if (curInputId > 0) {
					columnExpr = NULL;
				}
				else if (columnExpr->inputId_ > 0) {
					curInputId = columnExpr->inputId_ - 1;
				}
				else {
					curKey = tableNarrowingList.resolve(
							curNode->tableIdInfo_, columnExpr->columnId_);
					columnExpr = NULL;
				}
			}
		}
		else if (type == SQLType::EXEC_UNION) {
			if (curInputId >= 2) {
				curKey.mergeWith(lastKey);

				if (curKey.isFull()) {
					columnExpr = NULL;
				}
			}
			else {
				curKey = lastKey;
			}
		}
		else {
			assert(false);
			SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
		}

		if (curNode->limit_ == 0) {
			curKey.setNone();
			columnExpr = NULL;
		}

		if (columnExpr != NULL && curInputId < curNode->inputList_.size()) {
			nodePath.push_back(curNode);
			inputIdPath.push_back(curInputId + 1);
			columnIdPath.push_back(curColumnId);
			keyPath.push_back(curKey);

			curNode = &curNode->getInput(plan, curInputId);
			curInputId = 0;
			curColumnId = columnExpr->columnId_;

			lastKey = NarrowingKey();
			curKey = NarrowingKey();
		}
		else if (!nodePath.empty()) {
			curNode = nodePath.back();
			curInputId = inputIdPath.back();
			curColumnId = columnIdPath.back();

			lastKey = curKey;
			curKey = keyPath.back();

			nodePath.pop_back();
			inputIdPath.pop_back();
			columnIdPath.pop_back();
			keyPath.pop_back();
		}
		else {
			break;
		}
	}

	key = curKey;
	return !key.isFull();
}

SQLCompiler::TableNarrowingList&
SQLCompiler::prepareTableNarrowingList(const Plan &plan) {
	if (tableNarrowingList_ != NULL) {
		return *tableNarrowingList_;
	}

	const TableInfoList &infoList = getTableInfoList();

	tableNarrowingList_ = ALLOC_NEW(alloc_) TableNarrowingList(alloc_);
	for (PlanNodeList::const_iterator it = plan.nodeList_.begin();
			it != plan.nodeList_.end(); ++it) {
		const TableInfo::IdInfo &idInfo = it->tableIdInfo_;
		if (idInfo.isEmpty()) {
			continue;
		}

		if (tableNarrowingList_->contains(idInfo.id_)) {
			continue;
		}

		tableNarrowingList_->put(infoList.resolve(idInfo.id_));
	}

	return *tableNarrowingList_;
}

uint32_t SQLCompiler::getAcceptableJoinExpansionCount(const Plan &plan) {
	if (expansionLimitReached_) {
		return 0;
	}

	if (generatedScanCount_ == std::numeric_limits<uint64_t>::max()) {
		util::StackAllocator::Scope scope(alloc_);

		typedef util::MultiMap<uint32_t, SQLTableInfo::IdInfo> IdSet;
		typedef IdSet::value_type Entry;
		IdSet idSet(alloc_);
		for (Plan::NodeList::const_iterator it = plan.nodeList_.begin();
				it != plan.nodeList_.end(); ++it) {
			if (it->type_ == SQLType::EXEC_SCAN) {
				const uint32_t hash = SubPlanHasher()(it->tableIdInfo_);
				const Entry &entry = Entry(hash, it->tableIdInfo_);

				const std::pair<IdSet::iterator, IdSet::iterator> &ret =
						idSet.equal_range(hash);
				if (std::find(ret.first, ret.second, entry) == ret.second) {
					idSet.insert(ret.second, entry);
				}
			}
		}

		generatedScanCount_ = idSet.size();
	}

	const uint64_t maxByRatio = applyRatio(
			std::max<uint64_t>(generatedScanCount_, 10),
			planSizeLimitRatio_);
	const uint64_t maxByHint = getMaxExpansionCount(plan);

	const uint64_t max = std::min(maxByRatio, maxByHint);
	const uint64_t acceptable = max - std::min(max, expandedJoinCount_);

	return static_cast<uint32_t>(std::min<uint64_t>(
			acceptable, std::numeric_limits<uint32_t>::max()));
}

void SQLCompiler::acceptJoinExpansion(uint32_t extraCount, bool reached) {
	expandedJoinCount_ += extraCount;
	expansionLimitReached_ |= reached;
}

void SQLCompiler::makeJoinExpansionScoreIdList(
		const Plan &plan, JoinExpansionScoreList &scoreList,
		JoinExpansionScoreIdList &scoreIdList) {
	scoreIdList.clear();

	makeJoinExpansionScoreList(plan, scoreList);

	for (JoinExpansionScoreList::iterator it = scoreList.begin();
			it != scoreList.end(); ++it) {
		const PlanNodeId nodeId =
				static_cast<PlanNodeId>(it - scoreList.begin());
		if (plan.nodeList_[nodeId].type_ != SQLType::EXEC_JOIN) {
			continue;
		}
		scoreIdList.push_back(JoinExpansionScoredId(*it, nodeId));
	}
	std::sort(scoreIdList.begin(), scoreIdList.end());
}

void SQLCompiler::makeJoinExpansionScoreList(
		const Plan &plan, JoinExpansionScoreList &scoreList) {
	typedef JoinExpansionScore Score;

	const uint64_t initialScoreFirst = std::numeric_limits<uint64_t>::max();
	scoreList.assign(plan.nodeList_.size(), Score(initialScoreFirst, 0));

	util::StackAllocator::Scope scope(alloc_);

	uint32_t revTraversalOrder = static_cast<uint32_t>(scoreList.size());
	PlanNode::IdList pendingIdList(alloc_);

	for (PlanNodeList::const_reverse_iterator it = plan.nodeList_.rbegin();
			it != plan.nodeList_.rend(); ++it) {
		if (it->type_ == SQLType::EXEC_RESULT) {
			pendingIdList.push_back(static_cast<PlanNodeId>(
					&(*it) - &plan.nodeList_[0]));
			break;
		}
	}

	while (!pendingIdList.empty()) {
		const PlanNodeId nodeId = pendingIdList.back();
		pendingIdList.pop_back();

		Score &lastScore = scoreList[nodeId];
		if (lastScore.first != initialScoreFirst) {
			continue;
		}

		const PlanNode &node = plan.nodeList_[nodeId];
		uint64_t scoreFirst = (node.type_ == SQLType::EXEC_SCAN ? 1 : 0);
		bool pending = false;

		for (PlanNode::IdList::const_iterator it = node.inputList_.begin();
				it != node.inputList_.end(); ++it) {
			const Score &inScore = scoreList[*it];
			const bool inPending = (inScore.first == initialScoreFirst);

			if (pending || inPending) {
				if (!pending) {
					pendingIdList.push_back(nodeId);
					pending = true;
				}
				if (inPending) {
					pendingIdList.push_back(*it);
				}
			}
			else {
				scoreFirst += inScore.first;
			}
		}

		if (lastScore.second == 0) {
			assert(revTraversalOrder != 0);
			lastScore.second = revTraversalOrder;
			--revTraversalOrder;
		}

		if (!pending) {
			lastScore.first = scoreFirst;
		}
	}
}

bool SQLCompiler::isJoinExpansionUnmatchInputRequired(
		uint32_t joinInputId, SQLType::JoinType joinType) {
	const uint32_t left = JOIN_LEFT_INPUT;
	const uint32_t right = JOIN_RIGHT_INPUT;

	switch (joinType) {
	case SQLType::JOIN_INNER:
		return false;
	case SQLType::JOIN_LEFT_OUTER:
		return (joinInputId == left);
	case SQLType::JOIN_RIGHT_OUTER:
		return (joinInputId == right);
	case SQLType::JOIN_FULL_OUTER:
		return true;
	default:
		assert(false);
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}
}

bool SQLCompiler::isJoinExpansionMetaInputRequired(
		uint32_t joinInputId, SQLType::JoinType joinType,
		const JoinNodeIdList &joinInputList, bool &metaOnly) {
	const uint32_t left = JOIN_LEFT_INPUT;
	const uint32_t right = JOIN_RIGHT_INPUT;

	metaOnly = false;

	const uint32_t anotherInputId = getJoinAnotherSide(joinInputId);
	switch (joinType) {
	case SQLType::JOIN_INNER:
		if (!joinInputList[joinInputId].empty()) {
			return true;
		}
		break;
	case SQLType::JOIN_LEFT_OUTER:
		if (!joinInputList[joinInputId].empty()) {
			if (joinInputId == right) {
				return true;
			}
			else if (!joinInputList[anotherInputId].empty()) {
				return false;
			}
		}
		break;
	case SQLType::JOIN_RIGHT_OUTER:
		if (!joinInputList[joinInputId].empty()) {
			if (joinInputId == left) {
				return true;
			}
			else if (!joinInputList[anotherInputId].empty()) {
				return false;
			}
		}
		break;
	case SQLType::JOIN_FULL_OUTER:
		if (!joinInputList[joinInputId].empty() &&
				!joinInputList[anotherInputId].empty()) {
			return false;
		}
		break;
	default:
		assert(false);
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	if (!joinInputList[anotherInputId].empty()) {
		metaOnly = true;
		return true;
	}

	return false;
}

size_t SQLCompiler::getJoinExpansionMetaUnificationCount(
		uint32_t joinInputId, SQLType::JoinType joinType,
		const JoinNodeIdList &joinInputList,
		const JoinNodeIdList &metaInputList) {
	const uint32_t left = JOIN_LEFT_INPUT;
	const uint32_t right = JOIN_RIGHT_INPUT;

	const size_t metaCount = metaInputList[joinInputId].size();
	if (metaCount <= 1) {
		return 0;
	}

	if (joinType == SQLType::JOIN_INNER) {
		return metaCount;
	}
	else if (joinType == SQLType::JOIN_LEFT_OUTER) {
		if (joinInputId == left) {
			return metaCount;
		}
	}
	else if (joinType == SQLType::JOIN_RIGHT_OUTER) {
		if (joinInputId == right) {
			return metaCount;
		}
	}

	const size_t another = getJoinAnotherSide(joinInputId);
	const size_t unmatchJoinCount =
			std::min(metaCount, joinInputList[another].size());
	const size_t consumingCount = std::max<size_t>(unmatchJoinCount, 1U) - 1;

	const size_t unificationCount = metaCount - consumingCount;
	if (unificationCount <= 1) {
		return 0;
	}

	return unificationCount;
}

bool SQLCompiler::getJoinExpansionTarget(
		SQLType::JoinType joinType, const JoinNodeIdList &joinInputList,
		const JoinNodeIdList &metaInputList,
		const PlanNode::IdList &unifiedIdList,
		const JoinExpansionInfo &expansionInfo, size_t ordinal,
		JoinExpansionEntry &entry) {
	const uint32_t inCount = JOIN_INPUT_COUNT;
	const uint32_t left = JOIN_LEFT_INPUT;
	const uint32_t right = JOIN_RIGHT_INPUT;

	entry = std::make_pair(UNDEF_PLAN_NODEID, UNDEF_PLAN_NODEID);

	size_t rest = ordinal;

	const JoinExpansionList &expansionList = *expansionInfo.expansionList_;
	const size_t expansionSize = expansionList.size();
	if (rest < expansionSize) {
		entry.first = unifiedIdList[expansionList[ordinal].first];
		entry.second = unifiedIdList[expansionList[ordinal].second];
		return true;
	}
	else {
		rest -= expansionSize;
	}

	do {
		if (metaInputList[left].empty() ||
				metaInputList[right].empty()) {
			break;
		}

		const bool leftUnmatch = !joinInputList[left].empty();
		const bool rightUnmatch = !joinInputList[right].empty();

		if (joinType == SQLType::JOIN_LEFT_OUTER &&
				(leftUnmatch || !rightUnmatch)) {
			break;
		}

		if (joinType == SQLType::JOIN_RIGHT_OUTER &&
				(!leftUnmatch || rightUnmatch)) {
			break;
		}

		if (joinType == SQLType::JOIN_FULL_OUTER) {
			break;
		}

		if (rest < 1) {
			entry = std::make_pair(
					metaInputList[left].back(),
					metaInputList[right].back());
			return true;
		}
		else {
			rest -= 1;
		}
	}
	while (false);

	if (joinType == SQLType::JOIN_INNER) {
		return false;
	}

	for (uint32_t i = 0; i < inCount; i++) {
		if (i == left && joinType == SQLType::JOIN_RIGHT_OUTER) {
			continue;
		}
		if (i == right && joinType == SQLType::JOIN_LEFT_OUTER) {
			continue;
		}

		const size_t unmatchCount = joinInputList[i].size();

		if (rest < unmatchCount) {
			const size_t another = getJoinAnotherSide(i);
			const size_t metaCount = metaInputList[another].size();
			if (metaCount <= 0) {
				assert(false);
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
			}

			const size_t offset =
					metaCount - std::min(metaCount, unmatchCount);
			const size_t index = std::min(offset + rest, metaCount - 1);

			entry = std::make_pair(
					joinInputList[i][rest], metaInputList[another][index]);
			if (i != left) {
				std::swap(entry.first, entry.second);
			}
			return true;
		}
		else {
			rest -= unmatchCount;
		}
	}

	return false;
}

bool SQLCompiler::simplifySubquery(Plan &plan) {
	typedef util::Vector< std::pair<PlanNodeId, bool> > JoinList;

	PlanNode::IdList pendingIdList(alloc_);
	JoinList subqueryJoinList(alloc_);
	util::Set<PlanNodeId> rewroteJoinIdSet(alloc_);
	util::Set<PlanNodeId> visitedNodeIdSet(alloc_);
	bool found = false;

	while (listRelatedSubqueries(
			plan, pendingIdList, subqueryJoinList, rewroteJoinIdSet,
			visitedNodeIdSet)) {

		for (JoinList::iterator it = subqueryJoinList.begin();
				it != subqueryJoinList.end(); ++it) {
			found |=
					rewriteSubquery(plan, it->first, it->second, rewroteJoinIdSet);
		}
	}

	return found;
}

bool SQLCompiler::listRelatedSubqueries(
		const Plan &plan, PlanNode::IdList &pendingIdList,
		util::Vector< std::pair<PlanNodeId, bool> > &subqueryJoinList,
		util::Set<PlanNodeId> &rewroteJoinIdSet,
		util::Set<PlanNodeId> &visitedNodeIdSet) {
	subqueryJoinList.clear();

	if (pendingIdList.empty()) {
		bool found = false;
		PlanNodeList::const_iterator resultIt = plan.nodeList_.end();

		for (PlanNodeList::const_iterator nodeIt = plan.nodeList_.begin();
				nodeIt != plan.nodeList_.end(); ++nodeIt) {
			if (nodeIt->type_ == SQLType::EXEC_RESULT) {
				resultIt = nodeIt;
			}
			else if (isFoldAggregationNode(*nodeIt)) {
				found = true;
			}
		}

		if (!found) {
			return false;
		}

		if (resultIt == plan.nodeList_.end()) {
			assert(false);
			return false;
		}

		pendingIdList.assign(
				resultIt->inputList_.begin(), resultIt->inputList_.end());
	}

	util::Vector<ColumnId> foldedColumnList(alloc_);
	PlanNode::IdList nodePath(alloc_);
	bool outsidePredFound = false;

	while (!pendingIdList.empty()) {
		const PlanNodeId nodeId = pendingIdList.back();
		pendingIdList.pop_back();

		if (visitedNodeIdSet.find(nodeId) != visitedNodeIdSet.end()) {
			continue;
		}
		visitedNodeIdSet.insert(nodeId);

		const PlanNode &node = plan.nodeList_[nodeId];
		assert(!isDisabledNode(node));

		pendingIdList.insert(
				pendingIdList.end(),
				node.inputList_.rbegin(), node.inputList_.rend());
		nodePath.push_back(nodeId);

		if (node.type_ == SQLType::EXEC_JOIN &&
				rewroteJoinIdSet.find(nodeId) == rewroteJoinIdSet.end()) {
			const PlanNode &rightNode = node.getInput(plan, 1);
			if (isFoldAggregationNode(rightNode)) {
				ColumnId outColumnId;

				uint32_t inputId = 1;
				ColumnId inColumnId = 1;
				findOutputColumn(node, inputId, inColumnId, outColumnId, true);

				foldedColumnList.push_back(outColumnId);
				subqueryJoinList.push_back(std::make_pair(nodeId, false));

				continue;
			}
		}

		if (getNodeAggregationLevel(node.outputList_) == 0 &&
				node.limit_ < 0 && !node.inputList_.empty() &&
				(node.type_ == SQLType::EXEC_SELECT ||
				(node.type_ == SQLType::EXEC_JOIN &&
						subqueryJoinList.empty()))) {

			const size_t condIndex =
					(node.type_ == SQLType::EXEC_JOIN ? 1 : 0);

			if (subqueryJoinList.empty() &&
					node.predList_.size() > condIndex &&
					!isTrueExpr(node.predList_[condIndex])) {

				nodePath.clear();
				nodePath.push_back(nodeId);

				outsidePredFound = true;
			}
			continue;
		}

		if (!subqueryJoinList.empty()) {
			break;
		}

		nodePath.clear();
		outsidePredFound = false;
	}

	if (subqueryJoinList.empty()) {
		return false;
	}

	if (!outsidePredFound || nodePath.size() <= 1) {
		return true;
	}

	typedef util::Vector< std::pair<PlanNodeId, bool> > JoinList;
	JoinList::reverse_iterator joinIdIt = subqueryJoinList.rbegin();
	util::Vector<ColumnId>::reverse_iterator subColumnEnd =
			foldedColumnList.rbegin();

	for (PlanNode::IdList::reverse_iterator idIt = nodePath.rbegin();
			idIt != nodePath.rend(); ++idIt) {
		if (idIt == nodePath.rbegin()) {
			continue;
		}
		else if (idIt + 1 == nodePath.rend()) {
			break;
		}

		const PlanNode &node = plan.nodeList_[*idIt];
		const uint32_t inputId = getNodeInputId(node, *(idIt - 1));
		for (util::Vector<ColumnId>::reverse_iterator it =
				foldedColumnList.rbegin(); it != subColumnEnd; ++it) {
			if (*it == UNDEF_COLUMNID) {
				continue;
			}
			ColumnId columnId;
			findOutputColumn(node, inputId, *it, columnId, true);
			*it = columnId;
		}

		if (joinIdIt != subqueryJoinList.rend() && joinIdIt->first == *idIt) {
			assert(subColumnEnd != foldedColumnList.rend());
			++subColumnEnd;
			++joinIdIt;
		}
	}

	do {
		const PlanNode &node = plan.nodeList_[nodePath.front()];
		const uint32_t inputId = getNodeInputId(node, nodePath[1]);

		if (node.type_ == SQLType::EXEC_JOIN &&
				!(node.joinType_ == SQLType::JOIN_INNER ||
				(node.joinType_ == SQLType::JOIN_LEFT_OUTER &&
						inputId == 0) ||
				(node.joinType_ == SQLType::JOIN_RIGHT_OUTER &&
						inputId == 1))) {
			break;
		}

		util::Vector<bool> columnVisitedList(
				subqueryJoinList.size(), false, alloc_);

		{
			const size_t condIndex =
					(node.type_ == SQLType::EXEC_JOIN ? 1 : 0);
			const bool forOutput = false;
			listFilteringSubqueryColumns(
					node.predList_[condIndex], inputId, forOutput,
					foldedColumnList, columnVisitedList, subqueryJoinList);
		}

		for (PlanExprList::const_iterator it = node.outputList_.begin();
				it != node.outputList_.end(); ++it) {
			const bool forOutput = true;
			listFilteringSubqueryColumns(
					*it, inputId, forOutput,
					foldedColumnList, columnVisitedList, subqueryJoinList);
		}
	}
	while (false);

	return true;
}

void SQLCompiler::listFilteringSubqueryColumns(
		const Expr &expr, uint32_t inputId, bool forOutput,
		const util::Vector<ColumnId> &foldedColumnList,
		util::Vector<bool> &columnVisitedList,
		util::Vector< std::pair<PlanNodeId, bool> > &subqueryJoinList) {

	if (expr.op_ == SQLType::EXPR_COLUMN) {
		assert(foldedColumnList.size() == subqueryJoinList.size());
		do {
			if (expr.inputId_ != inputId) {
				break;
			}

			util::Vector<ColumnId>::const_iterator it = std::find(
					foldedColumnList.begin(), foldedColumnList.end(),
					expr.columnId_);
			if (it == foldedColumnList.end()) {
				break;
			}

			util::Vector<bool>::iterator visitedIt = columnVisitedList.begin() +
					(it - foldedColumnList.begin());

			bool &last = subqueryJoinList[it - foldedColumnList.begin()].second;
			const bool filteringOnly = ((!*visitedIt || last) && !forOutput);

			last = filteringOnly;
			*visitedIt = true;
		}
		while (false);
		return;
	}

	const bool forOutputSub = (forOutput || expr.op_ != SQLType::EXPR_AND);
	for (ExprArgsIterator<> it(expr); it.exists(); it.next()) {
		listFilteringSubqueryColumns(
				it.get(), inputId, forOutputSub,
				foldedColumnList, columnVisitedList, subqueryJoinList);
	}
}

uint32_t SQLCompiler::getNodeInputId(
		const PlanNode &node, PlanNodeId inNodeId) {
	PlanNode::IdList::const_iterator it = std::find(
			node.inputList_.begin(), node.inputList_.end(), inNodeId);

	if (it == node.inputList_.end()) {
		assert(false);
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	return static_cast<uint32_t>(it - node.inputList_.begin());
}

bool SQLCompiler::findOutputColumn(
		const PlanNode &node, uint32_t inputId, ColumnId inColumnId,
		ColumnId &outColumnId, bool reverse) {
	outColumnId = UNDEF_COLUMNID;

	static_cast<void>(reverse);

	for (PlanExprList::const_reverse_iterator it = node.outputList_.rbegin();
			it != node.outputList_.rend(); ++it) {
		if (it->op_ == SQLType::EXPR_COLUMN &&
				it->inputId_ == inputId && it->columnId_ == inColumnId) {
			outColumnId =
					static_cast<ColumnId>((node.outputList_.rend() - it) - 1);
			return true;
		}
	}

	return false;
}

bool SQLCompiler::isFoldAggregationNode(const PlanNode &node) {
	return (node.type_ == SQLType::EXEC_GROUP &&
			node.outputList_.size() > 1 &&
			isFoldExprType(node.outputList_[1].op_));
}

bool SQLCompiler::isFoldExprType(Type type) {
	switch (type) {
	case SQLType::AGG_FOLD_EXISTS:
	case SQLType::AGG_FOLD_NOT_EXISTS:
	case SQLType::AGG_FOLD_IN:
	case SQLType::AGG_FOLD_NOT_IN:
	case SQLType::AGG_FOLD_UPTO_ONE:
		return true;
	default:
		return false;
	}
}

bool SQLCompiler::isNegativeFoldType(Type foldType) {
	return (foldType == SQLType::AGG_FOLD_NOT_EXISTS ||
			foldType == SQLType::AGG_FOLD_NOT_IN);
}

bool SQLCompiler::isMatchingFoldType(Type foldType) {
	return (foldType == SQLType::AGG_FOLD_IN ||
			foldType == SQLType::AGG_FOLD_NOT_IN);
}

bool SQLCompiler::getSubqueryCorrelation(
		const Plan &plan, const PlanNode &subqueryJoinNode,
		PlanNodeId &insidePredId, PlanNode::IdList &bothSideJoinList,
		util::Vector<ColumnId> &correlatedColumnList,
		ExprList &correlationList, Expr *&foldExpr) {

	insidePredId = std::numeric_limits<PlanNodeId>::max();
	bothSideJoinList.clear();
	correlatedColumnList.clear();
	correlationList.clear();
	foldExpr = NULL;

	const PlanNodeId outsideId = subqueryJoinNode.inputList_[0];
	const PlanNodeId foldId = subqueryJoinNode.inputList_[1];
	assert(isFoldAggregationNode(plan.nodeList_[foldId]));

	ColumnId outsideIdColumn;
	{
		const PlanExprList &list = plan.nodeList_[outsideId].outputList_;
		for (PlanExprList::const_iterator it = list.begin();; ++it) {
			if (it == list.end()) {
				assert(false);
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
			}

			if (it->op_ == SQLType::EXPR_ID) {
				outsideIdColumn = static_cast<ColumnId>(it - list.begin());
				break;
			}
		}
	}

	util::Vector<bool> visitedList_(plan.nodeList_.size(), false, alloc_);
	PlanNode::IdList idJoinList(alloc_);

	typedef  std::pair<uint32_t, uint32_t> NodePathStatus;
	typedef std::pair<PlanNodeId, NodePathStatus> NodePathEntry;
	typedef util::Vector<NodePathEntry> NodePath;

	bool simple = true;
	NodePath nodePath(alloc_);
	nodePath.push_back(NodePathEntry(foldId, NodePathStatus()));

	while (!nodePath.empty()) {
		const NodePathEntry entry = nodePath.back();
		const NodePathStatus &status = entry.second;
		nodePath.pop_back();

		const PlanNodeId nodeId = entry.first;
		const PlanNode &node = plan.nodeList_[nodeId];

		if (nodeId == outsideId) {
			if (!simple) {
				continue;
			}

			const ColumnId columnCount =
					static_cast<ColumnId>(node.outputList_.size());
			for (ColumnId i = 0; i < columnCount; i++) {
				correlatedColumnList.push_back(i);
			}
		}
		else if (status.first < node.inputList_.size()) {
			const NodePathStatus curStatus(status.first + 1, status.second);
			nodePath.push_back(NodePathEntry(nodeId, curStatus));

			const PlanNodeId inId = node.inputList_[status.first];
			const NodePathStatus inStatus(
					0, static_cast<uint32_t>(correlatedColumnList.size()));
			nodePath.push_back(NodePathEntry(inId, inStatus));
		}
		else if (node.inputList_.empty()) {
			if (!simple) {
				continue;
			}

			correlatedColumnList.insert(
					correlatedColumnList.end(),
					node.outputList_.size(), UNDEF_COLUMNID);
		}
		else {
			const bool visited = visitedList_[nodeId];
			visitedList_[nodeId] = true;

			bool forBothSide = false;
			if (!visited && node.type_ == SQLType::EXEC_JOIN && std::find(
					node.inputList_.begin(), node.inputList_.end(), outsideId) !=
					node.inputList_.end()) {
				bothSideJoinList.push_back(nodeId);
				forBothSide = true;
			}

			if (!simple) {
				continue;
			}

			if (!visited && !forBothSide && isSubqueryIdJoinNode(
					plan, node, outsideIdColumn, correlatedColumnList)) {
				idJoinList.push_back(nodeId);
			}

			const bool lastUncorrelated = correlationList.empty();
			if (!mergeCorrelatedOutput(
					plan, node, outsideId, outsideIdColumn,
					(nodeId == foldId ? &foldExpr : NULL), visited,
					correlatedColumnList, correlationList)) {
				simple = false;
				continue;
			}

			if (!(lastUncorrelated && !correlationList.empty())) {
				continue;
			}

			bool neCorrelated = false;
			for (ExprList::iterator it = correlationList.begin();
					it != correlationList.end(); ++it) {
				if ((*it)->op_ == SQLType::OP_NE) {
					neCorrelated = true;
					break;
				}
			}

			const PlanNode &foldNode = plan.nodeList_[foldId];
			for (NodePath::iterator it = nodePath.begin();
					it != nodePath.end(); ++it) {
				if (!isSimpleCorrelatedSubqueryNode(
						plan.nodeList_[it->first], &foldNode, neCorrelated)) {
					simple = false;
					break;
				}
			}

			if (!simple) {
				continue;
			}

			insidePredId = nodeId;
		}
	}

	correlatedColumnList.clear();

	if (!bothSideJoinList.empty()) {
		typedef util::Vector<bool> CheckEntry;
		typedef util::Vector<CheckEntry*> CheckList;

		CheckList checkList(alloc_);

		typedef PlanNode::IdList::const_iterator IdIt;
		for (IdIt joinIt = bothSideJoinList.begin();
				joinIt != bothSideJoinList.end(); ++joinIt) {
			const PlanNodeId nodeId = *joinIt;
			if (nodeId >= checkList.size()) {
				checkList.resize(nodeId + 1);
			}

			CheckEntry *&entry = checkList[nodeId];
			if (entry != NULL) {
				continue;
			}
			entry = ALLOC_NEW(alloc_) CheckEntry(alloc_);

			const PlanNode &node = plan.nodeList_[*joinIt];

			for (PlanExprList::const_iterator it = node.outputList_.begin();
					it != node.outputList_.end(); ++it) {
				entry->push_back(!isDisabledExpr(*it));
			}
		}

		for (IdIt joinIt = bothSideJoinList.begin();
				joinIt != bothSideJoinList.end(); ++joinIt) {
			const PlanNode &node = plan.nodeList_[*joinIt];

			for (IdIt it = node.inputList_.begin();
					it != node.inputList_.end(); ++it) {
				const PlanNodeId inNodeId = *it;
				if (inNodeId >= checkList.size()) {
					checkList.resize(inNodeId + 1);
				}
				CheckEntry *&entry = checkList[inNodeId];
				if (entry != NULL) {
					continue;
				}
				entry = ALLOC_NEW(alloc_) CheckEntry(
						plan.nodeList_[*it].outputList_.size(), false,
						alloc_);
			}

			checkReference(plan, node, node.predList_, checkList, false);
			checkReference(plan, node, node.outputList_, checkList, true);
		}

		do {
			CheckEntry *entry = checkList[outsideId];
			if (entry == NULL) {
				break;
			}
			for (CheckEntry::iterator it = entry->begin();
					it != entry->end(); ++it) {
				if (*it) {
					correlatedColumnList.push_back(
							static_cast<ColumnId>(it - entry->begin()));
				}
			}
		}
		while (false);
	}

	if (simple) {
		util::Vector<ColumnId>::iterator columnIt = std::find(
				correlatedColumnList.begin(), correlatedColumnList.end(),
				outsideIdColumn);
		if (columnIt != correlatedColumnList.end()) {
			correlatedColumnList.erase(columnIt);
		}

		bothSideJoinList.insert(
				bothSideJoinList.end(), idJoinList.begin(), idJoinList.end());
	}

	if (simple && insidePredId != std::numeric_limits<PlanNodeId>::max()) {
		const PlanNode &predNode = plan.nodeList_[insidePredId];

		util::Vector<uint32_t> nodeRefList(alloc_);
		for (ExprList::const_iterator it = correlationList.begin();
				it != correlationList.end(); ++it) {
			for (uint32_t i = 0; i < 2; i++) {
				const Expr *opExpr = (i ==0 ? (*it)->left_ : (*it)->right_);
				if (opExpr->op_ == SQLType::EXPR_COLUMN) {
					continue;
				}

				if (nodeRefList.empty()) {
					makeNodeRefList(plan, nodeRefList);
				}

				const PlanNodeId opNodeId = predNode.inputList_[i];
				const PlanNode &opNode = plan.nodeList_[opNodeId];

				if (nodeRefList[opNodeId] > (2 - i) ||
						opNode.type_ != SQLType::EXEC_SELECT) {
					simple = false;
					break;
				}
			}
		}
	}

	if (!simple) {
		insidePredId = std::numeric_limits<PlanNodeId>::max();
		correlationList.clear();
		foldExpr = NULL;
	}

	return simple;
}

bool SQLCompiler::isSimpleCorrelatedSubqueryNode(
		const PlanNode &node, const PlanNode *foldNode, bool neCorrelated) {

	if (isNodeAggregated(node) || isWindowNode(node)) {
		return false;
	}
	else if (node.type_ == SQLType::EXEC_UNION) {
		return false;
	}
	else if (node.type_ == SQLType::EXEC_SORT) {
		return (node.subLimit_ < 0 && node.subOffset_ <= 0);
	}
	else if (node.type_ == SQLType::EXEC_GROUP) {
		const Type foldType = foldNode->outputList_[1].op_;

		if (&node == foldNode) {
			if (neCorrelated) {
				if (foldType == SQLType::AGG_FOLD_UPTO_ONE) {
					return false;
				}
			}
			return true;
		}

		if (getNodeAggregationLevel(node.outputList_) > 0 &&
				foldType != SQLType::AGG_FOLD_UPTO_ONE) {
			return false;
		}

		return !neCorrelated && !isFoldAggregationNode(node);
	}
	else {
		return true;
	}
}

bool SQLCompiler::mergeCorrelatedOutput(
		const Plan &plan, const PlanNode &node, PlanNodeId outsideId,
		ColumnId outsideIdColumn, Expr **foldExpr, bool visited,
		util::Vector<ColumnId> &correlatedColumnList,
		ExprList &correlationList) {

	if (node.type_ == SQLType::EXEC_UNION) {
		assert(node.predList_.empty());

		typedef PlanNode::IdList::const_reverse_iterator IdRevIt;
		for (IdRevIt it = node.inputList_.rbegin();
				it != node.inputList_.rend(); ++it) {
			if (it == node.inputList_.rbegin()) {
				continue;
			}

			const size_t count = plan.nodeList_[*it].outputList_.size();
			assert(count == plan.nodeList_[*(it - 1)].outputList_.size());
			assert(count * 2 <= correlatedColumnList.size());

			for (size_t i = count; i > 0; i--) {
				const ColumnId id1 = correlatedColumnList.back();
				correlatedColumnList.pop_back();

				const ColumnId id2 = *(correlatedColumnList.end() - count);
				if (id1 != id2) {
					return false;
				}
			}
		}
	}
	else {
		assert(!node.inputList_.empty());

		const bool correlationAccepting =
				(!visited && correlationList.empty() &&
				node.type_ == SQLType::EXEC_JOIN &&
				node.inputList_.front() == outsideId &&
				!isSubqueryIdJoinNode(
						plan, node, outsideIdColumn, correlatedColumnList));

		const std::pair<ColumnId, ColumnId> &inPos =
				makeCorrelatedInPositions(plan, node, correlatedColumnList);

		for (PlanExprList::const_iterator it = node.predList_.begin();
				it != node.predList_.end(); ++it) {
			if (!getSubqueryPredicateCorrelation(
					*it, outsideIdColumn, correlatedColumnList, inPos,
					(correlationAccepting ? &correlationList : NULL))) {
				return false;
			}
		}

		for (PlanExprList::const_iterator it = node.outputList_.begin();
				it != node.outputList_.end(); ++it) {
			if (!getSubqueryOutputCorrelation(
					*it, correlatedColumnList, inPos, foldExpr)) {
				return false;
			}
		}

		const size_t lastPos = correlatedColumnList.size();

		size_t index = inPos.first;
		for (PlanExprList::const_iterator it = node.outputList_.begin();
				it != node.outputList_.end(); ++it, ++index) {
			ColumnId columnId;
			if (it->op_ == SQLType::EXPR_COLUMN) {
				const ColumnId offset =
						(it->inputId_ == 0 ? inPos.first : inPos.second);
				columnId = correlatedColumnList[offset + it->columnId_];
			}
			else {
				columnId = UNDEF_COLUMNID;
			}
			correlatedColumnList.push_back(columnId);
		}

		correlatedColumnList.erase(
				correlatedColumnList.begin() + inPos.first,
				correlatedColumnList.begin() + lastPos);
	}

	return true;
}

bool SQLCompiler::isSubqueryIdJoinNode(
		const Plan &plan, const PlanNode &node, ColumnId outsideIdColumn,
		const util::Vector<ColumnId> &correlatedColumnList) {

	if (node.type_ != SQLType::EXEC_JOIN) {
		return false;
	}

	bool predFound = false;
	for (PlanExprList::const_iterator it = node.predList_.begin();
			it != node.predList_.end(); ++it) {
		if (!isTrueExpr(*it)) {
			predFound = true;
			break;
		}
	}

	if (!predFound) {
		return false;
	}

	const std::pair<ColumnId, ColumnId> &inPos =
			makeCorrelatedInPositions(plan, node, correlatedColumnList);

	for (PlanExprList::const_reverse_iterator it = node.predList_.rbegin();
			it != node.predList_.rend(); ++it) {
		if (findSubqueryOutsideIdRef(
				*it, outsideIdColumn, correlatedColumnList, inPos)) {
			return true;
		}
	}

	return false;
}

std::pair<ColumnId, ColumnId> SQLCompiler::makeCorrelatedInPositions(
		const Plan &plan, const PlanNode &node,
		const util::Vector<ColumnId> &correlatedColumnList) {

	assert(node.type_ != SQLType::EXEC_UNION);

	const size_t posCount = 2;
	if (node.inputList_.size() > posCount) {
		assert(false);
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	size_t lastPos = correlatedColumnList.size();
	ColumnId posList[posCount];

	for (size_t i = posCount; i > 0; i--) {
		const size_t index = i - 1;
		if (node.inputList_.size() > index) {
			const PlanNodeId nodeId = node.inputList_[index];
			const size_t size = plan.nodeList_[nodeId].outputList_.size();
			if (lastPos < size) {
				assert(false);
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
			}
			lastPos -= size;
			posList[index] = static_cast<ColumnId>(lastPos);
		}
		else {
			posList[index] = UNDEF_COLUMNID;
		}
	}

	return std::make_pair(posList[0], posList[1]);
}

bool SQLCompiler::getSubqueryPredicateCorrelation(
		const Expr &expr, ColumnId outsideIdColumn,
		const util::Vector<ColumnId> &correlatedColumnList,
		const std::pair<ColumnId, ColumnId> &inPos,
		ExprList *correlationList) {

	if (expr.op_ != SQLType::EXPR_AND && findSubqueryOutsideIdRef(
			expr, outsideIdColumn, correlatedColumnList, inPos)) {
		return true;
	}

	if (correlationList == NULL) {
		int32_t forOutside;
		return findSubquerySingleSideExpr(
				expr, NULL, correlatedColumnList, inPos, forOutside) &&
				forOutside <= 0;
	}

	if (expr.op_ == SQLType::OP_EQ || expr.op_ == SQLType::OP_NE) {
		int32_t forOutsideL;
		Expr *outExprL;
		if (!findSubquerySingleSideExpr(
				*expr.left_, &outExprL,
				correlatedColumnList, inPos, forOutsideL)) {
			return false;
		}

		int32_t forOutsideR;
		Expr *outExprR;
		if (!findSubquerySingleSideExpr(
				*expr.right_, &outExprR,
				correlatedColumnList, inPos, forOutsideR)) {
			return false;
		}

		if (forOutsideL > 0 && forOutsideR > 0) {
			return false;
		}

		if (forOutsideL > 0 || forOutsideR > 0) {
			if (forOutsideR > 0) {
				std::swap(outExprL, outExprR);
			}
			correlationList->push_back(ALLOC_NEW(alloc_) Expr(genOpExprBase(
					NULL, expr.op_, outExprL, outExprR, MODE_WHERE, true)));
		}
		return true;
	}
	else if (expr.op_ == SQLType::EXPR_AND) {
		if (!getSubqueryPredicateCorrelation(
				*expr.left_, outsideIdColumn,
				correlatedColumnList, inPos, correlationList)) {
			return false;
		}

		if (!getSubqueryPredicateCorrelation(
				*expr.right_, outsideIdColumn,
				correlatedColumnList, inPos, correlationList)) {
			return false;
		}

		return true;
	}
	else {
		int32_t forOutside;
		return findSubquerySingleSideExpr(
				expr, NULL, correlatedColumnList, inPos, forOutside) &&
				forOutside <= 0;
	}
}

bool SQLCompiler::getSubqueryOutputCorrelation(
		const Expr &expr, const util::Vector<ColumnId> &correlatedColumnList,
		const std::pair<ColumnId, ColumnId> &inPos, Expr **foldExpr) {
	if (expr.op_ == SQLType::EXPR_COLUMN || isDisabledExpr(expr)) {
		return true;
	}

	if (foldExpr != NULL && isFoldExprType(expr.op_)) {
		assert(expr.left_ == NULL);
		assert(expr.right_ == NULL);

		ExprList *foldExprList = ALLOC_NEW(alloc_) ExprList(alloc_);

		for (ExprList::const_iterator it = expr.next_->begin();
				it != expr.next_->end(); ++it) {
			Expr *subExpr;
			int32_t forOutside;
			if (!findSubquerySingleSideExpr(
					**it, &subExpr, correlatedColumnList, inPos, forOutside)) {
				return false;
			}

			if (it - expr.next_->begin() == 1 &&
					(expr.op_ == SQLType::AGG_FOLD_IN ||
					expr.op_ == SQLType::AGG_FOLD_NOT_IN)) {
				if (forOutside <= 0 || (*it)->op_ != SQLType::EXPR_COLUMN) {
					return false;
				}
			}
			else {
				if (forOutside >= 0) {
					return false;
				}
			}
			foldExprList->push_back(subExpr);
		}

		*foldExpr = ALLOC_NEW(alloc_) Expr(expr);
		(*foldExpr)->next_ = foldExprList;

		return true;
	}

	int32_t forOutside;
	return findSubquerySingleSideExpr(
			expr, NULL, correlatedColumnList, inPos, forOutside) &&
			forOutside <= 0;
}

bool SQLCompiler::findSubqueryOutsideIdRef(
		const Expr &expr, ColumnId outsideIdColumn,
		const util::Vector<ColumnId> &correlatedColumnList,
		const std::pair<ColumnId, ColumnId> &inPos) {

	if (expr.op_ == SQLType::EXPR_COLUMN) {
		assert(expr.inputId_ <= 1);
		const ColumnId offset =
				(expr.inputId_ == 0 ? inPos.first : inPos.second);
		const ColumnId correlatedColumn =
				correlatedColumnList[offset + expr.columnId_];
		return (correlatedColumn == outsideIdColumn);
	}
	else if (expr.op_ == SQLType::OP_EQ) {
		if (expr.left_->op_ != SQLType::EXPR_COLUMN ||
				expr.right_->op_ != SQLType::EXPR_COLUMN) {
			return false;
		}
		if (!findSubqueryOutsideIdRef(
				*expr.left_, outsideIdColumn, correlatedColumnList, inPos)) {
			return false;
		}
		if (!findSubqueryOutsideIdRef(
				*expr.right_, outsideIdColumn, correlatedColumnList, inPos)) {
			return false;
		}
		return true;
	}
	else if (expr.op_ == SQLType::EXPR_AND) {
		bool found = false;
		found |= !found && findSubqueryOutsideIdRef(
				*expr.left_, outsideIdColumn, correlatedColumnList, inPos);
		found |= !found && findSubqueryOutsideIdRef(
				*expr.right_, outsideIdColumn, correlatedColumnList, inPos);
		return found;
	}
	else {
		return false;
	}
}

bool SQLCompiler::findSubquerySingleSideExpr(
		const Expr &inExpr, Expr **outExpr,
		const util::Vector<ColumnId> &correlatedColumnList,
		const std::pair<ColumnId, ColumnId> &inPos, int32_t &forOutside) {
	forOutside = 0;

	if (outExpr != NULL) {
		*outExpr = NULL;
	}

	if (inExpr.op_ == SQLType::EXPR_COLUMN) {
		assert(inExpr.inputId_ <= 1);
		const ColumnId offset =
				(inExpr.inputId_ == 0 ? inPos.first : inPos.second);
		const ColumnId correlatedColumn =
				correlatedColumnList[offset + inExpr.columnId_];
		forOutside = (correlatedColumn == UNDEF_COLUMNID ? -1 : 1);

		if (outExpr != NULL) {
			*outExpr = ALLOC_NEW(alloc_) Expr(inExpr);
			if (forOutside > 0) {
				(*outExpr)->inputId_ = 0;
				(*outExpr)->columnId_ = correlatedColumn;
			}
		}

		return true;
	}

	Expr *outLeft = NULL;
	Expr *outRight = NULL;
	ExprList *outNext = NULL;

	for (ExprArgsIterator<> it(inExpr); it.exists(); it.next()) {
		const Expr &inSubExpr = it.get();
		Expr *outSubExpr;
		int32_t forOutsideSub;
		if (!findSubquerySingleSideExpr(
				inSubExpr, (outExpr == NULL ? NULL : &outSubExpr),
				correlatedColumnList, inPos, forOutsideSub)) {
			return false;
		}

		if (forOutside != forOutsideSub &&
				forOutside != 0 && forOutsideSub != 0) {
			return false;
		}

		if (forOutsideSub != 0) {
			forOutside = forOutsideSub;
		}

		if (outExpr != NULL) {
			if (&inSubExpr == inExpr.left_) {
				outLeft = outSubExpr;
			}
			else if (&inSubExpr == inExpr.right_) {
				outRight = outSubExpr;
			}
			else {
				if (outNext == NULL) {
					outNext = ALLOC_NEW(alloc_) ExprList(alloc_);
				}
				outNext->push_back(outSubExpr);
			}
		}
	}

	if (outExpr != NULL) {
		*outExpr = ALLOC_NEW(alloc_) Expr(inExpr);
		(*outExpr)->left_ = outLeft;
		(*outExpr)->right_ = outRight;
		(*outExpr)->next_ = outNext;
	}

	return true;
}

bool SQLCompiler::rewriteSubquery(
		Plan &plan, PlanNodeId subqueryJoinId, bool filterable,
		util::Set<PlanNodeId> &rewroteJoinIdSet) {
	bool rewrote = false;

	PlanNodeId outsideId;
	PlanNodeId foldNodeId;
	ColumnId outsideColumnId;
	{
		PlanNode &node = plan.nodeList_[subqueryJoinId];
		outsideId = node.inputList_[0];
		foldNodeId = node.inputList_[1];
		outsideColumnId = (node.predList_.size() > 1 &&
				node.predList_[1].op_ == SQLType::OP_EQ &&
				node.predList_[1].left_->op_ == SQLType::EXPR_COLUMN &&
				node.predList_[1].left_->inputId_ == 0 ?
						node.predList_[1].left_->columnId_ : UNDEF_COLUMNID);
	}

	if (rewroteJoinIdSet.find(foldNodeId) != rewroteJoinIdSet.end()) {
		return false;
	}

	PlanNodeId insidePredId;
	PlanNode::IdList bothSideJoinList(alloc_);
	util::Vector<ColumnId> correlatedColumnList(alloc_);
	ExprList correlationList(alloc_);
	Expr *foldExpr;
	const bool simple = getSubqueryCorrelation(
			plan, plan.nodeList_[subqueryJoinId], insidePredId,
			bothSideJoinList, correlatedColumnList, correlationList, foldExpr);

	if (simple) {
		bool unique = false;
		util::Vector<ColumnId> outsideCorrelationList(alloc_);
		util::Vector<ColumnId> insideCorrelationList(alloc_);

		pushDownCorrelatedColumns(
				plan, outsideId, insidePredId, correlationList,
				outsideCorrelationList, insideCorrelationList);
		correlatedColumnList.assign(
				outsideCorrelationList.begin(), outsideCorrelationList.end());
		if (isMatchingFoldType(foldExpr->op_)) {
			correlatedColumnList.push_back((*foldExpr->next_)[1]->columnId_);
		}

		util::Vector<Type> orgOutsideOpList(alloc_);
		{
			PlanExprList &outputList = plan.nodeList_[outsideId].outputList_;

			for (PlanExprList::iterator it = outputList.begin();
					it != outputList.end(); ++it) {
				orgOutsideOpList.push_back(it->op_);
				it->op_ = SQLType::START_EXPR;
			}

			for (util::Vector<ColumnId>::iterator it =
					outsideCorrelationList.begin();
					it != outsideCorrelationList.end(); ++it) {
				outputList[*it].op_ = orgOutsideOpList[*it];
			}
		}

		util::Vector<PlanNodeId> pendingLimitOffsetIdList(alloc_);

		typedef std::pair<uint32_t, ColumnId> NodePathStatus;
		typedef std::pair<PlanNodeId, NodePathStatus> NodePathEntry;
		typedef util::Vector<NodePathEntry> NodePath;

		util::Vector<bool> visitedList(plan.nodeList_.size(), false, alloc_);
		util::Vector<bool> fullDisabledList(plan.nodeList_.size(), false, alloc_);
		PlanNode::IdList insidePredPath(alloc_);
		NodePath nodePath(alloc_);
		nodePath.push_back(NodePathEntry(
				foldNodeId, NodePathStatus(0, UNDEF_COLUMNID)));

		while (!nodePath.empty()) {
			const PlanNodeId nodeId = nodePath.back().first;
			const uint32_t inputId = nodePath.back().second.first;
			const ColumnId insideColumnId = nodePath.back().second.second;
			nodePath.pop_back();

			PlanNode &node = plan.nodeList_[nodeId];

			if (visitedList[nodeId] || nodeId == outsideId) {
				continue;
			}

			if (inputId < node.inputList_.size()) {
				const PlanNodeId inNodeId = node.inputList_[inputId];

				ColumnId inColumnId = UNDEF_COLUMNID;
				if (nodeId == foldNodeId) {
					const Expr &expr = *(*foldExpr->next_)[0];
					assert(expr.op_ == SQLType::EXPR_COLUMN);
					inColumnId = expr.columnId_;
				}
				else if (insideColumnId != UNDEF_COLUMNID) {
					const Expr &expr = node.outputList_[insideColumnId];
					if (expr.op_ == SQLType::EXPR_COLUMN &&
							expr.inputId_ == inputId) {
						inColumnId = expr.columnId_;
					}
				}

				nodePath.push_back(NodePathEntry(
						nodeId, NodePathStatus(inputId + 1, insideColumnId)));
				nodePath.push_back(NodePathEntry(
						inNodeId, NodePathStatus(0, inColumnId)));

				continue;
			}
			else {
				ColumnId resolvedInsideColumnId = insideColumnId;
				do {
					if (!(resolvedInsideColumnId == UNDEF_COLUMNID &&
							node.type_ == SQLType::EXEC_JOIN &&
							node.inputList_[0] == outsideId)) {
						break;
					}

					for (PlanExprList::iterator it = node.outputList_.begin();
							it != node.outputList_.end(); ++it) {
						if (it->op_ == SQLType::EXPR_COLUMN &&
								it->inputId_ == 0 &&
								it->columnId_ == outsideColumnId) {
							resolvedInsideColumnId =
									static_cast<ColumnId>(it - node.outputList_.begin());
							break;
						}
					}

					if (resolvedInsideColumnId == UNDEF_COLUMNID) {
						break;
					}

					PlanNodeId prevNodeId = nodeId;
					ColumnId prevInsideColumnId = resolvedInsideColumnId;
					for (NodePath::reverse_iterator pathIt = nodePath.rbegin();
							pathIt != nodePath.rend(); ++pathIt) {
						const PlanNodeId curNodeId = pathIt->first;
						ColumnId &curInsideColumnId = pathIt->second.second;
						if (curNodeId == foldNodeId ||
								curInsideColumnId != UNDEF_COLUMNID) {
							break;
						}
						PlanNode &curNode = plan.nodeList_[curNodeId];

						for (PlanExprList::iterator it = curNode.outputList_.begin();
								it != curNode.outputList_.end(); ++it) {
							if (it->op_ == SQLType::EXPR_COLUMN &&
									curNode.inputList_[it->inputId_] == prevNodeId &&
									it->columnId_ == prevInsideColumnId) {
								curInsideColumnId = static_cast<ColumnId>(
										it - curNode.outputList_.begin());
								break;
							}
						}

						if (curInsideColumnId == UNDEF_COLUMNID) {
							break;
						}
						prevNodeId = curNodeId;
						prevInsideColumnId = curInsideColumnId;
					}
				}
				while (false);

				visitedList[nodeId] = true;

				bool typeRefreshRequired;
				disableSubqueryOutsideColumns(
						plan, node, outsideId, resolvedInsideColumnId,
						typeRefreshRequired, fullDisabledList,
						pendingLimitOffsetIdList);

				if (typeRefreshRequired) {
					refreshColumnTypes(
							plan, node, node.outputList_, true, false, true);

					for (NodePath::reverse_iterator pathIt = nodePath.rbegin();
							pathIt != nodePath.rend(); ++pathIt) {
						PlanNode &curNode = plan.nodeList_[pathIt->first];
						refreshColumnTypes(
								plan, curNode, curNode.outputList_,
								true, false, true);
					}
				}

				if (nodeId != insidePredId || insideCorrelationList.empty()) {
					continue;
				}
			}

			assert(insidePredPath.empty());
			insidePredPath.clear();

			PlanNodeId parentId = node.inputList_.front();
			nodePath.push_back(NodePathEntry(
					nodeId, NodePathStatus(inputId, insideColumnId)));
			for (NodePath::reverse_iterator pathIt = nodePath.rbegin();
					pathIt != nodePath.rend() && pathIt->first != foldNodeId;
					++pathIt) {
				assert(pathIt == nodePath.rbegin() || !visitedList[pathIt->first]);

				PlanNode &curNode = plan.nodeList_[pathIt->first];
				assert(!isNodeAggregated(curNode));

				const uint32_t curInputId = getNodeInputId(curNode, parentId);
				ColumnId nextColumnId =
						static_cast<ColumnId>(curNode.outputList_.size());

				for (util::Vector<ColumnId>::iterator it =
						insideCorrelationList.begin();
						it != insideCorrelationList.end(); ++it) {
					curNode.outputList_.push_back(genColumnExprBase(
							&plan, &curNode, curInputId, *it, NULL));

					*it = nextColumnId;
					++nextColumnId;
				}

				if (curNode.type_ == SQLType::EXEC_GROUP) {
					unique = (curNode.predList_.size() <= 1);
					const size_t correlationCount = insideCorrelationList.size();

					curNode.predList_.insert(
							curNode.predList_.begin(),
							curNode.outputList_.end() - correlationCount,
							curNode.outputList_.end());
				}

				parentId = pathIt->first;
				insidePredPath.push_back(pathIt->first);
			}
			std::reverse(insidePredPath.begin(), insidePredPath.end());
			nodePath.pop_back();
		}

		for (util::Vector<PlanNodeId>::iterator it =
				pendingLimitOffsetIdList.begin();
				it != pendingLimitOffsetIdList.end(); ++it) {
			disableSubqueryLimitOffset(plan, *it);
		}

		{
			PlanExprList &outputList = plan.nodeList_[outsideId].outputList_;
			for (PlanExprList::iterator it = outputList.begin();
					it != outputList.end(); ++it) {
				it->op_ = orgOutsideOpList[it - outputList.begin()];
			}
		}

		rewriteSimpleSubquery(
				plan, subqueryJoinId, filterable, unique,
				*foldExpr, correlationList,
				outsideCorrelationList, insideCorrelationList, insidePredPath);
		rewrote = true;
	}

	rewrote |= pushDownSubquery(
			plan, subqueryJoinId, outsideId, filterable, bothSideJoinList,
			correlatedColumnList, rewroteJoinIdSet);

	plan.validateReference(tableInfoList_);

	return rewrote;
}

void SQLCompiler::rewriteSimpleSubquery(
		Plan &plan, PlanNodeId subqueryJoinId, bool filterable,
		bool unique, const Expr &foldExpr, const ExprList &correlationList,
		const util::Vector<ColumnId> &outsideCorrelationList,
		const util::Vector<ColumnId> &insideCorrelationList,
		const PlanNode::IdList &insidePredPath) {



	bool neFound = false;
	bool eqFound = false;
	for (ExprList::const_iterator it = correlationList.begin();
			it != correlationList.end(); ++it) {
		if ((*it)->op_ == SQLType::OP_NE) {
			neFound = true;
		}
		else {
			eqFound = true;
		}
	}

	const bool correlated = !correlationList.empty();
	const bool negative = isNegativeFoldType(foldExpr.op_);
	const bool matching = isMatchingFoldType(foldExpr.op_);
	const bool scalar = (foldExpr.op_ == SQLType::AGG_FOLD_UPTO_ONE);

	const bool filtering =
			(filterable && !negative && !neFound && (correlated || matching) &&
			!scalar);

	const bool normal = !(matching && filtering);

	const PlanNodeId undefId = std::numeric_limits<PlanNodeId>::max();

	PlanNodeId outsideId;
	PlanNodeId foldId;
	{
		const PlanNode &node = plan.nodeList_[subqueryJoinId];
		outsideId = node.inputList_[0];
		foldId = node.inputList_[1];
	}

	PlanNodeId preFoldId;
	{
		const PlanNode &node = plan.nodeList_[foldId];
		preFoldId = node.inputList_[0];
	}

	PlanNodeId compensationId = undefId;
	const bool compensating = scalar && setUpSimpleSubqueryCompensationGroup(
			plan, foldExpr, insidePredPath, correlationList.size(),
			compensationId);

	if (!unique && !correlated && !matching) {
		PlanNode &node = plan.nodeList_[preFoldId];
		if (node.subLimit_ < 0 && node.subOffset_ <= 0) {
			const int64_t upperLimit = (scalar ? 2 : 1);
			const int64_t orgLimit = node.limit_;
			mergeLimit(upperLimit, orgLimit, 0, &node.limit_);
		}
	}

	const size_t groupIdCount = 4;
	PlanNodeId groupIdList[groupIdCount];

	for (size_t i = 0; i < groupIdCount; i++) {
		bool enabled = false;
		bool reusable = false;
		PlanNodeId inNodeId = undefId;
		switch (i) {
		case 0:
			enabled = (neFound && matching);
			inNodeId = preFoldId;
			break;
		case 1:
			enabled = (neFound && normal);
			inNodeId = (groupIdList[0] == undefId ?
					preFoldId : groupIdList[0]);
			break;
		case 2:
			enabled = matching || compensating;
			reusable = !normal;
			inNodeId = (groupIdList[0] == undefId ?
					preFoldId : groupIdList[0]);
			break;
		case 3:
			enabled = normal;
			reusable = normal;
			inNodeId = (groupIdList[1] == undefId ?
					(groupIdList[2] == undefId || compensating ?
							preFoldId : groupIdList[2]) :
					groupIdList[1]);
			break;
		}

		if (!enabled) {
			groupIdList[i] = undefId;
			continue;
		}

		const bool withNE = (i <= 1);
		const bool withValue = (i % 2 == 0);
		const bool neReducing = (neFound && !withNE);

		if (!withNE && withValue && compensating) {
			groupIdList[i] = compensationId;
			continue;
		}

		const bool preGrouped =
				(unique && !matching && inNodeId == preFoldId);
		const Type nodeType =
				(preGrouped || (!withNE && !withValue && !eqFound) ?
				SQLType::EXEC_SELECT : SQLType::EXEC_GROUP);

		PlanNodeId nodeId;
		PlanNode *node;
		if (reusable) {
			nodeId = foldId;
			node = &plan.nodeList_[nodeId];

			*node = PlanNode(alloc_);
			node->id_ = nodeId;
			node->type_ = nodeType;
		}
		else {
			node = &plan.append(nodeType);
			nodeId = node->id_;
		}

		node->inputList_.push_back(inNodeId);

		setUpSimpleSubqueryGroup(
				plan, *node, foldExpr,
				withValue, withNE, neReducing, preGrouped, preFoldId,
				correlationList, insideCorrelationList, compensating);

		assert(nodeType != SQLType::EXEC_GROUP ||
				!node->predList_.empty());

		groupIdList[i] = nodeId;
	}

	PlanExprList subqueryOutputList(alloc_);
	{
		PlanNode &node = plan.nodeList_[subqueryJoinId];
		subqueryOutputList.swap(node.outputList_);
	}

	const size_t outsideColumnCount =
			plan.nodeList_[outsideId].outputList_.size();

	const size_t joinIdCount = 4;
	PlanNodeId lastJoinId = outsideId;

	for (size_t i = 0; i < joinIdCount; i++) {
		const size_t groupIndex = (groupIdCount - i - 1);

		const PlanNodeId leftId = lastJoinId;
		const PlanNodeId rightId = groupIdList[groupIndex];

		if (rightId == undefId) {
			lastJoinId = leftId;
			continue;
		}

		const bool forResult = (static_cast<size_t>(std::count(
				&groupIdList[0], &groupIdList[groupIndex], undefId)) ==
				groupIndex);

		const bool withNE = (i > 1);
		const bool withValue = (i % 2 != 0);

		const Type nodeType = SQLType::EXEC_JOIN;
		const SQLType::JoinType joinType =
				(filtering || (!withValue && !correlated) ?
						SQLType::JOIN_INNER : SQLType::JOIN_LEFT_OUTER);

		PlanNodeId nodeId;
		PlanNode *node;

		if (forResult) {
			nodeId = subqueryJoinId;
			node = &plan.nodeList_[nodeId];

			*node = PlanNode(alloc_);
			node->id_ = nodeId;
			node->type_ = nodeType;
		}
		else {
			node = &plan.append(nodeType);
			nodeId = node->id_;
		}

		node->joinType_ = joinType;
		node->inputList_.push_back(leftId);
		node->inputList_.push_back(rightId);

		setUpSimpleSubqueryJoin(
				plan, *node, foldExpr, filtering, withValue, withNE,
				forResult, subqueryOutputList, outsideColumnCount,
				correlationList, outsideCorrelationList, compensating);

		lastJoinId = nodeId;
	}
}

void SQLCompiler::setUpSimpleSubqueryGroup(
		Plan &plan, PlanNode &node, const Expr &foldExpr,
		bool withValue, bool withNE, bool neReducing, bool preGrouped,
		PlanNodeId preFoldId, const ExprList &correlationList,
		const util::Vector<ColumnId> &insideCorrelationList,
		bool compensating) {

	const bool correlated = !correlationList.empty();
	const bool matching = isMatchingFoldType(foldExpr.op_);
	const PlanNodeId inNodeId = node.inputList_[0];

	ColumnId reducedNECount = 0;
	ColumnId eqCount = 0;
	for (ExprList::const_iterator it = correlationList.begin();; ++it) {
		const bool forValue = (it == correlationList.end());
		if (forValue) {
			if (!(withValue && matching)) {
				break;
			}
		}
		else if ((*it)->op_ == SQLType::OP_NE && !withNE) {
			if (neReducing) {
				reducedNECount++;
			}
			continue;
		}

		ColumnId inColumnId;
		if (inNodeId == preFoldId) {
			if (forValue) {
				inColumnId = (*foldExpr.next_)[2]->columnId_;
			}
			else {
				inColumnId =
						insideCorrelationList[it - correlationList.begin()];
			}
		}
		else {
			inColumnId = static_cast<ColumnId>(node.outputList_.size()) +
					reducedNECount;
		}

		const Expr &expr =
				genColumnExprBase(&plan, &node, 0, inColumnId, NULL);

		if (!preGrouped) {
			assert(node.type_ == SQLType::EXEC_GROUP);
			node.predList_.push_back(expr);
		}
		node.outputList_.push_back(expr);

		if (forValue) {
			break;
		}
		eqCount++;
	}

	const Mode mode = (node.type_ == SQLType::EXEC_SELECT ?
			MODE_AGGR_SELECT : MODE_GROUP_SELECT);

	Expr *nullCondExpr = NULL;
	if (eqCount > 0) {
		ExprList condList(alloc_);
		{
			PlanExprList::const_iterator it = node.outputList_.begin();
			PlanExprList::const_iterator end = it + eqCount;
			for (; it != end; ++it) {
				Expr *columnExpr = ALLOC_NEW(alloc_) Expr(alloc_);
				copyExpr(*it, *columnExpr);

				Expr *subExpr = ALLOC_NEW(alloc_) Expr(genOpExprBase(
						NULL, SQLType::OP_IS_NOT_NULL, columnExpr,
						NULL, mode, true));
				subExpr->columnType_ = TupleList::TYPE_BOOL;

				condList.push_back(subExpr);
			}
		}

		exprListToTree(
				NULL, SQLType::EXPR_AND, condList, mode, nullCondExpr);
	}

	Expr *baseIdExpr = ALLOC_NEW(alloc_) Expr(
			genConstExpr(plan, TupleValue(static_cast<int64_t>(1)), mode));

	Expr *nullableIdExpr;
	if (nullCondExpr == NULL) {
		nullableIdExpr = baseIdExpr;
	}
	else {
		ExprList *caseExprList =  ALLOC_NEW(alloc_) ExprList(alloc_);
		caseExprList->push_back(nullCondExpr);
		caseExprList->push_back(baseIdExpr);
		nullableIdExpr = ALLOC_NEW(alloc_) Expr(genExprWithArgs(
				plan, SQLType::EXPR_CASE, caseExprList, mode, true));
	}

	if (foldExpr.op_ == SQLType::AGG_FOLD_UPTO_ONE) {
		assert(!withValue);
		assert(!withNE);
		assert(inNodeId == preFoldId);

		const ColumnId inColumnId = (*foldExpr.next_)[1]->columnId_;
		const Expr &colExpr =
				genColumnExprBase(&plan, &node, 0, inColumnId, NULL);

		if (preGrouped && correlated) {
			node.outputList_.push_back(colExpr);
		}
		else {
			Expr *idExpr = nullableIdExpr;

			ExprList *aggrArgs = ALLOC_NEW(alloc_) ExprList(alloc_);

			aggrArgs->push_back(idExpr);
			aggrArgs->push_back(ALLOC_NEW(alloc_) Expr(colExpr));

			node.outputList_.push_back(genExprWithArgs(
					plan, foldExpr.op_, aggrArgs, mode, true));
		}

		if (compensating) {
			const TupleValue &constValue = TupleValue(static_cast<int64_t>(1));
			node.outputList_.push_back(genConstExpr(plan, constValue, mode));
		}
	}
	else if (matching && !withValue) {
		assert(!preGrouped);

		ColumnId baseInColumnId;
		if (inNodeId == preFoldId) {
			baseInColumnId = (*foldExpr.next_)[2]->columnId_;
		}
		else {
			baseInColumnId = static_cast<ColumnId>(
					node.outputList_.size()) + reducedNECount;
		}

		for (ColumnId outIndex = 0; outIndex < 2; outIndex++) {
			Type aggrType;
			Expr *argExpr = NULL;
			if (neReducing) {
				aggrType = SQLType::AGG_SUM;
			}
			else {
				if (outIndex == 0) {
					argExpr = nullableIdExpr;
				}
				aggrType = SQLType::AGG_COUNT_COLUMN;
			}

			ExprList *aggrArgs = ALLOC_NEW(alloc_) ExprList(alloc_);
			if (argExpr == NULL) {
				const ColumnId inColumnId =
						baseInColumnId + (neReducing ? outIndex : 0);

				Expr *colExpr = ALLOC_NEW(alloc_) Expr(genColumnExprBase(
						&plan, &node, 0, inColumnId, NULL));

				if (nullCondExpr == NULL) {
					argExpr = colExpr;
				}
				else {
					ExprList *caseExprList =  ALLOC_NEW(alloc_) ExprList(alloc_);
					caseExprList->push_back(nullCondExpr);
					caseExprList->push_back(colExpr);
					argExpr = ALLOC_NEW(alloc_) Expr(genExprWithArgs(
							plan, SQLType::EXPR_CASE, caseExprList, mode, true));
				}
			}
			aggrArgs->push_back(argExpr);

			node.outputList_.push_back(genExprWithArgs(
					plan, aggrType, aggrArgs, mode, true));
		}
	}
	else {
		if (withNE || preGrouped) {
			node.outputList_.push_back(*nullableIdExpr);
		}
		else {
			Type aggrType;
			ExprList *exprList =  ALLOC_NEW(alloc_) ExprList(alloc_);
			if (neReducing) {
				const ColumnId inColumnId = static_cast<ColumnId>(
						node.outputList_.size()) + reducedNECount;

				exprList->push_back(ALLOC_NEW(alloc_) Expr(
						genColumnExprBase(
								&plan, &node, 0, inColumnId, NULL)));
				aggrType = SQLType::AGG_SUM;
			}
			else {
				exprList->push_back(nullableIdExpr);
				aggrType = SQLType::AGG_COUNT_COLUMN;
			}
			node.outputList_.push_back(genExprWithArgs(
					plan, aggrType, exprList, mode, true));
		}
	}
}

bool SQLCompiler::setUpSimpleSubqueryCompensationGroup(
		Plan &plan, const Expr &foldExpr,
		const PlanNode::IdList &insidePredPath, size_t correlationCount,
		PlanNodeId &outNodeId) {
	outNodeId = UNDEF_PLAN_NODEID;
	bool compensating = false;
	for (PlanNode::IdList::const_reverse_iterator pathIt =
			insidePredPath.rbegin(); pathIt != insidePredPath.rend();
			++pathIt) {
		const PlanNodeId orgNodeId = *pathIt;

		if (!compensating) {
			PlanNodeId inNodeId;
			{
				const PlanNode &orgNode = plan.nodeList_[orgNodeId];
				if (orgNode.type_ != SQLType::EXEC_GROUP ||
						getNodeAggregationLevel(orgNode.outputList_) <= 0 ||
						orgNode.predList_.size() > correlationCount) {
					continue;
				}
				inNodeId = orgNode.inputList_.front();
				compensating = true;
			}

			PlanNode &node = plan.append(SQLType::EXEC_LIMIT);
			node.inputList_.push_back(inNodeId);
			node.outputList_ = mergeOutput(plan, &node);
			node.limit_ = 0;
		}

		const PlanNodeId prevNodeId = plan.back().id_;
		const Type orgNodeType = plan.nodeList_[orgNodeId].type_;

		PlanNode &node = plan.append(orgNodeType);
		node.inputList_.push_back(prevNodeId);
		{
			const PlanNode &orgNode = plan.nodeList_[orgNodeId];
			const bool correlatingPredKey =
					(orgNode.type_ == SQLType::EXEC_GROUP ||
					orgNode.type_ == SQLType::EXEC_SORT);

			if (orgNode.inputList_.size() != 1 ||
					orgNode.subLimit_ >= 0 || orgNode.subOffset_ > 0) {
				assert(false);
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
			}

			for (size_t i = 0; i < 2; i++) {
				const bool forPred = (i == 0);
				const PlanExprList &srcList =
						(forPred ? orgNode.predList_ : orgNode.outputList_);
				PlanExprList &destList =
						(forPred ? node.predList_ : node.outputList_);

				const size_t ignorableSize =
						(forPred && !correlatingPredKey ? 0 : correlationCount);
				if (ignorableSize > srcList.size() ||
						(!forPred && srcList.size() - ignorableSize <= 0)) {
					assert(false);
					SQL_COMPILER_THROW_ERROR(
							GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
				}

				const PlanExprList::const_iterator endIt =
						srcList.end() - ignorableSize;
				for (PlanExprList::const_iterator it = srcList.begin();
						it != endIt; ++it) {
					destList.push_back(Expr(alloc_));
					copyExpr(*it, destList.back());
				}
			}
			if (correlatingPredKey && node.predList_.empty()) {
				node.type_ = SQLType::EXEC_SELECT;
			}
			node.limit_ = orgNode.limit_;
			node.offset_ = orgNode.offset_;
		}
	}

	if (compensating) {
		const PlanNodeId prevNodeId = plan.back().id_;

		PlanNode &node = plan.append(SQLType::EXEC_SELECT);
		node.inputList_.push_back(prevNodeId);

		const ColumnId inColumnId = (*foldExpr.next_)[1]->columnId_;
		node.outputList_.push_back(
				genColumnExprBase(&plan, &node, 0, inColumnId, NULL));

		outNodeId = node.id_;
	}
	return compensating;
}

void SQLCompiler::setUpSimpleSubqueryJoin(
		Plan &plan, PlanNode &node, const Expr &foldExpr,
		bool filtering, bool withValue, bool withNE,
		bool forResult, const PlanExprList &subqueryOutputList,
		size_t outsideColumnCount, const ExprList &correlationList,
		const util::Vector<ColumnId> &outsideCorrelationList,
		bool compensating) {

	const bool matching = isMatchingFoldType(foldExpr.op_);

	const PlanNodeId leftId = node.inputList_[0];
	const PlanNodeId rightId = node.inputList_[1];

	ColumnId resultColumnId = UNDEF_COLUMNID;
	if (forResult) {
		for (PlanExprList::const_iterator it = subqueryOutputList.begin();
				it != subqueryOutputList.end(); ++it) {

			const bool forResultColumn =
					(it->op_ == SQLType::EXPR_COLUMN && it->inputId_ != 0);

			if (forResultColumn || isDisabledExpr(*it)) {
				if (forResultColumn) {
					if (resultColumnId != UNDEF_COLUMNID) {
						resultColumnId = UNDEF_COLUMNID;
						break;
					}
					resultColumnId = static_cast<ColumnId>(
							it - subqueryOutputList.begin());
				}

				node.outputList_.push_back(*it);
				continue;
			}

			node.outputList_.push_back(genColumnExprBase(
					&plan, &node, 0, it->columnId_, NULL));
		}

		if (forResult && resultColumnId == UNDEF_COLUMNID) {
			assert(false);
			SQL_COMPILER_THROW_ERROR(
					GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
		}
	}
	else {
		const PlanNode &inNode = plan.nodeList_[leftId];

		for (ColumnId columnId = 0; columnId < outsideColumnCount;
				++columnId) {

			if (isDisabledExpr(inNode.outputList_[columnId])) {
				node.outputList_.push_back(genDisabledExpr());
				continue;
			}

			node.outputList_.push_back(genColumnExprBase(
					&plan, &node, 0, columnId, NULL));
		}
	}

	ExprList condList(alloc_);
	ExprList *nullCondList = NULL;
	if (forResult && withNE) {
		nullCondList = ALLOC_NEW(alloc_) ExprList(alloc_);
	}

	for (ExprList::const_iterator it = correlationList.begin();; ++it) {
		const bool forValue = (it == correlationList.end());
		if (forValue) {
			if (!(withValue && matching)) {
				break;
			}
		}
		else if ((*it)->op_ == SQLType::OP_NE && !withNE) {
			continue;
		}
		else if (forResult && compensating) {
			continue;
		}

		ColumnId leftColumnId;
		if (forValue) {
			leftColumnId = (*foldExpr.next_)[1]->columnId_;
		}
		else {
			leftColumnId =
					outsideCorrelationList[it - correlationList.begin()];
		}

		const ColumnId rightColumnId = static_cast<ColumnId>(condList.size());

		Expr *leftExpr = ALLOC_NEW(alloc_) Expr(genColumnExprBase(
				&plan, &node, 0, leftColumnId, NULL));
		Expr *rightExpr = ALLOC_NEW(alloc_) Expr(genColumnExprBase(
				&plan, &node, 1, rightColumnId, NULL));

		condList.push_back(ALLOC_NEW(alloc_) Expr(genOpExpr(
				plan, SQLType::OP_EQ, leftExpr, rightExpr,
				MODE_WHERE, true)));

		if (nullCondList != NULL && !forValue) {
			Expr *condExpr = ALLOC_NEW(alloc_) Expr(genOpExpr(
					plan, SQLType::OP_IS_NOT_NULL, leftExpr, NULL,
					MODE_NORMAL_SELECT, true));
			condExpr->columnType_ = TupleList::TYPE_BOOL;
			nullCondList->push_back(condExpr);
		}

		if (forValue) {
			break;
		}
	}

	const size_t condCount = condList.size();
	if (condCount > 0) {
		const size_t predPos =
				(node.joinType_ == SQLType::JOIN_LEFT_OUTER ? 2 : 1);
		while (node.predList_.size() < predPos) {
			node.predList_.push_back(genConstExpr(
					plan, makeBoolValue(true), MODE_WHERE));
		}

		Expr *condExpr;
		exprListToTree(
				NULL, SQLType::EXPR_AND, condList, MODE_WHERE, condExpr);
		node.predList_.push_back(*condExpr);

		condList.clear();
	}

	Expr *nullCondExpr = NULL;
	if (nullCondList != NULL) {
		exprListToTree(
				NULL, SQLType::EXPR_AND, *nullCondList,
				MODE_NORMAL_SELECT, nullCondExpr);
	}

	if (forResult) {
		assert(outsideColumnCount > 0);

		const ColumnId baseColumnIdList[2] = {
			static_cast<ColumnId>(outsideColumnCount),
			static_cast<ColumnId>(condCount)
		};

		Expr &outExpr = node.outputList_[resultColumnId];
		const ColumnType orgColumnType = outExpr.columnType_;

		outExpr = makeSimpleSubqueryJoinResult(
				plan, node, baseColumnIdList, foldExpr, filtering, withNE,
				compensating, nullCondExpr);
		do {
			if (outExpr.columnType_ == orgColumnType ||
					ColumnTypeUtils::isNull(orgColumnType)) {
				break;
			}

			if (outExpr.op_ == SQLType::EXPR_CONSTANT &&
					setColumnTypeNullable(outExpr.columnType_, true) ==
							orgColumnType) {
				outExpr.columnType_ = orgColumnType;
				break;
			}

			assert(false);
			SQL_COMPILER_THROW_ERROR(
					GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
		}
		while (false);
	}
	else {
		PlanExprList &outList = node.outputList_;
		for (uint32_t c = 0; c < 2; c++) {
			const PlanNodeId inNodeId = (c == 0 ? leftId : rightId);
			const PlanExprList &inList = plan.nodeList_[inNodeId].outputList_;

			size_t outPos = outsideColumnCount;
			const size_t inPos = (c == 0 ? outPos : condCount);

			assert(outPos <= outList.size());
			assert(inPos <= inList.size());

			const bool merging = (c != 0 && !withValue && withNE);
			if (merging) {
				const size_t inCount = (inList.size() - inPos);
				assert(outList.size() >= outsideColumnCount + inCount);

				outPos = outList.size() - inCount;
			}

			PlanExprList::const_iterator inIt = inList.begin() + inPos;
			PlanExprList::iterator outIt = outList.begin() + outPos;

			for (; inIt != inList.end(); ++inIt, ++outIt) {
				const ColumnId columnId =
						static_cast<ColumnId>(inIt - inList.begin());
				const Expr &colExpr =
						genColumnExprBase(&plan, &node, c, columnId, NULL);

				if (!merging) {
					outIt = node.outputList_.insert(outIt, colExpr);
					continue;
				}

				ExprList *exprList = ALLOC_NEW(alloc_) ExprList(alloc_);
				exprList->push_back(ALLOC_NEW(alloc_) Expr(colExpr));
				exprList->push_back(ALLOC_NEW(alloc_) Expr(genConstExpr(
						plan, TupleValue(static_cast<int64_t>(0)),
						MODE_NORMAL_SELECT)));
				Expr *opExpr = ALLOC_NEW(alloc_) Expr(genExprWithArgs(
						plan, SQLType::FUNC_COALESCE, exprList,
						MODE_NORMAL_SELECT, true));

				const Expr &outExpr = genOpExpr(
						plan, SQLType::OP_SUBTRACT,
						ALLOC_NEW(alloc_) Expr(*outIt), opExpr,
						MODE_NORMAL_SELECT, true);
				*outIt = outExpr;
			}
		}
	}
}

SQLCompiler::Expr SQLCompiler::makeSimpleSubqueryJoinResult(
		const Plan &plan, const PlanNode &node,
		const ColumnId (&baseColumnIdList)[2],
		const Expr &foldExpr, bool filtering, bool withNE,
		bool compensating, Expr *nullCondExpr) {

	const Mode mode = MODE_NORMAL_SELECT;

	if (filtering) {
		return genConstExpr(plan, makeBoolValue(true), mode);
	}

	const bool negative = isNegativeFoldType(foldExpr.op_);
	const bool matching = isMatchingFoldType(foldExpr.op_);

	const size_t forValue = 0;
	const size_t forMatch = 1;
	const size_t forCount = 2;
	const size_t forNonNull = 3;
	ColumnId columnIdList[4][2];

	for (uint32_t c = 0; c < 2; c++) {
		ColumnId baseColumnId = baseColumnIdList[c];

		for (size_t type = 0; type < 4; type++) {
			ColumnId columnId = UNDEF_COLUMNID;

			if (foldExpr.op_ == SQLType::AGG_FOLD_UPTO_ONE) {
				if (type == forValue) {
					if (!(c == 0 && !compensating)) {
						columnId = baseColumnId;
					}
				}
				else if (type == forNonNull) {
					if (c == 0 && compensating) {
						columnId = baseColumnId + 1;
					}
				}
			}
			else if (matching) {
				if (type == forValue) {
					if (c == 0) {
						columnId = (*foldExpr.next_)[1]->columnId_;
					}
				}
				else if (type == forMatch) {
					if (c != 0 || withNE) {
						columnId = baseColumnId;
					}
				}
				else if (c == 0) {
					columnId = baseColumnId + static_cast<ColumnId>(
							type - (withNE ? 1 : 2));
				}
			}
			else {
				if (type == forCount && (c != 0 || withNE)) {
					columnId = baseColumnId;
				}
			}

			columnIdList[type][c] = columnId;
		}
	}

	const Expr zeroExpr(genConstExpr(
			plan, TupleValue(static_cast<int64_t>(0)), mode));

	if (foldExpr.op_ == SQLType::AGG_FOLD_UPTO_ONE) {
		if (!compensating) {
			return genColumnExprBase(
					&plan, &node, 1, columnIdList[forValue][1], NULL);
		}

		Expr *condColumnExpr = ALLOC_NEW(alloc_) Expr(genColumnExprBase(
				&plan, &node, 0, columnIdList[forNonNull][0], NULL));
		Expr *condExpr = ALLOC_NEW(alloc_) Expr(genOpExpr(
				plan, SQLType::OP_IS_NULL, condColumnExpr, NULL, mode, true));

		ExprList *exprList = ALLOC_NEW(alloc_) ExprList(alloc_);
		exprList->push_back(condExpr);
		for (uint32_t c = 1;; c--) {
			exprList->push_back(ALLOC_NEW(alloc_) Expr(genColumnExprBase(
					&plan, &node, c, columnIdList[forValue][c], NULL)));
			if (c <= 0) {
				break;
			}
		}

		return genExprWithArgs(plan, SQLType::EXPR_CASE, exprList, mode, true);
	}
	else if (matching) {
		Expr *opList[4][2] = { { NULL } };
		for (size_t type = 0; type < 4; type++) {
			for (uint32_t c = 0; c < 2; c++) {
				Expr *&opExpr = opList[type][c];
				const ColumnId columnId = columnIdList[type][c];

				if (columnId == UNDEF_COLUMNID) {
					if (!withNE || type == forCount) {
						opExpr = ALLOC_NEW(alloc_) Expr(zeroExpr);
					}
					continue;
				}

				Expr *colExpr = ALLOC_NEW(alloc_) Expr(
						genColumnExprBase(
								&plan, &node, c, columnId, NULL));

				if (type == forValue) {
					opExpr = ALLOC_NEW(alloc_) Expr(genOpExpr(
							plan, SQLType::OP_IS_NULL, colExpr, NULL,
							mode, true));
				}
				else {
					ExprList *exprList =
							ALLOC_NEW(alloc_) ExprList(alloc_);
					exprList->push_back(colExpr);
					exprList->push_back(ALLOC_NEW(alloc_) Expr(zeroExpr));
					opExpr = ALLOC_NEW(alloc_) Expr(genExprWithArgs(
							plan, SQLType::FUNC_COALESCE, exprList,
							mode, true));
				}
			}
		}

		Expr *caseCountExpr = ALLOC_NEW(alloc_) Expr(genOpExpr(
				plan, SQLType::OP_NE,
				opList[forCount][0], opList[forCount][1], mode, true));

		Expr *caseNullExprList[2];
		{
			caseNullExprList[0] = ALLOC_NEW(alloc_) Expr(alloc_);
			caseNullExprList[1] = opList[forNonNull][0];
			copyExpr(*opList[forCount][0], *caseNullExprList[0]);
		}

		Expr *caseNullExpr = ALLOC_NEW(alloc_) Expr(genOpExpr(
				plan, SQLType::OP_NE,
				caseNullExprList[0], caseNullExprList[1], mode, true));

		Expr *caseBothNullExpr = ALLOC_NEW(alloc_) Expr(genOpExpr(
				plan, SQLType::EXPR_OR,
				caseNullExpr, opList[forValue][0], mode, true));

		Expr *caseCondExpr = ALLOC_NEW(alloc_) Expr(genOpExpr(
				plan, SQLType::EXPR_AND,
				caseCountExpr, caseBothNullExpr, mode, true));

		ExprList *caseExprList = ALLOC_NEW(alloc_) ExprList(alloc_);
		caseExprList->push_back(caseCondExpr);
		caseExprList->push_back(ALLOC_NEW(alloc_) Expr(
				genConstExpr(plan, TupleValue(), mode)));
		caseExprList->push_back(ALLOC_NEW(alloc_) Expr(
				genConstExpr(plan, makeBoolValue(negative), mode)));

		Expr *caseExpr = ALLOC_NEW(alloc_) Expr(genExprWithArgs(
				plan, SQLType::EXPR_CASE, caseExprList, mode, true));

		Expr *matchExpr = ALLOC_NEW(alloc_) Expr(genOpExpr(
				plan, (negative ? SQLType::OP_EQ : SQLType::OP_NE),
				opList[forMatch][0], opList[forMatch][1], mode, true));

		Expr *condExpr = ALLOC_NEW(alloc_) Expr(genOpExpr(
				plan, (negative ?
						SQLType::EXPR_AND : SQLType::EXPR_OR),
				matchExpr, caseExpr, mode, true));
		if (nullCondExpr == NULL) {
			return *condExpr;
		}

		ExprList *exprList = ALLOC_NEW(alloc_) ExprList(alloc_);
		exprList->push_back(nullCondExpr);
		exprList->push_back(condExpr);
		exprList->push_back(ALLOC_NEW(alloc_) Expr(
				genConstExpr(plan, makeBoolValue(negative), mode)));

		return genExprWithArgs(
				plan, SQLType::EXPR_CASE, exprList, mode, true);
	}
	else {
		Expr *opList[2];
		for (uint32_t c = 0; c < 2; c++) {
			Expr *&opExpr = opList[c];
			const ColumnId columnId = columnIdList[forCount][c];

			if (columnId == UNDEF_COLUMNID) {
				opExpr = ALLOC_NEW(alloc_) Expr(zeroExpr);
				continue;
			}

			Expr *colExpr = ALLOC_NEW(alloc_) Expr(
					genColumnExprBase(&plan, &node, c, columnId, NULL));

			ExprList *exprList = ALLOC_NEW(alloc_) ExprList(alloc_);
			exprList->push_back(colExpr);
			exprList->push_back(
					ALLOC_NEW(alloc_) Expr(zeroExpr));
			Expr *countExpr = ALLOC_NEW(alloc_) Expr(genExprWithArgs(
					plan, SQLType::FUNC_COALESCE, exprList,
					mode, true));

			opExpr = ALLOC_NEW(alloc_) Expr(genCastExpr(
					plan, TupleList::TYPE_LONG, countExpr, mode, true, true));
		}

		Expr *condExpr = ALLOC_NEW(alloc_) Expr(genOpExpr(
				plan, (negative ? SQLType::OP_EQ : SQLType::OP_NE),
				opList[0], opList[1], mode, true));
		if (nullCondExpr == NULL) {
			return *condExpr;
		}

		ExprList *exprList = ALLOC_NEW(alloc_) ExprList(alloc_);
		exprList->push_back(nullCondExpr);
		exprList->push_back(condExpr);
		exprList->push_back(ALLOC_NEW(alloc_) Expr(
				genConstExpr(plan, makeBoolValue(negative), mode)));

		return genExprWithArgs(
				plan, SQLType::EXPR_CASE, exprList, mode, true);
	}
}

bool SQLCompiler::pushDownSubquery(
		Plan &plan, PlanNodeId subqueryJoinId, PlanNodeId outsideId,
		bool filterable, const PlanNode::IdList &bothSideJoinList,
		const util::Vector<ColumnId> &correlatedColumnList,
		util::Set<PlanNodeId> &rewroteJoinIdSet) {

	static_cast<void>(filterable);

	rewroteJoinIdSet.insert(subqueryJoinId);

	if (correlatedColumnList.empty()) {
		return false;
	}

	util::Vector<ColumnId> usedColumnList(
			correlatedColumnList.begin(), correlatedColumnList.end(), alloc_);
	util::Vector<ColumnId> nextUsedColumnList(alloc_);

	size_t distanceFromOutside = 0;
	bool outsideFound = false;
	bool joinFound = false;
	PlanNode::IdList nodePath(alloc_);
	for (PlanNodeId nodeId = subqueryJoinId;;) {
		nodePath.push_back(nodeId);

		const PlanNode &node = plan.nodeList_[nodeId];
		if (!(node.type_ == SQLType::EXEC_SELECT ||
				node.type_ == SQLType::EXEC_JOIN) ||
				getNodeAggregationLevel(node.outputList_) != 0 ||
				node.limit_ >= 0 || node.offset_ > 0) {
			break;
		}

		if (!outsideFound) {
			if (nodeId != outsideId) {
				nodeId = node.inputList_[0];
				continue;
			}
			outsideFound = true;
		}

		uint32_t inputId = std::numeric_limits<uint32_t>::max();
		for (util::Vector<ColumnId>::iterator it = usedColumnList.begin();
				it != usedColumnList.end(); ++it) {
			if (*it == UNDEF_COLUMNID) {
				nextUsedColumnList.push_back(UNDEF_COLUMNID);
				continue;
			}

			const Expr &expr = node.outputList_[*it];

			if (expr.op_ == SQLType::EXPR_COLUMN) {
				if (inputId != std::numeric_limits<uint32_t>::max() &&
						expr.inputId_ != inputId) {
					nextUsedColumnList.clear();
					break;
				}
				inputId = expr.inputId_;
				nextUsedColumnList.push_back(expr.columnId_);
			}
			else if (expr.op_ == SQLType::EXPR_ID && nodeId == outsideId) {
				nextUsedColumnList.push_back(UNDEF_COLUMNID);
			}
			else {
				nextUsedColumnList.clear();
				break;
			}
		}

		if (inputId == std::numeric_limits<uint32_t>::max() ||
				nextUsedColumnList.empty()) {
			break;
		}

		if (node.type_ == SQLType::EXEC_JOIN &&
				!(node.joinType_ == SQLType::JOIN_INNER ||
				(node.joinType_ == SQLType::JOIN_LEFT_OUTER &&
						inputId == 0) ||
				(node.joinType_ == SQLType::JOIN_RIGHT_OUTER &&
						inputId == 1))) {
			break;
		}

		if (nodeId != outsideId) {
			distanceFromOutside++;
		}

		nextUsedColumnList.swap(usedColumnList);
		nextUsedColumnList.clear();

		joinFound |= (node.type_ == SQLType::EXEC_JOIN);
		nodeId = node.inputList_[inputId];
	}

	if (distanceFromOutside == 0 || !joinFound) {
		return false;
	}

	ColumnId subqueryResultColumn = UNDEF_COLUMNID;
	bool resultFiltering = false;
	{
		PlanNode &destNode = plan.append(getDisabledNodeType());
		PlanNode &srcNode = plan.nodeList_[nodePath.front()];

		const PlanNodeId destNodeId = destNode.id_;
		destNode = srcNode;
		destNode.id_ = destNodeId;
		rewroteJoinIdSet.insert(destNodeId);

		Expr filteringResultExpr = genDisabledExpr();

		nextUsedColumnList.clear();
		for (PlanExprList::iterator it = srcNode.outputList_.begin();
				it != srcNode.outputList_.end(); ++it) {
			ColumnId columnId =
					static_cast<ColumnId>(it - srcNode.outputList_.begin());

			if (it->op_ != SQLType::EXPR_COLUMN || it->inputId_ != 0) {
				if (!isDisabledExpr(*it)) {
					if (subqueryResultColumn != UNDEF_COLUMNID) {
						subqueryResultColumn = UNDEF_COLUMNID;
						break;
					}
					subqueryResultColumn = columnId;
					if (isTrueExpr(*it)) {
						filteringResultExpr = *it;
						resultFiltering = true;
					}
				}
				columnId = UNDEF_COLUMNID;
			}

			nextUsedColumnList.push_back(columnId);
		}

		if (subqueryResultColumn == UNDEF_COLUMNID) {
			assert(false);
			SQL_COMPILER_THROW_ERROR(
					GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
		}

		for (PlanNode::IdList::iterator nodeIt = nodePath.begin();
				nodeIt != nodePath.end(); ++nodeIt) {
			if (*nodeIt == outsideId) {
				break;
			}

			PlanExprList &outList = plan.nodeList_[*nodeIt].outputList_;

			for (util::Vector<ColumnId>::iterator it =
					nextUsedColumnList.begin();
					it != nextUsedColumnList.end(); ++it) {
				if (*it == UNDEF_COLUMNID) {
					continue;
				}

				Expr &expr = outList[*it];
				if (expr.op_ != SQLType::EXPR_COLUMN || expr.inputId_ != 0) {
					assert(false);
					SQL_COMPILER_THROW_ERROR(
							GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
				}
				*it = expr.columnId_;
			}
		}

		assert(srcNode.outputList_.size() == nextUsedColumnList.size());

		const PlanNodeId srcNodeId = srcNode.id_;
		srcNode = PlanNode(alloc_);
		srcNode.id_ = srcNodeId;
		srcNode.type_ = SQLType::EXEC_SELECT;
		srcNode.inputList_.push_back(outsideId);

		for (util::Vector<ColumnId>::iterator it = nextUsedColumnList.begin();
				it != nextUsedColumnList.end(); ++it) {
			if (*it == UNDEF_COLUMNID) {
				srcNode.outputList_.push_back(genDisabledExpr());
			}
			else {
				srcNode.outputList_.push_back(genColumnExprBase(
						&plan, &srcNode, 0, *it, NULL));
			}
		}

		if (resultFiltering) {
			srcNode.outputList_[subqueryResultColumn] = filteringResultExpr;
		}

		nodePath.front() = destNodeId;
	}

	PlanNodeId newOutsideId;
	{
		const PlanNodeId inNodeId = nodePath.back();

		PlanNode &node = plan.append(SQLType::EXEC_SELECT);
		node.inputList_.push_back(inNodeId);

		const PlanExprList &outsideOutList =
				plan.nodeList_[outsideId].outputList_;
		const size_t outsideColumnCount = outsideOutList.size();
		for (PlanExprList::const_iterator it = outsideOutList.begin();
				it != outsideOutList.end(); ++it) {
			if (it->op_ == SQLType::EXPR_ID) {
				node.outputList_.push_back(*it);
			}
			else {
				node.outputList_.push_back(genDisabledExpr());
			}
		}

		nextUsedColumnList.assign(outsideColumnCount, UNDEF_COLUMNID);

		for (util::Vector<ColumnId>::iterator it = usedColumnList.begin();
				it != usedColumnList.end(); ++it) {
			const ptrdiff_t index = it - usedColumnList.begin();
			const ColumnId inColumnId = *it;
			const ColumnId outColumnId = correlatedColumnList[index];

			Expr &outExpr = node.outputList_[outColumnId];
			if (!isDisabledExpr(outExpr)) {
				continue;
			}

			if (inColumnId == UNDEF_COLUMNID) {
				outExpr = genTupleIdExpr(plan, 0, MODE_NORMAL_SELECT);
			}
			else {
				outExpr = genColumnExprBase(&plan, &node, 0, inColumnId, NULL);
			}
			nextUsedColumnList[outColumnId] = inColumnId;
		}

		const ColumnId inColumnCount = static_cast<ColumnId>(
				plan.nodeList_[inNodeId].outputList_.size());
		for (ColumnId inColumnId = 0;
				inColumnId < inColumnCount; inColumnId++) {
			if (std::find(usedColumnList.begin(), usedColumnList.end(),
					inColumnId) != usedColumnList.end()) {
				continue;
			}
			if (isDisabledExpr(node.getInput(plan, 0).outputList_[inColumnId])) {
				continue;
			}
			node.outputList_.push_back(
					genColumnExprBase(&plan, &node, 0, inColumnId, NULL));
			nextUsedColumnList.push_back(inColumnId);
		}

		nextUsedColumnList.swap(usedColumnList);
		newOutsideId = node.id_;
	}

	for (PlanNode::IdList::const_iterator nodeIt = bothSideJoinList.begin();
			nodeIt != bothSideJoinList.end(); ++nodeIt) {
		PlanNode &node = plan.nodeList_[*nodeIt];
		for (PlanNode::IdList::iterator it = node.inputList_.begin();
				it != node.inputList_.end(); ++it) {
			if (*it == outsideId) {
				*it = newOutsideId;
			}
		}
	}

	{
		PlanNode::IdList::iterator nodeIt = nodePath.begin();
		for (; nodeIt != nodePath.end(); ++nodeIt) {
			if (*nodeIt == outsideId) {
				assert(nodeIt != nodePath.begin());
				--nodeIt;
				break;
			}
		}

		{
			PlanNode &node = plan.nodeList_[*nodeIt];
			assert(node.inputList_.front() == outsideId);
			node.inputList_.front() = newOutsideId;
		}

		PlanNode::IdList::iterator targetNodeIt = nodeIt;
		for (nodeIt = targetNodeIt;; --nodeIt) {
			PlanNode &node = plan.nodeList_[*nodeIt];

			if (nodeIt == nodePath.begin()) {
				break;
			}

			const PlanNode &inNode = node.getInput(plan, 0);
			for (PlanExprList::iterator it = node.outputList_.begin();
					it != node.outputList_.end(); ++it) {
				if (it->op_ != SQLType::EXPR_COLUMN || it->inputId_ != 0) {
					continue;
				}

				const ColumnId id = it->columnId_;
				if (isDisabledExpr(inNode.outputList_[id])) {
					*it = genDisabledExpr();
				}
			}
		}

		for (nodeIt = targetNodeIt;; --nodeIt) {
			PlanNode &node = plan.nodeList_[*nodeIt];

			if (nodeIt == nodePath.begin()) {
				const Expr resultExpr = node.outputList_[subqueryResultColumn];
				node.outputList_.clear();

				for (util::Vector<ColumnId>::iterator it =
						usedColumnList.begin();
						it != usedColumnList.end(); ++it) {
					const ColumnId orgColumnId = *it;
					if (*it == UNDEF_COLUMNID) {
						continue;
					}

					while (orgColumnId >= node.outputList_.size()) {
						node.outputList_.push_back(genDisabledExpr());
					}

					const ColumnId inColumnId =
							static_cast<ColumnId>(it - usedColumnList.begin());

					node.outputList_[orgColumnId] = genColumnExprBase(
							&plan, &node, 0, inColumnId, NULL);
				}

				if (!resultFiltering) {
					node.outputList_.push_back(resultExpr);
				}
				break;
			}

			nextUsedColumnList.clear();

			for (PlanExprList::iterator it = node.outputList_.begin();
					it != node.outputList_.end(); ++it) {
				if (it->op_ != SQLType::EXPR_COLUMN || it->inputId_ != 0) {
					nextUsedColumnList.push_back(UNDEF_COLUMNID);
				}
				else {
					const ColumnId id = it->columnId_;
					nextUsedColumnList.push_back(usedColumnList[id]);
					usedColumnList[id] = UNDEF_COLUMNID;
				}
			}

			PlanExprList::iterator outIt = node.outputList_.begin();
			for (util::Vector<ColumnId>::iterator it = usedColumnList.begin();
					it != usedColumnList.end(); ++it) {
				if (*it == UNDEF_COLUMNID) {
					continue;
				}

				for (;; ++outIt) {
					if (outIt == node.outputList_.end()) {
						node.outputList_.push_back(genDisabledExpr());
						nextUsedColumnList.push_back(UNDEF_COLUMNID);
						outIt = (node.outputList_.end() - 1);
						break;
					}
					else if (isDisabledExpr(*outIt)) {
						break;
					}
				}

				const ColumnId columnId =
						static_cast<ColumnId>(it - usedColumnList.begin());
				*outIt = genColumnExprBase(&plan, &node, 0, columnId, NULL);
				nextUsedColumnList[outIt - node.outputList_.begin()] = *it;
			}

			nextUsedColumnList.swap(usedColumnList);
			assert(usedColumnList.size() == node.outputList_.size());
		}
	}

	{
		PlanNode::IdList::reverse_iterator afterTarget = nodePath.rbegin() + 1;
		for (PlanNode::IdList::reverse_iterator it = afterTarget;
				it != nodePath.rend(); ++it) {
			PlanNode &node = plan.nodeList_[*it];

			PlanNodeId inNodeId = *(it - 1);
			const uint32_t inputId = getNodeInputId(node, *(it - 1));

			if (it == afterTarget) {
				inNodeId = nodePath.front();
				node.inputList_[inputId] = inNodeId;

				if (resultFiltering) {
					break;
				}
			}

			const ColumnId columnId = static_cast<ColumnId>(
					plan.nodeList_[inNodeId].outputList_.size()) - 1;
			node.outputList_.push_back(
					genColumnExprBase(&plan, &node, inputId, columnId, NULL));

			if (*it == outsideId) {
				break;
			}
		}

		if (!resultFiltering) {
			PlanNode &node = plan.nodeList_[subqueryJoinId];
			assert(node.inputList_.size() == 1);
			assert(node.inputList_.front() == outsideId);

			const ColumnId columnId = static_cast<ColumnId>(
					node.getInput(plan, 0).outputList_.size()) - 1;
			node.outputList_[subqueryResultColumn] =
					genColumnExprBase(&plan, &node, 0, columnId, NULL);
		}
	}

	return true;
}

void SQLCompiler::pushDownCorrelatedColumns(
		Plan &plan, PlanNodeId outsideId, PlanNodeId insidePredId,
		const ExprList &correlationList,
		util::Vector<ColumnId> &outsideCorrelationList,
		util::Vector<ColumnId> &insideCorrelationList) {

	util::Vector<uint32_t> nodeRefList(alloc_);

	for (ExprList::const_iterator it = correlationList.begin();
			it != correlationList.end(); ++it) {
		for (uint32_t i = 0; i < 2; i++) {
			const Expr *opExpr = (i ==0 ? (*it)->left_ : (*it)->right_);
			util::Vector<ColumnId> &columnList =
					(i == 0 ? outsideCorrelationList : insideCorrelationList);

			ColumnId columnId;
			if (opExpr->op_ == SQLType::EXPR_COLUMN) {
				columnId = opExpr->columnId_;
			}
			else {
				PlanNode &predNode = plan.nodeList_[insidePredId];
				if (predNode.type_ != SQLType::EXEC_JOIN ||
						predNode.inputList_.size() != 2 ||
						(i == 0 && predNode.inputList_[0] != outsideId) ||
						!isSingleInputExpr(*opExpr, i)) {
					assert(false);
					SQL_COMPILER_THROW_ERROR(
							GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
				}

				if (nodeRefList.empty()) {
					makeNodeRefList(plan, nodeRefList);
				}

				const PlanNodeId opNodeId = predNode.inputList_[i];
				PlanNode &opNode = plan.nodeList_[opNodeId];

				if (nodeRefList[opNodeId] > (2 - i) ||
						opNode.type_ != SQLType::EXEC_SELECT) {
					assert(false);
					SQL_COMPILER_THROW_ERROR(
							GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
				}

				Expr *outExpr;
				dereferenceExpr(plan, predNode, i, *opExpr, outExpr, NULL);

				columnId = static_cast<ColumnId>(opNode.outputList_.size());
				opNode.outputList_.push_back(*outExpr);
			}
			columnList.push_back(columnId);
		}
	}
}

void SQLCompiler::disableSubqueryOutsideColumns(
		Plan &plan, PlanNode &node, PlanNodeId outsideId,
		ColumnId insideColumnId, bool &typeRefreshRequired,
		util::Vector<bool> &fullDisabledList,
		util::Vector<PlanNodeId> &pendingLimitOffsetIdList) {
	typeRefreshRequired = false;

	if (!node.tableIdInfo_.isEmpty() || node.inputList_.empty()) {
		return;
	}

	bool inputDisabled = false;
	for (PlanNode::IdList::iterator it = node.inputList_.begin();
			it != node.inputList_.end(); ++it) {
		if (*it != outsideId) {
			continue;
		}

		if (inputDisabled && node.type_ == SQLType::EXEC_JOIN &&
				it != node.inputList_.begin()) {
			assert(false);
			SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
		}

		*it = std::numeric_limits<PlanNodeId>::max();
		inputDisabled = true;
	}

	if (node.type_ == SQLType::EXEC_GROUP ||
			node.type_ == SQLType::EXEC_SORT) {
		for (PlanExprList::iterator it = node.predList_.begin();
				it != node.predList_.end();) {
			if (findDisabledColumnRef(plan, node, *it, NULL)) {
				it = node.predList_.erase(it);
			}
			else {
				++it;
			}
		}

		if (node.predList_.empty() && !isWindowNode(node)) {
			if (node.type_ == SQLType::EXEC_GROUP &&
					getNodeAggregationLevel(node.outputList_) == 0) {
				const PlanNodeId nodeId =
						static_cast<PlanNodeId>(&node - &plan.nodeList_[0]);
				if (fullDisabledList[nodeId]) {
					assert(false);
				}
				else {
					node.outputList_.push_back(genExprWithArgs(
							plan, SQLType::AGG_COUNT_ALL, NULL,
							MODE_GROUP_SELECT, true));
				}
			}
			node.type_ = SQLType::EXEC_SELECT;
			typeRefreshRequired = true;
		}

		if (insideColumnId != UNDEF_COLUMNID &&
				(node.subLimit_ >= 0 || node.subOffset_ > 0)) {
			assert(!(node.limit_ >= 0 || node.offset_ > 0));

			if (node.subOffset_ > 0) {
				const PlanNodeId nodeId =
						static_cast<PlanNodeId>(&node - &plan.nodeList_[0]);
				pendingLimitOffsetIdList.push_back(nodeId);
			}
			else {
				node.limit_ = node.subLimit_;
				node.offset_ = node.subOffset_;

				node.subLimit_ = -1;
				node.subOffset_ = 0;
			}
		}
	}
	else {
		for (PlanExprList::iterator it = node.predList_.begin();
				it != node.predList_.end(); ++it) {
			Expr *outExpr;
			if (removeDisabledColumnRefPredicate(plan, node, *it, outExpr)) {
				if (outExpr == NULL) {
					*it = genConstExpr(
							plan, makeBoolValue(true), MODE_WHERE);
				}
				else {
					*it = *outExpr;
				}
			}
		}
	}

	bool fullDisabled = true;
	for (PlanExprList::iterator it = node.outputList_.begin();
			it != node.outputList_.end(); ++it) {
		Expr *outExpr;
		if (removeDisabledColumnRefOutput(plan, node, *it, outExpr)) {
			if (outExpr == NULL) {
				*it = genDisabledExpr();
			}
			else {
				*it = *outExpr;
			}
		}

		if (insideColumnId != UNDEF_COLUMNID &&
				insideColumnId == it - node.outputList_.begin()) {
			*it = genDisabledExpr();
		}

		if (!isDisabledExpr(*it)) {
			fullDisabled = false;
		}
	}

	if (inputDisabled) {
		for (PlanNode::IdList::iterator it = node.inputList_.begin();
				it != node.inputList_.end();) {
			if (*it == std::numeric_limits<PlanNodeId>::max()) {
				it = node.inputList_.erase(it);
			}
			else {
				++it;
			}
		}

		ExprList condList(alloc_);
		for (PlanExprList::iterator it = node.predList_.begin();
				it != node.predList_.end(); ++it) {
			if (isTrueExpr(*it)) {
				continue;
			}
			condList.push_back(ALLOC_NEW(alloc_) Expr(*it));
		}
		node.predList_.clear();

		Expr *condExpr;
		exprListToTree(
				NULL, SQLType::EXPR_AND, condList, MODE_WHERE, condExpr);
		if (condExpr != NULL) {
			node.predList_.push_back(*condExpr);
		}

		node.type_ = SQLType::EXEC_SELECT;
		node.joinType_ = SQLType::JOIN_INNER;
		typeRefreshRequired = true;
	}

	if (fullDisabled) {
		util::Vector<PlanNodeId> pendingList(alloc_);
		bool first = true;
		for (;;) {
			PlanNodeId curNodeId;
			if (first) {
				curNodeId =
						static_cast<PlanNodeId>(&node - &plan.nodeList_[0]);
				first = false;
			}
			else {
				if (pendingList.empty()) {
					break;
				}
				curNodeId = pendingList.back();
				pendingList.pop_back();
			}

			if (fullDisabledList[curNodeId]) {
				continue;
			}

			PlanNode &curNode = plan.nodeList_[curNodeId];
			const bool sameInput =
					(curNode.type_ == SQLType::EXEC_UNION ||
					curNode.type_ == SQLType::EXEC_LIMIT);
			if (!sameInput) {
				curNode.outputList_.push_back(genConstExpr(
						plan, TupleValue(static_cast<int64_t>(0)),
						MODE_NORMAL_SELECT));
				fullDisabledList[curNodeId] = true;
				continue;
			}

			assert(curNodeId != outsideId);

			bool pending = false;
			for (PlanNode::IdList::iterator it = curNode.inputList_.begin();
					it != curNode.inputList_.end(); ++it) {
				if (!fullDisabledList[*it]) {
					pending = true;

					if (!pending) {
						pendingList.push_back(curNodeId);
					}
					pendingList.push_back(*it);
				}
			}

			if (!fullDisabled) {
				if (!pending) {
					assert(false);
					GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
				}
				break;
			}

			if (sameInput && !pending) {
				const PlanNode &inNode = curNode.getInput(plan, 0);

				assert(!inNode.outputList_.empty());
				const ColumnId inColumnId = static_cast<ColumnId>(
						inNode.outputList_.size() - 1);

				curNode.outputList_.push_back(genColumnExprBase(
						&plan, &curNode, 0, inColumnId, NULL));
				fullDisabledList[curNodeId] = true;
			}
		}
	}
}

bool SQLCompiler::removeDisabledColumnRefPredicate(
		Plan &plan, PlanNode &node, const Expr &inExpr, Expr *&outExpr) {
	assert(node.tableIdInfo_.isEmpty());

	if (inExpr.op_ == SQLType::EXPR_AND) {
		Expr *leftExpr;
		Expr *rightExpr;

		bool disabled = false;
		disabled |= removeDisabledColumnRefPredicate(
				plan, node, *inExpr.left_, leftExpr);
		disabled |= removeDisabledColumnRefPredicate(
				plan, node, *inExpr.right_, rightExpr);

		if (disabled) {
			mergeAndOpBase(leftExpr, rightExpr, NULL, MODE_WHERE, outExpr);
			return true;
		}
		else {
			return false;
		}
	}

	return removeDisabledColumnRefOutput(plan, node, inExpr, outExpr);
}

bool SQLCompiler::removeDisabledColumnRefOutput(
		Plan &plan, PlanNode &node, const Expr &inExpr, Expr *&outExpr) {
	outExpr = NULL;
	bool columnFound;
	if (findDisabledColumnRef(plan, node, inExpr, &columnFound)) {
		return true;
	}

	if (std::find(node.inputList_.begin(), node.inputList_.end(),
			std::numeric_limits<PlanNodeId>::max()) != node.inputList_.end() &&
			columnFound) {
		outExpr = ALLOC_NEW(alloc_) Expr(inExpr);

		if (inExpr.op_ == SQLType::EXPR_COLUMN) {
			uint32_t disabledCount = 0;
			for (PlanNode::IdList::iterator it = node.inputList_.begin();
					it != node.inputList_.end(); ++it) {
				if (inExpr.inputId_ <
						static_cast<uint32_t>(it - node.inputList_.begin())) {
					break;
				}
				if (*it == std::numeric_limits<PlanNodeId>::max()) {
					disabledCount++;
				}
			}
			if (disabledCount > 0) {
				outExpr->inputId_ -= disabledCount;
			}
		}

		ExprList *next = NULL;
		for (ExprArgsIterator<false> it(*outExpr); it.exists(); it.next()) {
			Expr &inSubExpr = it.get();
			Expr *outSubExpr;

			removeDisabledColumnRefPredicate(
					plan, node, inSubExpr, outSubExpr);

			if (outSubExpr == NULL) {
				outSubExpr = &inSubExpr;
			}

			if (&inSubExpr == outExpr->left_) {
				outExpr->left_ = outSubExpr;
			}
			else if (&inSubExpr == outExpr->right_) {
				outExpr->right_ = outSubExpr;
			}
			else {
				if (next == NULL) {
					next = ALLOC_NEW(alloc_) ExprList(alloc_);
				}
				next->push_back(outSubExpr);
			}
		}
		outExpr->next_ = next;
		return true;
	}

	return false;
}

bool SQLCompiler::findDisabledColumnRef(
		const Plan &plan, const PlanNode &node, const Expr &expr,
		bool *columnFound) {
	assert(node.tableIdInfo_.isEmpty());

	if (columnFound != NULL) {
		*columnFound = false;
	}

	if (expr.op_ == SQLType::EXPR_COLUMN) {
		if (columnFound != NULL) {
			*columnFound = true;
		}

		const PlanNodeId inNodeId = node.inputList_[expr.inputId_];
		if (inNodeId == std::numeric_limits<PlanNodeId>::max()) {
			return true;
		}

		const PlanNode &inNode = plan.nodeList_[inNodeId];
		if (isDisabledExpr(inNode.outputList_[expr.columnId_])) {
			return true;
		}

		return false;
	}

	bool subFound = false;
	for (ExprArgsIterator<> it(expr); it.exists(); it.next()) {
		bool columnFoundSub;
		subFound |= findDisabledColumnRef(
				plan, node, it.get(), &columnFoundSub);

		if (columnFoundSub && columnFound != NULL) {
			*columnFound |= columnFoundSub;
		}

		if (subFound && columnFound == NULL) {
			break;
		}
	}

	return subFound;
}

void SQLCompiler::disableSubqueryLimitOffset(Plan &plan, PlanNodeId nodeId) {
	PlanNode &limitNode = plan.append(SQLType::EXEC_LIMIT);
	PlanNode &orgNode = plan.nodeList_[nodeId];

	if (orgNode.inputList_.size() != 1 || !orgNode.tableIdInfo_.isEmpty()) {
		assert(false);
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	assert(orgNode.subLimit_ >= 0 || orgNode.subOffset_ > 0);

	limitNode.inputList_ = orgNode.inputList_;
	limitNode.outputList_ = inputListToOutput(plan, limitNode, NULL);
	limitNode.limit_ = orgNode.subLimit_;
	limitNode.offset_ = orgNode.subOffset_;

	orgNode.inputList_.clear();
	orgNode.inputList_.push_back(
			static_cast<PlanNodeId>(&limitNode - &plan.nodeList_[0]));
	orgNode.subLimit_ = -1;
	orgNode.subOffset_ = 0;
}

void SQLCompiler::removeEmptyNodes(OptimizationContext &cxt, Plan &plan) {
	static_cast<void>(cxt);

	if (optimizationOption_->isSomeCheckOnly()) {
		return;
	}

	plan.removeEmptyNodes();
}

void SQLCompiler::removeEmptyColumns(OptimizationContext &cxt, Plan &plan) {
	static_cast<void>(cxt);

	plan.removeEmptyColumns();
}

SQLCompiler::Expr SQLCompiler::genDisabledExpr() {
	Expr expr(alloc_);
	expr.op_ = getDisabledExprType();
	return expr;
}

bool SQLCompiler::isDisabledExpr(const Expr &expr) {
	return Plan::isDisabledExpr(expr);
}

SQLCompiler::Type SQLCompiler::getDisabledExprType() {
	return Plan::getDisabledExprType();
}

uint64_t SQLCompiler::applyRatio(uint64_t denom, uint64_t ratio) {

	if (ratio == 0) {
		return 0;
	}

	if (denom >= std::numeric_limits<size_t>::max() / ratio) {
		if (denom / 100 >= std::numeric_limits<size_t>::max() / ratio) {
			return std::numeric_limits<size_t>::max();
		}
		else {
			return denom / 100 * ratio;
		}
	}
	else {
		return denom * ratio / 100;
	}
}

void SQLCompiler::makeNodeRefList(
		const Plan &plan, util::Vector<uint32_t> &nodeRefList) {
	nodeRefList.assign(plan.nodeList_.size(), 0);

	for (PlanNodeList::const_iterator nodeIt = plan.nodeList_.begin();
			nodeIt != plan.nodeList_.end(); ++nodeIt) {
		const PlanNode &node = *nodeIt;
		if (isDisabledNode(node)) {
			continue;
		}

		for (PlanNode::IdList::const_iterator it = node.inputList_.begin();
				it != node.inputList_.end(); ++it) {
			nodeRefList[*it]++;
		}
	}
}

void SQLCompiler::findAllInput(
		const Plan &plan, const PlanNode &node,
		util::Set<PlanNodeId> &allInput) {

	for (PlanNode::IdList::const_iterator it = node.inputList_.begin();
			it != node.inputList_.end(); ++it) {
		allInput.insert(*it);
	}

	bool found;
	do {
		found = false;
		for (util::Set<PlanNodeId>::iterator inIt = allInput.begin();
				inIt != allInput.end();) {
			const PlanNode &inNode = plan.nodeList_[*inIt];
			util::Set<PlanNodeId>::iterator nextInIt = inIt;
			++nextInIt;

			for (PlanNode::IdList::const_iterator it =
					inNode.inputList_.begin();
					it != inNode.inputList_.end(); ++it) {
				found |= allInput.insert(*it).second;
			}
			inIt = nextInIt;
		}
	}
	while (found);
}

void SQLCompiler::checkReference(
		const Plan &plan, const PlanNode &node,
		const PlanExprList &exprList,
		util::Vector<util::Vector<bool>*> &checkList, bool forOutput) {

	const PlanNodeId nodeId =
			static_cast<PlanNodeId>(&node - &plan.nodeList_[0]);

	if (forOutput && isSameOutputRequired(plan, node)) {
		(*checkList[nodeId]).assign(checkList[nodeId]->size(), true);
	}

	for (PlanExprList::const_iterator it = exprList.begin();
			it != exprList.end(); ++it) {
		assert(!forOutput ||
				static_cast<size_t>(it - exprList.begin()) <
				checkList[nodeId]->size());
		if (forOutput && !(*checkList[nodeId])[it - exprList.begin()]) {
			continue;
		}

		checkReference(node, *it, checkList);
	}
}

void SQLCompiler::checkReference(
		const PlanNode &node, const Expr &expr,
		util::Vector<util::Vector<bool>*> &checkList) {

	if (node.inputList_.empty()) {
		return;
	}

	if (expr.op_ == SQLType::EXPR_COLUMN) {
		assert(!isDisabledNode(node));

		if (node.type_ == SQLType::EXEC_JOIN ||
				node.type_ == SQLType::EXEC_SELECT ||
				node.type_ == SQLType::EXEC_SCAN) {
			const uint32_t tableCount = node.getInputTableCount();
			if (expr.inputId_ >= tableCount) {
				const uint32_t pos = expr.inputId_ - tableCount;
				assert(pos < node.inputList_.size());

				const PlanNodeId nodeId = node.inputList_[pos];
				assert(expr.columnId_ < checkList[nodeId]->size());
				(*checkList[nodeId])[expr.columnId_] = true;
			}
		}
		else {
			for (PlanNode::IdList::const_iterator it = node.inputList_.begin();
					it != node.inputList_.end(); ++it) {
				assert(expr.columnId_ < checkList[*it]->size());
				(*checkList[*it])[expr.columnId_] = true;
			}
		}
		return;
	}

	for (ExprArgsIterator<> it(expr); it.exists(); it.next()) {
		checkReference(node, it.get(), checkList);
	}
}

bool SQLCompiler::isSameOutputRequired(
		const Plan &plan, const PlanNode &node) {
	if (node.type_ == SQLType::EXEC_LIMIT ||
			(node.type_ == SQLType::EXEC_UNION &&
			node.unionType_ != SQLType::UNION_ALL)) {
		return true;
	}

	if (node.type_ != SQLType::EXEC_UNION) {
		return false;
	}

	util::StackAllocator::Scope scope(alloc_);

	typedef util::Set<PlanNodeId> IdSet;
	IdSet idSet(alloc_);
	IdSet nextIdSet(alloc_);
	idSet.insert(node.inputList_.begin(), node.inputList_.end());
	do {
		for (IdSet::iterator it = idSet.begin(); it != idSet.end(); ++it) {
			const PlanNode &inNode = plan.nodeList_[*it];
			if (inNode.type_ == SQLType::EXEC_LIMIT ||
					(inNode.type_ == SQLType::EXEC_UNION &&
					inNode.unionType_ != SQLType::UNION_ALL)) {
				return true;
			}

			if (node.type_ != SQLType::EXEC_UNION) {
				continue;
			}

			idSet.insert(inNode.inputList_.begin(), inNode.inputList_.end());
		}

		idSet.swap(nextIdSet);
		nextIdSet.clear();
	}
	while (!idSet.empty());

	return false;
}

void SQLCompiler::disableNode(Plan &plan, PlanNodeId nodeId) {
	setDisabledNodeType(plan.nodeList_[nodeId]);

	for (PlanNodeList::iterator nodeIt = plan.nodeList_.begin();
			nodeIt != plan.nodeList_.end(); ++nodeIt) {
		PlanNode &node = *nodeIt;

		if (isDisabledNode(node)) {
			continue;
		}

		for (PlanNode::IdList::iterator it = node.inputList_.begin();
				it != node.inputList_.end(); ++it) {
			PlanNodeId inNodeId = *it;
			if (inNodeId == nodeId) {
				*it = plan.nodeList_[nodeId].inputList_.front();
			}
		}
	}
}

void SQLCompiler::setNodeAndRefDisabled(
		Plan &plan, PlanNodeId nodeId,
		util::Vector<uint32_t> &nodeRefList) {
	bool forInputRef = false;
	PlanNode::IdList::iterator inIt;
	PlanNode::IdList::iterator inEnd;
	while (!forInputRef || inIt != inEnd) {
		const PlanNodeId curNodeId = (forInputRef ? *inIt : nodeId);

		uint32_t &nodeRef = nodeRefList[curNodeId];
		PlanNode &node = plan.nodeList_[curNodeId];

		if (isDisabledNode(node)) {
			assert(false);
			SQL_COMPILER_THROW_ERROR(
					GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
		}

		if (forInputRef) {
			if (nodeRef <= 0) {
				assert(false);
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
			}
			nodeRef--;
			++inIt;
		}
		else {
			if (nodeRef == 0) {
				setDisabledNodeType(node);
			}
			inIt = node.inputList_.begin();
			inEnd = node.inputList_.end();
			forInputRef = true;
		}
	}
}

void SQLCompiler::setDisabledNodeType(PlanNode &node) {
	node.type_ = getDisabledNodeType();
}

bool SQLCompiler::isDisabledNode(const PlanNode &node) {
	return Plan::isDisabledNode(node);
}

SQLCompiler::Type SQLCompiler::getDisabledNodeType() {
	return Plan::getDisabledNodeType();
}

bool SQLCompiler::factorizeExpr(
		Expr &inExpr, Expr *&outExpr, ExprList *andList) {
	outExpr = NULL;

	if (inExpr.op_ == SQLType::EXPR_AND) {
		Expr *leftExpr;
		Expr *rightExpr;

		bool found = false;
		found |= factorizeExpr(*inExpr.left_, leftExpr, andList);
		found |= factorizeExpr(*inExpr.right_, rightExpr, andList);

		if (leftExpr != NULL || rightExpr != NULL) {
			if (leftExpr == NULL) {
				leftExpr = inExpr.left_;
			}
			if (rightExpr == NULL) {
				rightExpr = inExpr.right_;
			}

			mergeAndOpBase(leftExpr, rightExpr, NULL, MODE_WHERE, outExpr);
		}
		return found;
	}
	else if (inExpr.op_ == SQLType::EXPR_OR) {
		return factorizeOrExpr(inExpr, outExpr, andList);
	}
	else {
		if (andList != NULL) {
			andList->push_back(&inExpr);
		}
		return false;
	}
}

bool SQLCompiler::factorizeOrExpr(
		Expr &inExpr, Expr *&outExpr, ExprList *andList) {
	assert(inExpr.op_ == SQLType::EXPR_OR);
	const Mode mode = MODE_WHERE;
	bool found;

	if (andList == NULL) {
		ExprList localAndList(alloc_);
		found = factorizeOrExpr(inExpr, outExpr, &localAndList);

		if (localAndList.empty()) {
			outExpr = NULL;
			return found;
		}

		if (outExpr != NULL) {
			localAndList.push_back(outExpr);
		}

		exprListToTree(
				NULL, SQLType::EXPR_AND, localAndList, mode, outExpr);
		return found;
	}

	outExpr = NULL;

	Expr *leftExpr;
	Expr *rightExpr;

	found = false;
	const size_t startPos = andList->size();
	found |= factorizeExpr(*inExpr.left_, leftExpr, andList);
	const size_t endPos = andList->size();
	found |= factorizeExpr(*inExpr.right_, rightExpr, andList);

	{
		const ExprList::iterator totalBegin = andList->begin();
		const ExprList::iterator totalEnd = andList->end();

		const ExprList::iterator destBegin = totalBegin + startPos;
		const ExprList::iterator destEnd = totalBegin + endPos;

		ExprList::iterator destIt = destBegin;
		ExprList::iterator middleIt = destEnd;

		for (ExprList::iterator srcIt = destEnd; srcIt != totalEnd; ++srcIt) {
			for (ExprList::iterator it = destIt; it != destEnd; ++it) {
				if (SubPlanEq()(**it, **srcIt)) {
					if (it != destIt) {
						std::swap(*it, *destIt);
					}
					++destIt;
					if (middleIt != srcIt) {
						std::swap(*middleIt, *srcIt);
					}
					++middleIt;
					break;
				}
			}
		}

		if (destIt == destBegin) {
			andList->erase(destIt, totalEnd);
			andList->push_back(&inExpr);
			return found;
		}

		found = true;

		ExprList subExprList(alloc_);

		if (destIt != destEnd) {
			subExprList.assign(destIt, destEnd);

			if (leftExpr != NULL) {
				subExprList.push_back(leftExpr);
			}

			Expr *subExpr;
			exprListToTree(
					NULL, SQLType::EXPR_AND, subExprList, mode, subExpr);

			leftExpr = subExpr;
		}

		if (middleIt != totalEnd) {
			subExprList.assign(middleIt, totalEnd);

			if (rightExpr != NULL) {
				subExprList.push_back(rightExpr);
			}

			Expr *subExpr;
			exprListToTree(
					NULL, SQLType::EXPR_AND, subExprList, mode, subExpr);

			rightExpr = subExpr;
		}

		andList->erase(destIt, totalEnd);
	}

	if (leftExpr != NULL && rightExpr != NULL) {
		mergeOrOp(leftExpr, rightExpr, mode, outExpr);
	}

	return found;
}

bool SQLCompiler::splitJoinPushDownCond(
		const Plan &plan, Mode mode, const Expr &inExpr, Expr *&outExpr,
		Expr *&condExpr, uint32_t inputId, SQLType::JoinType joinType,
		bool complex) {
	outExpr = NULL;
	condExpr = NULL;

	assert(joinType != SQLType::JOIN_FULL_OUTER);

	if (joinType == SQLType::JOIN_LEFT_OUTER) {
		if ((inputId == 0 && mode == MODE_ON) ||
				(inputId == 1 && mode == MODE_WHERE)) {
			return false;
		}
	}
	else if (joinType == SQLType::JOIN_RIGHT_OUTER) {
		if ((inputId == 0 && mode == MODE_WHERE) ||
				(inputId == 1 && mode == MODE_ON)) {
			return false;
		}
	}

	return splitJoinPushDownCondSub(
				plan, mode, inExpr, outExpr, condExpr, inputId, complex);
}

bool SQLCompiler::splitJoinPushDownCondSub(
		const Plan &plan, Mode mode, const Expr &inExpr, Expr *&outExpr,
		Expr *&condExpr, uint32_t inputId, bool complex) {
	if (inExpr.op_ == SQLType::EXPR_AND) {

		Expr *outExprL;
		Expr *condExprL;
		if (!splitJoinPushDownCondSub(
				plan, mode, *inExpr.left_, outExprL, condExprL, inputId,
				complex)) {
			outExprL = inExpr.left_;
		}

		Expr *outExprR;
		Expr *condExprR;
		if (!splitJoinPushDownCondSub(
				plan, mode, *inExpr.right_, outExprR, condExprR, inputId,
				complex)) {
			outExprR = inExpr.right_;
		}

		mergeAndOp(outExprL, outExprR, plan, mode, outExpr);
		mergeAndOp(condExprL, condExprR, plan, mode, condExpr);

		if (condExpr != NULL && outExpr == NULL) {
			outExpr = ALLOC_NEW(alloc_) Expr(
					genConstExpr(plan, makeBoolValue(true), mode));
		}

		return (condExpr != NULL);
	}

	if (!isSingleInputExpr(inExpr, inputId)) {
		outExpr = NULL;
		condExpr = NULL;
		return false;
	}

	outExpr = ALLOC_NEW(alloc_) Expr(
			genConstExpr(plan, makeBoolValue(true), mode));
	condExpr = ALLOC_NEW(alloc_) Expr(inExpr);
	return true;
}

bool SQLCompiler::isSubqueryResultPredicateNode(
		const Plan &plan, const PlanNode &node) {
	if (node.type_ != SQLType::EXEC_SELECT || node.inputList_.size() != 1) {
		return false;
	}

	const uint32_t joinSubqueryInputId = 1;

	const PlanNode &inNode = node.getInput(plan, 0);
	if (inNode.type_ != SQLType::EXEC_JOIN ||
			inNode.inputList_.size() != 2 ||
			!isFoldAggregationNode(
					inNode.getInput(plan, joinSubqueryInputId))) {
		return false;
	}

	return true;
}

bool SQLCompiler::findSingleInputExpr(
		const Expr &expr, uint32_t &inputId, bool *multiFound) {
	if (multiFound == NULL) {
		bool multiFoundLocal = false;
		return findSingleInputExpr(expr, inputId, &multiFoundLocal);
	}

	if (expr.op_ == SQLType::EXPR_COLUMN) {
		inputId = expr.inputId_;
		return true;
	}

	inputId = std::numeric_limits<uint32_t>::max();
	bool found = false;

	for (ExprArgsIterator<> it(expr); it.exists(); it.next()) {
		if (found) {
			if (!isSingleInputExpr(it.get(), inputId)) {
				found = false;
				*multiFound = true;
				break;
			}
		}
		else if (*multiFound) {
			break;
		}
		else {
			if (findSingleInputExpr(it.get(), inputId, multiFound)) {
				found = true;
			}
		}
	}

	if (!found) {
		inputId = std::numeric_limits<uint32_t>::max();
	}

	return found;
}

bool SQLCompiler::isSingleInputExpr(const Expr &expr, uint32_t inputId) {
	if (expr.op_ == SQLType::EXPR_COLUMN) {
		return (expr.inputId_ == inputId);
	}

	for (ExprArgsIterator<> it(expr); it.exists(); it.next()) {
		if (!isSingleInputExpr(it.get(), inputId)) {
			return false;
		}
	}

	return true;
}

bool SQLCompiler::isNodeAggregated(const PlanNode &node) {
	return isNodeAggregated(
			node.outputList_, node.type_, isWindowNode(node));
}

bool SQLCompiler::isNodeAggregated(
		const PlanExprList &exprList, SQLType::Id type, bool forWindow) {
	return (getNodeAggregationLevel(exprList) > 0 &&
			type != SQLType::EXEC_GROUP && !forWindow);
}

uint32_t SQLCompiler::getNodeAggregationLevel(const PlanExprList &exprList) {
	uint32_t level = 0;

	for (PlanExprList::const_iterator it = exprList.begin();
			it != exprList.end(); ++it) {
		if (it->op_ == SQLType::START_EXPR) {
			continue;
		}
		level = static_cast<uint32_t>(std::max(level, it->aggrNestLevel_));
	}

	return level;
}

void SQLCompiler::assignReducedScanCostHints(Plan &plan) {
	if (optimizationOption_->isSomeEnabled()) {
		OptimizationContext cxt(*this, plan);

		{
			OptimizationUnit unit(cxt, OPT_PUSH_DOWN_PREDICATE);
			unit(unit() && pushDownPredicate(plan, true));
		}
	}

	const bool costCheckOnly = true;
	genDistributedPlan(plan, costCheckOnly);
}

bool SQLCompiler::applyScanCostHints(const Plan &srcPlan, Plan &destPlan) {
	if (srcPlan.nodeList_.size() != destPlan.nodeList_.size()) {
		assert(false);
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	bool found = false;
	for (PlanNodeList::const_iterator nodeIt = srcPlan.nodeList_.begin();
			nodeIt != srcPlan.nodeList_.end(); ++nodeIt) {
		const PlanNode &srcNode = *nodeIt;
		PlanNode &destNode =
				destPlan.nodeList_[nodeIt - srcPlan.nodeList_.begin()];

		const int64_t srcSize = srcNode.tableIdInfo_.approxSize_;
		if (!(srcNode.type_ == SQLType::EXEC_SCAN &&
				destNode.type_ == SQLType::EXEC_SCAN &&
				srcSize >= 0)) {
			continue;
		}

		int64_t &destSize = destNode.tableIdInfo_.approxSize_;
		const int64_t orgSize = destSize;

		destSize = srcSize;
		found |= (orgSize != destSize);
	}

	return found;
}

bool SQLCompiler::findJoinPushDownHintingTarget(const Plan &plan) {
	const PlanNode::CommandOptionFlag mask = getJoinDrivingOptionMask();

	for (PlanNodeList::const_iterator nodeIt = plan.nodeList_.begin();
			nodeIt != plan.nodeList_.end(); ++nodeIt) {
		if (nodeIt->type_ == SQLType::EXEC_JOIN &&
				(nodeIt->cmdOptionFlag_ & mask) == 0) {
			return true;
		}
	}

	return false;
}

bool SQLCompiler::applyJoinPushDownHints(const Plan &srcPlan, Plan &destPlan) {
	if (srcPlan.nodeList_.size() != destPlan.nodeList_.size()) {
		assert(false);
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	const PlanNode::CommandOptionFlag mask = getJoinDrivingOptionMask();

	bool found = false;
	for (PlanNodeList::const_iterator nodeIt = srcPlan.nodeList_.begin();
			nodeIt != srcPlan.nodeList_.end(); ++nodeIt) {
		const PlanNode &srcNode = *nodeIt;
		PlanNode &destNode =
				destPlan.nodeList_[nodeIt - srcPlan.nodeList_.begin()];

		if (!(srcNode.type_ == SQLType::EXEC_JOIN &&
				destNode.type_ == SQLType::EXEC_JOIN &&
				(srcNode.cmdOptionFlag_ & mask) != 0 &&
				(destNode.cmdOptionFlag_ & mask) == 0)) {
			continue;
		}

		PlanNode::CommandOptionFlag &destFlags = destNode.cmdOptionFlag_;
		const PlanNode::CommandOptionFlag orgFlags = destFlags;

		destFlags = (
				(orgFlags & (~mask)) | (srcNode.cmdOptionFlag_ & mask));
		found |= (orgFlags != destFlags);
	}

	return found;
}

SQLCompiler::PlanNode::CommandOptionFlag
SQLCompiler::getJoinDrivingOptionMask() {
	return (
			PlanNode::Config::CMD_OPT_JOIN_DRIVING_NONE_LEFT |
			PlanNode::Config::CMD_OPT_JOIN_DRIVING_SOME);
}

bool SQLCompiler::isNarrowingJoinInput(
		const Plan &plan, const PlanNode &node, uint32_t inputId) {
	assert(node.type_ == SQLType::EXEC_JOIN);
	assert(node.joinType_ == SQLType::JOIN_INNER);

	if (isNarrowingNode(node.getInput(plan, inputId))) {
		return true;
	}

	if (node.predList_.size() > 1 &&
			isNarrowingPredicate(node.predList_[1], &inputId)) {
		return true;
	}

	util::StackAllocator::Scope scope(alloc_);
	util::Vector<const PlanNode*> pendingList(alloc_);
	util::Set<const PlanNode*> visitedSet(alloc_);

	pendingList.push_back(&node.getInput(plan, inputId));
	while (!pendingList.empty()) {
		const PlanNode &inNode = *pendingList.back();
		pendingList.pop_back();

		if (!visitedSet.insert(&inNode).second) {
			continue;
		}

		if (isNarrowingNode(inNode)) {
			continue;
		}
		else if (inNode.inputList_.empty()) {
			return false;
		}
		else if (inNode.type_ == SQLType::EXEC_SCAN) {
			continue;
		}
		else if (!(inNode.type_ == SQLType::EXEC_GROUP ||
				inNode.type_ == SQLType::EXEC_LIMIT ||
				inNode.type_ == SQLType::EXEC_SELECT ||
				inNode.type_ == SQLType::EXEC_SORT ||
				inNode.type_ == SQLType::EXEC_UNION)) {
			return false;
		}

		for (PlanNode::IdList::const_iterator it = inNode.inputList_.begin();
				it != inNode.inputList_.end(); ++it) {
			pendingList.push_back(&plan.nodeList_[*it]);
		}
	}

	return true;
}

bool SQLCompiler::isNarrowingNode(const PlanNode &node) {
	if (isNodeAggregated(node) || (node.limit_ >= 0 && node.limit_ <= 1)) {
		return true;
	}

	if (!(node.type_ == SQLType::EXEC_JOIN ||
			node.type_ == SQLType::EXEC_SCAN ||
			node.type_ == SQLType::EXEC_SELECT)) {
		return false;
	}

	if (node.type_ == SQLType::EXEC_SELECT &&
			node.inputList_.empty()) {
		return true;
	}

	if (!node.predList_.empty() &&
			isNarrowingPredicate(node.predList_.front(), NULL)) {
		return true;
	}

	return false;
}

bool SQLCompiler::isNarrowingPredicate(
		const Expr &expr, const uint32_t *inputId) {
	if (expr.op_ == SQLType::EXPR_AND) {
		return isNarrowingPredicate(*expr.left_, inputId) ||
				isNarrowingPredicate(*expr.right_, inputId);
	}
	else if (expr.op_ == SQLType::EXPR_OR) {
		return isNarrowingPredicate(*expr.left_, inputId) &&
				isNarrowingPredicate(*expr.right_, inputId);
	}
	else if (expr.op_ == SQLType::OP_EQ) {
		const Expr *columnExpr = expr.left_;
		const Expr *constExpr = expr.right_;

		if (columnExpr->op_ != SQLType::EXPR_COLUMN) {
			std::swap(columnExpr, constExpr);
			if (columnExpr->op_ != SQLType::EXPR_COLUMN) {
				return false;
			}
		}

		if (inputId != NULL && columnExpr->inputId_ != *inputId) {
			return false;
		}

		return isConstantOnExecution(*constExpr);
	}
	else {
		return false;
	}
}

void SQLCompiler::reduceTablePartitionCond(
		const Plan &plan, Expr &expr, const TableInfo &tableInfo,
		const util::Vector<uint32_t> &affinityRevList,
		int32_t subContainerId) {
	const Expr *dest = reduceTablePartitionCondSub(
			expr, tableInfo, affinityRevList, subContainerId);
	if (dest != &expr) {
		if (dest == NULL) {
			expr = genConstExpr(plan, makeBoolValue(true), MODE_WHERE);
		}
		else {
			expr = *dest;
		}
	}
}

const SQLCompiler::Expr* SQLCompiler::reduceTablePartitionCondSub(
		const Expr &expr, const TableInfo &tableInfo,
		const util::Vector<uint32_t> &affinityRevList,
		int32_t subContainerId) {
	if (expr.op_ == SQLType::EXPR_AND) {
		const Expr *leftExpr = reduceTablePartitionCondSub(
				*expr.left_, tableInfo, affinityRevList, subContainerId);
		const Expr *rightExpr = reduceTablePartitionCondSub(
				*expr.right_, tableInfo, affinityRevList, subContainerId);

		if (leftExpr != expr.left_ || rightExpr != expr.right_) {
			Expr *outExpr;
			mergeAndOpBase(
					(leftExpr == NULL ?
							NULL : ALLOC_NEW(alloc_) Expr(*leftExpr)),
					(rightExpr == NULL ?
							NULL : ALLOC_NEW(alloc_) Expr(*rightExpr)),
					NULL, MODE_WHERE, outExpr);
			return outExpr;
		}
	}
	else if (ProcessorUtils::isReducibleTablePartitionCondition(
			expr, tableInfo, affinityRevList, subContainerId)) {
		return NULL;
	}

	return &expr;
}

bool SQLCompiler::findIndexedPredicate(
		const Plan &plan, const PlanNode &node, uint32_t inputId,
		bool withUnionScan, bool *unique) {
	if (unique != NULL) {
		*unique = false;
	}

	const PlanNode &baseInNode = node.getInput(plan, inputId);

	if (!withUnionScan || !(
			baseInNode.type_ == SQLType::EXEC_UNION &&
			baseInNode.unionType_ == SQLType::UNION_ALL)) {
		return findIndexedPredicateSub(plan, node, inputId, NULL, unique);
	}

	bool retUnique = true;
	bool found = true;
	for (PlanNode::IdList::const_iterator it = baseInNode.inputList_.begin();
			it != baseInNode.inputList_.end(); ++it) {
		bool uniqueLocal;
		bool *uniqueSub = (unique == NULL ? NULL : &uniqueLocal);

		const uint32_t unionInputId =
				static_cast<uint32_t>(it - baseInNode.inputList_.begin());
		if (!findIndexedPredicateSub(
				plan, node, inputId, &unionInputId, uniqueSub)) {
			return false;
		}

		if (uniqueSub != NULL) {
			retUnique &= *uniqueSub;
		}
	}

	if (unique != NULL) {
		*unique = retUnique;
	}
	return found;
}

bool SQLCompiler::findIndexedPredicateSub(
		const Plan &plan, const PlanNode &node, uint32_t inputId,
		const uint32_t *unionInputId, bool *unique) {
	if (unique != NULL) {
		*unique = false;
	}

	const PlanNode &baseInNode = node.getInput(plan, inputId);
	const PlanNode &inNode = (unionInputId == NULL ?
			baseInNode : baseInNode.getInput(plan, *unionInputId));
	if (inNode.type_ != SQLType::EXEC_SCAN ||
			!inNode.inputList_.empty() || inNode.limit_ >= 0 ||
			inNode.aggPhase_ != SQLType::AGG_PHASE_ALL_PIPE ||
			(inNode.cmdOptionFlag_ &
					PlanNode::Config::CMD_OPT_SCAN_INDEX) == 0) {
		return false;
	}

	const SQLTableInfo::BasicIndexInfoList *indexInfoList =
			getTableInfoList().getIndexInfoList(inNode.tableIdInfo_);

	if (indexInfoList == NULL) {
		return false;
	}

	const bool uniquePossible = (unique != NULL &&
			getTableInfoList().resolve(inNode.tableIdInfo_.id_).hasRowKey_);

	bool firstUnmatched = false;
	bool found = false;
	for (PlanExprList::const_iterator it = node.predList_.begin();
			it != node.predList_.end(); ++it) {
		if (isTrueExpr(*it)) {
			continue;
		}

		if (it - node.predList_.begin() >= 2) {
			return false;
		}

		if (findIndexedPredicateSub(
				plan, node, *indexInfoList, inputId, unionInputId, *it,
				firstUnmatched, NULL)) {
			found = true;

			if (uniquePossible) {
				findIndexedPredicateSub(
						plan, node, *indexInfoList, inputId, unionInputId, *it,
						firstUnmatched, unique);
			}
		}
	}

	return found;
}

bool SQLCompiler::findIndexedPredicateSub(
		const Plan &plan, const PlanNode &node,
		const SQLTableInfo::BasicIndexInfoList &indexInfoList,
		uint32_t inputId, const uint32_t *unionInputId, const Expr &expr,
		bool &firstUnmatched, bool *unique) {
	if (unique != NULL) {
		*unique = false;
	}

	if (unique == NULL && firstUnmatched) {
		return false;
	}

	if (expr.op_ == SQLType::EXPR_AND) {
		bool uniqueLocal;
		bool *uniqueSub = (unique == NULL ? NULL : &uniqueLocal);
		bool retUnique = false;

		bool continuable = true;
		bool found = false;
		if (continuable && findIndexedPredicateSub(
				plan, node, indexInfoList, inputId, unionInputId, *expr.left_,
				firstUnmatched, uniqueSub)) {
			found = true;
			retUnique |= uniqueLocal;
			continuable = (unique != NULL);
		}

		if (continuable && findIndexedPredicateSub(
				plan, node, indexInfoList, inputId, unionInputId, *expr.right_,
				firstUnmatched, uniqueSub)) {
			found = true;
			retUnique |= uniqueLocal;
		}

		if (unique != NULL) {
			*unique = retUnique;
		}
		return found;
	}
	else if (expr.op_ == SQLType::OP_EQ) {
		if (expr.left_->op_ != SQLType::EXPR_COLUMN ||
				expr.right_->op_ != SQLType::EXPR_COLUMN) {
			return false;
		}

		const Expr *tableExpr = expr.left_;
		const Expr *anotherExpr = expr.right_;
		if (tableExpr->inputId_ != inputId) {
			std::swap(tableExpr, anotherExpr);
			if (tableExpr->inputId_ != inputId) {
				return false;
			}
		}

		if (anotherExpr->inputId_ == inputId) {
			return false;
		}

		const PlanNode &baseInNode = node.getInput(plan, inputId);
		const Expr &baseTableExpr =
				baseInNode.outputList_[tableExpr->columnId_];
		if (baseTableExpr.op_ != SQLType::EXPR_COLUMN) {
			return false;
		}

		ColumnId orgColumnId = baseTableExpr.columnId_;
		if (unionInputId != NULL) {
			const Expr &orgTableExpr = baseInNode.getInput(
					plan, *unionInputId).outputList_[orgColumnId];
			if (orgTableExpr.op_ != SQLType::EXPR_COLUMN) {
				return false;
			}
			orgColumnId = orgTableExpr.columnId_;
		}

		if (unique != NULL) {
			const bool byFrontColumn = (orgColumnId == 0);
			*unique = byFrontColumn;
		}
		return indexInfoList.isIndexed(orgColumnId);
	}
	else {
		if (getJoinPrimaryCondScore(expr, NULL, false) > 0) {
			firstUnmatched = true;
		}
		return false;
	}
}

void SQLCompiler::dereferenceAndAppendExpr(
		Plan &plan, PlanNodeRef &node, uint32_t inputId, const Expr &inExpr,
		Expr *&refExpr, util::Vector<uint32_t> &nodeRefList) {
	if (nodeRefList.empty()) {
		makeNodeRefList(plan, nodeRefList);
	}

	const PlanNodeId baseInNodeId = node->inputList_[inputId];

	PlanNode *inNode = &node->getInput(plan, inputId);
	if (nodeRefList[baseInNodeId] > 1 || !isDereferenceableNode(
			*inNode, NULL, SQLType::AGG_PHASE_ALL_PIPE, false)) {

		inNode = &plan.append(SQLType::EXEC_SELECT);
		inNode->inputList_.push_back(baseInNodeId);
		inNode->outputList_ = inputListToOutput(plan, *inNode, NULL);

		node->inputList_[inputId] = inNode->id_;
		nodeRefList.clear();
	}

	Expr *outExpr;
	dereferenceExpr(plan, *node, inputId, inExpr, outExpr, NULL);

	const ColumnId columnId =
			static_cast<ColumnId>(inNode->outputList_.size());
	inNode->outputList_.push_back(*outExpr);

	refreshColumnTypes(plan, *inNode, inNode->outputList_, true, false);

	refExpr = ALLOC_NEW(alloc_) Expr(genColumnExprBase(
			&plan, &(*node), inputId, columnId, NULL));
}

bool SQLCompiler::isDereferenceableNode(
		const PlanNode &inNode, const PlanExprList *outputList,
		AggregationPhase phase, bool multiInput) {

	switch (inNode.type_) {
	case SQLType::EXEC_GROUP:
	case SQLType::EXEC_JOIN:
	case SQLType::EXEC_SCAN:
	case SQLType::EXEC_SELECT:
	case SQLType::EXEC_SORT:
		break;
	default:
		return false;
	}

	if (getNodeAggregationLevel(inNode.outputList_) == 0 &&
			inNode.aggPhase_ != SQLType::AGG_PHASE_ALL_PIPE) {
		return false;
	}

	if (inNode.aggPhase_ != SQLType::AGG_PHASE_ALL_PIPE &&
			phase != SQLType::AGG_PHASE_ALL_PIPE) {
		return false;
	}

	if (outputList != NULL && getNodeAggregationLevel(*outputList) > 0) {
		if (getNodeAggregationLevel(inNode.outputList_) > 0 ||
				inNode.limit_ >= 0 ||
				inNode.type_ == SQLType::EXEC_GROUP) {
			return false;
		}
	}

	if (outputList != NULL && !isDereferenceableExprList(*outputList)) {
		if (getNodeAggregationLevel(inNode.outputList_) > 0 &&
				inNode.type_ != SQLType::EXEC_GROUP) {
			return false;
		}
	}

	if (multiInput && outputList != NULL &&
			!isDereferenceableExprList(*outputList)) {
		return false;
	}

	return true;
}

bool SQLCompiler::isDereferenceableExprList(const PlanExprList &exprList) {
	for (PlanExprList::const_iterator it = exprList.begin();
			it != exprList.end(); ++it) {
		if (!isDereferenceableExpr(*it)) {
			return false;
		}
	}

	return true;
}

bool SQLCompiler::isDereferenceableExpr(const Expr &expr) {
	if (!isDereferenceableExprType(expr.op_)) {
		return false;
	}

	for (ExprArgsIterator<> it(expr); it.exists(); it.next()) {
		if (!isDereferenceableExpr(it.get())) {
			return false;
		}
	}

	return true;
}

bool SQLCompiler::isDereferenceableExprType(Type exprType) {
	return (exprType != SQLType::EXPR_ID);
}

bool SQLCompiler::isInputDereferenceable(
		const Plan &plan, const PlanNode &node, uint32_t inputId,
		const PlanExprList &exprList, bool forOutput) {

	bool columnRefOnly;
	if (forOutput) {
		columnRefOnly = isInputNullable(node, inputId);
	}
	else {
		columnRefOnly =
				(node.type_ == SQLType::EXEC_GROUP ||
				node.type_ == SQLType::EXEC_SORT);
	}

	for (PlanExprList::const_iterator it = exprList.begin();
			it != exprList.end(); ++it) {
		if (!isInputDereferenceable(
				plan, node, &inputId, *it, columnRefOnly)) {
			return false;
		}
	}

	return true;
}

bool SQLCompiler::isInputDereferenceable(
		const Plan &plan, const PlanNode &node, const uint32_t *inputId,
		const Expr &expr, bool columnRefOnly) {
	if (isDisabledExpr(expr)) {
		return false;
	}
	else if (expr.op_ == SQLType::EXPR_COLUMN) {
		if (inputId != NULL && expr.inputId_ == *inputId) {
			assert(node.tableIdInfo_.isEmpty());

			const PlanNode &inNode = node.getInput(plan, *inputId);
			if (!isInputDereferenceable(
					plan, inNode, NULL, inNode.outputList_[expr.columnId_],
					columnRefOnly)) {
				return false;
			}
		}
	}
	else if (inputId == NULL) {
		if (!ProcessorUtils::isConstEvaluable(expr.op_) || columnRefOnly) {
			return false;
		}
	}

	for (ExprArgsIterator<> it(expr); it.exists(); it.next()) {
		if (!isInputDereferenceable(
				plan , node, inputId, it.get(), columnRefOnly)) {
			return false;
		}
	}

	return true;
}

void SQLCompiler::dereferenceExprList(
		const Plan &plan, const PlanNode &node, uint32_t inputId,
		const PlanExprList &inExprList, PlanExprList &outExprList,
		AggregationPhase &phase,
		const util::Map<uint32_t, uint32_t> *inputIdMap,
		const PlanNode *inputNode) {
	for (PlanExprList::const_iterator it = inExprList.begin();
			it != inExprList.end(); ++it) {
		if (it->op_ == SQLType::START_EXPR) {
			outExprList.push_back(*it);
		}
		else {
			Expr *outExpr;
			dereferenceExpr(
					plan, node, inputId, *it, outExpr, inputIdMap, inputNode);
			outExprList.push_back(*outExpr);
		}
	}

	setSplittedExprListType(outExprList, phase);
}

void SQLCompiler::dereferenceExpr(
		const Plan &plan, const PlanNode &node, uint32_t inputId,
		const Expr &inExpr, Expr *&outExpr,
		const util::Map<uint32_t, uint32_t> *inputIdMap,
		const PlanNode *inputNode) {

	if (!node.tableIdInfo_.isEmpty()) {
		outExpr = ALLOC_NEW(alloc_) Expr(inExpr);
		return;
	}

	if (inExpr.op_ == SQLType::EXPR_COLUMN) {
		bool inputReplacing = false;
		do {
			if (inputIdMap == NULL) {
				break;
			}

			util::Map<uint32_t, uint32_t>::const_iterator it =
					inputIdMap->find(inExpr.inputId_);
			if (it == inputIdMap->end()) {
				break;
			}

			if (it->first == inputId &&
					it->second == std::numeric_limits<uint32_t>::max()) {
				inputReplacing = true;
				break;
			}

			outExpr = ALLOC_NEW(alloc_) Expr(inExpr);
			outExpr->inputId_ = it->second;
			return;
		}
		while (false);

		const PlanNode &inNode =
				(inputNode != NULL ? *inputNode : node.getInput(plan, inputId));
		const Expr &expr = inNode.outputList_[inExpr.columnId_];
		const ColumnType outType = (expr.op_ == SQLType::EXPR_CONSTANT &&
				inExpr.columnType_ != TupleList::TYPE_NULL ?
				inExpr : expr).columnType_;

		outExpr = ALLOC_NEW(alloc_) Expr(alloc_);
		copyExpr(expr, *outExpr);
		if (inputReplacing) {
			replaceExprInput(*outExpr, inputId);
		}
		outExpr->sortAscending_ = inExpr.sortAscending_;

		if (outExpr->columnType_ != outType) {
			outExpr = ALLOC_NEW(alloc_) Expr(genCastExpr(
					plan, outType, outExpr, MODE_NORMAL_SELECT, true, true));

			if (outExpr->columnType_ != outType) {
				outExpr->columnType_ = outType;
			}
		}
		return;
	}

	uint32_t resolvedInputId = inExpr.inputId_;
	if (inExpr.op_ == SQLType::EXPR_ID) {
		const PlanNode &inNode =
				(inputNode != NULL ? *inputNode : node.getInput(plan, inputId));
		if (inNode.type_ == SQLType::EXEC_SELECT &&
				inNode.inputList_.empty()) {
			outExpr = ALLOC_NEW(alloc_) Expr(genConstExpr(
					plan, TupleValue(static_cast<int64_t>(1)),
					MODE_NORMAL_SELECT));
			return;
		}
		util::Map<uint32_t, uint32_t>::const_iterator inIt;
		if (inNode.type_ == SQLType::EXEC_SCAN &&
				inputIdMap != NULL && !inputIdMap->empty() &&
				(--(inIt = inputIdMap->end()))->second >= 1) {
			resolvedInputId = 1;
		}
	}

	outExpr = ALLOC_NEW(alloc_) Expr(inExpr);
	outExpr->inputId_ = resolvedInputId;

	if (outExpr->left_ != NULL) {
		dereferenceExpr(
				plan, node, inputId, *inExpr.left_, outExpr->left_,
				inputIdMap, inputNode);
	}

	if (outExpr->right_ != NULL) {
		dereferenceExpr(
				plan, node, inputId, *inExpr.right_, outExpr->right_,
				inputIdMap, inputNode);
	}

	if (outExpr->next_ != NULL) {
		outExpr->next_ = ALLOC_NEW(alloc_) ExprList(alloc_);

		for (ExprList::iterator it = inExpr.next_->begin();
				it != inExpr.next_->end(); ++it) {
			Expr *subExpr;
			dereferenceExpr(
					plan, node, inputId, **it, subExpr, inputIdMap, inputNode);
			outExpr->next_->push_back(subExpr);
		}
	}

	outExpr->aggrNestLevel_ = getAggregationLevel(*outExpr);

	if (!outExpr->constantEvaluable_ && isConstantEvaluable(*outExpr)) {
		outExpr->constantEvaluable_ = true;
		const bool strict = false;
		reduceExpr(*outExpr, strict);
	}
}

void SQLCompiler::replaceExprInput(Expr &expr, uint32_t inputId) {
	if (expr.op_ == SQLType::EXPR_COLUMN) {
		expr.inputId_ = inputId;
	}

	for (ExprArgsIterator<false> it(expr); it.exists(); it.next()) {
		replaceExprInput(it.get(), inputId);
	}
}

void SQLCompiler::copyPlan(const Plan &src, Plan &dest) {
	dest = src;

	for (PlanNodeList::const_iterator nodeIt = src.nodeList_.begin();
			nodeIt != src.nodeList_.end(); ++nodeIt) {
		const PlanNode &srcNode = *nodeIt;
		PlanNode &destNode = dest.nodeList_[nodeIt - src.nodeList_.begin()];
		copyNode(srcNode, destNode);
	}
}

void SQLCompiler::copyNode(const PlanNode &src, PlanNode &dest) {
	dest = src;

	dest.predList_.clear();
	copyExprList(src.predList_, dest.predList_, false);

	dest.outputList_.clear();
	copyExprList(src.outputList_, dest.outputList_, false);

	if (src.updateSetList_ != NULL) {
		dest.updateSetList_ = ALLOC_NEW(alloc_) PlanExprList(alloc_);
		copyExprList(*src.updateSetList_, *dest.updateSetList_, false);
	}
}

void SQLCompiler::copyExprList(
		const PlanExprList &src, PlanExprList &dest, bool shallow) {
	assert(&src != &dest);

	if (shallow) {
		dest = src;
	}

	dest.resize(src.size(), Expr(alloc_));

	PlanExprList::iterator destIt = dest.begin();
	for (PlanExprList::const_iterator it = src.begin();
			it != src.end(); ++it, ++destIt) {
		copyExpr(*it, *destIt);
	}
}

void SQLCompiler::copyExpr(const Expr &src, Expr &dest) {
	dest = src;

	if (dest.left_ != NULL) {
		dest.left_ = ALLOC_NEW(alloc_) Expr(alloc_);
		copyExpr(*src.left_, *dest.left_);
	}

	if (dest.right_ != NULL) {
		dest.right_ = ALLOC_NEW(alloc_) Expr(alloc_);
		copyExpr(*src.right_, *dest.right_);
	}

	if (dest.next_ != NULL) {
		dest.next_ = ALLOC_NEW(alloc_) ExprList(alloc_);

		for (ExprList::const_iterator it = src.next_->begin();
				it != src.next_->end(); ++it) {
			dest.next_->push_back(ALLOC_NEW(alloc_) Expr(alloc_));
			copyExpr(**it, *dest.next_->back());
		}
	}
}

void SQLCompiler::mergeAndOp(
		Expr *inExpr1, Expr *inExpr2,
		const Plan &plan, Mode mode, Expr *&outExpr) {
	mergeAndOpBase(inExpr1, inExpr2, &plan, mode, outExpr);
}

void SQLCompiler::mergeAndOpBase(
		Expr *inExpr1, Expr *inExpr2,
		const Plan *plan, Mode mode, Expr *&outExpr) {
	if (inExpr1 == NULL || isTrueExpr(*inExpr1)) {
		if (inExpr2 == NULL || isTrueExpr(*inExpr2)) {
			outExpr = NULL;
		}
		else {
			outExpr = ALLOC_NEW(alloc_) Expr(*inExpr2);
		}
	}
	else {
		if (inExpr2 == NULL || isTrueExpr(*inExpr2)) {
			outExpr = ALLOC_NEW(alloc_) Expr(*inExpr1);
		}
		else {
			outExpr = ALLOC_NEW(alloc_) Expr(genOpExprBase(
					plan, SQLType::EXPR_AND, inExpr1, inExpr2, mode, true));
		}
	}
}

void SQLCompiler::mergeOrOp(
		Expr *inExpr1, Expr *inExpr2, Mode mode, Expr *&outExpr) {
	if (inExpr1 == NULL) {
		if (inExpr2 == NULL) {
			outExpr = NULL;
		}
		else {
			outExpr = ALLOC_NEW(alloc_) Expr(*inExpr2);
		}
	}
	else {
		if (inExpr2 == NULL) {
			outExpr = ALLOC_NEW(alloc_) Expr(*inExpr1);
		}
		else {
			outExpr = ALLOC_NEW(alloc_) Expr(genOpExprBase(
					NULL, SQLType::EXPR_OR, inExpr1, inExpr2, mode, true));
		}
	}
}

bool SQLCompiler::mergeLimit(
		int64_t limit1, int64_t limit2, int64_t offset1, int64_t *merged) {

	if (limit1 >= 0 && limit2 >= 0) {
		*merged = std::min(limit1, limit2);
	}
	else {
		*merged = std::max(limit1, limit2);
	}

	if (*merged < 0) {
		*merged = -1;
	}
	else if (offset1 > 0) {
		if (*merged > std::numeric_limits<int64_t>::max() - offset1) {
			*merged = -1;
			return false;
		}
		else {
			*merged += offset1;
		}
	}

	return true;
}

void SQLCompiler::makeNodeLimitList(
		const Plan &plan,
		util::Vector< std::pair<int64_t, bool> > &nodeLimitList) {
	const int64_t unknown = std::numeric_limits<int64_t>::min();
	nodeLimitList.assign(
			plan.nodeList_.size(), std::make_pair(unknown, false));

	size_t resolvedPos = 0;
	for (;;) {
		size_t nextResolvedPos = std::numeric_limits<size_t>::max();

		for (PlanNodeList::const_iterator nodeIt =
				plan.nodeList_.begin() + resolvedPos;
				nodeIt != plan.nodeList_.end(); ++nodeIt) {
			const PlanNode &node = *nodeIt;

			int64_t limit;
			if (isDisabledNode(node)) {
				limit = -1;
			}
			else if (getNodeAggregationLevel(node.outputList_) > 0 &&
					!isWindowNode(node) &&
					(node.type_ != SQLType::EXEC_GROUP ||
							node.predList_.empty()) &&
					(node.aggPhase_ == SQLType::AGG_PHASE_ALL_PIPE ||
							node.aggPhase_ == SQLType::AGG_PHASE_MERGE_PIPE)) {
				limit = 1;
			}
			else if (node.inputList_.empty()) {
				if (node.type_ == SQLType::EXEC_SELECT &&
						node.tableIdInfo_.isEmpty()) {
					limit = 1;
				}
				else {
					limit = -1;
				}
			}
			else if (isNodeLimitUnchangeable(node)) {
				limit = 0;
				for (PlanNode::IdList::const_iterator it =
						node.inputList_.begin();
						it != node.inputList_.end(); ++it) {
					const int64_t inLimit = nodeLimitList[*it].first;
					if (inLimit == unknown) {
						limit = unknown;
						break;
					}
					else if (inLimit < 0 || inLimit >
							std::numeric_limits<int64_t>::max() - limit) {
						limit = -1;
						break;
					}
					limit += inLimit;
				}
			}
			else {
				limit = -1;
			}

			bool redundant = false;
			if (limit == unknown) {
				nextResolvedPos = std::min(
						nextResolvedPos,
						static_cast<size_t>(nodeIt - plan.nodeList_.begin()));
			}
			else if (node.limit_ >= 0) {
				int64_t merged;
				mergeLimit(node.limit_, limit, 0, &merged);
				assert(merged >= 0);

				if (limit == merged) {
					if (node.offset_ <= 0) {
						redundant = true;
					}
				}
				else {
					limit = merged;
				}
			}

			nodeLimitList[nodeIt - plan.nodeList_.begin()] =
					std::make_pair(limit, redundant);
		}

		if (nextResolvedPos >= nodeLimitList.size()) {
			break;
		}
		resolvedPos = nextResolvedPos;
	}
}

bool SQLCompiler::isNodeLimitUnchangeable(const PlanNode &node) {
	return (node.type_ == SQLType::EXEC_LIMIT ||
			(node.type_ == SQLType::EXEC_UNION &&
					node.unionType_ == SQLType::UNION_ALL) ||
			(node.type_ == SQLType::EXEC_SELECT &&
					node.inputList_.size() == 1 && (node.predList_.empty() ||
							isTrueExpr(node.predList_.front())) &&
					getNodeAggregationLevel(node.outputList_) == 0));
}

const SQLCompiler::TableInfo* SQLCompiler::findTable(
		const QualifiedName &name) {
	return getTableInfoList().find(name);
}

const SQLCompiler::TableInfo& SQLCompiler::resolveTable(
		const QualifiedName &name) {
	return getTableInfoList().resolve(name);
}

size_t SQLCompiler::resolveColumn(
		const Plan &plan, const PlanNode &node,
		const QualifiedName &name, Expr &out, Mode mode,
		const PlanNodeId *productedNodeId, bool *tableFoundRef,
		const PendingColumn **pendingColumnRef) {
	assert(name.name_ != NULL || name.table_ != NULL);

	if (tableFoundRef != NULL) {
		*tableFoundRef = false;
	}

	const size_t orgPendingCount = pendingColumnList_.size();

	const bool shallow = (&plan.back() == &node);
	assert(shallow ||
			mode == MODE_ORDERBY ||
			mode == MODE_GROUPBY ||
			mode == MODE_GROUP_SELECT);

	if (!node.tableIdInfo_.isEmpty()) {
		const TableInfoList &tableInfoList = getTableInfoList();
		const TableInfo &tableInfo = tableInfoList.resolve(node.tableIdInfo_.id_);

		if (name.table_ != NULL) {
			if ((node.qName_ == NULL &&
					tableInfoList.resolve(name).idInfo_.id_ != tableInfo.idInfo_.id_) ||
					(node.qName_ != NULL &&
							!isTableMatched(name, *node.qName_))) {
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_TABLE_NOT_FOUND, NULL,
						"Table not found (name=" <<
						QualifiedNameFormatter::asColumn(name) << ")");
			}
		}

		const uint32_t inputId = 0;

		if (name.name_ == NULL) {
			out.op_ = SQLType::EXPR_ALL_COLUMN;
			out.inputId_ = inputId;
			out.columnId_ = 0;

			out.left_ = ALLOC_NEW(alloc_) Expr(alloc_);
			out.left_->columnId_ =
					static_cast<ColumnId>(tableInfo.columnInfoList_.size());

			return true;
		}

		typedef TableInfo::SQLColumnInfoList List;
		const List &list = tableInfo.columnInfoList_;

		for (List::const_iterator it = list.begin(); it != list.end(); ++it) {
			if (compareStringWithCase(
					it->second.c_str(), name.name_->c_str(),
					name.nameCaseSensitive_) == 0) {

				const ColumnId columnId =
						static_cast<ColumnId>(it - list.begin());
				out = genResolvedColumn(
						plan, node, inputId, columnId, pendingColumnRef);

				return true;
			}
		}

		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_TABLE_NOT_FOUND, NULL,
				"Column not found (name=" <<
				QualifiedNameFormatter::asColumn(name) << ")");
	}

	size_t shallowCount = 0;
	size_t deepCount = 0;

	bool tableFound = false;
	const Expr *foundExpr = NULL;

	const bool forNormalGroupSelect =
			(mode == MODE_GROUP_SELECT && !isRangeGroupNode(node));
	bool deepError = false;
	Expr deepOut(alloc_);
	const PendingColumn *deepPendingColumn = NULL;
	if (shallow &&
			(mode == MODE_GROUPBY ||
			forNormalGroupSelect ||
			mode == MODE_ORDERBY)) {
		if (productedNodeId != NULL) {
			deepCount = resolveColumnDeep(
					plan, plan.nodeList_[*productedNodeId],
					name, deepOut, mode, &tableFound, &deepError,
					&deepPendingColumn);
		}
		else {
			assert(false);
		}
	}

	const PlanNode::IdList &inputList = node.inputList_;

	PlanNode::IdList::const_iterator inIt = inputList.begin();
	if (shallow && forNormalGroupSelect) {
		inIt = inputList.end();
	}

	for (; inIt != inputList.end(); ++inIt) {
		if (*inIt == UNDEF_PLAN_NODEID) {
			continue;
		}

		const PlanNode &inNode = plan.nodeList_[*inIt];
		const QualifiedName *tableAlias = inNode.qName_;

		const PlanExprList &columnList = inNode.outputList_;
		for (PlanExprList::const_iterator it = columnList.begin();
				it != columnList.end(); ++it) {
			const QualifiedName *inName = it->qName_;
			if (inName == NULL) {
				continue;
			}

			if (name.table_ != NULL) {
				const QualifiedName *tableName =
						(tableAlias == NULL ? inName : tableAlias);

				if (!isTableMatched(name, *tableName)) {
					continue;
				}
				tableFound = true;
			}

			const uint32_t inputId =
					static_cast<uint32_t>(inIt - inputList.begin());
			const ColumnId columnId =
					static_cast<ColumnId>(it - columnList.begin());

			if (name.name_ == NULL) {
				out.op_ = SQLType::EXPR_ALL_COLUMN;

				if (shallowCount > 0) {
					if (out.inputId_ != inputId) {
						if (out.subqueryLevel_ > it->subqueryLevel_) {
							continue;
						}
						else if (out.subqueryLevel_ < it->subqueryLevel_) {
							shallowCount = 0;
						}
					}
				}
				else {
					out.columnId_ = columnId;
					out.left_ = ALLOC_NEW(alloc_) Expr(alloc_);
					shallowCount++;
				}

				out.inputId_ = inputId;
				out.left_->columnId_ = columnId + 1;
				out.subqueryLevel_ = it->subqueryLevel_;
			}
			else if (inName->name_ != NULL &&
					isNameOnlyMatched(*inName, name)) {
				if (shallowCount > 0) {
					if (out.subqueryLevel_ > it->subqueryLevel_) {
						continue;
					}
					else if (out.subqueryLevel_ < it->subqueryLevel_) {
						shallowCount = 0;
					}
				}

				out = genResolvedColumn(
						plan, node, inputId, columnId, pendingColumnRef);

				shallowCount++;
				foundExpr = &(*it);
			}
		}
	}

	size_t foundCount = shallowCount;
	if (deepCount > 0) {
		if (deepPendingColumn == NULL) {
			assert(false);
			SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
		}

		const PlanNode &deepNode = plan.nodeList_[deepPendingColumn->first];
		const Expr &deepExpr = deepPendingColumn->second;

		if (shallowCount > 0) {
			if (out.subqueryLevel_ < deepOut.subqueryLevel_) {
				out = deepOut;
				foundExpr = NULL;
				foundCount = deepCount;
			}
			else if (out.subqueryLevel_ > deepOut.subqueryLevel_ ||
					mode == MODE_ORDERBY) {
				clearPendingColumnList(orgPendingCount);
			}
			else {
				const Expr *shallowExpr = followResolvedColumn(
						plan, deepNode, plan.back(), out, false);
				if (!(shallowExpr == NULL ||
						shallowExpr->inputId_ != deepExpr.inputId_ ||
						shallowExpr->columnId_ != deepExpr.columnId_)) {
					clearPendingColumnList(orgPendingCount);
				}
				else {
					out = deepOut;
					foundExpr = NULL;
				}
				foundCount = deepCount;
			}
		}
		else if (mode == MODE_ORDERBY) {
			out = deepOut;
			foundCount = deepCount;
		}
		else {
			Expr curExpr(alloc_);
			curExpr.op_ = SQLType::EXPR_COLUMN;
			bool replaced = false;

			PlanNode::IdList::const_iterator inIt = inputList.begin();
			for (inIt = inputList.begin(); inIt != inputList.end(); ++inIt) {
				if (*inIt == UNDEF_PLAN_NODEID) {
					continue;
				}

				curExpr.inputId_ =
						static_cast<uint32_t>(inIt - inputList.begin());
				const PlanNode &inNode = plan.nodeList_[*inIt];

				const PlanExprList &columnList = inNode.outputList_;
				for (PlanExprList::const_iterator it = columnList.begin();
						it != columnList.end(); ++it) {
					curExpr.columnId_ =
							static_cast<ColumnId>(it - columnList.begin());

					const Expr *shallowExpr = followResolvedColumn(
							plan, deepNode, plan.back(), curExpr, false);
					if (shallowExpr == NULL ||
							shallowExpr->inputId_ != deepExpr.inputId_ ||
							shallowExpr->columnId_ != deepExpr.columnId_) {
						continue;
					}

					clearPendingColumnList(orgPendingCount);
					out = genResolvedColumn(
							plan, node, curExpr.inputId_, curExpr.columnId_,
							pendingColumnRef);
					replaced = true;
					break;
				}

				if (replaced) {
					break;
				}
			}

			if (!replaced) {
				out = deepOut;
			}
			foundCount = deepCount;
		}
	}

	if (foundCount > 1 && shallow) {
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_COLUMN_NOT_RESOLVED, NULL,
				"Multiple " << (name.name_ == NULL ? "table" : "column") <<
				" matched (name=" <<
				QualifiedNameFormatter::asColumn(name) << ")");
	}

	do {
		if (foundCount == 0 && shallow) {
			break;
		}

		if (tableFoundRef != NULL) {
			*tableFoundRef = tableFound;
		}

		if (foundExpr != NULL &&
				foundExpr->autoGenerated_ &&
				foundExpr->op_ == SQLType::EXPR_CONSTANT &&
				foundExpr->columnId_ == REPLACED_AGGR_COLUMN_ID) {
			if (mode != MODE_GROUPBY) {
				break;
			}
			SQL_COMPILER_THROW_ERROR(
					GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
					"Grouping key cannot contain aggregation functions (name=" <<
					QualifiedNameFormatter::asColumn(name) << ")");
		}

		return foundCount;
	}
	while (false);

	if (deepError && mode == MODE_ORDERBY) {
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
				"Ordering key must be found in "
				"projection or grouping key (name=" <<
				QualifiedNameFormatter::asColumn(name) << ")");
	}

	if (name.table_ != NULL && !tableFound) {
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_TABLE_NOT_FOUND, NULL,
				"Table not found (name=" <<
				QualifiedNameFormatter::asColumn(name) << ")");
	}

	SQL_COMPILER_THROW_ERROR(
			GS_ERROR_SQL_COMPILE_COLUMN_NOT_FOUND, NULL,
			"Column not found (name=" <<
			QualifiedNameFormatter::asColumn(name) << ")");
}

size_t SQLCompiler::resolveColumnDeep(
		const Plan &plan, const PlanNode &node, const QualifiedName &name,
		Expr &out, Mode mode, bool *tableFoundRef, bool *errorRef,
		const PendingColumn **pendingColumnRef) {
	if (errorRef != NULL) {
		*errorRef = false;
	}

	const size_t orgPendingCount = pendingColumnList_.size();

	const PendingColumn *deepPendingColumn = NULL;
	const size_t deepCount = resolveColumn(
			plan, node, name, out, mode, NULL, tableFoundRef,
			&deepPendingColumn);
	if (deepCount == 0) {
		return deepCount;
	}

	if (deepPendingColumn == NULL) {
		assert(false);
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	bool errorFound = false;
	for (const PlanNode *curNode = &plan.back();;) {
		switch (curNode->type_) {
		case SQLType::EXEC_GROUP:
			if (curNode != &plan.back()) {
				const Expr &resolved = deepPendingColumn->second;
				bool autoGenOnly = true;

				for (PlanExprList::const_iterator predIt =
						curNode->predList_.begin();; ++predIt) {

					if (predIt == curNode->predList_.end()) {
						errorFound = !autoGenOnly;
						break;
					}

					const Expr *pred = followResolvedColumn(
							plan, node, *curNode, *predIt, false);
					if (pred != NULL &&
							pred->inputId_ == resolved.inputId_ &&
							pred->columnId_ == resolved.columnId_) {
						break;
					}

					autoGenOnly &= predIt->autoGenerated_;
				}
			}
			break;
		case SQLType::EXEC_JOIN:
			break;
		case SQLType::EXEC_SELECT:
			if (curNode != &plan.back() &&
					getNodeAggregationLevel(curNode->outputList_) > 0) {
				errorFound = true;
			}
			break;
		default:
			if (curNode != &plan.back()) {
				errorFound = true;
			}
			break;
		}

		if (errorFound) {
			clearPendingColumnList(orgPendingCount);
			break;
		}

		if (curNode == &node) {
			break;
		}

		if (curNode->inputList_.empty()) {
			assert(false);
			SQL_COMPILER_THROW_ERROR(
					GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
		}
		curNode = &curNode->getInput(plan, 0);
	}

	if (errorFound) {
		if (errorRef != NULL) {
			*errorRef = true;
		}
		return 0;
	}

	if (pendingColumnRef != NULL) {
		*pendingColumnRef = deepPendingColumn;
	}

	return deepCount;
}

const SQLCompiler::Expr* SQLCompiler::followResolvedColumn(
		const Plan &plan, const PlanNode &srcNode,
		const PlanNode &destNode, const Expr &expr, bool strict) {

	const Expr *curExpr = &expr;
	const PlanNode *curNode = &destNode;

	while (curNode != &srcNode) {
		if (curExpr->op_ != SQLType::EXPR_COLUMN) {
			break;
		}

		curNode = &curNode->getInput(plan, curExpr->inputId_);
		curExpr = &curNode->outputList_[curExpr->columnId_];
	}

	if (curExpr->op_ != SQLType::EXPR_COLUMN) {
		if (!strict) {
			return NULL;
		}
		assert(false);
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	return curExpr;
}

SQLCompiler::Expr SQLCompiler::genResolvedColumn(
		const Plan &plan, const PlanNode &node, uint32_t inputId,
		ColumnId columnId, const PendingColumn **pendingColumnRef) {

	const bool shallow = (&plan.back() == &node);

	if (shallow) {
		return genColumnExpr(plan, inputId, columnId);
	}

	const PlanNodeId nodeId =
			static_cast<PlanNodeId>(&node - &plan.nodeList_[0]);

	const uint32_t srcInputId = 0;
	ColumnId srcColumnId = static_cast<ColumnId>(
			plan.back().getInput(plan, srcInputId).outputList_.size());

	const PendingColumn *foundColumn = NULL;

	for (PendingColumnList::iterator it = pendingColumnList_.begin();
			it != pendingColumnList_.end(); ++it) {
		PendingColumn &entry = *it;

		if (entry.first != nodeId) {
			continue;
		}

		const Expr &expr = entry.second;
		if (expr.inputId_ == inputId && expr.columnId_ == columnId) {
			foundColumn = &(*it);
			break;
		}

		srcColumnId++;
	}

	if (foundColumn == NULL) {
		pendingColumnList_.push_back(PendingColumn(
				nodeId,
				genColumnExprBase(&plan, &node, inputId, columnId, NULL)));
		foundColumn = &pendingColumnList_.back();
	}

	if (pendingColumnRef != NULL) {
		*pendingColumnRef = foundColumn;
	}

	return genColumnExprBase(
			&plan, &node, srcInputId, srcColumnId, &foundColumn->second);
}

void SQLCompiler::applyPendingColumnList(Plan &plan) {
	PlanNode &node = plan.back();

	for (PendingColumnList::iterator it = pendingColumnList_.begin();
			it != pendingColumnList_.end(); ++it) {
		applyPendingColumn(plan, node, *it);
	}

	pendingColumnList_.clear();
}

void SQLCompiler::applyPendingColumn(
		Plan &plan, PlanNode &node, const PendingColumn &pendingColumn) {

	const PlanNodeId nodeId =
			static_cast<PlanNodeId>(&node - &plan.nodeList_[0]);

	Expr *appliedExpr = NULL;
	if (nodeId == pendingColumn.first) {
		node.outputList_.push_back(pendingColumn.second);
		appliedExpr = &node.outputList_.back();
	}
	else {
		uint32_t inputId = 0;
		PlanNode &inNode = node.getInput(plan, inputId);

		applyPendingColumn(plan, inNode, pendingColumn);

		if (&plan.back() != &node) {
			const ColumnId columnId =
					static_cast<ColumnId>(inNode.outputList_.size() - 1);

			node.outputList_.push_back(genColumnExprBase(
					&plan, &node, inputId, columnId, NULL));
			appliedExpr = &node.outputList_.back();
		}
	}

	if (appliedExpr != NULL) {
		appliedExpr->qName_ = NULL;
		appliedExpr->autoGenerated_ = true;
	}
}

void SQLCompiler::clearPendingColumnList(size_t orgSize) {
	while (pendingColumnList_.size() > orgSize) {
		pendingColumnList_.pop_back();
	}
	assert(pendingColumnList_.size() == orgSize);
}

SQLCompiler::Type SQLCompiler::resolveFunction(const Expr &expr) {
	if (expr.qName_ == NULL || expr.qName_->name_ == NULL) {
		assert(false);
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	const util::String &name = *expr.qName_->name_;
	const size_t orgArgCount = (expr.next_ == NULL ? 0 : expr.next_->size());
	const bool distinct =
			(expr.aggrOpts_ & SyntaxTree::AGGR_OPT_DISTINCT) != 0;

	util::StackAllocator::Scope scope(alloc_);
	util::XArray<char8_t> nameBuf(alloc_);

	bool argCountUnmatched = false;

	const char8_t *prefixList[] = { "FUNC_", "AGG_", NULL };
	for (const char8_t **it = prefixList; *it != NULL; ++it) {
		const char8_t *prefix = *it;

		nameBuf.clear();
		nameBuf.insert(nameBuf.end(), prefix, prefix + strlen(prefix));
		nameBuf.insert(nameBuf.end(), name.begin(), name.end());

		nameBuf.push_back('\0');

		SQLProcessor::ValueUtils::toUpper(nameBuf.data(), nameBuf.size());

		size_t argCount = orgArgCount;
		Type type;
		if (it - prefixList == 1 && strcmp(nameBuf.data(), "AGG_COUNT") == 0) {
			if (expr.next_ != NULL && !expr.next_->empty() &&
					(*expr.next_)[0] != NULL &&
					(*expr.next_)[0]->op_ == SQLType::EXPR_ALL_COLUMN) {
				type = SQLType::AGG_COUNT_ALL;
				argCount = 0;
			}
			else {
				type = SQLType::AGG_COUNT_COLUMN;
			}
		}
		else if (!SQLType::Coder()(&nameBuf[0], type)) {
			continue;
		}

		if (!filterFunction(type, argCount)) {
			continue;
		}

		if (type == SQLType::AGG_GROUP_CONCAT && argCount == 1) {
			argCount = 2;
		}

		if (!ProcessorUtils::checkArgCount(
				type, argCount, SQLType::AGG_PHASE_ALL_PIPE)) {
			argCountUnmatched = true;
			continue;
		}

		if (distinct) {
			type = getAggregationType(type, distinct);
		}
		bool windowOnly;
		bool pseudoWindow;
		bool isWindowExprType = ProcessorUtils::isWindowExprType(type, windowOnly, pseudoWindow);
		if (isWindowExprType) {
			if (windowOnly && !expr.windowOpt_) {
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, &expr,
						"The function '" << name << "' must have an OVER clause");
			}
		}
		else {
			if (expr.windowOpt_) {
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, &expr,
						"The OVER clause cannot be used with the function '" << name << "'");
			}
		}

		const bool orderingAggr =
				ProcessorUtils::isExplicitOrderingAggregation(type);
		const bool withOrderingArg =
				(expr.next_ != NULL && !expr.next_->empty() &&
				expr.next_->front()->op_ == SQLType::EXPR_AGG_ORDERING);
		if (!orderingAggr != !withOrderingArg) {
			if (orderingAggr) {
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, &expr,
						"The function '" << name <<
						"' must have an WITHIN GROUP clause");
			}
			else {
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, &expr,
						"The WITHIN GROUP clause cannot be used "
						"with the function '" << name << "'");
			}
		}
		return type;
	}

	if (argCountUnmatched) {
		SQL_COMPILER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_INVALID_ARG_COUNT, &expr,
				"Invalid argument count (count=" << orgArgCount <<
				", functionName=" << name << ")");
	}

	SQL_COMPILER_THROW_ERROR(
			GS_ERROR_SQL_COMPILE_FUNCTION_NOT_FOUND, &expr,
			"Unknown function (name=" << name << ")");
}

bool SQLCompiler::filterFunction(Type &type, size_t argCount) {
	switch (type) {
	case SQLType::AGG_STDDEV:
		if (varianceCompatible_) {
			type = SQLType::AGG_STDDEV_POP;
		}
		else {
			type = SQLType::AGG_STDDEV_SAMP;
		}
		return true;
	case SQLType::AGG_VARIANCE:
		if (varianceCompatible_) {
			type = SQLType::AGG_VAR_POP;
		}
		else {
			type = SQLType::AGG_VAR_SAMP;
		}
		return true;
	case SQLType::FUNC_SUBSTR:
		if (substrCompatible_) {
			type = SQLType::FUNC_SUBSTR_WITH_BOUNDS;
		}
		return true;
	case SQLType::FUNC_MAKE_TIMESTAMP:
		if (argCount <= 4) {
			type = SQLType::FUNC_MAKE_TIMESTAMP_BY_DATE;
		}
		return true;
	default:
		if (!experimentalFuncEnabled_ &&
				ProcessorUtils::isExperimentalFunction(type)) {
			return false;
		}
		return !ProcessorUtils::isInternalFunction(type);
	}
}

void SQLCompiler::arrangeFunctionArgs(Type type, Expr &expr) {
	ExprList *exprList = expr.next_;
	if (exprList == NULL) {
		return;
	}

	bool checkRequired = false;
	for (size_t lastIndex = 0;;) {
		size_t index;
		ColumnType argType;
		if (!ProcessorUtils::findConstantArgType(
				type, lastIndex, index, argType)) {
			break;
		}

		Expr *argExpr = (*exprList)[index];
		const bool forConst = (argExpr->op_ == SQLType::EXPR_CONSTANT);
		if (!forConst && argExpr->op_ != SQLType::EXPR_PLACEHOLDER) {
			SQL_COMPILER_THROW_ERROR(
					GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
					"The argument in the specified function must be constant "
					"(function=" << SQLType::Coder()(type, "") << ")");
		}
		checkRequired |= forConst;

		do {
			TupleValue &argValue = argExpr->value_;
			if (!forConst || argValue.getType() == argType) {
				break;
			}

			const bool implicit = true;
			argValue = ProcessorUtils::convertType(
					getVarContext(), argValue, argType, implicit);
			if (argValue.getType() == argType) {
				argExpr->columnType_ = argValue.getType();
				break;
			}

			if (!ColumnTypeUtils::isNull(argValue.getType())) {
				SQL_COMPILER_THROW_ERROR(
						GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
			}

			SQL_COMPILER_THROW_ERROR(
					GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
					"The argument in the specified function must not be null "
					"(function=" << SQLType::Coder()(type, "") << ")");
		}
		while (false);

		lastIndex = index + 1;
	}

	if (checkRequired) {
		try {
			Expr subExpr = expr;
			subExpr.op_ = type;
			ProcessorUtils::checkExprArgs(
					getVarContext(), subExpr, getCompileOption());
		}
		catch (std::exception &e) {
			SQL_COMPILER_RETHROW_ERROR(
					e, NULL, GS_EXCEPTION_MESSAGE(e) <<
					" (function=" << SQLType::Coder()(type, "") << ")");
		}
	}
}

uint32_t SQLCompiler::genSourceId() {
	return ++lastSrcId_;
}

const SQLCompiler::PlanExprList& SQLCompiler::getOutput(const Plan &in) {
	return in.back().outputList_;
}

SQLCompiler::ColumnType SQLCompiler::getResultType(
		const Expr &expr, const Expr *baseExpr, ColumnType columnType,
		Mode mode, size_t index, AggregationPhase phase) {
	const Type exprType = expr.op_;
	if (exprType == SQLType::EXPR_CONSTANT) {
		const ColumnType valueType = expr.value_.getType();

		if (valueType == TupleList::TYPE_NULL) {
			return TupleList::TYPE_ANY;
		}

		return valueType;
	}
	else if (exprType == SQLType::EXPR_TYPE) {
		if (columnType != TupleList::TYPE_NULL) {
			return columnType;
		}
		else if (baseExpr == NULL) {
			return expr.columnType_;
		}
		else {
			return baseExpr->columnType_;
		}
	}
	else if (exprType == SQLType::EXPR_PLACEHOLDER ||
			exprType == SQLType::EXPR_LIST) {
		return TupleList::TYPE_NULL;
	}

	if (index > 0 && ProcessorUtils::getResultCount(exprType, phase) <= 1) {
		return TupleList::TYPE_NULL;
	}

	util::Vector<ColumnType> argTypeList(expr.alloc_);
	for (ExprArgsIterator<> it(expr); it.exists(); it.next()) {
		const ColumnType columnType = it.get().columnType_;
		if (columnType == TupleList::TYPE_NULL) {
			return TupleList::TYPE_NULL;
		}

		argTypeList.push_back(columnType);
	}

	ColumnType resultType = TupleList::TYPE_NULL;
	do {
		if (exprType == SQLType::OP_CAST) {
			const bool evaluating = false;
			resultType = ProcessorUtils::findConversionType(
					argTypeList[0], argTypeList[1],
					expr.autoGenerated_, evaluating);
			break;
		}

		resultType = ProcessorUtils::getResultType(
				exprType, index, phase, argTypeList,
				mode == MODE_GROUP_SELECT);
	}
	while (false);

	if (resultType == TupleList::TYPE_NULL) {
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_PROC_UNSUPPORTED_TYPE_CONVERSION, NULL,
				"Illegal type conversion");
	}

	return resultType;
}

bool SQLCompiler::isInputNullable(const PlanNode &planNode, uint32_t inputId) {
	if (planNode.type_ != SQLType::EXEC_JOIN) {
		return false;
	}

	switch (planNode.joinType_) {
	case SQLType::JOIN_INNER:
		return false;
	case SQLType::JOIN_LEFT_OUTER:
		return inputId == 1;
	case SQLType::JOIN_RIGHT_OUTER:
		return inputId == 0;
	case SQLType::JOIN_FULL_OUTER:
		return true;
	default:
		assert(false);
		return false;
	}
}

bool SQLCompiler::isConstantEvaluable(const Expr &expr) {
	if (!ProcessorUtils::isConstEvaluable(expr.op_)) {
		return false;
	}

	bool evaluable = true;
	for (ExprArgsIterator<> it(expr); it.exists(); it.next()) {
		evaluable &= it.get().constantEvaluable_;
	}

	return evaluable;
}

bool SQLCompiler::isConstantOnExecution(const Expr &expr) {
	if (expr.op_ == SQLType::EXPR_CONSTANT ||
			expr.op_ == SQLType::EXPR_PLACEHOLDER ||
			expr.op_ == SQLType::FUNC_NOW) {
		return true;
	}

	for (ExprArgsIterator<> it(expr); it.exists(); it.next()) {
		if (!isConstantOnExecution(it.get())) {
			return false;
		}
	}

	return ProcessorUtils::isConstEvaluable(expr.op_);
}

uint32_t SQLCompiler::getAggregationLevel(const Expr &expr) {
	uint32_t level = 0;
	for (ExprArgsIterator<> it(expr); it.exists(); it.next()) {
		const uint32_t subLevel = it.get().aggrNestLevel_;
		level = static_cast<uint32_t>(std::max(level, subLevel));
	}
	if (SQLType::START_AGG < expr.op_ && expr.op_ < SQLType::END_AGG) {
		level++;
	}
	return level;
}

size_t SQLCompiler::getArgCount(const Expr &expr) {
	size_t count = 0;
	for (ExprArgsIterator<> it(expr); it.exists(); it.next()) {
		count++;
	}
	return count;
}

SQLCompiler::Type SQLCompiler::getAggregationType(
		Type exprType, bool distinct) {
	if (!distinct) {
		return exprType;
	}

	switch (exprType) {
	case SQLType::AGG_AVG:
		return SQLType::AGG_DISTINCT_AVG;
	case SQLType::AGG_COUNT_COLUMN:
		return SQLType::AGG_DISTINCT_COUNT_COLUMN;
	case SQLType::AGG_GROUP_CONCAT:
		return SQLType::AGG_DISTINCT_GROUP_CONCAT;
	case SQLType::AGG_MAX:
		return SQLType::AGG_MAX;
	case SQLType::AGG_MIN:
		return SQLType::AGG_MIN;
	case SQLType::AGG_STDDEV:
		return SQLType::AGG_DISTINCT_STDDEV;
	case SQLType::AGG_STDDEV_POP:
		return SQLType::AGG_DISTINCT_STDDEV_POP;
	case SQLType::AGG_STDDEV0:
		return SQLType::AGG_DISTINCT_STDDEV0;
	case SQLType::AGG_STDDEV_SAMP:
		return SQLType::AGG_DISTINCT_STDDEV_SAMP;
	case SQLType::AGG_SUM:
		return SQLType::AGG_DISTINCT_SUM;
	case SQLType::AGG_TOTAL:
		return SQLType::AGG_DISTINCT_TOTAL;
	case SQLType::AGG_VARIANCE:
		return SQLType::AGG_DISTINCT_VARIANCE;
	case SQLType::AGG_VAR_POP:
		return SQLType::AGG_DISTINCT_VAR_POP;
	case SQLType::AGG_VAR_SAMP:
		return SQLType::AGG_DISTINCT_VAR_SAMP;
	case SQLType::AGG_VARIANCE0:
		return SQLType::AGG_DISTINCT_VARIANCE0;
	default:
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, NULL,
				"Illegal distinct option found");
	}
}

TupleList::TupleColumnType SQLCompiler::setColumnTypeNullable(
		ColumnType type, bool nullable) {
	if (ColumnTypeUtils::isNull(type) || ColumnTypeUtils::isAny(type)) {
		return type;
	}

	if (nullable) {
		return static_cast<ColumnType>(type | TupleList::TYPE_MASK_NULLABLE);
	}
	else {
		return static_cast<ColumnType>(type & ~TupleList::TYPE_MASK_NULLABLE);
	}
}

bool SQLCompiler::isTableMatched(
		const QualifiedName &name1, const QualifiedName &name2) {
	if (name1.table_ == NULL || name2.table_ == NULL ||
			!isTableOnlyMatched(name1, name2)) {
		return false;
	}

	if (name1.db_ != NULL) {
		if (name2.db_ == NULL) {
			return false;
		}
		else if (!isDBOnlyMatched(name1, name2)) {
			return false;
		}
		else {
			return true;
		}
	}

	return true;
}

TupleValue SQLCompiler::makeBoolValue(bool value) {
	return TupleValue(value);
}

bool SQLCompiler::isTrueExpr(const Expr &expr) {
	return expr.op_ == SQLType::EXPR_CONSTANT &&
			SQLProcessor::ValueUtils::isTrueValue(expr.value_);
}

TupleValue::VarContext& SQLCompiler::getVarContext() {
	return varCxt_;
}

const SQLCompiler::CompileOption& SQLCompiler::getCompileOption() {
	if (compileOption_ == NULL) {
		return CompileOption::getEmptyOption();
	}

	return *compileOption_;
}

bool SQLCompiler::isIndexScanEnabledDefault() {
	return true;
}

const SQLTableInfoList& SQLCompiler::getTableInfoList() const {
	if (tableInfoList_ == NULL) {
		assert(false);
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	return *tableInfoList_;
}

SQLCompiler::SubqueryStack& SQLCompiler::getSubqueryStack() {
	return *subqueryStack_;
}

util::AllocUniquePtr<SQLCompiler::SubqueryStack>::ReturnType
SQLCompiler::makeSubqueryStack(
		util::StackAllocator &alloc) {
	util::AllocUniquePtr<SubqueryStack> stack;
	stack = ALLOC_UNIQUE(alloc, SubqueryStack, alloc);
	return stack;
}

util::AllocUniquePtr<SQLCompiler::OptimizationOption>::ReturnType
SQLCompiler::makeOptimizationOption(
		util::StackAllocator &alloc, const OptimizationOption &src) {
	util::AllocUniquePtr<OptimizationOption> option;
	option = ALLOC_UNIQUE(alloc, OptimizationOption, src);
	return option;
}

util::StackAllocator& SQLCompiler::varContextToAllocator(
		TupleValue::VarContext &varCxt) {
	util::StackAllocator *alloc = varCxt.getStackAllocator();

	if (alloc == NULL) {
		assert(false);
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, NULL, "");
	}

	return *alloc;
}

template<typename T> T* SQLCompiler::emptyPtr() {
	typedef T *Ptr;
	return Ptr();
}

const char8_t SQLCompiler::Meta::SYSTEM_NAME_SEPARATOR[] = "#";

const SQLCompiler::Meta::SystemTableInfoList
SQLCompiler::Meta::SYSTEM_TABLE_INFO_LIST;

SQLCompiler::Meta::Meta(SQLCompiler &base) :
		base_(base),
		alloc_(base.alloc_) {
}

bool SQLCompiler::Meta::isSystemTable(const QualifiedName &name) {
	return (strstr(name.table_->c_str(), SYSTEM_NAME_SEPARATOR) != NULL);
}

bool SQLCompiler::Meta::isSystemTable(const char *name) {
	return (strstr(name, SYSTEM_NAME_SEPARATOR) != NULL);
}

const char8_t* SQLCompiler::Meta::getTableNameSystemPart(
		const QualifiedName &name) {
	const char8_t *table = name.table_->c_str();
	if (strstr(table, SYSTEM_NAME_SEPARATOR) != table) {
		return "";
	}
	return table + strlen(SYSTEM_NAME_SEPARATOR);
}

SQLTableInfo::IdInfo SQLCompiler::Meta::setSystemTableColumnInfo(
		util::StackAllocator &alloc, const QualifiedName &tableName,
		SQLTableInfo &tableInfo, bool metaVisible, bool internalMode,
		bool forDriver, bool failOnError) {

	tableInfo.idInfo_ = SQLTableInfo::IdInfo();

	do {
		if (!metaVisible && !forDriver) {
			break;
		}

		SystemTableInfo info;
		if (!getSystemTableInfoList().findInfo(
				tableName, info, internalMode)) {
			break;
		}

		if (info.forDriver_ && !forDriver) {
			break;
		}

		setSystemColumnInfoList(alloc, info, tableInfo.columnInfoList_);

		const bool forCore = true;
		return info.getMetaIdInfo(forCore);
	}
	while (false);

	if (!failOnError) {
		return SQLTableInfo::IdInfo();
	}

	GS_THROW_USER_ERROR(
			GS_ERROR_SQL_COMPILE_TABLE_NOT_FOUND,
			"Unknown system table (name=" << *tableName.table_ << ")");
}

void SQLCompiler::Meta::getCoreMetaTableName(
		util::StackAllocator &alloc, const SQLTableInfo::IdInfo &idInfo,
		QualifiedName &name) {
	const bool forCore = true;
	const MetaType::InfoTable &infoTable = MetaType::InfoTable::getInstance();
	const MetaContainerInfo &metaInfo =
			infoTable.resolveInfo(idInfo.containerId_, forCore);

	util::String *table = ALLOC_NEW(alloc) util::String(alloc);
	table->append(SYSTEM_NAME_SEPARATOR);
	table->append(MetaType::InfoTable::nameOf(
			metaInfo, MetaType::NAMING_NEUTRAL, false));
	name.table_ = table;
}

void SQLCompiler::Meta::getMetaColumnInfo(
		util::StackAllocator &alloc, const SQLTableInfo::IdInfo &idInfo,
		bool forCore, SQLTableInfo::SQLColumnInfoList &columnInfoList,
		int8_t metaNamingType) {
	const MetaType::InfoTable &infoTable = MetaType::InfoTable::getInstance();
	const MetaContainerInfo &metaInfo =
			infoTable.resolveInfo(idInfo.containerId_, forCore);

	MetaType::NamingType namingType;
	if (forCore) {
		namingType = MetaType::NAMING_NEUTRAL;
	}
	else {
		assert(metaNamingType >= 0);
		namingType = static_cast<MetaType::NamingType>(metaNamingType);
	}

	for (size_t i = 0; i < metaInfo.columnCount_; i++) {
		const MetaColumnInfo &columnInfo = metaInfo.columnList_[i];
		ColumnType type;
		ProcessorUtils::toTupleColumnType(
				columnInfo.type_, columnInfo.nullable_, type, true);
		const char8_t *name =
				MetaType::InfoTable::nameOf(columnInfo, namingType, false);

		columnInfoList.push_back(SQLTableInfo::SQLColumnInfo(
				type, util::String(name, alloc)));
	}
}

void SQLCompiler::Meta::genSystemTable(
		const Expr &table, const Expr* &whereExpr, Plan &plan) {
	SystemTableInfo info;

	if (getSystemTableInfoList().findInfo(*table.qName_, info, true)) {
		(this->*(info.tableGenFunc_))(whereExpr, plan, info);
		return;
	}

	GS_THROW_USER_ERROR(
			GS_ERROR_SQL_COMPILE_UNSUPPORTED,
			"Unsupported system table (name=" << *table.qName_->table_ << ")");
}

bool SQLCompiler::Meta::isTableMatched(
		const QualifiedName &qName, const char8_t *name) {
	assert(qName.table_ != NULL);
	return compareStringWithCase(
			qName.table_->c_str(), name, qName.tableCaseSensitive_) == 0;
}

bool SQLCompiler::Meta::findCommonColumnId(
		const Plan &plan, const PlanNode &node,
		SQLTableInfo::Id tableInfoId, const MetaContainerInfo &metaInfo,
		uint32_t &partitionColumn, uint32_t &tableNameColumn,
		uint32_t &tableIdColumn, uint32_t &partitionNameColumn) {
	partitionColumn = std::numeric_limits<uint32_t>::max();
	tableNameColumn = std::numeric_limits<uint32_t>::max();
	tableIdColumn = std::numeric_limits<uint32_t>::max();
	partitionNameColumn = std::numeric_limits<uint32_t>::max();

	const PlanExprList &inExprList = node.outputList_;

	bool found = false;
	for (PlanExprList::const_iterator it = inExprList.begin();
			it != inExprList.end(); ++it) {
		const PlanNode *inNode = &node;
		PlanExprList::const_iterator inIt = it;
		bool onColumn = true;
		while (inNode->type_ != SQLType::EXEC_SCAN) {
			if (inIt->op_ != SQLType::EXPR_COLUMN ||
					inIt->inputId_ >= inNode->inputList_.size()) {
				onColumn = false;
				break;
			}
			inNode = &inNode->getInput(plan, inIt->inputId_);
			inIt = inNode->outputList_.begin() + inIt->columnId_;
		}

		if (!onColumn || inNode->tableIdInfo_.isEmpty() ||
				inNode->tableIdInfo_.id_ != tableInfoId) {
			continue;
		}

		const uint32_t outColumnId = static_cast<uint32_t>(it - inExprList.begin());
		const uint32_t inColumnId = inIt->columnId_;
		if (inColumnId == metaInfo.commonInfo_.containerNameColumn_) {
			tableNameColumn = outColumnId;
			found = true;
		}
		else if (inColumnId == metaInfo.commonInfo_.partitionIndexColumn_) {
			partitionColumn = outColumnId;
		}
		else if (inColumnId == metaInfo.commonInfo_.containerIdColumn_) {
			tableIdColumn = outColumnId;
		}
		else if (inColumnId == metaInfo.commonInfo_.partitionNameColumn_) {
			partitionNameColumn = outColumnId;
			found = true;
		}
		else {
			continue;
		}

		if (partitionColumn != std::numeric_limits<uint32_t>::max() &&
				tableIdColumn != std::numeric_limits<uint32_t>::max()) {
			return true;
		}
	}

	return found;
}

void SQLCompiler::Meta::genDriverClusterInfo(
		const Expr* &whereExpr, Plan &plan, const SystemTableInfo &info) {
	static_cast<void>(whereExpr);
	static_cast<void>(info);

	PlanNode &node = plan.append(SQLType::EXEC_SELECT);

	{
		int32_t major;
		int32_t minor;
		NoSQLCommonUtils::getDatabaseVersion(major, minor);
		node.outputList_.push_back(genConstColumn(
				plan, TupleValue(major), STR_DATABASE_MAJOR_VERSION));
		node.outputList_.push_back(genConstColumn(
				plan, TupleValue(minor), STR_DATABASE_MINOR_VERSION));
	}

	{
		const char8_t *chars = FullContainerKey::getExtendedSymbol();
		node.outputList_.push_back(genConstColumn(
				plan,
				SyntaxTree::makeStringValue(alloc_, chars, strlen(chars)),
				STR_EXTRA_NAME_CHARACTERS));
	}
}

void SQLCompiler::Meta::genMetaTableGeneral(
		const Expr *&whereExpr, Plan &plan, const SystemTableInfo &info) {
	const PlanNode &scanNode = genMetaScan(plan, info);
	PlanNode &projNode = genMetaSelect(plan);
	const PlanExprList &scanOutputList = base_.inputListToOutput(plan, false);

	addDBNamePred(plan, info, scanOutputList, scanNode, projNode);

	if (info.metaId_ == MetaType::TYPE_KEY) {
		addPrimaryKeyPred<MetaType::ColumnMeta>(
				plan, MetaType::COLUMN_KEY, scanOutputList, projNode);
	}
	else if (info.metaId_ == MetaType::TYPE_CONTAINER) {
		addContainerAttrPred<MetaType::ContainerMeta>(
				plan, MetaType::CONTAINER_ATTRIBUTE, scanOutputList, projNode);
	}
	genAllRefColumns(plan, info, scanOutputList, projNode.outputList_);
	genMetaWhere(plan, whereExpr, scanNode);
}

SQLCompiler::PlanNode& SQLCompiler::Meta::genMetaScan(
		Plan &plan, const SystemTableInfo &info) {
	QualifiedName qName(alloc_);

	const bool forCore = true;
	getCoreMetaTableName(alloc_, info.getMetaIdInfo(forCore), qName);

	const SQLTableInfo &tableInfo = base_.resolveTable(qName);

	PlanNode &node = plan.append(SQLType::EXEC_SCAN);
	node.tableIdInfo_ = tableInfo.idInfo_;
	node.outputList_ =
			base_.tableInfoToOutput(plan, tableInfo, MODE_NORMAL_SELECT);

	return node;
}

SQLCompiler::PlanNode& SQLCompiler::Meta::genMetaSelect(
		Plan &plan) {
	const PlanNodeId inputId = plan.back().id_;
	PlanNode &node = plan.append(SQLType::EXEC_SELECT);
	node.inputList_.push_back(inputId);

	return node;
}

SQLCompiler::PlanNode* SQLCompiler::Meta::genMetaWhere(
		Plan &plan, const Expr *&whereExpr, const PlanNode &scanNode) {
	if (whereExpr == NULL) {
		return NULL;
	}

	{
		const PlanNodeId inputId = plan.back().id_;
		PlanNode &node = plan.append(SQLType::EXEC_SELECT);
		node.inputList_.push_back(inputId);
		node.outputList_ = base_.inputListToOutput(plan);
	}

	const Expr& exprWhere = base_.genExpr(*whereExpr, plan, MODE_WHERE);
	whereExpr = NULL; 

	{
		PlanNode &node = plan.back();
		node.predList_.push_back(exprWhere);

		const Plan::ValueList *parameterList = &plan.parameterList_;
		if (parameterList->empty()) {
			parameterList = NULL;
		}

		bool placeholderAffected;
		appendMetaTableInfoId(
				plan, node, exprWhere, scanNode.tableIdInfo_.id_,
				parameterList, placeholderAffected);

		base_.recompileOnBindRequired_ |= placeholderAffected;

		return &node;
	}
}

void SQLCompiler::Meta::addDBNamePred(
		Plan &plan, const SystemTableInfo &info,
		const PlanExprList &scanOutputList,
		const PlanNode &scanNode, PlanNode &projNode) {
	const bool forCore = true;
	const MetaType::InfoTable &infoTable = MetaType::InfoTable::getInstance();
	const MetaContainerInfo &metaInfo =
			infoTable.resolveInfo(info.coreMetaId_, forCore);

	if (metaInfo.commonInfo_.dbNameColumn_ == UNDEF_COLUMNID) return;
	const ColumnId dbNameColumn = metaInfo.commonInfo_.dbNameColumn_;

	const SQLTableInfo &tableInfo =
			base_.tableInfoList_->resolve(scanNode.tableIdInfo_.id_);
	const Expr dbColumExpr =
			genRefColumnById(plan, scanOutputList, dbNameColumn, NULL);
	const Expr dbNameCondExpr = genDBNameCondExpr(
			plan, tableInfo.dbName_, dbColumExpr);

	assert(projNode.predList_.empty());
	projNode.predList_.push_back(dbNameCondExpr);
}

template<typename T>
void SQLCompiler::Meta::addPrimaryKeyPred(
		Plan &plan, T metaColumn, const PlanExprList &scanOutputList,
		PlanNode &projNode) {
	const Expr primaryKeyCondExpr = base_.genOpExpr(
			plan, SQLType::OP_EQ,
			ALLOC_NEW(alloc_) Expr(genRefColumn<T>(
					plan, scanOutputList, metaColumn, END_STR)),
			ALLOC_NEW(alloc_) Expr(base_.genConstExpr(
					plan, makeBoolValue(true), MODE_WHERE)),
			MODE_WHERE, true);

	PlanExprList &predList = projNode.predList_;
	if (predList.empty()) {
		predList.push_back(base_.genConstExpr(
				plan, makeBoolValue(true), MODE_WHERE));
	}

	Expr *mergedExpr;
	base_.mergeAndOp(
			ALLOC_NEW(alloc_) Expr(predList[0]),
			ALLOC_NEW(alloc_) Expr(primaryKeyCondExpr),
			plan, MODE_WHERE, mergedExpr);

	if (mergedExpr == NULL) {
		assert(false);
		return;
	}

	projNode.predList_.front() = *mergedExpr;
}

template<typename T>
void SQLCompiler::Meta::addContainerAttrPred(
		Plan &plan, T metaColumn, const PlanExprList &scanOutputList,
		PlanNode &projNode) {
	const Expr containerAttrCondExpr = base_.genOpExpr(
			plan, SQLType::OP_NE,
			ALLOC_NEW(alloc_) Expr(genRefColumn<T>(
					plan, scanOutputList, metaColumn, END_STR)),
			ALLOC_NEW(alloc_) Expr(base_.genConstExpr(
					plan, TupleValue(static_cast<int32_t>(CONTAINER_ATTR_VIEW)), MODE_WHERE)),
			MODE_WHERE, true);

	PlanExprList &predList = projNode.predList_;
	if (predList.empty()) {
		predList.push_back(base_.genConstExpr(
				plan, makeBoolValue(true), MODE_WHERE));
	}

	Expr *mergedExpr;
	base_.mergeAndOp(
			ALLOC_NEW(alloc_) Expr(predList[0]),
			ALLOC_NEW(alloc_) Expr(containerAttrCondExpr),
			plan, MODE_WHERE, mergedExpr);

	if (mergedExpr == NULL) {
		assert(false);
		return;
	}

	projNode.predList_.front() = *mergedExpr;
}

void SQLCompiler::Meta::setSystemColumnInfoList(
		util::StackAllocator &alloc, const SystemTableInfo &info,
		SQLTableInfo::SQLColumnInfoList &columnInfoList) {
	if (info.metaId_ == UNDEF_CONTAINERID) {
		const ColumnDefinition *it = info.columnList_;
		const ColumnDefinition *const end = it + info.columnCount_;
		for (; it != end; ++it) {
			columnInfoList.push_back(SQLTableInfo::SQLColumnInfo(
					it->first, util::String(str(it->second), alloc)));
		}
	}
	else {
		const bool forCore = false;
		getMetaColumnInfo(
				alloc, info.getMetaIdInfo(forCore), forCore, columnInfoList,
				info.metaNamingType_);
	}
}

void SQLCompiler::Meta::genAllRefColumns(
		const Plan &plan, const SystemTableInfo &info,
		const PlanExprList &scanOutputList, PlanExprList &destOutputList) {
	const bool forCore = false;
	const MetaType::InfoTable &infoTable = MetaType::InfoTable::getInstance();
	const MetaContainerInfo &metaInfo =
			infoTable.resolveInfo(info.metaId_, forCore);

	assert(info.metaNamingType_ >= 0);
	const MetaType::NamingType namingType =
			static_cast<MetaType::NamingType>(info.metaNamingType_);

	const MetaColumnInfo *it = metaInfo.columnList_;
	const MetaColumnInfo *end = it + metaInfo.columnCount_;
	for (; it != end; ++it) {
		const char8_t *name =
				MetaType::InfoTable::nameOf(*it, namingType, false);
		destOutputList.push_back(genRefColumnById(
				plan, scanOutputList, it->refId_, name));
	}
}

template<typename T>
SQLCompiler::Expr SQLCompiler::Meta::genRefColumn(
		const Plan &plan, const PlanExprList &scanOutputList,
		T metaColumn, StringConstant name) {
	return genRefColumnById(
			plan, scanOutputList, metaColumn,
			(name == END_STR ? NULL : str(name)));
}

SQLCompiler::Expr SQLCompiler::Meta::genRefColumnById(
		const Plan &plan, const PlanExprList &scanOutputList,
		ColumnId metaColumnId, const char8_t *name) {
	const uint32_t inputId = 0;
	const ColumnId columnId = scanOutputList[metaColumnId].columnId_;

	Expr expr = base_.genColumnExpr(plan, inputId, columnId);

	if (name != NULL) {
		assert(expr.qName_ != NULL);
		expr.qName_->name_ = ALLOC_NEW(alloc_) util::String(name, alloc_);
	}

	return expr;
}

SQLCompiler::Expr SQLCompiler::Meta::genEmptyColumn(
		const Plan &plan, ColumnType baseType, StringConstant name) {
	Expr expr = genConstColumn(plan, TupleValue(), name);
	assert(expr.columnType_ == TupleList::TYPE_ANY);
	expr.columnType_ = setColumnTypeNullable(baseType, true);
	return expr;
}

SQLCompiler::Expr SQLCompiler::Meta::genConstColumn(
		const Plan &plan, const TupleValue &value, StringConstant name) {
	const Mode mode = MODE_NORMAL_SELECT;
	return base_.genConstExpr(plan, value, mode, str(name));
}

SQLCompiler::QualifiedName* SQLCompiler::Meta::genColumnName(
		StringConstant name) {
	QualifiedName *qName = ALLOC_NEW(alloc_) QualifiedName(alloc_);
	qName->name_ = ALLOC_NEW(alloc_) util::String(str(name), alloc_);
	return qName;
}

TupleValue SQLCompiler::Meta::genStr(StringConstant constant) {
	const char8_t *value = str(constant);
	return SyntaxTree::makeStringValue(alloc_, value, strlen(value));
}

SQLCompiler::Meta::ColumnDefinition SQLCompiler::Meta::defineColumn(
		ColumnType type, StringConstant constant) {
	return ColumnDefinition(type, constant);
}

const char8_t* SQLCompiler::Meta::str(StringConstant constant) {
	const char8_t *value = CONSTANT_CODER(constant);
	if (value == NULL) {
		assert(false);
		return "";
	}
	return value;
}

const SQLCompiler::Meta::SystemTableInfoList&
SQLCompiler::Meta::getSystemTableInfoList() {
	return SYSTEM_TABLE_INFO_LIST;
}

SQLCompiler::Meta::SystemTableInfo::SystemTableInfo() :
		type_(END_SYSTEM_TABLE),
		name_(NULL),
		columnList_(NULL),
		columnCount_(0),
		metaId_(UNDEF_CONTAINERID),
		coreMetaId_(UNDEF_CONTAINERID),
		tableGenFunc_(NULL),
		forDriver_(false),
		metaNamingType_(-1) {
}

SQLTableInfo::IdInfo
SQLCompiler::Meta::SystemTableInfo::getMetaIdInfo(bool forCore) const {
	SQLTableInfo::IdInfo idInfo;
	if (forCore) {
		idInfo.containerId_ = coreMetaId_;
	}
	else {
		idInfo.containerId_ = metaId_;
	}
	idInfo.type_ = SQLType::TABLE_META;
	return idInfo;
}

SQLCompiler::Meta::SystemTableInfoList::SystemTableInfoList() {
	addDriverInfo(
			DRIVER_TABLES,
			DRIVER_TABLES_NAME,
			DRIVER_TABLES_COLUMN_LIST,
			MetaType::TYPE_CONTAINER, &Meta::genDriverTables);
	addDriverInfo(
			DRIVER_COLUMNS,
			DRIVER_COLUMNS_NAME,
			DRIVER_COLUMNS_COLUMN_LIST,
			MetaType::TYPE_COLUMN, &Meta::genDriverColumns);
	addDriverInfo(
			DRIVER_PRIMARY_KEYS,
			DRIVER_PRIMARY_KEYS_NAME,
			DRIVER_PRIMARY_KEYS_COLUMN_LIST,
			MetaType::TYPE_COLUMN, &Meta::genDriverPrimaryKeys);
	addDriverInfo(
			DRIVER_TYPE_INFO,
			DRIVER_TYPE_INFO_NAME,
			DRIVER_TYPE_INFO_COLUMN_LIST,
			MetaType::END_TYPE, &Meta::genDriverTypeInfo);
	addDriverInfo(
			DRIVER_INDEX_INFO,
			DRIVER_INDEX_INFO_NAME,
			DRIVER_INDEX_INFO_COLUMN_LIST,
			MetaType::TYPE_INDEX, &Meta::genDriverIndexInfo);
	addDriverInfo(
			DRIVER_CLUSTER_INFO,
			DRIVER_CLUSTER_INFO_NAME,
			DRIVER_CLUSTER_INFO_COLUMN_LIST,
			MetaType::END_TYPE, &Meta::genDriverClusterInfo);
	addDriverInfo(
			DRIVER_TABLE_TYPES,
			DRIVER_TABLE_TYPES_NAME,
			DRIVER_TABLE_TYPES_COLUMN_LIST,
			MetaType::END_TYPE, &Meta::genDriverTableTypes);}

bool SQLCompiler::Meta::SystemTableInfoList::findInfo(
		const QualifiedName &name, SystemTableInfo &info,
		bool internalMode) const {
	info = SystemTableInfo();

	const SystemTableInfo *it = entryList_;
	const SystemTableInfo *end = it + END_SYSTEM_TABLE;
	for (; it != end; ++it) {
		if (isTableMatched(name, it->name_)) {
			info = *it;
			return true;
		}
	}

	const bool forCore = false;
	const MetaType::InfoTable &infoTable = MetaType::InfoTable::getInstance();
	const char8_t *tableSystemPart = getTableNameSystemPart(name);
	MetaType::NamingType namingType;
	const MetaContainerInfo *metaInfo =
			infoTable.findInfo(tableSystemPart, forCore, namingType);
	if (metaInfo != NULL) {
		const char8_t *metaName =
				MetaType::InfoTable::nameOf(*metaInfo, namingType, false);
		if (name.tableCaseSensitive_ &&
				strcmp(metaName, tableSystemPart) != 0) {
			return false;
		}
		if (metaInfo->internal_ && !internalMode) {
			return false;
		}
		info.metaId_ = metaInfo->id_;
		info.coreMetaId_ = metaInfo->refId_;
		info.tableGenFunc_ = &Meta::genMetaTableGeneral;
		info.metaNamingType_ = MetaType::InfoTable::resolveNaming(
				namingType, MetaType::NAMING_TABLE);
		return true;
	}

	return false;
}

template<typename T, size_t N>
void SQLCompiler::Meta::SystemTableInfoList::addDriverInfo(
		SystemTableType type, const char8_t *name,
		const ColumnDefinition (&columnList)[N], T coreMetaType,
		TableGenFunc tableGenFunc) {
	const bool forDriver = true;
	addInfo(type, name, columnList, coreMetaType, tableGenFunc, forDriver);
}

template<typename T, size_t N>
void SQLCompiler::Meta::SystemTableInfoList::addInfo(
		SystemTableType type, const char8_t *name,
		const ColumnDefinition (&columnList)[N], T coreMetaType,
		TableGenFunc tableGenFunc, bool forDriver) {
	assert(0 <= type && type < END_SYSTEM_TABLE);
	UTIL_STATIC_ASSERT((util::IsSame<T, MetaType::MetaContainerType>::VALUE));

	SystemTableInfo &info = entryList_[type];
	assert(info.type_ == END_SYSTEM_TABLE);

	info.type_ = type;
	info.name_ = name;
	info.columnList_ = columnList;
	info.columnCount_ = N;
	info.metaId_ = UNDEF_CONTAINERID;
	info.coreMetaId_ = (coreMetaType == MetaType::END_TYPE ?
			UNDEF_CONTAINERID : static_cast<ContainerId>(coreMetaType));
	info.tableGenFunc_ = tableGenFunc;
	info.forDriver_ = forDriver;
}

SQLCompiler::SubqueryStack::SubqueryStack(util::StackAllocator &alloc) :
		base_(alloc), scopeLevel_(0) {
}

ColumnId SQLCompiler::SubqueryStack::addExpr(const Expr &inExpr, Mode mode) {
	switch (mode) {
	case MODE_NORMAL_SELECT:
	case MODE_AGGR_SELECT:
	case MODE_GROUP_SELECT:
	case MODE_ON:
	case MODE_WHERE:
	case MODE_HAVING:
		break;
	default:
		SQL_COMPILER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, &inExpr,
				"Illegal subquery occurrence");
	}

	assert(scopeLevel_ > 0);

	const ColumnId columnId = static_cast<ColumnId>(base_.size());
	base_.push_back(&inExpr);

	return columnId;
}

SQLCompiler::SubqueryStack::Scope::Scope(SQLCompiler &base) :
		base_(base),
		stack_(*base_.subqueryStack_),
		pos_(stack_.base_.size()) {
	++stack_.scopeLevel_;
}

SQLCompiler::SubqueryStack::Scope::~Scope() {
	while (stack_.base_.size() > pos_) {
		stack_.base_.pop_back();
	}
	--stack_.scopeLevel_;
}

void SQLCompiler::SubqueryStack::Scope::applyBefore(
		const Plan &plan, Expr &expr) {
	assert(hasSubquery());
	assert(plan.back().tableIdInfo_.isEmpty());

	const PlanNode &node = plan.back();

	OffsetList offsetList(*stack_.base_.get_allocator().base());
	ColumnId last = 0;
	for (PlanNode::IdList::const_iterator it = node.inputList_.begin();
			it != node.inputList_.end(); ++it) {
		offsetList.push_back(last);
		last += static_cast<ColumnId>(plan.nodeList_[*it].outputList_.size());
	}
	offsetList.push_back(last);

	applyBefore(offsetList, expr);
}

void SQLCompiler::SubqueryStack::Scope::applyAfter(
		const Plan &plan, Expr &expr, Mode mode, const bool aggregated) {
	assert(!hasSubquery());
	assert(plan.back().inputList_.size() == 1);
	assert(plan.back().tableIdInfo_.isEmpty());

	const bool subAggregated = (aggregated &&
			!(SQLType::START_AGG < expr.op_ && expr.op_ <= SQLType::END_AGG));

	if (expr.left_ != NULL) {
		applyAfter(plan, *expr.left_, mode, subAggregated);
	}

	if (expr.right_ != NULL) {
		applyAfter(plan, *expr.right_, mode, subAggregated);
	}

	if (expr.next_ != NULL) {
		for (ExprList::iterator it = expr.next_->begin();
				it != expr.next_->end(); ++it) {
			assert(*it != NULL);
			applyAfter(plan, **it, mode, subAggregated);
		}
	}

	if (expr.op_ == SQLType::EXPR_COLUMN) {
		const bool autoGenerated = expr.autoGenerated_;
		QualifiedName *qName = expr.qName_;

		expr = base_.genColumnExpr(plan, expr.inputId_, expr.columnId_);

		if (qName != NULL) {
			expr.qName_ = qName;
		}
		expr.autoGenerated_ = autoGenerated;
		setUpColumnExprAttributes(expr, aggregated);
	}
	else {
		expr.columnType_ =
				getResultType(expr, NULL, TupleList::TYPE_NULL, mode);
	}
}

bool SQLCompiler::SubqueryStack::Scope::hasSubquery() const {
	return stack_.base_.size() > pos_;
}

const SQLCompiler::Expr* SQLCompiler::SubqueryStack::Scope::takeExpr() {
	if (!hasSubquery()) {
		return NULL;
	}

	Base::iterator it = stack_.base_.begin() + pos_;
	const Expr *expr = *it;
	stack_.base_.erase(it);

	return expr;
}

void SQLCompiler::SubqueryStack::Scope::applyBefore(
		const OffsetList &offsetList, Expr &expr) {

	if (expr.left_ != NULL) {
		applyBefore(offsetList, *expr.left_);
	}

	if (expr.right_ != NULL) {
		applyBefore(offsetList, *expr.right_);
	}

	if (expr.next_ != NULL) {
		for (ExprList::iterator it = expr.next_->begin();
				it != expr.next_->end(); ++it) {
			assert(*it != NULL);
			applyBefore(offsetList, **it);
		}
	}

	if (expr.op_ == SQLType::EXPR_COLUMN) {
		if (expr.inputId_ == SyntaxTree::UNDEF_INPUTID) {
			assert(expr.columnId_ >= pos_);
			expr.columnId_ = static_cast<ColumnId>(
					(offsetList.size() > 1 ? offsetList[1] : offsetList[0]) +
					(expr.columnId_ - pos_));
			expr.inputId_ = 0;
		}
		else {
			assert(expr.inputId_ < offsetList.size() - 1);
			assert(expr.columnId_ <
					offsetList[expr.inputId_ + 1] - offsetList[expr.inputId_]);
		}
	}
}

template<bool Const>
SQLCompiler::ExprArgsIterator<Const>::ExprArgsIterator(Ref expr) :
		expr_(expr),
		next_(NULL),
		phase_(0) {
	next();
}

template<bool Const>
bool SQLCompiler::ExprArgsIterator<Const>::exists() const {
	return next_ != NULL;
}

template<bool Const>
typename SQLCompiler::ExprArgsIterator<Const>::Ref
SQLCompiler::ExprArgsIterator<Const>::get() const {
	assert(exists());
	return *next_;
}

template<bool Const>
void SQLCompiler::ExprArgsIterator<Const>::next() {
	for (;;) {
		switch (phase_) {
		case 0:
			phase_++;
			if (expr_.left_ == NULL) {
				break;
			}
			next_ = expr_.left_;
			return;
		case 1:
			phase_++;
			if (expr_.right_ == NULL) {
				break;
			}
			next_ = expr_.right_;
			return;
		case 2:
			phase_++;
			if (expr_.next_ == NULL || expr_.next_->empty()) {
				phase_++;
				break;
			}
			it_ = expr_.next_->begin();
			break;
		case 3:
			if (it_ == expr_.next_->end()) {
				phase_++;
				break;
			}
			next_ = *it_;
			assert(exists());
			++it_;
			return;
		default:
			next_ = NULL;
			return;
		}
	}
}

SQLCompiler::PlanNodeRef::PlanNodeRef() :
		plan_(NULL),
		nodeId_(UNDEF_PLAN_NODEID) {
}

SQLCompiler::PlanNodeRef::PlanNodeRef(Plan &plan, const PlanNode *node) :
		plan_(&plan),
		nodeId_(toNodeId(plan, node)) {
}

SQLCompiler::Plan* SQLCompiler::PlanNodeRef::getPlan() const {
	return plan_;
}

SQLCompiler::PlanNode& SQLCompiler::PlanNodeRef::operator*() const {
	if (plan_ == NULL || nodeId_ >= plan_->nodeList_.size()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}

	return plan_->nodeList_[nodeId_];
}

SQLCompiler::PlanNode* SQLCompiler::PlanNodeRef::operator->() const {
	return &(**this);
}

SQLCompiler::PlanNode* SQLCompiler::PlanNodeRef::get() const {
	if (plan_ == NULL) {
		return NULL;
	}

	return &(**this);
}

SQLCompiler::PlanNode::Id SQLCompiler::PlanNodeRef::toNodeId(
		const Plan &plan, const PlanNode *node) {

	if (plan.nodeList_.empty() || (node != NULL &&
			(node < &plan.nodeList_.front() ||
			node > &plan.nodeList_.back()))) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}

	const PlanNode &target = (node == NULL ? plan.back() : *node);
	return static_cast<PlanNode::Id>(&target - &plan.nodeList_.front());
}

SQLCompiler::ExprRef::ExprRef() :
		compiler_(NULL),
		nodeRef_(),
		columnId_(UNDEF_COLUMNID),
		dupExpr_(NULL) {
}

SQLCompiler::ExprRef::ExprRef(
		SQLCompiler &compiler, Plan &plan, const Expr &expr,
		const PlanNode *node) :
		compiler_(&compiler),
		nodeRef_(plan, node),
		columnId_(toColumnId(*nodeRef_, expr)),
		dupExpr_(ALLOC_NEW(compiler.alloc_) Expr(expr)) {
}

SQLCompiler::Expr& SQLCompiler::ExprRef::operator*() const {
	PlanNode &refNode = *nodeRef_;

	if (compiler_ == NULL ||
			columnId_ >= refNode.outputList_.size() ||
			!SubPlanEq()(refNode.outputList_[columnId_], *dupExpr_)) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}

	return refNode.outputList_[columnId_];
}

SQLCompiler::Expr* SQLCompiler::ExprRef::operator->() const {
	return &(**this);
}

SQLCompiler::Expr* SQLCompiler::ExprRef::get() const {
	if (compiler_ == NULL) {
		return NULL;
	}

	return &(**this);
}

SQLCompiler::Expr SQLCompiler::ExprRef::generate(const PlanNode *node) const {
	*(*this);

	const Plan *plan = nodeRef_.getPlan();
	const PlanNode &lastNode = (node == NULL ? plan->back() : *node);

	uint32_t inputId;
	ColumnId columnId;
	followColumn(lastNode, inputId, columnId, 0);

	return compiler_->genColumnExprBase(
			plan, &lastNode, inputId, columnId, NULL);
}

void SQLCompiler::ExprRef::update(const PlanNode *node) {
	*(*this);

	Plan *plan = nodeRef_.getPlan();
	const PlanNode &lastNode = (node == NULL ? plan->back() : *node);

	if (&lastNode == &(*nodeRef_)) {
		return;
	}

	uint32_t inputId;
	ColumnId columnId;
	const Expr *expr = followColumn(lastNode, inputId, columnId, 1);
	assert(expr != NULL);

	nodeRef_ = PlanNodeRef(*plan, &lastNode);
	columnId_ = static_cast<ColumnId>(expr - &lastNode.outputList_.front());
	dupExpr_ = ALLOC_NEW(compiler_->alloc_) Expr(*expr);
}

ColumnId SQLCompiler::ExprRef::toColumnId(
		const PlanNode &node, const Expr &expr) {
	const PlanExprList &exprList = node.outputList_;

	if (exprList.empty() ||
			&expr < &exprList.front() || &expr > &exprList.back()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}

	return static_cast<ColumnId>(&expr - &exprList.front());
}

const SQLCompiler::Expr* SQLCompiler::ExprRef::followColumn(
		const PlanNode &node, uint32_t &inputId, ColumnId &columnId,
		size_t depth) const {
	inputId = std::numeric_limits<uint32_t>::max();
	columnId = UNDEF_COLUMNID;

	if (&node == &(*nodeRef_)) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}

	const Plan *plan = nodeRef_.getPlan();
	if (depth > plan->nodeList_.size()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}

	const PlanNode *inNode = NULL;
	const Expr *inExpr = NULL;
	{
		if (node.inputList_.empty()) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
		}

		PlanNode::IdList::const_iterator it = node.inputList_.begin();
		for (; it != node.inputList_.end(); ++it) {
			if (&plan->nodeList_[*it] == &(*nodeRef_)) {
				break;
			}
		}

		if (it == node.inputList_.end()) {
			uint32_t subInputId;
			ColumnId subColumnId;

			inputId = 0;
			inNode = &node.getInput(*plan, inputId);
			inExpr = followColumn(*inNode, subInputId, subColumnId, depth + 1);
			columnId = static_cast<ColumnId>(
					inExpr - &inNode->outputList_.front());
		}
		else {
			inputId = static_cast<uint32_t>(it - node.inputList_.begin());
			inNode = &(*nodeRef_);
			inExpr = &inNode->outputList_[columnId_];
			columnId = columnId_;
		}
	}

	if (inExpr == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}

	if (depth == 0) {
		return NULL;
	}

	const PlanExprList &outList = node.outputList_;
	if (outList.empty() || inNode->outputList_.empty()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}

	const uint32_t checkinInputId =
			(node.type_ == SQLType::EXEC_UNION ? 0 : inputId);

	const ColumnId boundColumnId = static_cast<ColumnId>(std::min(
			static_cast<ColumnId>(inExpr - &inNode->outputList_.front()),
			static_cast<ColumnId>(outList.size() - 1)));
	PlanExprList::const_iterator bound = outList.begin() + boundColumnId;

	for (PlanExprList::const_iterator it = bound;;) {
		if (it->op_ == SQLType::EXPR_COLUMN &&
				&inNode->outputList_[it->columnId_] == inExpr &&
				it->inputId_ == checkinInputId) {
			return &(*it);
		}

		if (++it == outList.end()) {
			it = outList.begin();
		}

		if (it == bound) {
			break;
		}
	}

	assert(false);
	GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
}

SQLCompiler::ExprHint::ExprHint() :
		noIndex_(false) {
}

bool SQLCompiler::ExprHint::isNoIndex() const {
	return noIndex_;
}

void SQLCompiler::ExprHint::setNoIndex(bool enabled) {
	noIndex_ = enabled;
}

SQLCompiler::QualifiedNameFormatter
SQLCompiler::QualifiedNameFormatter::asColumn(const QualifiedName &qName) {
	return QualifiedNameFormatter(qName, true);
}

SQLCompiler::QualifiedNameFormatter
SQLCompiler::QualifiedNameFormatter::asTable(const QualifiedName &qName) {
	return QualifiedNameFormatter(qName, false);
}

void SQLCompiler::QualifiedNameFormatter::format(std::ostream &os) const {
	if (qName_.db_ != NULL) {
		os << *qName_.db_;
	}

	if (qName_.table_ != NULL) {
		if (qName_.db_ != NULL) {
			os << ".";
		}

		os << *qName_.table_;
	}

	if (forColumn_) {
		if (qName_.table_ != NULL) {
			os << ".";
		}

		if (qName_.name_ != NULL) {
			os << *qName_.name_;
		}
		else {
			os << "*";
		}
	}
}

SQLCompiler::QualifiedNameFormatter::QualifiedNameFormatter(
		const QualifiedName &qName, bool forColumn) :
		qName_(qName),
		forColumn_(forColumn) {
}

std::ostream& operator<<(
		std::ostream &os,
		const SQLCompiler::QualifiedNameFormatter &formatter) {
	formatter.format(os);
	return os;
}

uint32_t SQLCompiler::SubPlanHasher::operator()(
		const PlanNode &value, const uint32_t *base) const {
	uint32_t hash;
	hash = (*this)(value.type_, base);
	hash = (*this)(value.qName_, &hash);
	hash = (*this)(value.inputList_, &hash);
	hash = (*this)(value.predList_, &hash);
	hash = (*this)(value.outputList_, &hash);
	hash = (*this)(value.tableIdInfo_, &hash);
	hash = (*this)(value.offset_, &hash);
	hash = (*this)(value.limit_, &hash);
	hash = (*this)(value.subLimit_, &hash);
	hash = (*this)(value.aggPhase_, &hash);
	hash = (*this)(value.joinType_, &hash);
	hash = (*this)(value.unionType_, &hash);
	return hash;
}

uint32_t SQLCompiler::SubPlanHasher::operator()(
		const Expr &value, const uint32_t *base) const {
	uint32_t hash;
	hash = (*this)(value.op_, base);
	hash = (*this)(value.qName_, &hash);
	hash = (*this)(value.aliasName_, &hash);
	hash = (*this)(value.columnType_, &hash);
	hash = (*this)(value.value_, &hash);
	hash = (*this)(value.left_, &hash);
	hash = (*this)(value.right_, &hash);
	hash = (*this)(value.mid_, &hash);
	hash = (*this)(value.next_, &hash);
	hash = (*this)(value.sortAscending_, &hash);
	hash = (*this)(value.placeHolderCount_, &hash);
	hash = (*this)(value.inputId_, &hash);
	hash = (*this)(value.columnId_, &hash);
	hash = (*this)(value.subqueryLevel_, &hash);
	hash = (*this)(value.aggrNestLevel_, &hash);
	hash = (*this)(value.constantEvaluable_, &hash);
	hash = (*this)(value.autoGenerated_, &hash);
	return hash;
}

uint32_t SQLCompiler::SubPlanHasher::operator()(
		const SQLTableInfo::IdInfo &value,
		const uint32_t *base) const {
	uint32_t hash;
	hash = (*this)(value.id_, base);
	hash = (*this)(value.type_, &hash);
	hash = (*this)(value.dbId_, &hash);
	hash = (*this)(value.partitionId_, &hash);
	hash = (*this)(value.containerId_, &hash);
	hash = (*this)(value.schemaVersionId_, &hash);
	return hash;
}

uint32_t SQLCompiler::SubPlanHasher::operator()(
		const QualifiedName &value, const uint32_t *base) const {
	uint32_t hash;
	hash = (*this)(value.db_, base);
	hash = (*this)(value.table_, &hash);
	hash = (*this)(value.name_, &hash);
	return hash;
}

uint32_t SQLCompiler::SubPlanHasher::operator()(
		const TupleValue &value, const uint32_t *base) const {
	return SQLProcessor::ValueUtils::hashValue(value, base);
}

uint32_t SQLCompiler::SubPlanHasher::operator()(
		const util::String &value, const uint32_t *base) const {
	return SQLProcessor::ValueUtils::hashString(value.c_str(), base);
}

uint32_t SQLCompiler::SubPlanHasher::operator()(
		int64_t value, const uint32_t *base) const {
	return SQLProcessor::ValueUtils::hashValue(TupleValue(value), base);
}

template<typename T>
uint32_t SQLCompiler::SubPlanHasher::operator()(
		const T *value, const uint32_t *base) const {
	if (value == NULL) {
		return (*this)(0, base);
	}

	return (*this)(*value, base);
}

template<typename T, typename Alloc>
uint32_t SQLCompiler::SubPlanHasher::operator()(
			const std::vector<T, Alloc> &value, const uint32_t *base) const {
	uint32_t hash = (*this)(static_cast<int64_t>(value.size()), base);

	for (typename std::vector<T, Alloc>::const_iterator it = value.begin();
			it != value.end(); ++it) {
		hash = (*this)(*it, &hash);
	}

	return hash;
}

template<typename T>
uint32_t SQLCompiler::SubPlanHasher::operator()(
		const T &value,
		const typename util::EnableIf<
				IsUInt<T>::VALUE, uint32_t>::Type *base) const {
	return (*this)(
			TupleValue(static_cast<int64_t>(value)), base);
}

bool SQLCompiler::SubPlanEq::operator()(
		const PlanNode &value1, const PlanNode &value2) const {
	return (*this)(value1.type_, value2.type_) &&
			(*this)(value1.qName_, value2.qName_) &&
			(*this)(value1.inputList_, value2.inputList_) &&
			(*this)(value1.predList_, value2.predList_) &&
			(*this)(value1.outputList_, value2.outputList_) &&
			(*this)(value1.tableIdInfo_, value2.tableIdInfo_) &&
			(*this)(value1.offset_, value2.offset_) &&
			(*this)(value1.limit_, value2.limit_) &&
			(*this)(value1.subLimit_, value2.subLimit_) &&
			(*this)(value1.aggPhase_, value2.aggPhase_) &&
			(*this)(value1.joinType_, value2.joinType_) &&
			(*this)(value1.unionType_, value2.unionType_);
}

bool SQLCompiler::SubPlanEq::operator()(
		const Expr &value1, const Expr &value2) const {
	return (*this)(value1.op_, value2.op_) &&
			(*this)(value1.qName_, value2.qName_) &&
			(*this)(value1.aliasName_, value2.aliasName_) &&
			(*this)(value1.columnType_, value2.columnType_) &&
			(*this)(value1.value_, value2.value_) &&
			(*this)(value1.left_, value2.left_) &&
			(*this)(value1.right_, value2.right_) &&
			(*this)(value1.mid_, value2.mid_) &&
			(*this)(value1.next_, value2.next_) &&
			(*this)(value1.sortAscending_, value2.sortAscending_) &&
			(*this)(value1.placeHolderCount_, value2.placeHolderCount_) &&
			(*this)(value1.inputId_, value2.inputId_) &&
			(*this)(value1.columnId_, value2.columnId_) &&
			(*this)(value1.subqueryLevel_, value2.subqueryLevel_) &&
			(*this)(value1.aggrNestLevel_, value2.aggrNestLevel_) &&
			(*this)(value1.constantEvaluable_, value2.constantEvaluable_) &&
			(*this)(value1.autoGenerated_, value2.autoGenerated_);
}

bool SQLCompiler::SubPlanEq::operator()(
		const SQLTableInfo::IdInfo &value1,
		const SQLTableInfo::IdInfo &value2) const {
	return (*this)(value1.id_, value2.id_) &&
			(*this)(value1.type_, value2.type_) &&
			(*this)(value1.dbId_, value2.dbId_) &&
			(*this)(value1.partitionId_, value2.partitionId_) &&
			(*this)(value1.containerId_, value2.containerId_) &&
			(*this)(value1.schemaVersionId_, value2.schemaVersionId_);
}

bool SQLCompiler::SubPlanEq::operator()(
		const QualifiedName &value1, const QualifiedName &value2) const {
	return (*this)(value1.db_, value2.db_) &&
			(*this)(value1.table_, value2.table_) &&
			(*this)(value1.name_, value2.name_);
}

bool SQLCompiler::SubPlanEq::operator()(
		const TupleValue &value1, const TupleValue &value2) const {
	return SQLProcessor::ValueUtils::orderValue(value1, value2) == 0;
}

bool SQLCompiler::SubPlanEq::operator()(
		const util::String &value1, const util::String &value2) const {
	return value1 == value2;
}

bool SQLCompiler::SubPlanEq::operator()(
		int64_t value1, int64_t value2) const {
	return value1 == value2;
}

template<typename T>
bool SQLCompiler::SubPlanEq::operator()(
		const T *value1, const T *value2) const {
	if (value1 == NULL || value2 == NULL) {
		return value1 == value2;
	}

	return (*this)(*value1, *value2);
}

template<typename T, typename Alloc>
bool SQLCompiler::SubPlanEq::operator()(
		const std::vector<T, Alloc> &value1,
		const std::vector<T, Alloc> &value2) const {
	if (value1.size() != value2.size()) {
		return false;
	}

	typename std::vector<T, Alloc>::const_iterator it1 = value1.begin();
	typename std::vector<T, Alloc>::const_iterator it2 = value2.begin();
	for (; it1 != value1.end(); ++it1, ++it2) {
		if (!(*this)(*it1, *it2)) {
			return false;
		}
	}

	return true;
}

template<typename T>
bool SQLCompiler::SubPlanEq::operator()(
		const T &value1,
		const typename util::EnableIf<
				IsUInt<T>::VALUE, T>::Type &value2) const {
	return value1 == value2;
}

SQLCompiler::NarrowingKey::NarrowingKey() :
		longRange_(getRangeFull()),
		hashIndex_(0),
		hashCount_(0) {
}

bool SQLCompiler::NarrowingKey::overlapsWith(
		const NarrowingKey &another) const {
	if (isRangeNone() || another.isRangeNone()) {
		return false;
	}

	if (!overlapsWithRange(another)) {
		return false;
	}

	if (!overlapsWithHash(another)) {
		return false;
	}

	return true;
}

void SQLCompiler::NarrowingKey::mergeWith(const NarrowingKey &another) {
	if (isNone()) {
		*this = another;
	}
	else if (!another.isNone()) {
		longRange_ = std::make_pair(
				std::min(longRange_.first, another.longRange_.first),
				std::max(longRange_.second, another.longRange_.second));

		if (hashIndex_ != another.hashIndex_ ||
				hashCount_ != another.hashCount_) {
			setHash(0, 0);
		}
	}
}

bool SQLCompiler::NarrowingKey::isFull() const {
	return isRangeFull() && isHashFull();
}

bool SQLCompiler::NarrowingKey::isNone() const {
	return isRangeNone();
}

void SQLCompiler::NarrowingKey::setNone() {
	*this = NarrowingKey();
	longRange_ = getRangeNone();
}

void SQLCompiler::NarrowingKey::setRange(const LongRange &longRange) {
	longRange_ = longRange;
	assert(!isRangeNone() || longRange_ == getRangeNone());
}

void SQLCompiler::NarrowingKey::setHash(
		uint32_t hashIndex, uint32_t hashCount) {
	assert((hashIndex < hashCount && !isRangeNone()) ||
			(hashIndex == 0 && hashCount == 0));

	hashIndex_ = hashIndex;
	hashCount_ = hashCount;
}

SQLCompiler::NarrowingKey::LongRange
SQLCompiler::NarrowingKey::getRangeFull() {
	return LongRange(
			std::numeric_limits<LongRange::first_type>::min(),
			std::numeric_limits<LongRange::second_type>::max());
}

SQLCompiler::NarrowingKey::LongRange 
SQLCompiler::NarrowingKey::getRangeNone() {
	return LongRange(0, -1);
}

bool SQLCompiler::NarrowingKey::isRangeFull() const {
	return longRange_ == getRangeFull();
}

bool SQLCompiler::NarrowingKey::isRangeNone() const {
	return longRange_.first > longRange_.second;
}

bool SQLCompiler::NarrowingKey::isHashFull() const {
	return hashCount_ == 0;
}

bool SQLCompiler::NarrowingKey::overlapsWithRange(
		const NarrowingKey &another) const {
	assert(!isRangeNone() && !another.isRangeNone());

	if (longRange_.first > another.longRange_.second ||
			longRange_.second < another.longRange_.first) {
		return false;
	}

	return true;
}

bool SQLCompiler::NarrowingKey::overlapsWithHash(
		const NarrowingKey &another) const {
	assert(!isRangeNone() && !another.isRangeNone());

	if (hashIndex_ != another.hashIndex_ &&
			hashCount_ == another.hashCount_) {
		return false;
	}

	return true;
}

SQLCompiler::NarrowingKeyTable::NarrowingKeyTable(
		util::StackAllocator &alloc) :
		alloc_(alloc),
		columnList_(alloc),
		size_(0),
		indexBuilt_(false),
		columnScoreList_(alloc),
		visitedList_(alloc) {
}

uint32_t SQLCompiler::NarrowingKeyTable::getSize() const {
	return size_;
}

bool SQLCompiler::NarrowingKeyTable::isNone() const {
	for (ColumnList::const_iterator it = columnList_.begin();
			it != columnList_.end(); ++it) {
		if (isNoneColumn(*it)) {
			return true;
		}
	}
	return false;
}

void SQLCompiler::NarrowingKeyTable::addKey(
		uint32_t columnIndex, const NarrowingKey &key) {
	while (columnIndex >= columnList_.size()) {
		columnList_.push_back(ColumnEntry(alloc_));
	}

	ColumnEntry &columnEntry = columnList_[columnIndex];

	const uint32_t id = static_cast<uint32_t>(columnEntry.keyList_.size());
	columnEntry.keyList_.push_back(key);

	if (key.isFull()) {
		columnEntry.fullList_.push_back(id);
	}
	else if (!key.isNone()) {
		if (!key.isHashFull()) {
			IndexEntry &entry = resolveIndex(columnEntry, key, false);
			entry.elements_.push_back(IndexElement(key.hashIndex_, id));
		}
		if (!key.isRangeFull()) {
			IndexEntry &entry = resolveIndex(columnEntry, key, true);
			entry.elements_.push_back(IndexElement(key.longRange_.first, id));
		}
	}

	size_ = std::max<uint32_t>(size_, id + 1);
	indexBuilt_ = false;
}

uint32_t SQLCompiler::NarrowingKeyTable::getMatchCount(
		const NarrowingKeyTable &srcTable, uint32_t srcId) {
	prepareVisits();
	buildIndex();

	const size_t lastCapacity = visitedList_.capacity();
	uint32_t count;
	{
		util::StackAllocator::Scope scope(alloc_);

		KeyIdList idList(alloc_);
		bool exact;
		find(srcTable, srcId, idList, visitedList_, 0, exact);

		count = static_cast<uint32_t>(idList.size());
		clearVisits(idList, 0, visitedList_);
	}

	if (visitedList_.capacity() != lastCapacity) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}

	return count;
}

bool SQLCompiler::NarrowingKeyTable::findAll(
		NarrowingKeyTable &srcTable, size_t srcOffset, KeyIdList &srcList,
		KeyIdList &destList, bool shallow) {
	NarrowingKeyTable *tableList[] = { &srcTable, this };
	KeyIdList *bothIdList[] = { &srcList, &destList };
	const size_t offsetList[] = { srcOffset, destList.size() };
	size_t sizeList[] = { 0, 0 };

	const size_t tableCount = sizeof(tableList) / sizeof(*tableList);

	for (size_t i = 0; i < tableCount; i++) {
		tableList[i]->prepareVisits();

		assert(offsetList[i] <= bothIdList[i]->size());
		sizeList[i] = bothIdList[i]->size() - offsetList[i];
	}

	{
		util::Vector<bool> &visitedList = tableList[0]->visitedList_;
		for (KeyIdList::iterator it = srcList.begin() + srcOffset;
				it != srcList.end(); ++it) {
			if (visitedList[*it]) {
				assert(false);
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
			}
			visitedList[*it] = true;
		}
	}

	for (size_t i = 0;;) {
		const size_t another = ((i + 1) % tableCount);

		tableList[another]->buildIndex();

		KeyIdList &idList = *bothIdList[another];
		util::Vector<bool> &visitedList = tableList[another]->visitedList_;

		bool exact = true;
		bool found = false;
		for (KeyIdList::iterator it = bothIdList[i]->begin() + offsetList[i];
				it != bothIdList[i]->end(); ++it) {
			bool exactLocal;
			found |= tableList[another]->find(
					*tableList[i], *it, idList, visitedList, offsetList[another],
					exactLocal);
			exact &= exactLocal;
		}

		sizeList[another] = bothIdList[another]->size() - offsetList[another];
		if (sizeList[another] > tableList[another]->getSize()) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
		}

		if (!found) {
			if (sizeList[another] == 0) {
				clearVisits(*bothIdList[i], offsetList[i], visitedList);
				bothIdList[i]->resize(offsetList[i]);
			}
			break;
		}

		if (shallow || exact) {
			break;
		}

		i = another;
	}

	for (size_t i = 0; i < tableCount; i++) {
		KeyIdList &idList = *bothIdList[i];
		util::Vector<bool> &visitedList = tableList[i]->visitedList_;

		std::sort(idList.begin() + offsetList[i], idList.end());

		clearVisits(idList, offsetList[i], visitedList);
	}

	return (sizeList[0] > 0 && sizeList[1] > 0);
}

bool SQLCompiler::NarrowingKeyTable::find(
		const NarrowingKeyTable &src, uint32_t srcId,
		KeyIdList &idList, util::Vector<bool> &visitedList,
		size_t offset, bool &exact) const {
	exact = true;

	assert(indexBuilt_);

	assert(visitedList.size() == size_);
	assert(idList.size() <= size_);

	assert(srcId < src.size_);
	assert(src.columnList_.size() == columnList_.size());

	const size_t orgSize = idList.size();
	assert(offset <= orgSize);

	bool listed = false;
	IndexElementList::const_iterator columnIt = columnScoreList_.begin();
	for (; columnIt != columnScoreList_.end(); ++columnIt) {
		const ColumnEntry &srcEntry = src.columnList_[columnIt->second];
		const ColumnEntry &destEntry = columnList_[columnIt->second];

		const KeyList &destKeyList = destEntry.keyList_;
		const KeyIdList &destFullList = destEntry.fullList_;
		const NarrowingKey &key = srcEntry.keyList_[srcId];

		if (key.isNone()) {
			clearVisits(idList, orgSize, visitedList);
			idList.resize(orgSize);
			exact = true;
			return false;
		}

		if (key.isFull()) {
			if (!listed && columnIt + 1 == columnScoreList_.end()) {
				matchKeyList(destKeyList, key, listed, idList, visitedList);
				exact = false;
				listed = true;
			}
			continue;
		}

		for (KeyIdList::const_iterator it = destFullList.begin();
				it != destFullList.end(); ++it) {
			matchKey(key, destKeyList[*it], *it, listed, idList, visitedList);
			exact = false;
		}

		const IndexList &indexList = destEntry.indexList_;
		for (IndexList::const_iterator it = indexList.begin();
				it != indexList.end(); ++it) {
			matchIndex(destKeyList, key, *it, listed, idList, visitedList);
			exact = false;
		}

		if (listed) {
			finishMerging(idList, visitedList, offset, orgSize);
		}

		listed = true;
	}

	return idList.size() > orgSize;
}

bool SQLCompiler::NarrowingKeyTable::matchIndex(
		const KeyList &keyList, const NarrowingKey &key,
		const IndexEntry &indexEntry, bool merging,
		KeyIdList &idList, util::Vector<bool> &visitedList) {
	bool exact = false;
	bool full = true;
	int64_t indexKeyBase = std::numeric_limits<uint64_t>::min();
	const bool forRange = indexEntry.forRange_;
	do {
		if (forRange) {
			if (key.isRangeFull()) {
				break;
			}
			indexKeyBase = key.longRange_.first;
		}
		else {
			if (key.isHashFull()) {
				break;
			}
			if (indexEntry.baseValue_ != getIndexBaseValue(key, false)) {
				break;
			}
			exact = true;
			indexKeyBase = key.hashIndex_;
		}
		full = false;
	}
	while (false);

	const IndexElementList &elements = indexEntry.elements_;

	IndexElementList::const_iterator beginIt;
	IndexElementList::const_iterator endIt;
	do {
		if (full) {
			beginIt = elements.begin();
			endIt = elements.end();
			break;
		}

		IndexElement lowerKey(indexKeyBase, 0);
		IndexElement upperKey(
				indexKeyBase, std::numeric_limits<uint32_t>::max());

		beginIt = std::lower_bound(elements.begin(), elements.end(), lowerKey);
		endIt = std::upper_bound(elements.begin(), elements.end(), upperKey);

		if (!forRange || exact) {
			break;
		}

		while (beginIt != elements.begin()) {
			if (!keyList[(beginIt - 1)->second].overlapsWithRange(key)) {
				break;
			}
			lowerKey.first = (beginIt - 1)->first;
			beginIt = std::lower_bound(elements.begin(), beginIt, lowerKey);
		}
		while (endIt != elements.end()) {
			if (!keyList[endIt->second].overlapsWithRange(key)) {
				break;
			}
			upperKey.first = endIt->first;
			endIt = std::upper_bound(endIt, elements.end(), upperKey);
		}
	}
	while (false);

	for (IndexElementList::const_iterator it = beginIt; it != endIt; ++it) {
		const uint32_t id = it->second;
		matchKey(key, keyList[id], id, merging, idList, visitedList);
	}

	return exact;
}

void SQLCompiler::NarrowingKeyTable::matchKeyList(
		const KeyList &keyList, const NarrowingKey &key, bool merging,
		KeyIdList &idList, util::Vector<bool> &visitedList) {
	for (KeyList::const_iterator it = keyList.begin();
			it != keyList.end(); ++it) {
		const uint32_t id = static_cast<uint32_t>(it - keyList.begin());
		matchKey(key, *it, id, merging, idList, visitedList);
	}
}

void SQLCompiler::NarrowingKeyTable::matchKey(
		const NarrowingKey &srcKey, const NarrowingKey &destKey,
		uint32_t destId, bool merging, KeyIdList &idList,
		util::Vector<bool> &visitedList) {
	if (!destKey.overlapsWith(srcKey)) {
		return;
	}

	util::Vector<bool>::reference visited = visitedList[destId];
	if (visited) {
		if (merging) {
			visited = false;
		}
	}
	else {
		if (!merging) {
			visited = true;
			idList.push_back(destId);
		}
	}
}

void SQLCompiler::NarrowingKeyTable::finishMerging(
		KeyIdList &idList, util::Vector<bool> &visitedList, size_t offset,
		size_t orgSize) {
	{
		KeyIdList::iterator destIt = idList.begin() + orgSize;
		for (KeyIdList::const_iterator it = destIt; it != idList.end(); ++it) {
			util::Vector<bool>::reference visited = visitedList[*it];
			if (visited) {
				visited = false;
			}
			else {
				visited = true;
				*destIt = *it;
				++destIt;
			}
		}
		idList.erase(destIt, idList.end());
	}

	assert(offset <= orgSize);
	setVisits(
			idList.begin() + offset, idList.begin() + orgSize, visitedList,
			true);
}

void SQLCompiler::NarrowingKeyTable::clearVisits(
		const KeyIdList &idList, size_t offset,
		util::Vector<bool> &visitedList) {
	assert(offset <= idList.size());
	setVisits(idList.begin() + offset, idList.end(), visitedList, false);
}

void SQLCompiler::NarrowingKeyTable::setVisits(
		KeyIdList::const_iterator beginIt, KeyIdList::const_iterator endIt,
		util::Vector<bool> &visitedList, bool visited) {
	for (KeyIdList::const_iterator it = beginIt; it != endIt; ++it) {
		visitedList[*it] = visited;
	}
}

void SQLCompiler::NarrowingKeyTable::prepareVisits() {
	const uint32_t size = getSize();
	if (visitedList_.size() != size) {
		visitedList_.assign(size, false);
	}
}

void SQLCompiler::NarrowingKeyTable::buildIndex() {
	if (indexBuilt_) {
		return;
	}

	columnScoreList_.clear();

	for (ColumnList::iterator columnIt = columnList_.begin();
			columnIt != columnList_.end(); ++columnIt) {
		if (columnIt->keyList_.size() != size_) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
		}

		IndexList &indexList = columnIt->indexList_;
		for (IndexList::iterator it = indexList.begin();
				it != indexList.end(); ++it) {
			IndexElementList &elements = it->elements_;
			std::sort(elements.begin(), elements.end());
		}

		const uint32_t columnIndex =
				static_cast<uint32_t>(columnIt - columnList_.begin());
		columnScoreList_.push_back(
				IndexElement(makeColumnScore(*columnIt), columnIndex));
	}

	std::sort(columnScoreList_.begin(), columnScoreList_.end());

	indexBuilt_ = true;
}

int64_t SQLCompiler::NarrowingKeyTable::makeColumnScore(
		const ColumnEntry &columnEntry) {
	if (isNoneColumn(columnEntry)) {
		return std::numeric_limits<int64_t>::min();
	}

	int64_t score = 0;

	const IndexList &indexList = columnEntry.indexList_;
	for (IndexList::const_iterator indexIt = indexList.begin();
			indexIt != indexList.end(); ++indexIt) {
		if (!indexIt->forRange_) {
			score -= indexIt->baseValue_;
			continue;
		}

		const IndexElementList &elements = indexIt->elements_;
		if (elements.empty()) {
			continue;
		}

		IndexElementList::const_iterator it = elements.begin();
		int64_t last = it->first;
		while ((++it) != elements.end()) {
			if (it->first != last) {
				last = it->first;
				score--;
			}
		}
		score--;
	}

	score += static_cast<int64_t>(columnEntry.fullList_.size()) *
			static_cast<int64_t>(columnEntry.keyList_.size());

	return score;
}

bool SQLCompiler::NarrowingKeyTable::isNoneColumn(
		const ColumnEntry &columnEntry) {
	return columnEntry.fullList_.empty() && columnEntry.indexList_.empty();
}

SQLCompiler::NarrowingKeyTable::IndexEntry&
SQLCompiler::NarrowingKeyTable::resolveIndex(
		ColumnEntry &columnEntry, const NarrowingKey &key, bool forRange) {
	IndexList &indexList = columnEntry.indexList_;

	IndexList::const_iterator it;
	if (!findIndex(columnEntry, key, forRange, it)) {
		indexList.push_back(IndexEntry(alloc_));
		IndexEntry &entry = indexList.back();

		entry.baseValue_ = getIndexBaseValue(key, forRange);
		entry.forRange_ = forRange;

		return entry;
	}

	return indexList[it - indexList.begin()];
}

bool SQLCompiler::NarrowingKeyTable::findIndex(
		const ColumnEntry &columnEntry, const NarrowingKey &key,
		bool forRange, IndexList::const_iterator &it) {
	const int64_t baseValue = getIndexBaseValue(key, forRange);

	const IndexList &indexList = columnEntry.indexList_;
	for (it = indexList.begin(); it != indexList.end(); ++it) {
		if (!it->forRange_ == !forRange && it->baseValue_ == baseValue) {
			return true;
		}
	}

	return false;
}

int64_t SQLCompiler::NarrowingKeyTable::getIndexBaseValue(
		const NarrowingKey &key, bool forRange) {
	assert(!key.isNone());

	int64_t baseValue;
	if (forRange) {
		assert(!key.isRangeFull());
		baseValue = key.longRange_.second - key.longRange_.first;
	}
	else {
		assert(!key.isHashFull());
		baseValue = key.hashCount_;
	}
	assert(baseValue >= 0);

	return baseValue;
}

SQLCompiler::NarrowingKeyTable::IndexEntry::IndexEntry(
		util::StackAllocator &alloc) :
		elements_(alloc),
		baseValue_(0),
		forRange_(false) {
}

SQLCompiler::NarrowingKeyTable::ColumnEntry::ColumnEntry(
		util::StackAllocator &alloc) :
		keyList_(alloc),
		fullList_(alloc),
		indexList_(alloc) {
}

SQLCompiler::TableNarrowingList::TableNarrowingList(
		util::StackAllocator &alloc) :
		alloc_(alloc),
		entryList_(alloc),
		affinityRevList_(alloc) {
}

SQLCompiler::TableNarrowingList* SQLCompiler::TableNarrowingList::tryDuplicate(
		const TableNarrowingList *src) {
	if (src == NULL) {
		return NULL;
	}
	return ALLOC_NEW(src->alloc_) TableNarrowingList(*src);
}

void SQLCompiler::TableNarrowingList::put(const TableInfo &tableInfo) {
	const TableInfo::Id id = tableInfo.idInfo_.id_;
	if (id >= entryList_.size()) {
		entryList_.resize(id + 1, Entry(alloc_));
	}

	Entry &entry = entryList_[id];

	entry = Entry(alloc_);
	entry.assigned_ = true;

	ProcessorUtils::getTablePartitionAffinityRevList(tableInfo, affinityRevList_);
	if (!ProcessorUtils::getTablePartitionKeyList(
			tableInfo, affinityRevList_, entry.keyList_, entry.subKeyList_)) {
		return;
	}

	const TableInfo::PartitioningInfo *partitioning = tableInfo.partitioning_;
	assert(partitioning != NULL);

	if (partitioning->partitioningColumnId_ >= 0) {
		entry.columnId_ = partitioning->partitioningColumnId_;
		assert(entry.keyList_.size() == partitioning->subInfoList_.size());
	}

	if (partitioning->subPartitioningColumnId_ >= 0) {
		entry.subColumnId_ = partitioning->subPartitioningColumnId_;
		assert(entry.subKeyList_.size() == partitioning->subInfoList_.size());
	}

	affinityRevList_.clear();
}

bool SQLCompiler::TableNarrowingList::contains(TableInfo::Id id) const {
	return (id < entryList_.size() && entryList_[id].assigned_);
}

SQLCompiler::NarrowingKey SQLCompiler::TableNarrowingList::resolve(
		const TableInfo::IdInfo &idInfo, ColumnId columnId) const {
	const TableInfo::Id id = idInfo.id_;
	if (id >= entryList_.size()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}

	if (idInfo.subContainerId_ < 0) {
		return NarrowingKey();
	}

	const Entry &entry = entryList_[id];
	const KeyList *keyList;

	if (entry.columnId_ == columnId) {
		keyList = &entry.keyList_;
	}
	else if (entry.subColumnId_ == columnId) {
		keyList = &entry.subKeyList_;
	}
	else {
		return NarrowingKey();
	}

	const size_t subIndex = static_cast<size_t>(idInfo.subContainerId_);
	if (subIndex >= keyList->size()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}

	return (*keyList)[subIndex];
}

SQLCompiler::TableNarrowingList::Entry::Entry(util::StackAllocator &alloc) :
		keyList_(alloc),
		subKeyList_(alloc),
		columnId_(UNDEF_COLUMNID),
		subColumnId_(UNDEF_COLUMNID),
		assigned_(false) {
}

SQLCompiler::JoinExpansionInfo::JoinExpansionInfo() :
		unificationList_(NULL),
		unifyingUnitList_(NULL),
		expansionList_(NULL),
		unificationCount_(0),
		unifyingUnitCount_(0),
		expansionCount_(0),
		extraCount_(0),
		limitReached_(false) {
}

bool SQLCompiler::JoinExpansionInfo::isEstimationOnly() const {
	return unificationList_ == NULL ||
			unifyingUnitList_ == NULL ||
			expansionList_ == NULL;
}

void SQLCompiler::JoinExpansionInfo::reserve(const JoinExpansionInfo &info) {
	assert(!isEstimationOnly());

	unificationList_->reserve(info.unificationCount_);
	unifyingUnitList_->reserve(info.unifyingUnitCount_);
	expansionList_->reserve(info.expansionCount_);
}

void SQLCompiler::JoinExpansionInfo::clear() {
	tryClear(unificationList_);
	tryClear(unifyingUnitList_);
	tryClear(expansionList_);
	unificationCount_ = 0;
	unifyingUnitCount_ = 0;
	expansionCount_ = 0;
	extraCount_ = 0;
	limitReached_ = 0;
}

template<typename T>
void SQLCompiler::JoinExpansionInfo::tryAppend(
		util::Vector<T> *list, const T &element) {
	if (list == NULL) {
		return;
	}

	if (list->size() + 1 > list->capacity()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}

	void *lastAddr = (list->empty() ? NULL : &list->front());
	list->push_back(element);

	if (lastAddr != NULL && &list->front() != lastAddr) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}
}

template<typename T, typename It>
void SQLCompiler::JoinExpansionInfo::tryAppend(
		util::Vector<T> *list, It beginIt, It endIt) {
	if (list == NULL) {
		return;
	}

	if (list->size() + (endIt - beginIt) > list->capacity()) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}

	void *lastAddr = (list->empty() ? NULL : &list->front());
	list->insert(list->end(), beginIt, endIt);

	if (lastAddr != NULL && &list->front() != lastAddr) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}
}

template<typename T>
void SQLCompiler::JoinExpansionInfo::tryClear(util::Vector<T> *list) {
	if (list != NULL) {
		list->clear();
	}
}

SQLCompiler::JoinExpansionBuildInfo::JoinExpansionBuildInfo(
		util::StackAllocator &alloc, JoinExpansionInfo &expansionInfo) :
		expansionInfo_(expansionInfo),
		idList_(alloc),
		unifyingMap_(alloc) {
}

SQLCompiler::IntegralPartitioner::IntegralPartitioner(
		ValueType totalAmount, ValueType groupCount) :
		totalAmount_(totalAmount),
		groupCount_(groupCount) {
}

SQLCompiler::IntegralPartitioner::ValueType
SQLCompiler::IntegralPartitioner::getGroupAmount() const {
	if (groupCount_ <= 0) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}

	const ValueType base = totalAmount_ / groupCount_;
	const ValueType mod = totalAmount_ % groupCount_;
	const ValueType groupId = 0;
	if (groupId < mod) {
		return base + 1;
	}
	else {
		return base;
	}
}

SQLCompiler::IntegralPartitioner::ValueType
SQLCompiler::IntegralPartitioner::popGroup() {
	return popGroupFull(getGroupAmount());
}

SQLCompiler::IntegralPartitioner::ValueType
SQLCompiler::IntegralPartitioner::popGroupLimited(ValueType amount) {
	return popGroupFull(std::min(amount, getGroupAmount()));
}

SQLCompiler::IntegralPartitioner::ValueType
SQLCompiler::IntegralPartitioner::popGroupFull(ValueType amount) {
	if (groupCount_ <= 0) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}

	groupCount_--;

	if (amount > totalAmount_) {
		totalAmount_ = 0;
		return totalAmount_;
	}
	else {
		totalAmount_ -= amount;
		return amount;
	}
}

SQLCompiler::OptimizationContext::OptimizationContext(
		SQLCompiler &compiler, Plan &plan) :
		compiler_(compiler),
		plan_(plan) {
}

SQLCompiler& SQLCompiler::OptimizationContext::getCompiler() {
	return compiler_;
}

SQLCompiler::Plan& SQLCompiler::OptimizationContext::getPlan() {
	return plan_;
}

const util::NameCoderEntry<SQLCompiler::OptimizationUnitType>
		SQLCompiler::OptimizationUnit::TYPE_LIST[] = {
	UTIL_NAME_CODER_ENTRY(OPT_MERGE_SUB_PLAN),
	UTIL_NAME_CODER_ENTRY(OPT_MINIMIZE_PROJECTION),
	UTIL_NAME_CODER_ENTRY(OPT_PUSH_DOWN_PREDICATE),
	UTIL_NAME_CODER_ENTRY(OPT_PUSH_DOWN_AGGREGATE),
	UTIL_NAME_CODER_ENTRY(OPT_PUSH_DOWN_JOIN),
	UTIL_NAME_CODER_ENTRY(OPT_PUSH_DOWN_LIMIT),
	UTIL_NAME_CODER_ENTRY(OPT_REMOVE_REDUNDANCY),
	UTIL_NAME_CODER_ENTRY(OPT_REMOVE_INPUT_REDUNDANCY),
	UTIL_NAME_CODER_ENTRY(OPT_REMOVE_PARTITION_COND_REDUNDANCY),
	UTIL_NAME_CODER_ENTRY(OPT_ASSIGN_JOIN_PRIMARY_COND),
	UTIL_NAME_CODER_ENTRY(OPT_MAKE_SCAN_COST_HINTS),
	UTIL_NAME_CODER_ENTRY(OPT_MAKE_JOIN_PUSH_DOWN_HINTS),
	UTIL_NAME_CODER_ENTRY(OPT_MAKE_INDEX_JOIN),
	UTIL_NAME_CODER_ENTRY(OPT_FACTORIZE_PREDICATE),
	UTIL_NAME_CODER_ENTRY(OPT_REORDER_JOIN),
	UTIL_NAME_CODER_ENTRY(OPT_SIMPLIFY_SUBQUERY)
};

const util::NameCoder<
		SQLCompiler::OptimizationUnitType, SQLCompiler::OPT_END>
		SQLCompiler::OptimizationUnit::TYPE_CODER(TYPE_LIST, 1);

SQLCompiler::OptimizationUnit::OptimizationUnit(
		OptimizationContext &cxt, OptimizationUnitType type) :
		cxt_(cxt),
		type_(type) {
}

SQLCompiler::OptimizationUnit::~OptimizationUnit() {
}

bool SQLCompiler::OptimizationUnit::operator()() {
	SQLCompiler &compiler = cxt_.getCompiler();

	compiler.checkOptimizationUnitSupport(type_);

	if (!compiler.isOptimizationUnitEnabled(type_)) {
		return false;
	}

	if (compiler.profiler_ != NULL) {
		compiler.profiler_->startOptimization(type_);
	}

	return true;
}

bool SQLCompiler::OptimizationUnit::operator()(bool optimized) {
	SQLCompiler &compiler = cxt_.getCompiler();

	if (!compiler.isOptimizationUnitEnabled(type_)) {
		return false;
	}


	if (compiler.profiler_ != NULL) {
		compiler.profiler_->endOptimization(optimized);
	}

	return optimized;
}

bool SQLCompiler::OptimizationUnit::isCheckOnly() const {
	SQLCompiler &compiler = cxt_.getCompiler();

	if (!compiler.isOptimizationUnitCheckOnly(type_)) {
		return false;
	}

	return true;
}

SQLCompiler::OptimizationOption::OptimizationOption(ProfilerManager *manager) :
		supportFlags_(manager == NULL ? 0 : getFullFlags()),
		enablementFlags_(manager == NULL ? 0 : manager->getOptimizationFlags()),
		checkOnlyFlags_(0) {
	UTIL_STATIC_ASSERT(OPT_END < sizeof(OptimizationFlags) * CHAR_BIT);
}

bool SQLCompiler::OptimizationOption::isEnabled(
		OptimizationUnitType type) const {
	return isFlagOn(enablementFlags_, type);
}

bool SQLCompiler::OptimizationOption::isSupported(
		OptimizationUnitType type) const {
	return isFlagOn(supportFlags_, type);
}

bool SQLCompiler::OptimizationOption::isCheckOnly(
		OptimizationUnitType type) const {
	return isFlagOn(checkOnlyFlags_, type);
}

bool SQLCompiler::OptimizationOption::isSomeEnabled() const {
	return enablementFlags_ != 0;
}

SQLCompiler::OptimizationFlags
SQLCompiler::OptimizationOption::getEnablementFlags() const {
	return enablementFlags_;
}

bool SQLCompiler::OptimizationOption::isSomeCheckOnly() const {
	return (checkOnlyFlags_ != 0);
}

void SQLCompiler::OptimizationOption::supportFull() {
	supportFlags_ = getFullFlags();
	enablementFlags_ = getFullFlags();
}

void SQLCompiler::OptimizationOption::support(
		OptimizationUnitType type, bool enabled) {
	setFlag(supportFlags_, type, true);
	setFlag(enablementFlags_, type, enabled);
}

void SQLCompiler::OptimizationOption::setCheckOnly(OptimizationUnitType type) {
	setFlag(checkOnlyFlags_, type, true);
}

SQLCompiler::OptimizationFlags SQLCompiler::OptimizationOption::getFullFlags() {
	return ((static_cast<OptimizationFlags>(1) << OPT_END) - 1);
}

bool SQLCompiler::OptimizationOption::isFlagOn(
		OptimizationFlags flags, OptimizationUnitType type) {
	return (flags & (static_cast<OptimizationFlags>(1) << type)) != 0;
}

void SQLCompiler::OptimizationOption::setFlag(
		OptimizationFlags &flags, OptimizationUnitType type, bool value) {
	const OptimizationFlags mask = (static_cast<OptimizationFlags>(1) << type);
	flags = (value ? (flags | mask) : (flags & (~mask)));
}

const SQLCompiler::CompileOption SQLCompiler::CompileOption::EMPTY_OPTION;

SQLCompiler::CompileOption::CompileOption(const CompilerSource *src) :
		costBasedJoin_(false),
		costBasedJoinDriving_(false),
		indexStatsRequester_(NULL),
		baseCompiler_(NULL) {
	const SQLCompiler *baseCompiler = CompilerSource::findBaseCompiler(src);
	if (baseCompiler != NULL) {
		if (baseCompiler->compileOption_ != NULL) {
			*this = *baseCompiler->compileOption_;
		}
		baseCompiler_ = baseCompiler;
	}
}

void SQLCompiler::CompileOption::setTimeZone(const util::TimeZone &timeZone) {
	timeZone_ = timeZone;
}

const util::TimeZone& SQLCompiler::CompileOption::getTimeZone() const {
	return timeZone_;
}

void SQLCompiler::CompileOption::setPlanningVersion(
		const SQLPlanningVersion &version) {
	planningVersion_ = version;
}

const SQLPlanningVersion& SQLCompiler::CompileOption::getPlanningVersion() const {
	return planningVersion_;
}

void SQLCompiler::CompileOption::setCostBasedJoin(bool value) {
	costBasedJoin_ = value;
}

bool SQLCompiler::CompileOption::isCostBasedJoin() const {
	return costBasedJoin_;
}

void SQLCompiler::CompileOption::setCostBasedJoinDriving(bool value) {
	costBasedJoinDriving_ = value;
}

bool SQLCompiler::CompileOption::isCostBasedJoinDriving() const {
	return costBasedJoinDriving_;
}

void SQLCompiler::CompileOption::setIndexStatsRequester(
		SQLIndexStatsCache::RequesterHolder *value) {
	indexStatsRequester_ = value;
}

SQLIndexStatsCache::RequesterHolder*
SQLCompiler::CompileOption::getIndexStatsRequester() const {
	return indexStatsRequester_;
}

const SQLCompiler::CompileOption&
SQLCompiler::CompileOption::getEmptyOption() {
	return EMPTY_OPTION;
}

const SQLCompiler* SQLCompiler::CompileOption::findBaseCompiler(
		const CompileOption *option, CompilerInitializer&) {
	return (option == NULL ? NULL : option->baseCompiler_);
}

SQLCompiler::Profiler::Profiler(util::StackAllocator &alloc) :
		alloc_(alloc),
		plan_(alloc),
		tableInfoList_(alloc),
		optimizationResult_(alloc),
		target_(NULL) {
}

SQLCompiler::Profiler* SQLCompiler::Profiler::tryDuplicate(
		const Profiler *src) {
	if (src == NULL) {
		return NULL;
	}
	Profiler *dest = ALLOC_NEW(src->alloc_) Profiler(src->alloc_);
	dest->plan_ = src->plan_;
	dest->tableInfoList_ = src->tableInfoList_;
	dest->optimizationResult_ = src->optimizationResult_;
	dest->target_ = src->target_;
	return dest;
}

void SQLCompiler::Profiler::setPlan(const Plan &plan) {
	plan_ = plan;
}

void SQLCompiler::Profiler::setTableInfoList(const SQLTableInfoList &list) {
	tableInfoList_.clear();

	const uint32_t size = list.size();
	for (SQLTableInfo::Id id = 0; id < size; id++) {
		tableInfoList_.push_back(list.resolve(id));
	}
}

void SQLCompiler::Profiler::startOptimization(OptimizationUnitType type) {
	if (target_ == NULL || !target_->optimize_) {
		return;
	}

	OptimizationUnitResult result;
	result.type_ = type;

	optimizationResult_.push_back(result);
	watch_.reset();
	watch_.start();
}

void SQLCompiler::Profiler::endOptimization(bool optimized) {
	const uint64_t elapsedNanos = watch_.elapsedNanos();

	if (target_ == NULL || !target_->optimize_) {
		return;
	}

	assert(!optimizationResult_.empty());

	OptimizationUnitResult &result = optimizationResult_.back();
	result.optimized_ = optimized;
	result.elapsedNanos_ = elapsedNanos;
}

void SQLCompiler::Profiler::setTarget(const Target &target) {
	target_ = ALLOC_NEW(alloc_) Target(target);
}

SQLCompiler::Profiler::Stats::Stats() :
		singleInputPredExtraction_(0),
		complexJoinPrimaryCond_(0) {
}

SQLCompiler::Profiler::Target::Target() :
		optimize_(false) {
}

bool SQLCompiler::Profiler::Target::isEmpty() const {
	return !optimize_;
}

SQLCompiler::Profiler::OptimizationUnitResult::OptimizationUnitResult() :
		type_(OPT_END),
		elapsedNanos_(-1),
		optimized_(false) {
}

SQLCompiler::ProfilerManager SQLCompiler::ProfilerManager::instance_;

const uint32_t SQLCompiler::ProfilerManager::MAX_PROFILER_COUNT = 10;

SQLCompiler::ProfilerManager::ProfilerManager() :
		optimizationFlags_(getDefaultOptimizationFlags()) {
}

SQLCompiler::ProfilerManager& SQLCompiler::ProfilerManager::getInstance() {
	return instance_;
}

bool SQLCompiler::ProfilerManager::isEnabled() {
	return false;
}

SQLCompiler::Profiler::Target SQLCompiler::ProfilerManager::getTarget() {
	util::LockGuard<util::Mutex> guard(mutex_);
	return target_;
}

void SQLCompiler::ProfilerManager::setTarget(const Profiler::Target &target) {
	if (!isEnabled()) {
		return;
	}

	util::LockGuard<util::Mutex> guard(mutex_);
	target_ = target;

	if (target.isEmpty()) {
		entryList_.clear();
	}
}

SQLCompiler::OptimizationFlags
SQLCompiler::ProfilerManager::getOptimizationFlags() {
	return optimizationFlags_;
}

void SQLCompiler::ProfilerManager::setOptimizationFlags(
		OptimizationFlags flags, bool forDefault) {
	if (!isEnabled()) {
		return;
	}

	util::LockGuard<util::Mutex> guard(mutex_);
	if (forDefault) {
		optimizationFlags_ =
				(~flags & optimizationFlags_) |
				(flags & getDefaultOptimizationFlags());
	}
	else {
		optimizationFlags_ = flags;
	}
}

SQLCompiler::Profiler* SQLCompiler::ProfilerManager::tryCreateProfiler(
		util::StackAllocator &alloc) {
	if (!isEnabled()) {
		return NULL;
	}

	const Profiler::Target &target = getTarget();
	if (target.isEmpty()) {
		return NULL;
	}

	Profiler *profiler = ALLOC_NEW(alloc) Profiler(alloc);
	profiler->setTarget(target);

	return profiler;
}

SQLCompiler::Profiler* SQLCompiler::ProfilerManager::getProfiler(
		util::StackAllocator &alloc, uint32_t index) {
	util::XArray<uint8_t> buf(alloc);
	{
		util::LockGuard<util::Mutex> guard(mutex_);

		uint32_t rest = index;
		for (EntryList::const_iterator it = entryList_.begin();; ++it) {
			if (it == entryList_.end()) {
				return NULL;
			}
			if (rest == 0) {
				const Entry &src = *it;
				if (!src.empty()) {
					buf.resize(src.size());
					memcpy(&buf[0], &src[0], src.size());
				}
				break;
			}
			rest--;
		}
	}

	util::ArrayByteInStream in((util::ArrayInStream(buf.data(), buf.size())));

	TupleValue::VarContext varCxt;
	varCxt.setStackAllocator(&alloc);

	Profiler *profiler = NULL;
	TupleValue::coder(util::ObjectCoder::withAllocator(alloc), &varCxt).decode(
			in, profiler);

	return profiler;
}

void SQLCompiler::ProfilerManager::addProfiler(const Profiler &profiler) {
	if (!isEnabled()) {
		return;
	}

	util::StackAllocator &alloc = profiler.alloc_;
	util::StackAllocator::Scope scope(alloc);

	util::XArray<uint8_t> buf(alloc);
	util::XArrayByteOutStream out((util::XArrayOutStream<>(buf)));
	TupleValue::coder(util::ObjectCoder(), NULL).encode(out, &profiler);

	{
		util::LockGuard<util::Mutex> guard(mutex_);

		Entry dest;
		if (!buf.empty()) {
			dest.resize(buf.size());
			memcpy(&dest[0], &buf[0], buf.size());
		}

		entryList_.push_front(dest);
		while (entryList_.size() > MAX_PROFILER_COUNT) {
			entryList_.pop_back();
		}
	}
}

SQLCompiler::JoinNode::JoinNode(util::StackAllocator &alloc) :
		edgeIdList_(alloc), approxSize_(-1), cardinalityList_(alloc) {
}

SQLCompiler::JoinEdge::JoinEdge() :
		type_(SQLType::START_EXPR),
		weakness_(0),
		simple_(false) {
	std::fill(
			nodeIdList_ + 0, nodeIdList_ + JOIN_INPUT_COUNT,
			std::numeric_limits<JoinNodeId>::max());
	std::fill(
			columnIdList_ + 0, columnIdList_ + JOIN_INPUT_COUNT,
			UNDEF_COLUMNID);
}

void SQLCompiler::JoinEdge::dump(std::ostream &os) const {
	os << "JoinEdge: ";

	if (type_ != SQLType::START_EXPR) {
		os << SQLType::Coder()(type_);
	}

	os << "(";
	for (size_t i = 0; i < JOIN_INPUT_COUNT; i++) {
		if (nodeIdList_[i] != std::numeric_limits<JoinNodeId>::max()) {
			if (i > 0) {
				os << ", ";
			}

			os << nodeIdList_[i];

			if (columnIdList_[i] != UNDEF_COLUMNID) {
				os << "_" << columnIdList_[i];
			}
		}
	}
	os << ")";

	if (simple_) {
		os << ", simple";
	}

	if (weakness_ > 0) {
		os << ", weakness: " << weakness_;
	}
}

SQLCompiler::JoinOperation::JoinOperation() :
		type_(SQLType::JOIN_INNER),
		hintType_(HINT_LEADING_NONE),
		profile_(NULL),
		tableCostAffected_(false) {
	std::fill(
			nodeIdList_ + 0, nodeIdList_ + JOIN_INPUT_COUNT,
			std::numeric_limits<JoinNodeId>::max());
}

void SQLCompiler::JoinOperation::dump(std::ostream &os) const {
	os << "JoinOp: " << SQLType::Coder()(type_) <<
			"(" << nodeIdList_[0] << "," << nodeIdList_[1] <<
			",[" << hintType_ << "]" <<
			")";
}

const uint64_t SQLCompiler::JoinScore::DEFAULT_TABLE_ROW_APPROX_SIZE = 1000000; 

SQLCompiler::JoinScore::JoinScore(util::StackAllocator &alloc)
: alloc_(alloc), edgeList_(alloc), degree_(0), opLevel_(0), opCount_(0),
  approxTotalCount_(DEFAULT_TABLE_ROW_APPROX_SIZE),
  approxFilteredCount_(DEFAULT_TABLE_ROW_APPROX_SIZE),
  cardinalityList_(alloc) {
	nodeIdList_[0] = UNDEF_JOIN_NODEID;
	nodeIdList_[1] = UNDEF_JOIN_NODEID;
	scoreNodeIdList_[0] = 0;
	scoreNodeIdList_[1] = 0;
}

SQLCompiler::JoinScore::JoinScore(util::StackAllocator &alloc, JoinNodeId id)
: alloc_(alloc), edgeList_(alloc), degree_(0), opLevel_(0), opCount_(0),
  approxTotalCount_(DEFAULT_TABLE_ROW_APPROX_SIZE),
  approxFilteredCount_(DEFAULT_TABLE_ROW_APPROX_SIZE),
  cardinalityList_(alloc) {
	nodeIdList_[0] = id;
	nodeIdList_[1] = UNDEF_JOIN_NODEID;
	scoreNodeIdList_[0] = id + 1;
	scoreNodeIdList_[1] = 0;
}

SQLCompiler::JoinScore::JoinScore(
		util::StackAllocator &alloc, JoinNodeId id1, JoinNodeId id2)
: alloc_(alloc), edgeList_(alloc), degree_(0), opLevel_(0), opCount_(0),
  approxTotalCount_(DEFAULT_TABLE_ROW_APPROX_SIZE),
  approxFilteredCount_(DEFAULT_TABLE_ROW_APPROX_SIZE),
  cardinalityList_(alloc) {
	assert(id1 < id2);
	nodeIdList_[0] = id1;
	nodeIdList_[1] = id2;
	scoreNodeIdList_[0] = id1 + 1;
	scoreNodeIdList_[1] = id2 + 1;
}

uint32_t SQLCompiler::JoinScore::getOpLevel(SQLType::Id type) const {
	uint32_t scoreOpType = JOIN_SCORE_OP_START;
	switch(type) {
		case SQLType::OP_EQ:
		case SQLType::OP_IS:
		case SQLType::OP_IS_NULL:
		case SQLType::EXPR_IN:
			scoreOpType = JOIN_SCORE_OP_LV4;  
			break;
		case SQLType::EXPR_BETWEEN:
		case SQLType::FUNC_LIKE:
		case SQLType::OP_LT:
		case SQLType::OP_LE:
		case SQLType::OP_GT:
		case SQLType::OP_GE:
			scoreOpType = JOIN_SCORE_OP_LV3;  
			break;
		case SQLType::OP_NE:
		case SQLType::OP_IS_NOT:
		case SQLType::OP_IS_NOT_NULL:
			scoreOpType = JOIN_SCORE_OP_LV1;  
			break;
		default:
			scoreOpType = JOIN_SCORE_OP_LV2;  
			break;
	}
	return scoreOpType;
}

void SQLCompiler::JoinScore::applyAnd(const JoinEdge &edge) {
	edgeList_.push_back(&edge);
	uint32_t opLevel = getOpLevel(edge.type_);
	uint64_t weakness = (edge.weakness_ < 100) ? (edge.weakness_ + 1) : 100;
	if (edge.type_ == SQLType::EXPR_BETWEEN) {
		weakness = (weakness * 2 < 100) ? weakness * 2 : 100;
	}
	if (opLevel_ == opLevel) {
		opCount_ += 100 / weakness;
	}
	else if (opLevel > opLevel_) {
		uint64_t correctionValue = (opLevel_ == 0) ? 0 : 1;
		opLevel_ = opLevel;
		opCount_ = 100 / weakness + correctionValue;
	}
	assert(approxTotalCount_ > 0);
	assert(approxFilteredCount_ > 0);
	if (approxFilteredCount_ > 1) {
		double newApproxCount = static_cast<double>(approxFilteredCount_);
		switch(opLevel) {
		case JOIN_SCORE_OP_LV4:
			if ((edge.weakness_ + 1) < approxFilteredCount_) {
				newApproxCount = static_cast<double>(edge.weakness_ + 1);
			}
			break;
		case JOIN_SCORE_OP_LV3:
			newApproxCount = static_cast<double>(approxFilteredCount_) * 0.5;
			break;
		case JOIN_SCORE_OP_LV2:
			newApproxCount = static_cast<double>(approxFilteredCount_) * 0.7;
			break;
		case JOIN_SCORE_OP_LV1:
			newApproxCount = static_cast<double>(approxFilteredCount_) * 0.9;
			break;
		}
		approxFilteredCount_ = static_cast<uint64_t>(newApproxCount);
		if (approxFilteredCount_ == 0) {
			approxFilteredCount_ = 1;
		}
	}
}

SQLCompiler::JoinScore SQLCompiler::JoinScore::applyAnd(const JoinScore &another) const {
	JoinScore result = *this;
	if (opLevel_ == another.opLevel_) {
		result.opCount_ += another.opCount_;
	}
	else if (another.opLevel_ > opLevel_) {
		uint64_t correctionValue = (opLevel_ == 0) ? 0 : 1;
		result.opLevel_ = another.opLevel_;
		result.opCount_ = another.opCount_ + correctionValue;
	}
	else {
	}
	return result;
}

void SQLCompiler::JoinScore::dumpScore(std::ostream &os) const {
	os << "score(FC,LV,OPC,TC)=(" << approxFilteredCount_ <<
			", " << opLevel_ << ", " << opCount_ <<
			", " << approxTotalCount_ << ")";
}

void SQLCompiler::JoinScore::dump(std::ostream &os) const {
	os << "JoinScore: ";

	os << "(";
	for (size_t i = 0; i < JOIN_INPUT_COUNT; i++) {
		if (nodeIdList_[i] != std::numeric_limits<JoinNodeId>::max()) {
			if (i > 0) {
				os << ", ";
			}
			os << nodeIdList_[i];
		}
	}
	os << ")";

	os << ",(";
	for (size_t i = 0; i < JOIN_INPUT_COUNT; i++) {
		if (i > 0) {
			os << ", ";
		}
		os << scoreNodeIdList_[i];
	}
	os << "), ";
	dumpScore(os);
}

bool SQLCompiler::reorderJoinSubLegacy(
		util::StackAllocator &alloc,
		const util::Vector<JoinNode> &joinNodeList,
		const util::Vector<JoinEdge> &joinEdgeList,
		const util::Vector<JoinOperation> &srcJoinOpList,
		const util::Vector<SQLHintInfo::ParsedHint> &joinHintList,
		util::Vector<JoinOperation> &destJoinOpList, bool profiling) {

	destJoinOpList.clear();
	if (joinNodeList.size() < 3 || joinEdgeList.size() == 0) {
		return false;
	}

	util::Set<JoinNodeId> checkNodeSet(alloc);

	util::Vector<JoinNode>::const_iterator nodeItr = joinNodeList.begin();
	JoinNodeId joinNodeId = 0;
	for (; nodeItr != joinNodeList.end(); ++nodeItr, ++joinNodeId) {
		checkNodeSet.insert(joinNodeId);
	}
	JoinNodeId joinNodeIdEnd = joinNodeId;


	JoinScoreMap joinScoreMap(alloc); 
	std::pair<util::Map<JoinScoreKey, JoinScore>::iterator, bool> result;
	util::Vector<JoinNodeId> orphanNodeList(alloc);
	for (joinNodeId = 0; joinNodeId < joinNodeIdEnd; ++joinNodeId) {
		const JoinNode &joinNode = joinNodeList[joinNodeId];

		JoinScoreKey key = std::make_pair(joinNodeId, UNDEF_JOIN_NODEID);
		JoinScore joinScore(alloc, joinNodeId);

		const uint64_t approxSize = JoinScore::DEFAULT_TABLE_ROW_APPROX_SIZE;
		joinScore.setApproxTotalCount(approxSize);
		joinScore.setApproxFilteredCount(approxSize);

		util::Set<JoinNodeId> neighborNodeSet(alloc);
		for (size_t edgePos = 0; edgePos < joinNode.edgeIdList_.size(); ++edgePos) {
			const JoinEdge &joinEdge = joinEdgeList[joinNode.edgeIdList_[edgePos]];
			if (joinEdge.nodeIdList_[1] != UNDEF_JOIN_NODEID
				&& joinEdge.nodeIdList_[1] != joinNodeId) {
				neighborNodeSet.insert(joinEdge.nodeIdList_[1]);
			}
			if (joinEdge.nodeIdList_[0] != joinNodeId) {
				assert(joinEdge.nodeIdList_[0] != UNDEF_JOIN_NODEID);
					neighborNodeSet.insert(joinEdge.nodeIdList_[0]);
			}
		}
		size_t degree = neighborNodeSet.size(); 
		joinScore.setDegree(degree);

		result = joinScoreMap.insert(std::make_pair(key, joinScore));
		assert(result.second == true);
		if (degree == 0) {
			orphanNodeList.push_back(joinNodeId);
		}
	}

	util::Vector< util::Vector<JoinScore*> > joinGroupList(alloc);
	util::Map<JoinNodeId, size_t> joinGroupMap(alloc);

	JoinNodeId lastJoinResultNodeId = static_cast<JoinNodeId>(joinNodeList.size() - 1);
	util::Vector<JoinNodeId> lastGraphJoinNodeIdList(alloc);
	util::Set<JoinNodeId> resultNodeSet(alloc);

	if (joinHintList.size() > 0) {
		JoinNodeId joinGroupId = static_cast<JoinNodeId>(joinGroupList.size());
		util::Vector<JoinScore*> groupScoreList(alloc);
		joinGroupList.push_back(groupScoreList);

		util::Vector<SQLHintInfo::ParsedHint>::const_iterator hintItr = joinHintList.begin();
		for (; hintItr != joinHintList.end(); ++hintItr) {
			SQLHintInfo::PlanNodeIdList::const_iterator idItr = hintItr->hintTableIdList_.begin();
			assert(hintItr->hintTableIdList_.size() > 1);
			if (hintItr->isTree_) {
				util::Deque<JoinNodeId> nodeIdStack(alloc);
				for (; idItr != hintItr->hintTableIdList_.end(); ++idItr) {
					if (*idItr != UNDEF_PLAN_NODEID) {
						nodeIdStack.push_back(*idItr);
					}
					else {
						JoinOperation joinOp;
						joinOp.hintType_ = JoinOperation::HINT_LEADING_TREE;
						assert(nodeIdStack.size() > 1);

						joinOp.nodeIdList_[1] = nodeIdStack.back();
						nodeIdStack.pop_back();
						resultNodeSet.insert(joinOp.nodeIdList_[1]);
						joinGroupMap.insert(std::make_pair(joinOp.nodeIdList_[1], joinGroupId));

						joinOp.nodeIdList_[0] = nodeIdStack.back();
						nodeIdStack.pop_back();
						resultNodeSet.insert(joinOp.nodeIdList_[0]);
						joinGroupMap.insert(std::make_pair(joinOp.nodeIdList_[0], joinGroupId));

						destJoinOpList.push_back(joinOp);

						++lastJoinResultNodeId;
						nodeIdStack.push_back(lastJoinResultNodeId);
					}
				}
				assert((nodeIdStack.size() < 3));
				if (nodeIdStack.size() == 2) {
					JoinOperation joinOp;
					joinOp.hintType_ = JoinOperation::HINT_LEADING_TREE;

					joinOp.nodeIdList_[1] = nodeIdStack.back();
					nodeIdStack.pop_back();
					resultNodeSet.insert(joinOp.nodeIdList_[1]);
					joinGroupMap.insert(std::make_pair(joinOp.nodeIdList_[1], joinGroupId));

					joinOp.nodeIdList_[0] = nodeIdStack.back();
					nodeIdStack.pop_back();
					resultNodeSet.insert(joinOp.nodeIdList_[0]);
					destJoinOpList.push_back(joinOp);
					joinGroupMap.insert(std::make_pair(joinOp.nodeIdList_[0], joinGroupId));

					++lastJoinResultNodeId;
				}
			}
			else {
				for (; idItr != hintItr->hintTableIdList_.end(); ++idItr) {
					JoinOperation joinOp;
					joinOp.hintType_ = JoinOperation::HINT_LEADING_LIST;
					if (idItr == hintItr->hintTableIdList_.begin()) {
						assert(resultNodeSet.find(*idItr) == resultNodeSet.end());
						joinOp.nodeIdList_[0] = *idItr;
						resultNodeSet.insert(*idItr);
						joinGroupMap.insert(std::make_pair(*idItr, joinGroupId));
						idItr++;
						assert(resultNodeSet.find(*idItr) == resultNodeSet.end());
						joinOp.nodeIdList_[1] = *idItr;
						resultNodeSet.insert(*idItr);
						joinGroupMap.insert(std::make_pair(*idItr, joinGroupId));
					}
					else {
						joinOp.nodeIdList_[0] = lastJoinResultNodeId; 
						assert(resultNodeSet.find(*idItr) == resultNodeSet.end());
						joinOp.nodeIdList_[1] = *idItr;
						resultNodeSet.insert(*idItr);
						joinGroupMap.insert(std::make_pair(*idItr, joinGroupId));
					}
					destJoinOpList.push_back(joinOp);
					++lastJoinResultNodeId;
				}
			}
		}
		util::Vector<JoinNodeId>::iterator orphanItr = orphanNodeList.begin();
		while (orphanItr != orphanNodeList.end()) {
			if (resultNodeSet.find(*orphanItr) != resultNodeSet.end()) {
				orphanItr = orphanNodeList.erase(orphanItr);
			}
			else {
				++orphanItr;
			}
		}
	}

	util::Set<JoinScoreKey> checkEdgeSet(alloc); 

	util::Vector<JoinEdge>::const_iterator edgeItr = joinEdgeList.begin();
	for (; edgeItr != joinEdgeList.end(); ++edgeItr) {
		JoinScoreKey key = (edgeItr->nodeIdList_[0] <= edgeItr->nodeIdList_[1]) ?
				std::make_pair(edgeItr->nodeIdList_[0], edgeItr->nodeIdList_[1]) :
				std::make_pair(edgeItr->nodeIdList_[1], edgeItr->nodeIdList_[0]);
		util::Set<JoinNodeId>::const_iterator rsItr = resultNodeSet.find(key.first);
		if (rsItr != resultNodeSet.end()) {
			if (edgeItr->nodeIdList_[1] == UNDEF_PLAN_NODEID
				|| resultNodeSet.find(key.second) != resultNodeSet.end()) {
				continue;
			}
		}
		JoinScoreMap::iterator mapItr = joinScoreMap.find(key);
		if (mapItr == joinScoreMap.end()) {
			JoinScore joinScore(alloc, key.first, key.second);
			result = joinScoreMap.insert(std::make_pair(key, joinScore));
			assert(result.second == true);
			mapItr = result.first;

			JoinScoreKey node0Key = std::make_pair(edgeItr->nodeIdList_[0], UNDEF_JOIN_NODEID);
			JoinScoreMap::iterator mapNode0Itr = joinScoreMap.find(node0Key);
			assert(mapNode0Itr != joinScoreMap.end());

			JoinScoreKey node1Key = std::make_pair(edgeItr->nodeIdList_[1], UNDEF_JOIN_NODEID);
			JoinScoreMap::iterator mapNode1Itr = joinScoreMap.find(node1Key);
			assert(mapNode1Itr != joinScoreMap.end());
			uint64_t approxTotalCount = JoinScore::multiplyWithUpperLimit(
					mapNode0Itr->second.getApproxTotalCount(),
					mapNode1Itr->second.getApproxTotalCount());
			mapItr->second.setApproxTotalCount(approxTotalCount);
			mapItr->second.setApproxFilteredCount(approxTotalCount);
		}
		mapItr->second.applyAnd(*edgeItr);
		if (edgeItr->nodeIdList_[1] != UNDEF_JOIN_NODEID) {
			JoinScoreKey key = (edgeItr->nodeIdList_[0] < edgeItr->nodeIdList_[1]) ?
					std::make_pair(edgeItr->nodeIdList_[0], edgeItr->nodeIdList_[1]) :
					std::make_pair(edgeItr->nodeIdList_[1], edgeItr->nodeIdList_[0]);
			checkEdgeSet.insert(key);
#ifndef NDEBUG
			key = std::make_pair(edgeItr->nodeIdList_[0], UNDEF_JOIN_NODEID);
			assert(joinScoreMap.find(key)!= joinScoreMap.end());

			key = std::make_pair(edgeItr->nodeIdList_[1], UNDEF_JOIN_NODEID);
			assert(joinScoreMap.find(key)!= joinScoreMap.end());
#endif /* NDEBUG */
		}
#ifndef NDEBUG
		else {
			key = std::make_pair(edgeItr->nodeIdList_[0], UNDEF_JOIN_NODEID);
			assert(joinScoreMap.find(key)!= joinScoreMap.end());
		}
#endif /* NDEBUG */
	}


	util::Deque<JoinScore*> joinScoreQueue(alloc); 
	JoinScoreMap::iterator mapItr = joinScoreMap.begin();
	for( ; mapItr != joinScoreMap.end(); mapItr++) {
		if (mapItr->first.second != UNDEF_JOIN_NODEID) {
			assert(mapItr->first.first != UNDEF_JOIN_NODEID);
#ifndef NDEBUG
			{
				std::pair<JoinNodeId, JoinNodeId> nodeIdPair = mapItr->second.getJoinNodeId();
				assert(nodeIdPair.first == mapItr->first.first
					   && nodeIdPair.second == mapItr->first.second);
			}
#endif
			JoinScore& pathScore = mapItr->second;
			JoinScoreKey key = std::make_pair(mapItr->first.first, UNDEF_JOIN_NODEID);
			JoinScoreMap::iterator nodeScoreItr =
					joinScoreMap.find(key);
			if (nodeScoreItr != joinScoreMap.end()) {
				pathScore = pathScore.applyAnd(nodeScoreItr->second);
			}
			else {
				assert(false);
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL,
						"NodeScore not found in joinScoreMap: nodeId=" << mapItr->first.first);
			}
			key = std::make_pair(mapItr->first.second, UNDEF_JOIN_NODEID);
			nodeScoreItr = joinScoreMap.find(key);
			if (nodeScoreItr != joinScoreMap.end()) {
				pathScore = pathScore.applyAnd(nodeScoreItr->second);
			}
			else {
				assert(false);
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL,
						"NodeScore not found in joinScoreMap: nodeId=" << mapItr->first.second);
			}
			joinScoreQueue.push_back(&pathScore);
		}
	}


	JoinNodeId startJoinNodeId = UNDEF_JOIN_NODEID;
	JoinScore maxScore(alloc);
	size_t minDegree = joinNodeIdEnd;

	util::Set<JoinNodeId> priorityNodeSet(alloc); 
	util::Set<JoinNodeId> candidateNodeSet(alloc); 
	for (joinNodeId = 0; joinNodeId < joinNodeIdEnd; ++joinNodeId) {
		JoinScoreMap::iterator mapItr =
		  joinScoreMap.find(std::make_pair(joinNodeId, UNDEF_JOIN_NODEID));
		assert(mapItr != joinScoreMap.end());
		JoinScore &joinScore = mapItr->second;
		size_t degree = joinScore.getDegree();

		if (degree != 0) {
			checkNodeSet.insert(joinNodeId);
			if (joinScore.getLevel() > 0) {
				priorityNodeSet.insert(joinNodeId);
			}
			if (minDegree == degree) {
				if (joinScore > maxScore) {
					startJoinNodeId = joinNodeId;
					maxScore = joinScore;
					candidateNodeSet.clear();
					candidateNodeSet.insert(joinNodeId);
				}
				else if (joinScore == maxScore) {
					candidateNodeSet.insert(joinNodeId);
				}
			}
			else if (degree < minDegree) {
				startJoinNodeId = joinNodeId;
				minDegree = degree;
				maxScore = joinScore;
				candidateNodeSet.clear();
				candidateNodeSet.insert(joinNodeId);
			}
		}
	}

	std::stable_sort(joinScoreQueue.begin(), joinScoreQueue.end(), &JoinScore::greater);


	if (candidateNodeSet.size() > 1) {
		util::Deque<JoinScore*>::iterator scoreItr = joinScoreQueue.begin();
		for (; scoreItr != joinScoreQueue.end(); ++scoreItr) {
			JoinScoreKey key = (*scoreItr)->getJoinNodeId();
			if (candidateNodeSet.find(key.first) != candidateNodeSet.end()) {
				startJoinNodeId = key.first;
				break;
			}
			else if (candidateNodeSet.find(key.second) != candidateNodeSet.end()) {
				startJoinNodeId = key.second;
				break;
			}
		}
	}

	if (startJoinNodeId != UNDEF_JOIN_NODEID) {
		util::Deque<JoinScore*>::iterator scoreItr = joinScoreQueue.begin();
		for (; scoreItr != joinScoreQueue.end(); ++scoreItr) {
			JoinScoreKey key = (*scoreItr)->getJoinNodeId();
			if (key.first == startJoinNodeId || key.second == startJoinNodeId) {
				assignJoinGroupId(alloc, key, **scoreItr, joinGroupMap, joinGroupList);
				priorityNodeSet.erase(key.first);
				priorityNodeSet.erase(key.second);
				scoreItr = joinScoreQueue.erase(scoreItr);
				break;
			}
		}
		assert(joinGroupList.size() != 0);
	}
	if (priorityNodeSet.size() > 0) {
		util::Deque<JoinScore*>::iterator scoreItr = joinScoreQueue.begin();
		while (scoreItr != joinScoreQueue.end()) {
			JoinScore &joinScore = **scoreItr;
			if (priorityNodeSet.size() == 0) {
				break;
			}
			JoinScoreKey key = joinScore.getJoinNodeId();
			if (priorityNodeSet.find(key.first) != priorityNodeSet.end()) {
				assignJoinGroupId(alloc, key, joinScore, joinGroupMap, joinGroupList);
				priorityNodeSet.erase(key.first);
				if (priorityNodeSet.find(key.second) != priorityNodeSet.end()) {
					priorityNodeSet.erase(key.second);
				}
				scoreItr = joinScoreQueue.erase(scoreItr);
				continue;
			}
			else if (priorityNodeSet.find(key.second) != priorityNodeSet.end()) {
				assignJoinGroupId(alloc, key, joinScore, joinGroupMap, joinGroupList);
				priorityNodeSet.erase(key.second);
				scoreItr = joinScoreQueue.erase(scoreItr);
				continue;
			}
			++scoreItr;
		}
	}

	util::Deque<JoinScore*>::iterator scoreItr = joinScoreQueue.begin();
	while (scoreItr != joinScoreQueue.end()) {
		scoreItr = joinScoreQueue.begin();
		for (; scoreItr != joinScoreQueue.end(); ++scoreItr) {
			JoinScore &score = **scoreItr;
			JoinScoreKey key = score.getJoinNodeId();
			assert(key.first != UNDEF_JOIN_NODEID);
			assert(key.second != UNDEF_JOIN_NODEID);
			if (joinGroupMap.find(key.first) == joinGroupMap.end()) {
				if (joinGroupMap.find(key.second) == joinGroupMap.end()) {
				}
				else {
					score.setScoreNodeId(0, key.first + 1);
				}
			}
			else {
				if (joinGroupMap.find(key.second) == joinGroupMap.end()) {
					score.setScoreNodeId(0, key.second + 1);
				}
				else {
				}
			}
		}
		std::stable_sort(joinScoreQueue.begin(), joinScoreQueue.end(), &JoinScore::greater);
		scoreItr = joinScoreQueue.begin();
		JoinScoreKey key = (*scoreItr)->getJoinNodeId();
		assignJoinGroupId(alloc, key, **scoreItr, joinGroupMap, joinGroupList);
		scoreItr = joinScoreQueue.erase(scoreItr);
	}
	for (size_t pos = 0; pos < joinGroupList.size(); ++pos) {
		util::Vector<JoinScore*> &scoreList = joinGroupList[pos];
		if (scoreList.size() == 0) {
			continue;
		}
		util::Vector<JoinScore*>::iterator itr = scoreList.begin();
		for (; itr != scoreList.end(); ++itr) {
			JoinOperation joinOp;
			std::pair<JoinNodeId, JoinNodeId> nodeIdPair = (*itr)->getJoinNodeId();
			if (resultNodeSet.find(nodeIdPair.first) == resultNodeSet.end()) {
				if (resultNodeSet.find(nodeIdPair.second) == resultNodeSet.end()) {
					joinOp.nodeIdList_[0] = nodeIdPair.first;
					joinOp.nodeIdList_[1] = nodeIdPair.second;
					resultNodeSet.insert(nodeIdPair.first);
					resultNodeSet.insert(nodeIdPair.second);
				}
				else {
					joinOp.nodeIdList_[0] = lastJoinResultNodeId; 
					joinOp.nodeIdList_[1] = nodeIdPair.first;
					resultNodeSet.insert(nodeIdPair.first);
				}
			}
			else {
				if (resultNodeSet.find(nodeIdPair.second) == resultNodeSet.end()) {
					joinOp.nodeIdList_[0] = lastJoinResultNodeId; 
					joinOp.nodeIdList_[1] = nodeIdPair.second;
					resultNodeSet.insert(nodeIdPair.second);
				}
				else {
					continue;
				}
			}
			destJoinOpList.push_back(joinOp);
			++lastJoinResultNodeId;
		}
		lastGraphJoinNodeIdList.push_back(lastJoinResultNodeId);
	}

	if (lastGraphJoinNodeIdList.size() > 1) {
		JoinNodeId prev = lastGraphJoinNodeIdList[0];
		for (size_t pos = 1; pos < lastGraphJoinNodeIdList.size(); ++pos) {
			JoinOperation joinOp;
			joinOp.nodeIdList_[0] = prev;
			joinOp.nodeIdList_[1] = lastGraphJoinNodeIdList[pos];
			destJoinOpList.push_back(joinOp);
			++lastJoinResultNodeId;
			prev = lastJoinResultNodeId;
		}
	}

	if (!orphanNodeList.empty()) {
		util::Deque<JoinScore*> scoreList(alloc);
		util::Vector<JoinNodeId>::const_iterator nodeIdItr = orphanNodeList.begin();
		for ( ; nodeIdItr != orphanNodeList.end(); ++nodeIdItr) {
			JoinScoreKey key = std::make_pair(*nodeIdItr, UNDEF_JOIN_NODEID);
			JoinScoreMap::iterator scoreMapItr =
			  joinScoreMap.find(key);
			assert(scoreMapItr != joinScoreMap.end());
			scoreList.push_back(&scoreMapItr->second);
		}
		std::stable_sort(scoreList.begin(), scoreList.end(), &JoinScore::greater);
		util::Deque<JoinScore*>::const_iterator scoreListItr = scoreList.begin();
		for ( ; scoreListItr != scoreList.end(); ++scoreListItr) {
			JoinOperation joinOp;
			JoinNodeId nodeId = (*scoreListItr)->getJoinNodeId().first;
			if (resultNodeSet.find(nodeId) != resultNodeSet.end()) {
				continue;
			}
			if (destJoinOpList.size() > 0) {
				joinOp.nodeIdList_[0] = lastJoinResultNodeId; 
				joinOp.nodeIdList_[1] = nodeId;
				destJoinOpList.push_back(joinOp);
			}
			else {
				if (orphanNodeList.size() > 1) {
					joinOp.nodeIdList_[0] = nodeId;
					resultNodeSet.insert(nodeId);
					++scoreListItr;
					nodeId = (*scoreListItr)->getJoinNodeId().first;
					joinOp.nodeIdList_[1] = nodeId;
					destJoinOpList.push_back(joinOp);
				}
				else {
					assert(false);
					GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
				}
			}
			resultNodeSet.insert(nodeId);
			++lastJoinResultNodeId;
		}
	}


	bool reordered;
	if (srcJoinOpList.size() == destJoinOpList.size()) {
		reordered = false;
		util::Vector<JoinOperation>::const_iterator srcItr = srcJoinOpList.begin();
		util::Vector<JoinOperation>::const_iterator resultItr = destJoinOpList.begin();
		for( ; srcItr != srcJoinOpList.end(); ++srcItr, ++resultItr) {
			if (srcItr->nodeIdList_[0] != resultItr->nodeIdList_[0]
					|| srcItr->nodeIdList_[1] != resultItr->nodeIdList_[1]
					|| srcItr->type_ != resultItr->type_) {
				reordered = true;
				break;
			}
		}
	}
	else {
		reordered = true;
	}

	if (profiling) {
		SQLPreparedPlan::OptProfile::Join *profile =
				ALLOC_NEW(alloc) SQLPreparedPlan::OptProfile::Join();
		profile->reordered_ = reordered;
		profile->costBased_ = false;
		destJoinOpList.back().profile_ = profile;
	}

	return reordered;
}

void SQLCompiler::assignJoinGroupId(
		util::StackAllocator &alloc,
		const JoinScoreKey &key, JoinScore &joinScore,
		util::Map<JoinNodeId, size_t> &joinGroupMap,
		util::Vector< util::Vector<JoinScore*> > &joinGroupList) {

	util::Map<JoinNodeId, size_t>::iterator itr1 = joinGroupMap.find(key.first);
	util::Map<JoinNodeId, size_t>::iterator itr2 = joinGroupMap.find(key.second);
	if (itr1 == joinGroupMap.end()) {
		if (itr2 == joinGroupMap.end()) {
			JoinNodeId joinGroupId = static_cast<JoinNodeId>(joinGroupList.size());
			joinGroupMap.insert(std::make_pair(key.first, joinGroupId));
			joinGroupMap.insert(std::make_pair(key.second, joinGroupId));
			util::Vector<JoinScore*> groupScoreList(alloc);
			groupScoreList.push_back(&joinScore);
			joinGroupList.push_back(groupScoreList);
		}
		else {
			joinGroupMap.insert(std::make_pair(key.first, itr2->second));
			joinGroupList[itr2->second].push_back(&joinScore);
		}
	}
	else {
		if (itr2 == joinGroupMap.end()) {
			joinGroupMap.insert(std::make_pair(key.second, itr1->second));
			joinGroupList[itr1->second].push_back(&joinScore);
		}
		else {
			if (itr1->second != itr2->second) {
			}
			else {
			}
		}
	}
}

void SQLCompiler::dumpJoinScoreList(
		std::ostream &os, const util::Deque<JoinScore*>& joinScoreQueue) {
	util::Deque<JoinScore*>::const_iterator scoreItr = joinScoreQueue.begin();
	for (; scoreItr != joinScoreQueue.end(); ++scoreItr) {
		const JoinScore &score = **scoreItr;
		JoinScoreKey key = score.getJoinNodeId();
		JoinScoreKey key2 = score.getScoreNodeId();
		os << "(" << key.first << "," << key.second << "), (" <<
				key2.first << "," << key2.second << "): ";
		score.dumpScore(os);
		os << std::endl;
	}
}


const int32_t JDBCDatabaseMetadata::TYPE_NO_NULLS = 0;	
const int32_t JDBCDatabaseMetadata::TYPE_NULLABLE = 1; 
const int32_t JDBCDatabaseMetadata::TYPE_NULLABLE_UNKNOWN = 2; 


const int32_t JDBCDatabaseMetadata::TYPE_PRED_NONE = 0; 
const int32_t JDBCDatabaseMetadata::TYPE_PRED_CHAR = 1; 
const int32_t JDBCDatabaseMetadata::TYPE_PRED_BASIC = 2; 
const int32_t JDBCDatabaseMetadata::TYPE_SEARCHABLE = 3; 


const int16_t JDBCDatabaseMetadata::TABLE_INDEX_STATISTIC = 0; 
const int16_t JDBCDatabaseMetadata::TABLE_INDEX_CLUSTERED = 1; 
const int16_t JDBCDatabaseMetadata::TABLE_INDEX_HASHED = 2; 
const int16_t JDBCDatabaseMetadata::TABLE_INDEX_OTHER = 3; 


struct SQLHint::Coder::NameTable {
public:
	NameTable();
	~NameTable();

	const char8_t* toString(Id id) const;
	bool find(const char8_t *name, Id &id) const;

private:
	typedef std::pair<const char8_t*, Id> Entry;

	enum {
		TYPE_COUNT = END_ID
	};

	struct EntryPred {
		bool operator()(const Entry &entry1, const Entry &entry2) const;
	};

	void add(Id id, const char8_t *name);

	const char8_t *idList_[TYPE_COUNT];
	Entry entryList_[TYPE_COUNT];
	Entry *entryEnd_;
};

const SQLHint::Coder::NameTable SQLHint::Coder::nameTable_;

SQLHint::Coder::NameTable::NameTable() : entryEnd_(entryList_) {
	std::fill(
			idList_, idList_ + TYPE_COUNT, static_cast<const char8_t*>(NULL));

#define SQL_HINT_CODER_NAME_TABLE_ADD(id) add(id, #id)
	SQL_HINT_CODER_NAME_TABLE_ADD(START_ID);
	SQL_HINT_CODER_NAME_TABLE_ADD(MAX_DEGREE_OF_PARALLELISM);
	SQL_HINT_CODER_NAME_TABLE_ADD(MAX_DEGREE_OF_EXPANSION);
	SQL_HINT_CODER_NAME_TABLE_ADD(MAX_GENERATED_ROWS);
	SQL_HINT_CODER_NAME_TABLE_ADD(DISTRIBUTED_POLICY);
	SQL_HINT_CODER_NAME_TABLE_ADD(INDEX_SCAN);
	SQL_HINT_CODER_NAME_TABLE_ADD(NO_INDEX_SCAN);
	SQL_HINT_CODER_NAME_TABLE_ADD(INDEX_JOIN);
	SQL_HINT_CODER_NAME_TABLE_ADD(NO_INDEX_JOIN);
	SQL_HINT_CODER_NAME_TABLE_ADD(LEADING);
	SQL_HINT_CODER_NAME_TABLE_ADD(LEGACY_PLAN);
	SQL_HINT_CODER_NAME_TABLE_ADD(COST_BASED_JOIN);
	SQL_HINT_CODER_NAME_TABLE_ADD(NO_COST_BASED_JOIN);
	SQL_HINT_CODER_NAME_TABLE_ADD(COST_BASED_JOIN_DRIVING);
	SQL_HINT_CODER_NAME_TABLE_ADD(NO_COST_BASED_JOIN_DRIVING);
	SQL_HINT_CODER_NAME_TABLE_ADD(OPTIMIZER_FAILURE_POINT);
	SQL_HINT_CODER_NAME_TABLE_ADD(TABLE_ROW_COUNT);
#undef SQL_HINT_CODER_NAME_TABLE_ADD

	std::sort(entryList_, entryEnd_, EntryPred());
}

SQLHint::Coder::NameTable::~NameTable() {
	for (size_t pos = 0; pos < TYPE_COUNT; ++pos) {
		delete [] idList_[pos];
		idList_[pos] = NULL;
	}
}

const char8_t* SQLHint::Coder::NameTable::toString(Id id) const {
	if (id < 0 || static_cast<ptrdiff_t>(id) >= TYPE_COUNT) {
		return NULL;
	}

	return idList_[id];
}

bool SQLHint::Coder::NameTable::find(const char8_t *name, Id &id) const {
	const Entry key(name, Id());
	const std::pair<const Entry*, const Entry*> &range =
			std::equal_range<const Entry*>(
					entryList_, entryEnd_, key, EntryPred());

	if (range.first == range.second) {
		id = key.second;
		return false;
	}

	id = range.first->second;
	return true;
}

void SQLHint::Coder::NameTable::add(Id id, const char8_t *name) {
	if (id < 0 || static_cast<ptrdiff_t>(id) >= TYPE_COUNT ||
			entryEnd_ - entryList_ >= TYPE_COUNT) {
		assert(false);
		return;
	}
	const char8_t *src = name;
	char8_t *destTop = UTIL_NEW char8_t[strlen(name) + 1];
	char8_t *dest = destTop;
	bool toUpper = true;
	for (; *src != '\0'; ++src) {
		if (*src == '_') {
			toUpper = true;
			continue;
		}
		else if (toUpper) {
			*dest = (*src >= 'a' && *src <= 'z') ? static_cast<char8_t>(*src + 'A'-'a') : *src;
			++dest;
			toUpper = false;
		}
		else {
			*dest = (*src >= 'A' && *src <= 'Z') ? static_cast<char8_t>(*src + 'a'-'A') : *src;
			++dest;
		}
	}
	*dest = '\0';
	idList_[id] = destTop;

	entryEnd_->first = destTop;
	entryEnd_->second = id;
	entryEnd_++;
}

bool SQLHint::Coder::NameTable::EntryPred::operator()(
		const Entry &entry1, const Entry &entry2) const {
	return SQLProcessor::ValueUtils::strICmp(entry1.first, entry2.first) < 0;
}

const char8_t* SQLHint::Coder::operator()(Id id) const {
	return nameTable_.toString(id);
}

bool SQLHint::Coder::operator()(const char8_t *name, Id &id) const {
	return nameTable_.find(name, id);
}


const SQLHintInfo::HintTableId SQLHintInfo::START_HINT_TABLEID = 0;
const SQLHintInfo::HintTableId SQLHintInfo::UNDEF_HINT_TABLEID =
		std::numeric_limits<SQLHintInfo::HintTableId>::max();

SQLHintInfo::SQLHintInfo(util::StackAllocator &alloc) :
		alloc_(alloc),
		hintExprMap_(alloc),
		statisticalHintMap_(alloc),
		hintTableIdMap_(alloc),
		joinHintMap_(alloc),
		tableInfoIdMap_(alloc),
		scanHintMap_(alloc),
		joinMethodHintMap_(alloc),
		symbolHintList_(alloc),
		reportLevel_(REPORT_WARNING) { 
}

SQLHintInfo::~SQLHintInfo() {
	HintExprMap::iterator itr = hintExprMap_.begin();
	for ( ; itr != hintExprMap_.end(); ++itr) {
		ALLOC_DELETE(alloc_, itr->second);
		itr->second = NULL;
	}
}

void SQLHintInfo::parseStatisticalHint(SQLHint::Id hintId, const Expr &hintExpr) {
	SQLHint::Coder hintCoder;
	const char8_t *hintKey = hintCoder(hintId);
	if (hintExpr.next_->size() > 2) { 
		util::NormalOStringStream strstrm;
		strstrm << "Too many arguments to hint(" << hintKey << ")";
		reportWarning(strstrm.str().c_str());
		return;
	}
	const ExprList &hintArgs = *hintExpr.next_;
	const Expr &hintArg0 = *hintArgs[0];
	StatisticalHintKey key =
			std::make_pair(util::String(alloc_), util::String(alloc_));
	if (hintArg0.op_ == SQLType::EXPR_COLUMN) {
		assert(hintArg0.qName_ != NULL);
		const util::String* tableStr = hintArg0.qName_->table_;
		const util::String* nameStr = hintArg0.qName_->name_;
		assert(nameStr != NULL);
		if (tableStr == NULL) {
			key.first = normalizeName(alloc_, nameStr->c_str());
		}
		else {
			key.first = normalizeName(alloc_, tableStr->c_str());
			key.second = normalizeName(alloc_, nameStr->c_str());
		}
	}
	else {
		util::NormalOStringStream strstrm;
		strstrm << "Invalid argument supplied for hint (" << hintKey << ")";
		reportWarning(strstrm.str().c_str());
		return;
	}
	const Expr &hintArg1 = *hintArgs[1];
	if (hintArg1.op_ == SQLType::EXPR_CONSTANT) {
	}
	SQLHintValue hintValue = SQLHintValue(hintId);
	switch(hintId) {
	case SQLHint::TABLE_ROW_COUNT:
		if (hintArg1.value_.getType() == TupleList::TYPE_LONG) {
			hintValue.setInt64(hintArg1.value_.get<int64_t>());
		}
		else {
			util::NormalOStringStream strstrm;
			strstrm << "Invalid argument supplied for hint (" << hintKey << ")";
			reportWarning(strstrm.str().c_str());
			return;
		}
		break;
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}
	StatisticalHintMap::iterator itr = statisticalHintMap_.find(key);
	if (itr == statisticalHintMap_.end()) {
		util::Vector<SQLHintValue> valueList(alloc_);
		valueList.push_back(hintValue);
		statisticalHintMap_.insert(std::make_pair(key, valueList));
	}
	else {
		itr->second.push_back(hintValue);
	}
}

bool SQLHintInfo::checkHintArguments(SQLHint::Id hintId, const Expr &hintExpr) const {
	switch(hintId) {
	case SQLHint::MAX_DEGREE_OF_PARALLELISM:
	case SQLHint::MAX_DEGREE_OF_TASK_INPUT:
	case SQLHint::MAX_GENERATED_ROWS:
		{
			if (!checkArgCount(hintExpr, 1, false) ||
				!checkArgSQLOpType(hintExpr, SQLType::EXPR_CONSTANT) ||
				!checkAllArgsValueType(hintExpr, TupleList::TYPE_LONG, 1)) {
				return false;
			}
		}
		break;
	case SQLHint::MAX_DEGREE_OF_EXPANSION:
		{
			if (!checkArgCount(hintExpr, 1, false) ||
				!checkArgSQLOpType(hintExpr, SQLType::EXPR_CONSTANT) ||
				!checkAllArgsValueType(hintExpr, TupleList::TYPE_LONG, 0)) {
				return false;
			}
		}
		break;
	case SQLHint::DISTRIBUTED_POLICY:
		{
			if (!checkArgCount(hintExpr, 1, false) ||
				!checkArgSQLOpType(hintExpr, SQLType::EXPR_CONSTANT) ||
				!checkAllArgsValueType(hintExpr, TupleList::TYPE_STRING)) {
				return false;
			}
		}
		break;
	case SQLHint::TASK_ASSIGNMENT:
		break;
	case SQLHint::INDEX_SCAN:
	case SQLHint::NO_INDEX_SCAN:
		{
			if (!checkArgCount(hintExpr, 1, false) ||
				!checkArgSQLOpType(hintExpr, SQLType::EXPR_COLUMN) ||
				!checkArgIsTable(hintExpr, 0)) {
				return false;
			}
		}
		break;
	case SQLHint::INDEX_JOIN:
	case SQLHint::NO_INDEX_JOIN:
		{
			if (!checkArgCount(hintExpr, 2, false) ||
				!checkArgSQLOpType(hintExpr, SQLType::EXPR_COLUMN) ||
				!checkArgIsTable(hintExpr, 0) ||
				!checkArgIsTable(hintExpr, 1)) {
				return false;
			}
		}
		break;
	case SQLHint::LEADING:
		{
			if (!checkArgCount(hintExpr, 1, true)) {
				return false;
			}
			if (hintExpr.next_->size() == 1) {
				if (!checkLeadingTreeArgumentsType(*hintExpr.next_->at(0))) {
					return false;
				}
			}
			else {
				assert(hintExpr.next_->size() > 1);
				if (!checkArgSQLOpType(hintExpr, SQLType::EXPR_COLUMN)) {
					return false;
				}
				for (size_t pos = 0; pos < hintExpr.next_->size(); ++pos) {
					if (!checkArgIsTable(hintExpr, pos)) {
						return false;
					}
				}
			}
		}
		break;
	case SQLHint::LEGACY_PLAN:
		{
			if (!checkArgCount(hintExpr, 2, true) ||
					!checkArgSQLOpType(hintExpr, SQLType::EXPR_CONSTANT) ||
					!checkAllArgsValueType(
							hintExpr, TupleList::TYPE_LONG,
							0, std::numeric_limits<int32_t>::max())) {
				return false;
			}
		}
		break;
	case SQLHint::COST_BASED_JOIN:
		if (!checkArgCount(hintExpr, 0, false)) {
			return false;
		}
		break;
	case SQLHint::NO_COST_BASED_JOIN:
		if (!checkArgCount(hintExpr, 0, false)) {
			return false;
		}
		break;
	case SQLHint::COST_BASED_JOIN_DRIVING:
		if (!checkArgCount(hintExpr, 0, false)) {
			return false;
		}
		break;
	case SQLHint::NO_COST_BASED_JOIN_DRIVING:
		if (!checkArgCount(hintExpr, 0, false)) {
			return false;
		}
		break;
	case SQLHint::OPTIMIZER_FAILURE_POINT:
		{
			if (!checkArgCount(hintExpr, 2, false) ||
				!checkArgIsTable(hintExpr, 0, true) ||
				!checkArgValueType(hintExpr, 1, TupleList::TYPE_LONG, 0, 1)) {
				return false;
			}
		}
		break;
	case SQLHint::TABLE_ROW_COUNT:
		{
			if (!checkArgCount(hintExpr, 2, false) ||
				!checkArgIsTable(hintExpr, 0) ||
				!checkArgValueType(hintExpr, 1, TupleList::TYPE_LONG, 1)) {
				return false;
			}
		}
		break;
	default:
		assert(false);
		break;
	}
	return true;
}

void SQLHintInfo::makeHintMap(const SyntaxTree::ExprList *hintList) {
	if (hintList) {
		SyntaxTree::ExprList::const_iterator itr = hintList->begin();
		for ( ; itr != hintList->end(); ++itr) {
			const Expr *hintExpr = *itr;
			assert(hintExpr && hintExpr->op_ == SQLType::EXPR_LIST);
			assert(hintExpr->next_);
			assert(hintExpr->qName_ && hintExpr->qName_->name_);
			util::String *hintKey = hintExpr->qName_->name_;
			if (hintKey) {
				SQLHint::Coder hintCoder;
				SQLHint::Id hintId;
				hintCoder(hintKey->c_str(), hintId);
				if (hintId != SQLHint::START_ID && hintId != SQLHint::END_ID) {
					HintExprMap::iterator itr = hintExprMap_.find(hintId);
					if (itr == hintExprMap_.end()) {
						HintExprArray* hintExprArray =
								ALLOC_NEW(alloc_) HintExprArray(alloc_);
						std::pair<HintExprMap::iterator, bool> retVal;
						retVal = hintExprMap_.insert(
								std::make_pair(hintId, hintExprArray));
						assert(retVal.second);
					}
					else {
						switch(hintId) {
						case SQLHint::MAX_DEGREE_OF_PARALLELISM:
						case SQLHint::MAX_DEGREE_OF_TASK_INPUT:
						case SQLHint::MAX_DEGREE_OF_EXPANSION:
						case SQLHint::MAX_GENERATED_ROWS:
						case SQLHint::DISTRIBUTED_POLICY:
						case SQLHint::TASK_ASSIGNMENT:
						case SQLHint::LEGACY_PLAN:
						case SQLHint::COST_BASED_JOIN:
						case SQLHint::NO_COST_BASED_JOIN:
						case SQLHint::COST_BASED_JOIN_DRIVING:
						case SQLHint::NO_COST_BASED_JOIN_DRIVING:
							{
								util::NormalOStringStream strstrm;
								strstrm << "Hint(" << hintKey->c_str() <<
										") specified more than once";
								reportWarning(strstrm.str().c_str());
								continue;
							}
							break;
						case SQLHint::INDEX_SCAN:
						case SQLHint::NO_INDEX_SCAN:
						case SQLHint::INDEX_JOIN:
						case SQLHint::NO_INDEX_JOIN:
						case SQLHint::LEADING:
						case SQLHint::OPTIMIZER_FAILURE_POINT:
						case SQLHint::TABLE_ROW_COUNT:
						default:
							break;
						}
					}
					if (!checkHintArguments(hintId, *hintExpr)) {
						continue;
					}
					hintExprMap_[hintId]->push_back(hintExpr);
					if (hintId >= SQLHint::TABLE_ROW_COUNT
						&& hintId < SQLHint::END_ID) {
						parseStatisticalHint(hintId, *hintExpr);
					}

					if (hintId == SQLHint::OPTIMIZER_FAILURE_POINT) {
						parseSymbolHint(hintId, hintExpr);
					}
				}
				else {
					util::NormalOStringStream strstrm;
					strstrm << "Hint(" << hintKey->c_str() <<
							") is not supported";
					reportWarning(strstrm.str().c_str());
					continue;
				}
			}
		}
		HintExprMap::iterator hintMapItr = hintExprMap_.begin();
		while(hintMapItr != hintExprMap_.end()) {
			if (hintMapItr->second->size() == 0) {
				ALLOC_DELETE(alloc_, hintMapItr->second);
				hintMapItr->second = NULL;
				hintExprMap_.erase(hintMapItr++);
			}
			else {
				++hintMapItr;
			}
		}
		checkExclusiveHints(
				SQLHint::COST_BASED_JOIN, SQLHint::NO_COST_BASED_JOIN);
		checkExclusiveHints(
				SQLHint::COST_BASED_JOIN_DRIVING,
				SQLHint::NO_COST_BASED_JOIN_DRIVING);
	}
}

void SQLCompiler::makeJoinHintMap(Plan &plan) {
	typedef uint32_t HintTableId;

	if (plan.hintInfo_ == NULL) {
		return;
	}

	SQLHintInfo *hintInfo = ALLOC_NEW(alloc_) SQLHintInfo(*plan.hintInfo_);
	HintTableId hintTableId = SQLHintInfo::START_HINT_TABLEID;
	for (SQLPreparedPlan::NodeList::const_iterator itr = plan.nodeList_.begin();
			itr != plan.nodeList_.end(); ++itr) {
		if (itr->qName_ && itr->qName_->table_) {
			const SyntaxTree::QualifiedName &qName = *itr->qName_;
			hintInfo->insertHintTableIdMap(*qName.table_, hintTableId);

			++hintTableId;
		}
		if (itr->type_ == SQLType::EXEC_SCAN) {
			const SQLTableInfo& tableInfo = getTableInfoList().resolve(itr->tableIdInfo_.id_);
			hintInfo->insertHintTableIdMap(tableInfo.tableName_, hintTableId);

			++hintTableId;

			hintInfo->insertTableInfoIdMap(tableInfo.tableName_, itr->tableIdInfo_.id_);
		}
	}
	hintInfo->makeJoinHintMap(plan);
	plan.hintInfo_ = hintInfo;
}

SQLHintInfo::HintTableId SQLHintInfo::getHintTableId(const util::String& name) const {
	const util::String &normName = normalizeName(alloc_, name.c_str());
	HintTableIdMap::const_iterator hintTableIdMapItr = hintTableIdMap_.find(normName);

	if (hintTableIdMapItr == hintTableIdMap_.end()) {
		return UNDEF_HINT_TABLEID;
	}
	else {
		return hintTableIdMapItr->second.id_;
	}
}

void SQLHintInfo::parseSymbolHint(SQLHint::Id hintId, const Expr *hintExpr) {
	assert(hintExpr != NULL);

	ParsedHint parsedHint(alloc_);
	parsedHint.hintId_ = hintId;
	parsedHint.hintExpr_ = hintExpr;

	do {
		const ExprList *exprList = hintExpr->next_;
		if (exprList == NULL || exprList->empty()) {
			break;
		}

		const Expr *argExpr = exprList->front();
		if (argExpr == NULL) {
			break;
		}

		const SyntaxTree::QualifiedName *name = argExpr->qName_;
		if (name == NULL) {
			break;
		}

		util::NameCoderOptions options;
		options.setCaseSensitive(false);

		size_t pos = 0;
		const size_t count = SymbolList::SYMBOL_LIST_SIZE;
		for (; pos < count; pos++) {
			const util::String *nameElem = (
					pos == 0 ? name->db_ :
					(pos == 1 ? name->table_ : name->name_));
			if (nameElem == NULL) {
				break;
			}

			StringConstant str;
			if (!SQLPreparedPlan::Constants::CONSTANT_CODER(
					nameElem->c_str(), str, options)) {
				break;
			}
			parsedHint.symbolList_.setValue(pos, str);
		}

		if (pos != count) {
			break;
		}
		symbolHintList_.push_back(parsedHint);
		return;
	}
	while (false);

	util::String token(alloc_);
	util::NormalOStringStream strstrm;
	assert(hintExpr->qName_ && hintExpr->qName_->name_);
	strstrm << "Invalid symbol argument for hint(" <<
			hintExpr->qName_->name_->c_str() << ")";
	getExprToken(*hintExpr, token);
	if (token.size() > 0) {
		strstrm << " at or near \"" << token.c_str() << "\"";
	}
	reportWarning(strstrm.str().c_str());
}

SQLHintInfo::HintTableId SQLHintInfo::getHintTableId(const Expr &hintArgExpr) const {
	assert(hintArgExpr.qName_ && hintArgExpr.qName_->name_);
	const SyntaxTree::QualifiedName &qName = *hintArgExpr.qName_;
	const util::String &normName = normalizeName(alloc_, qName.name_->c_str());
	HintTableIdMap::const_iterator hintTableIdMapItr = hintTableIdMap_.find(normName);


	if (hintTableIdMapItr == hintTableIdMap_.end()) {
		return UNDEF_HINT_TABLEID;
	}
	else {
		if (qName.nameCaseSensitive_) {
			if (strcmp(qName.name_->c_str(), hintTableIdMapItr->second.origName_.c_str()) != 0) {
				return UNDEF_HINT_TABLEID;
			}
		}
		return hintTableIdMapItr->second.id_;
	}
}

SQLHintInfo::HintTableId SQLHintInfo::getTableInfoId(const Expr &hintArgExpr) const {
	assert(hintArgExpr.qName_ && hintArgExpr.qName_->name_);
	const SyntaxTree::QualifiedName &qName = *hintArgExpr.qName_;
	const util::String &normName = normalizeName(alloc_, qName.name_->c_str());

	TableInfoIdMap::const_iterator tableInfoIdMapItr = tableInfoIdMap_.find(normName);


	if (tableInfoIdMapItr == tableInfoIdMap_.end()) {
		return UNDEF_HINT_TABLEID;
	}
	else {
		if (qName.nameCaseSensitive_) {
			if (strcmp(qName.name_->c_str(), tableInfoIdMapItr->second.origName_.c_str()) != 0) {
				return UNDEF_HINT_TABLEID;
			}
		}
		return tableInfoIdMapItr->second.id_;
	}
}

bool SQLHintInfo::resolveHintTableTreeArg(
		const Expr &startExpr, HintTableIdList &hintTableIdList) {
	if (startExpr.op_ != SQLType::EXPR_LIST) {
		reportWarning("Illegal argument for Hint(Leading)");
		return false;
	}
	if (startExpr.next_->size() == 2) {
		for (size_t pos = 0; pos < 2; ++pos) {
			const Expr *nextArg = (*startExpr.next_)[pos];
			assert(nextArg);
			if (nextArg->op_ == SQLType::EXPR_LIST) {
				bool result = resolveHintTableTreeArg(*nextArg, hintTableIdList);
				if (!result) {
					return result;
				}
				hintTableIdList.push_back(UNDEF_HINT_TABLEID);
			}
			else if (nextArg->op_ == SQLType::EXPR_COLUMN) {
				if (nextArg->qName_ == NULL
					|| nextArg->qName_->table_ != NULL
					|| nextArg->qName_->name_ == NULL) {
					reportWarning("Illegal argument for Hint(Leading)");
					return false;
				}
				HintTableId hintTableId = getHintTableId(*nextArg);
				if (hintTableId == UNDEF_HINT_TABLEID) {
					return false;
				}
				hintTableIdList.push_back(hintTableId);
			}
			else {
				reportWarning("Illegal argument for Hint(Leading)");
				return false;
			}
		}
		return true;
	}
	else if (startExpr.next_->size() == 1 && hintTableIdList.size() == 0) {
		const Expr *nextArg = (*startExpr.next_)[0];
		assert(nextArg);
		bool result = resolveHintTableTreeArg(*nextArg, hintTableIdList);
		hintTableIdList.push_back(UNDEF_HINT_TABLEID);
		return result;
	}
	else {
		util::NormalOStringStream strstrm;
		if (startExpr.next_->size() > 2) {
			strstrm << "Too many arguments(";
		}
		else {
			strstrm << "Too few arguments(";
		}
		strstrm << startExpr.next_->size() << ") for Hint(Leading)";
		reportWarning(strstrm.str().c_str());
		return false;
	}
}

bool SQLHintInfo::resolveHintTableArg(
		const Expr &oneHintExpr, HintTableIdList &hintTableIdList, bool &isTree) {
	if (oneHintExpr.next_ == NULL || oneHintExpr.next_->size() == 0) {
		util::NormalOStringStream strstrm;
		assert(oneHintExpr.qName_ && oneHintExpr.qName_->name_);
		strstrm << "Illegal argument for Hint(" <<
				oneHintExpr.qName_->name_->c_str() << ")";
		reportWarning(strstrm.str().c_str());
		return false;
	}
	isTree = false;
	bool succeeded = true;
	ExprList::const_iterator hintArgItr = oneHintExpr.next_->begin();
	const Expr *hintArgExpr = *hintArgItr;
	if (hintArgExpr->op_ == SQLType::EXPR_COLUMN) {
		for (; hintArgItr != oneHintExpr.next_->end(); ++hintArgItr) {
			hintArgExpr = *hintArgItr;
			assert(hintArgExpr != NULL);
			if (hintArgExpr->op_ != SQLType::EXPR_COLUMN
				|| hintArgExpr->qName_ == NULL
				|| hintArgExpr->qName_->table_ != NULL
				|| hintArgExpr->qName_->name_ == NULL) {
				reportWarning("Illegal argument for Hint(Leading)");
				succeeded = false;
				break;
			}
			HintTableId hintTableId = getHintTableId(*hintArgExpr);
			if (hintTableId == UNDEF_HINT_TABLEID) {
				succeeded = false;
				break;
			}
			hintTableIdList.push_back(hintTableId);
		}
	}
	else if (hintArgExpr->op_ == SQLType::EXPR_LIST) {
		isTree = true;
		return resolveHintTableTreeArg(
			*hintArgExpr, hintTableIdList);
	}
	else {
		reportWarning("Illegal argument for Hint(Leading)");
		succeeded = false;
	}
	return succeeded;
}

bool SQLHintInfo::resolveJoinMethodHintArg(
		const Expr &oneHintExpr, JoinMethodHintKey &hintKey) {
	if (oneHintExpr.next_ == NULL || oneHintExpr.next_->size() == 0) {
		util::NormalOStringStream strstrm;
		assert(oneHintExpr.qName_ && oneHintExpr.qName_->name_);
		strstrm << "Illegal argument for Hint(" <<
				oneHintExpr.qName_->name_->c_str() << ")";
		reportWarning(strstrm.str().c_str());
		return false;
	}
	bool succeeded = true;
	hintKey = std::make_pair(UNDEF_HINT_TABLEID, UNDEF_HINT_TABLEID);
	ExprList::const_iterator hintArgItr = oneHintExpr.next_->begin();
	const Expr *hintArgExpr = *hintArgItr;
	if (hintArgExpr->op_ == SQLType::EXPR_COLUMN) {
		for (; hintArgItr != oneHintExpr.next_->end(); ++hintArgItr) {
			hintArgExpr = *hintArgItr;
			assert(hintArgExpr != NULL);
			if (hintArgExpr->op_ != SQLType::EXPR_COLUMN
				|| hintArgExpr->qName_ == NULL
				|| hintArgExpr->qName_->table_ != NULL
				|| hintArgExpr->qName_->name_ == NULL) {
				util::NormalOStringStream strstrm;
				assert(oneHintExpr.qName_ && oneHintExpr.qName_->name_);
				strstrm << "Illegal argument for Hint(" <<
						oneHintExpr.qName_->name_->c_str() << ")";
				reportWarning(strstrm.str().c_str());
				succeeded = false;
				break;
			}
			HintTableId hintTableId = getHintTableId(*hintArgExpr);
			if (hintTableId == UNDEF_HINT_TABLEID) {
				succeeded = false;
				break;
			}
			if (hintArgItr == oneHintExpr.next_->begin()) {
				hintKey.first = hintTableId;
			}
			else {
				hintKey.second = hintTableId;
			}
		}
	}
	else {
		util::NormalOStringStream strstrm;
		assert(oneHintExpr.qName_ && oneHintExpr.qName_->name_);
		strstrm << "Illegal argument for Hint(" <<
				oneHintExpr.qName_->name_->c_str() << ")";
		reportWarning(strstrm.str().c_str());
		succeeded = false;
	}
	return succeeded;
}

void SQLHintInfo::setJoinMethodHintMap(
		SQLHint::Id hintId, const Expr *hintExpr, const JoinMethodHintKey &hintKey) {

	JoinMethodHintKey mapKey = hintKey;
	if (hintKey.first > hintKey.second) {
		mapKey = std::make_pair(hintKey.second, hintKey.first);
	}
	SimpleHint hint;
	hint.hintId_ = hintId;
	hint.hintExpr_ = hintExpr;

	std::pair<JoinMethodHintMap::iterator, bool> result;
	result = joinMethodHintMap_.insert(std::make_pair(mapKey, hint));
	if (!result.second) {
		SimpleHint &existHint = joinMethodHintMap_[mapKey];
		if (hintId != existHint.hintId_) {
			existHint.hintId_ = SQLHint::START_ID; 
			util::NormalOStringStream strstrm;
			assert(hintExpr->qName_ && hintExpr->qName_->name_);
			strstrm << "Table (" << mapKey.first << "," << mapKey.second <<
					") has already join hint(" <<
					hintExpr->qName_->name_->c_str() << ").";
			reportWarning(strstrm.str().c_str());
		}
	}
}

void SQLHintInfo::insertHintTableIdMap(
		const util::String& origName, HintTableId id) {
	const util::String &normName = normalizeName(alloc_, origName.c_str());
	HintTableIdMapValue val(id, origName);
	hintTableIdMap_.insert(std::make_pair(normName, val));
}

void SQLHintInfo::insertTableInfoIdMap(
		const util::String& origName, TableInfoId id) {
	const util::String &normName = normalizeName(alloc_, origName.c_str());
	TableInfoIdMapValue val(id, origName);
	tableInfoIdMap_.insert(std::make_pair(normName, val));
}

bool SQLHintInfo::resolveScanHintArg(
		const Expr &oneHintExpr, TableInfoId &tableInfoId) {
	if (oneHintExpr.next_ == NULL || oneHintExpr.next_->size() == 0) {
		util::NormalOStringStream strstrm;
		assert(oneHintExpr.qName_ && oneHintExpr.qName_->name_);
		strstrm << "Illegal argument for Hint(" <<
				oneHintExpr.qName_->name_->c_str() << ")";
		reportWarning(strstrm.str().c_str());
		return false;
	}
	bool succeeded = true;
	ExprList::const_iterator hintArgItr = oneHintExpr.next_->begin();
	const Expr *hintArgExpr = *hintArgItr;
	if (hintArgExpr->op_ == SQLType::EXPR_COLUMN) {
		for (; hintArgItr != oneHintExpr.next_->end(); ++hintArgItr) {
			hintArgExpr = *hintArgItr;
			assert(hintArgExpr != NULL);
			if (hintArgExpr->op_ != SQLType::EXPR_COLUMN
				|| hintArgExpr->qName_ == NULL
				|| hintArgExpr->qName_->table_ != NULL
				|| hintArgExpr->qName_->name_ == NULL) {
				util::NormalOStringStream strstrm;
				assert(oneHintExpr.qName_ && oneHintExpr.qName_->name_);
				strstrm << "Illegal argument for Hint(" <<
						oneHintExpr.qName_->name_->c_str() << ")";
				reportWarning(strstrm.str().c_str());
				succeeded = false;
				break;
			}
			tableInfoId = getTableInfoId(*hintArgExpr);
			if (tableInfoId == UNDEF_HINT_TABLEID) {
				succeeded = false;
				break;
			}
		}
	}
	else {
		util::NormalOStringStream strstrm;
		assert(oneHintExpr.qName_ && oneHintExpr.qName_->name_);
		strstrm << "Illegal argument for Hint(" <<
				oneHintExpr.qName_->name_->c_str() << ")";
		reportWarning(strstrm.str().c_str());
		succeeded = false;
	}
	return succeeded;
}

void SQLHintInfo::setScanHintMap(
		SQLHint::Id hintId, const Expr *hintExpr, const TableInfoId tableInfoId) {

	SimpleHint scanHint;
	scanHint.hintId_ = hintId;
	scanHint.hintExpr_ = hintExpr;

	std::pair<ScanHintMap::iterator, bool> result;
	result = scanHintMap_.insert(std::make_pair(tableInfoId, scanHint));
	if (!result.second) {
		SimpleHint &existHint = scanHintMap_[tableInfoId];
		if (hintId != existHint.hintId_) {
			existHint.hintId_ = SQLHint::START_ID; 
			util::NormalOStringStream strstrm;
			assert(hintExpr->qName_ && hintExpr->qName_->name_);
			strstrm << "Table (" << tableInfoId <<
					") has already scan hint(" <<
					hintExpr->qName_->name_->c_str() << ").";
			reportWarning(strstrm.str().c_str());
		}
	}
}

void SQLHintInfo::setJoinHintMap(
		SQLHint::Id hintId, const Expr *hintExpr,
		const HintTableIdList &hintTableIdList, bool isTree) {
	ParsedHint parsedHint(alloc_);
	parsedHint.hintId_ = hintId;
	parsedHint.hintExpr_ = hintExpr;
	parsedHint.hintTableIdList_.assign(hintTableIdList.begin(), hintTableIdList.end());
	parsedHint.isTree_ = isTree;

	HintTableIdListKey mapKey(alloc_);
	mapKey.idList_.assign(hintTableIdList.begin(), hintTableIdList.end());
	std::sort(mapKey.idList_.begin(), mapKey.idList_.end());

	if (mapKey.idList_.back() == UNDEF_HINT_TABLEID) {
		util::Vector<HintTableId>::iterator itr =
			std::find(mapKey.idList_.begin(), mapKey.idList_.end(), UNDEF_HINT_TABLEID);
		assert(itr != mapKey.idList_.end());
		mapKey.idList_.erase(itr, mapKey.idList_.end());
	}

	joinHintMap_.insert(std::make_pair(mapKey, parsedHint));
}

void SQLHintInfo::makeJoinHintMap(const SQLPreparedPlan &plan) {
	static_cast<void>(plan);
	for (HintExprMap::iterator hintMapItr = hintExprMap_.begin();
			hintMapItr != hintExprMap_.end(); ++hintMapItr) {
		SQLHint::Id hintId = hintMapItr->first;
		switch(hintId) {
		case SQLHint::INDEX_SCAN:
		case SQLHint::NO_INDEX_SCAN:
			{
				const HintExprArray& hintExprArray = *hintMapItr->second;
				for (HintExprArray::const_iterator oneHintItr = hintExprArray.begin();
						oneHintItr != hintExprArray.end(); ++oneHintItr) {
					const Expr &oneHintExpr = *(*oneHintItr);
					assert(oneHintExpr.op_ == SQLType::EXPR_LIST);
					assert(oneHintExpr.next_);
					TableInfoId tableInfoId;
					bool succeeded = resolveScanHintArg(
							oneHintExpr, tableInfoId);
					if (succeeded) {
						setScanHintMap(hintId, &oneHintExpr, tableInfoId);
					}
				}
			}
			break;
		case SQLHint::INDEX_JOIN:
		case SQLHint::NO_INDEX_JOIN:
			{
				const HintExprArray& hintExprArray = *hintMapItr->second;
				for (HintExprArray::const_iterator oneHintItr = hintExprArray.begin();
						oneHintItr != hintExprArray.end(); ++oneHintItr) {
					const Expr &oneHintExpr = *(*oneHintItr);
					assert(oneHintExpr.op_ == SQLType::EXPR_LIST);

					JoinMethodHintKey hintKey;
					bool succeeded = resolveJoinMethodHintArg(
							oneHintExpr, hintKey);
					if (hintKey.first > hintKey.second) {
						hintKey = std::make_pair(hintKey.second, hintKey.first);
					}
					if (succeeded) {
						setJoinMethodHintMap(hintId, &oneHintExpr, hintKey);
					}
				}
			}
			break;
		case SQLHint::LEADING:
			{
				const HintExprArray& hintExprArray = *hintMapItr->second;
				for (HintExprArray::const_iterator oneHintItr = hintExprArray.begin();
						oneHintItr != hintExprArray.end(); ++oneHintItr) {
					const Expr &oneHintExpr = *(*oneHintItr);
					assert(oneHintExpr.op_ == SQLType::EXPR_LIST);

					HintTableIdList hintTableIdList(alloc_);
					bool isTree = false;
					bool succeeded = resolveHintTableArg(
							oneHintExpr, hintTableIdList, isTree);
					if (succeeded) {
						setJoinHintMap(hintId, &oneHintExpr, hintTableIdList, isTree);
					}
				}
			}
			break;
		default:
			;
		}
	}
}

bool SQLHintInfo::hasHint(SQLHint::Id id) const {
	HintExprMap::const_iterator it = hintExprMap_.find(id);
	if (it == hintExprMap_.end()) {
		return false;
	}
	const SQLHintInfo::HintExprArray *exprList = it->second;
	return !exprList->empty();
}

const SQLHintInfo::HintExprArray* SQLHintInfo::getHintList(SQLHint::Id id) const {
	HintExprMap::const_iterator itr = hintExprMap_.find(id);
	if (itr != hintExprMap_.end()) {
		return (itr->second);
	}
	else {
		return NULL;
	}
}

void SQLHintInfo::getHintValueList(
		SQLHint::Id id, util::Vector<TupleValue> &valueList) const {
	valueList.clear();
	HintExprMap::const_iterator itr = hintExprMap_.find(id);
	if (itr != hintExprMap_.end()) {
		const HintExprArray *exprList = itr->second;
		assert(exprList);
		if (exprList->size() > 1) {
			reportWarning("More than one hint was found");
		}
		else if (exprList->size() == 0) {
			reportWarning("No hint was found");
			return;
		}
		const Expr* hintExpr = (*exprList)[0];
		assert(hintExpr && hintExpr->op_ == SQLType::EXPR_LIST);
		if (hintExpr->next_) {
			SyntaxTree::ExprList::const_iterator itr = hintExpr->next_->begin();
			for ( ; itr != hintExpr->next_->end(); ++itr) {
				if ((*itr)->op_ != SQLType::EXPR_CONSTANT) {
					assert(hintExpr->qName_ && hintExpr->qName_->name_);
					util::NormalOStringStream strstrm;
					strstrm << "Hint(" << hintExpr->qName_->name_->c_str() <<
							") arguments must be a constant expression";
					reportWarning(strstrm.str().c_str());
					break;
				}
				valueList.push_back((*itr)->value_);
			}
		}
	}
}

void SQLHintInfo::setReportLevel(ReportLevel level) {
	reportLevel_ = level;
}

bool SQLHintInfo::checkReportLevel(ReportLevel level) const {
	return level <= reportLevel_;
}

void SQLHintInfo::reportError(const char8_t* inMessage) const {
	const char8_t *message = (inMessage != NULL) ? inMessage : "";
	GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, message);
}

void SQLHintInfo::reportWarning(const char8_t* inMessage) const {
	const char8_t *message = (inMessage != NULL) ? inMessage : "";
	if (checkReportLevel(REPORT_WARNING)) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, message);
	}
	else {
		GS_TRACE_WARNING(SQL_HINT, GS_TRACE_SQL_HINT_WARNING, message);
	}
}

void SQLHintInfo::reportInfo(const char8_t* inMessage) const {
	const char8_t *message = (inMessage != NULL) ? inMessage : "";
	GS_TRACE_INFO(SQL_HINT, GS_TRACE_SQL_HINT_INFO, message);
}


SQLHintValue::SQLHintValue(SQLHint::Id id)
: hintId_(id), int64Value_(0), doubleValue_(0.0) {
}

SQLHint::Id SQLHintValue::getId() const {
	return hintId_;
}

int64_t SQLHintValue::getInt64() const {
	switch(hintId_) {
	case SQLHint::TABLE_ROW_COUNT:
		return int64Value_;
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}
}

void SQLHintValue::setInt64(int64_t value) {
	switch(hintId_) {
	case SQLHint::TABLE_ROW_COUNT:
		int64Value_ = value;
		break;
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}
}

double SQLHintValue::getDouble() const {
	switch(hintId_) {
	case SQLHint::START_ID: 
		return doubleValue_;
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}
}

void SQLHintValue::setDouble(double value) {
	switch(hintId_) {
	case SQLHint::START_ID: 
		doubleValue_ = value;
		break;
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL, "");
	}
}


SQLHintInfo::HintTableIdListKey::HintTableIdListKey(util::StackAllocator &alloc)
: alloc_(alloc), idList_(alloc) {
}

void SQLHintInfo::HintTableIdListKey::push_back(HintTableId value) {
	idList_.push_back(value);
}

void SQLHintInfo::HintTableIdListKey::clear() {
	idList_.clear();
}

void SQLHintInfo::HintTableIdListKey::reserve(size_t size) {
	idList_.reserve(size);
}

bool SQLHintInfo::HintTableIdListKey::operator<(const HintTableIdListKey &another) const {
	if (idList_.size() != another.idList_.size()) {
		return idList_.size() < another.idList_.size();
	}
	util::Vector<HintTableId>::const_iterator itr1 = idList_.begin();
	util::Vector<HintTableId>::const_iterator itr2 = another.idList_.begin();
	for (; itr1 != idList_.end(); ++itr1, ++itr2) {
		if (*itr1 != *itr2) {
			return *itr1 < *itr2;
		}
	}
	return false;
}

SQLHintInfo::ParsedHint::ParsedHint(util::StackAllocator &alloc)
: alloc_(alloc), hintId_(SQLHint::START_ID), hintExpr_(NULL),
  hintTableIdList_(alloc), isTree_(false) {
}


bool SQLHintInfo::findJoinHintList(
		const util::Vector<const util::String*> &nodeNameList,
		SQLHint::Id filter,
		util::Vector<ParsedHint> &hintList) const {
	HintTableIdList target(alloc_);
	util::Map<HintTableId, uint32_t> convertMap(alloc_);
	convertMap[UNDEF_HINT_TABLEID] = UNDEF_HINT_TABLEID;

	for (util::Vector<const util::String*>::const_iterator itr = nodeNameList.begin();
			itr != nodeNameList.end(); ++itr) {
		HintTableId hintTableId = UNDEF_HINT_TABLEID;
		if (*itr != NULL) {
			HintTableIdMap::const_iterator idMapItr = hintTableIdMap_.find(**itr);
			if (idMapItr != hintTableIdMap_.end()) {
				hintTableId = idMapItr->second.id_;
			}
			else {
			}
		}
		else {
		}
		target.push_back(hintTableId);
		uint32_t inputPos = static_cast<uint32_t>(itr - nodeNameList.begin());
		if (hintTableId != UNDEF_HINT_TABLEID) {
			convertMap[hintTableId] = inputPos;
		}
	}
	std::sort(target.begin(), target.end());

	typedef util::MultiMap<size_t, JoinHintMap::const_iterator, std::greater<size_t> > HintResultMap;
	HintResultMap hintResultMap(alloc_);
	JoinHintMap::const_iterator hintMapItr = joinHintMap_.begin();
	for (; hintMapItr != joinHintMap_.end(); ++hintMapItr) {
		if (hintMapItr->second.hintId_ != filter) {
			continue;
		}
		const HintTableIdList &hintKeyList = hintMapItr->first.idList_;
		bool found = std::includes(
				target.begin(), target.end(), hintKeyList.begin(), hintKeyList.end());
		if (found) {
			hintResultMap.insert(std::make_pair(hintKeyList.size(), hintMapItr));
		}
	}
	HintTableIdList checkList(alloc_);
	checkList.assign(target.begin(), target.end());
	HintResultMap::const_iterator resultItr = hintResultMap.begin();
	for (; resultItr != hintResultMap.end(); ++resultItr) {
		const HintTableIdList &keyIdList = resultItr->second->first.idList_;
		const ParsedHint &parsedHint = resultItr->second->second;
		bool found = std::includes(
			checkList.begin(), checkList.end(), keyIdList.begin(), keyIdList.end());
		if (found) {
			hintList.push_back(parsedHint);
			HintTableIdList diffList(alloc_);
			std::set_difference(checkList.begin(), checkList.end(),
								keyIdList.begin(), keyIdList.end(),
								std::back_inserter(diffList));
			util::Vector<SQLHintInfo::ParsedHint>::iterator hintItr = hintList.begin();
			for (; hintItr != hintList.end(); ++hintItr) {
				SQLHintInfo::HintTableIdList::iterator idItr = hintItr->hintTableIdList_.begin();
				for (; idItr != hintItr->hintTableIdList_.end(); ++idItr) {
					if (*idItr != UNDEF_HINT_TABLEID) {
						assert(convertMap.find(*idItr) != convertMap.end());
						*idItr = convertMap[*idItr];
					}
				}
			}

			checkList.swap(diffList);
			if (checkList.size() < 2) {
				return true;
			}
		}
		else {
		}
	}
	return hintList.size() > 0;
}

const SQLHintValue* SQLHintInfo::findStatisticalHint(
		SQLHint::Id hintId, const char8_t *tableName) const {
	assert(tableName != NULL);
	StatisticalHintKey key(
			normalizeName(alloc_, tableName), util::String(alloc_));

	StatisticalHintMap::const_iterator mapIt = statisticalHintMap_.find(key);
	if (mapIt == statisticalHintMap_.end()) {
		return NULL;
	}

	const util::Vector<SQLHintValue> &values = mapIt->second;
	for (util::Vector<SQLHintValue>::const_iterator it = values.begin();
			it != values.end(); ++it) {
		if (it->getId() != hintId) {
			continue;
		}
		return &(*it);
	}

	return NULL;
}

bool SQLHintInfo::findScanHint(const TableInfoId tableInfoId, const SimpleHint* &scanHint) const {
	scanHint = NULL;
	ScanHintMap::const_iterator itr = scanHintMap_.find(tableInfoId);
	if (itr != scanHintMap_.end()) {
		scanHint = &(itr->second);
		return true;
	}
	else {
		return false;
	}
}

bool SQLHintInfo::findJoinMethodHint(
		const util::Vector<const util::String*> &nodeNameList,
		SQLHint::Id filter, const SimpleHint* &scanHint) const
{
	scanHint = NULL;
	if (nodeNameList.size() != 2
			|| nodeNameList[0] == NULL || nodeNameList[1] == NULL) {
		return false;
	}
	HintTableId hintTableId0 = getHintTableId(*nodeNameList[0]);
	if (hintTableId0 == UNDEF_HINT_TABLEID) {
		return false;
	}
	HintTableId hintTableId1 = getHintTableId(*nodeNameList[1]);
	if (hintTableId1 == UNDEF_HINT_TABLEID) {
		return false;
	}
	JoinMethodHintKey key;
	if (hintTableId0 < hintTableId1) {
		key = std::make_pair(hintTableId0, hintTableId1);
	}
	else {
		key = std::make_pair(hintTableId1, hintTableId0);
	}
	return findJoinMethodHint(key, filter, scanHint);
}

bool SQLHintInfo::findJoinMethodHint(
		const JoinMethodHintKey &key, SQLHint::Id filter,
		const SimpleHint* &hint) const {
	hint = NULL;
	JoinMethodHintMap::const_iterator itr = joinMethodHintMap_.find(key);
	if (itr != joinMethodHintMap_.end()) {
		JoinMethodHintMap::const_iterator upperBound = joinMethodHintMap_.upper_bound(key);
		do {
			if (itr->second.hintId_ == filter) {
				hint = &(itr->second);
				break;
			}
			++itr;
		} while(itr != upperBound);
	}
	return (hint != NULL);
}

const util::Vector<SQLHintInfo::ParsedHint>&
SQLHintInfo::getSymbolHintList() const {
	return symbolHintList_;
}

void SQLHintInfo::dumpList(std::ostream &os, const HintTableIdList &target) const {
	os << "{";
	HintTableIdList::const_iterator itr = target.begin();
	for (; itr != target.end(); ++itr) {
		if (itr != target.begin()) {
			os << ", ";
		}
		os << (*itr);
	}
	os << "}";
}

void SQLHintInfo::getExprToken(const Expr &expr, util::String &exprToken) const {
	size_t exprTokenLen = 0;
	exprToken.clear();
	if (expr.startToken_.value_ != NULL) {
		if (expr.endToken_.value_ != NULL) {
			exprTokenLen =
					expr.endToken_.value_ - expr.startToken_.value_ +
					expr.endToken_.size_;
			exprToken = util::String(expr.startToken_.value_, exprTokenLen, alloc_);
		}
		else {
			exprTokenLen = expr.startToken_.size_;
			exprToken = util::String(expr.startToken_.value_, exprTokenLen, alloc_);
		}
	}
}

void SQLHintInfo::checkExclusiveHints(
		SQLHint::Id hintId1, SQLHint::Id hintId2) const {
	const SQLHintInfo::HintExprArray *list1 = getHintList(hintId1);
	const SQLHintInfo::HintExprArray *list2 = getHintList(hintId2);
	if ((list1 != NULL && !list1->empty()) &&
			(list2 != NULL && !list2->empty())) {
		const util::String *name1 = list1->front()->qName_->name_;
		const util::String *name2 = list2->front()->qName_->name_;
		util::NormalOStringStream strstrm;
		strstrm << "Hint(" << *name1 << ") and hint(" << *name2 << ") "
				"can not specified both";
		reportWarning(strstrm.str().c_str());
	}
}

bool SQLHintInfo::checkArgCount(
		const Expr &hintExpr,
		size_t count, bool isMinimum) const
{
	util::String token(alloc_);
	if (hintExpr.op_ != SQLType::EXPR_LIST) {
		assert(hintExpr.qName_ && hintExpr.qName_->name_);
		util::NormalOStringStream strstrm;
		strstrm << "Invalid argument type for hint(" <<
				hintExpr.qName_->name_->c_str() << ")";
		getExprToken(hintExpr, token);
		if (token.size() > 0) {
			strstrm << " at or near \"" << token.c_str() << "\"";
		}
		reportWarning(strstrm.str().c_str());
		return false;
	}
	assert(hintExpr.next_);
	const ExprList &hintArgList = *(hintExpr.next_);
	if ((isMinimum && hintArgList.size() >= count) ||
		(!isMinimum && hintArgList.size() == count)) {
		return true;
	}
	else {
		util::NormalOStringStream strstrm;
		assert(hintExpr.qName_ && hintExpr.qName_->name_);
		strstrm << "Invalid argument count for hint(" <<
				hintExpr.qName_->name_->c_str() << ") (count=" <<
				hintArgList.size() << ")";
		getExprToken(hintExpr, token);
		if (token.size() > 0) {
			strstrm << " at or near \"" << token.c_str() << "\"";
		}
		reportWarning(strstrm.str().c_str());
		return false;
	}
}

bool SQLHintInfo::checkArgSQLOpType(
		const Expr &hintExpr,
		SQLType::Id type) const {
	util::String token(alloc_);
	if (hintExpr.op_ != SQLType::EXPR_LIST) {
		assert(hintExpr.qName_ && hintExpr.qName_->name_);
		util::NormalOStringStream strstrm;
		strstrm << "Invalid argument type for hint(" <<
				hintExpr.qName_->name_->c_str() << ")";
		getExprToken(hintExpr, token);
		if (token.size() > 0) {
			strstrm << " at or near \"" << token.c_str() << "\"";
		}
		reportWarning(strstrm.str().c_str());
		return false;
	}
	assert(hintExpr.next_);
	const ExprList &hintArgList = *(hintExpr.next_);
	ExprList::const_iterator itr = hintArgList.begin();
	for(; itr != hintArgList.end(); ++itr) {
		if ((*itr) && (*itr)->op_ == type) {
			;
		}
		else {
			util::NormalOStringStream strstrm;
			assert(hintExpr.qName_ && hintExpr.qName_->name_);
			strstrm << "Invalid argument type for hint(" <<
					hintExpr.qName_->name_->c_str() << ")";
			getExprToken(**itr, token);
			if (token.size() > 0) {
				strstrm << " at or near \"" << token.c_str() << "\"";
			}
			reportWarning(strstrm.str().c_str());
			return false;
		}
	}
	return true;
}

bool SQLHintInfo::checkAllArgsValueType(
		const Expr &hintExpr, TupleList::TupleColumnType valueType,
		int64_t minValue, int64_t maxValue) const {
	util::String token(alloc_);
	if (hintExpr.op_ != SQLType::EXPR_LIST) {
		assert(hintExpr.qName_ && hintExpr.qName_->name_);
		util::NormalOStringStream strstrm;
		strstrm << "Invalid argument type for hint(" <<
				hintExpr.qName_->name_->c_str() << ")";
		getExprToken(hintExpr, token);
		if (token.size() > 0) {
			strstrm << " at or near \"" << token.c_str() << "\"";
		}
		reportWarning(strstrm.str().c_str());
		return false;
	}
	assert(hintExpr.next_);
	const ExprList &hintArgList = *(hintExpr.next_);
	for (size_t pos = 0; pos < hintArgList.size(); ++pos) {
		if (!checkArgValueType(hintExpr, pos, valueType, minValue, maxValue)) {
			return false;
		}
	}
	return true;
}

bool SQLHintInfo::checkArgValueType(
		const Expr &hintExpr, size_t pos, TupleList::TupleColumnType valueType,
		int64_t minValue, int64_t maxValue) const {
	util::String token(alloc_);
	if (hintExpr.op_ != SQLType::EXPR_LIST) {
		assert(hintExpr.qName_ && hintExpr.qName_->name_);
		util::NormalOStringStream strstrm;
		strstrm << "Invalid argument type for hint(" <<
				hintExpr.qName_->name_->c_str() << ")";
		getExprToken(hintExpr, token);
		if (token.size() > 0) {
			strstrm << " at or near \"" << token.c_str() << "\"";
		}
		reportWarning(strstrm.str().c_str());
		return false;
	}
	assert(hintExpr.next_);
	const Expr &hintArg = *(hintExpr.next_->at(pos));
	if (hintArg.op_ == SQLType::EXPR_CONSTANT) {
		switch(valueType) {
		case TupleList::TYPE_LONG:
			if (hintArg.value_.getType() == TupleList::TYPE_LONG &&
					checkValueRange(
							hintArg.value_.get<int64_t>(),
							minValue, maxValue)) {
				return true;
			}
			break;
		case TupleList::TYPE_DOUBLE:
			if (hintArg.value_.getType() == TupleList::TYPE_DOUBLE &&
					checkValueRange(
							static_cast<int64_t>(hintArg.value_.get<double>()),
							minValue, maxValue)) {
				return true;
			}
			break;
		case TupleList::TYPE_STRING:
			if (hintArg.value_.getType() == TupleList::TYPE_STRING) {
				return true;
			}
			break;
		default:
			break;
		}
		util::NormalOStringStream strstrm;
		assert(hintExpr.qName_ && hintExpr.qName_->name_);
		strstrm << "Invalid argument value for hint(" <<
				hintExpr.qName_->name_->c_str() << ")";
		if (minValue != std::numeric_limits<int64_t>::min()) {
			strstrm << " (min=" << minValue << ")";
		}
		getExprToken(hintExpr, token);
		if (token.size() > 0) {
			strstrm << " at or near \"" << token.c_str() << "\"";
		}
		reportWarning(strstrm.str().c_str());
		return false;
	}
	else {
		util::NormalOStringStream strstrm;
		assert(hintExpr.qName_ && hintExpr.qName_->name_);
		strstrm << "Invalid argument type for hint(" <<
				hintExpr.qName_->name_->c_str() << ")";
		getExprToken(hintExpr, token);
		if (token.size() > 0) {
			strstrm << " at or near \"" << token.c_str() << "\"";
		}
		reportWarning(strstrm.str().c_str());
		return false;
	}
}

bool SQLHintInfo::checkValueRange(
		int64_t value, int64_t min, int64_t max) const {
	if (min != std::numeric_limits<int64_t>::min()) {
		if (!(value >= min)) {
			return false;
		}
	}

	if (max != std::numeric_limits<int64_t>::max()) {
		if (!(value <= max)) {
			return false;
		}
	}

	return true;
}

bool SQLHintInfo::checkArgIsTable(
		const Expr &hintExpr, size_t pos, bool forSymbol) const
{
	const char8_t *category = (forSymbol ? "symbol" : "table");
	util::String token(alloc_);
	assert(hintExpr.op_ == SQLType::EXPR_LIST);
	const Expr &hintArgExpr = *hintExpr.next_->at(pos);
	if (hintArgExpr.op_ != SQLType::EXPR_COLUMN) {
		util::NormalOStringStream strstrm;
		assert(hintExpr.qName_ && hintExpr.qName_->name_);
		strstrm << "Invalid " << category << " type of argument for hint(" <<
				hintExpr.qName_->name_->c_str() << ")";
		getExprToken(hintExpr, token);
		if (token.size() > 0) {
			strstrm << " at or near \"" << token.c_str() << "\"";
		}
		reportWarning(strstrm.str().c_str());
		return false;
	}
	if ((hintArgExpr.qName_ == NULL) ||
			(!forSymbol &&
					((hintArgExpr.qName_->db_ != NULL) ||
					(hintArgExpr.qName_->table_ != NULL))) ||
			(hintArgExpr.qName_->name_ == NULL)) {
		util::NormalOStringStream strstrm;
		assert(hintExpr.qName_ && hintExpr.qName_->name_);
		strstrm << "Invalid " << category << " name of argument for hint(" <<
				hintExpr.qName_->name_->c_str() << ")";
		getExprToken(hintExpr, token);
		if (token.size() > 0) {
			strstrm << " at or near \"" << token.c_str() << "\"";
		}
		reportWarning(strstrm.str().c_str());
		return false;
	}
	return true;
}

bool SQLHintInfo::checkLeadingTreeArgumentsType(const Expr &expr) const {
	util::String token(alloc_);
	if (expr.op_ == SQLType::EXPR_LIST) {
		assert(expr.next_);
		if (expr.next_->size() != 2) {
			util::NormalOStringStream strstrm;
			strstrm << "Invalid argument for hint(Leading)";
			getExprToken(expr, token);
			if (token.size() > 0) {
				strstrm << " at or near \"" << token.c_str() << "\"";
			}
			reportWarning(strstrm.str().c_str());
			return false;
		}
		for (size_t pos = 0; pos < expr.next_->size(); ++pos) {
			if (expr.next_->at(pos)->op_ == SQLType::EXPR_LIST) {
				if (!checkLeadingTreeArgumentsType(*expr.next_->at(pos))) {
					return false;
				}
			}
			else if (expr.next_->at(pos)->op_ == SQLType::EXPR_COLUMN) {
				const Expr &hintArgExpr = *expr.next_->at(pos);
				if ((hintArgExpr.qName_ == NULL) ||
						(hintArgExpr.qName_->db_ != NULL) ||
						(hintArgExpr.qName_->table_ != NULL) ||
						(hintArgExpr.qName_->name_ == NULL)) {
					util::NormalOStringStream strstrm;
					strstrm << "Invalid argument for hint(Leading)";
					getExprToken(expr, token);
					if (token.size() > 0) {
						strstrm << " at or near \"" << token.c_str() << "\"";
					}
					reportWarning(strstrm.str().c_str());
					return false;
				}
			}
			else {
				util::NormalOStringStream strstrm;
				strstrm << "Invalid argument for hint(Leading)";
				getExprToken(expr, token);
				if (token.size() > 0) {
					strstrm << " at or near \"" << token.c_str() << "\"";
				}
				reportWarning(strstrm.str().c_str());
				return false;
			}
		}
		return true;
	}
	else {
		util::NormalOStringStream strstrm;
		strstrm << "Invalid argument for hint(Leading)";
		getExprToken(expr, token);
		if (token.size() > 0) {
			strstrm << " at or near \"" << token.c_str() << "\"";
		}
		reportWarning(strstrm.str().c_str());
		return false;
	}
}

SQLHintInfo::SymbolElement::SymbolElement() :
		value_(SQLPreparedPlan::Constants::END_STR) {
}

void SQLHintInfo::SymbolList::setValue(size_t pos, StringConstant value) {
	elems_[pos].value_ = value;
}

SQLHintInfo::StringConstant SQLHintInfo::SymbolList::getValue(
		size_t pos) const {
	assert(pos < SYMBOL_LIST_SIZE);
	return elems_[pos].value_;
}

SQLHintInfo::ParsedHint& SQLHintInfo::ParsedHint::operator=(
		const SQLHintInfo::ParsedHint &another) {
	hintId_ = another.hintId_;
	hintExpr_ = another.hintExpr_;
	hintTableIdList_.assign(another.hintTableIdList_.begin(), another.hintTableIdList_.end());
	isTree_ = another.isTree_;
	return *this;
}

SQLHintInfo::HintTableIdMapValue::HintTableIdMapValue(
		SQLHintInfo::HintTableId id, const util::String &origName)
: origName_(origName), id_(id) {
}

SQLHintInfo::TableInfoIdMapValue::TableInfoIdMapValue(
		SQLHintInfo::TableInfoId id, const util::String &origName)
: origName_(origName), id_(id) {
}

void SQLCompiler::dumpExprList(const PlanExprList &list) {
	for (size_t i = 0; i < list.size(); ++i) {
		const Expr &expr = list[i];
		std::cout << "(" << i << "," << expr.srcId_;
		if (expr.qName_ && expr.qName_->name_) {
			std::cout << "," << "name:" << expr.qName_->name_->c_str();
		}
		if (expr.aliasName_) {
			std::cout << "," << "alias:" << expr.aliasName_->c_str();
		}
		std::cout << "),";
	}
	std::cout << std::endl;
}

SQLCompiler::CompilerInitializer::CompilerInitializer(
		const CompileOption *option) :
		baseOption_(option) {
}

SQLCompiler::CompilerInitializer::operator bool() {
	assert(baseOption_ != &InitializerConstants::INVALID_OPTION);
	return (CompileOption::findBaseCompiler(baseOption_, *this) != NULL);
}

const SQLCompiler* SQLCompiler::CompilerInitializer::operator->() {
	assert(baseOption_ != &InitializerConstants::INVALID_OPTION);
	return CompileOption::findBaseCompiler(baseOption_, *this);
}

SQLCompiler::ProfilerManager&
SQLCompiler::CompilerInitializer::profilerManager() {
	return ProfilerManager::getInstance();
}

void SQLCompiler::CompilerInitializer::close() {
	assert(baseOption_ != &InitializerConstants::INVALID_OPTION);
	baseOption_ = &InitializerConstants::INVALID_OPTION;
}

SQLCompiler::CompilerInitializer::End::End(CompilerInitializer &init) {
	init.close();
}

SQLCompiler::CompileOption
SQLCompiler::CompilerInitializer::InitializerConstants::INVALID_OPTION;

SQLCompiler::CompilerSource::CompilerSource(const SQLCompiler *baseCompiler) :
		baseCompiler_(baseCompiler) {
}

const SQLCompiler* SQLCompiler::CompilerSource::findBaseCompiler(
		const CompilerSource *src) {
	return (src == NULL ? NULL : src->baseCompiler_);
}
