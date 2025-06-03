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
#ifndef SQL_COMPILER_H_
#define SQL_COMPILER_H_

#include "sql_parser.h"

struct MetaContainerInfo;
struct BindParm;
class SQLHintInfo;
class Query;

class TransactionContext;
class FullContainerKey;
class DataStoreV4;


struct SQLHint {
	class Coder;

	enum Id {
		START_ID,
		MAX_DEGREE_OF_PARALLELISM,
		MAX_DEGREE_OF_TASK_INPUT,
		MAX_DEGREE_OF_EXPANSION,

		MAX_GENERATED_ROWS,

		DISTRIBUTED_POLICY,

		INDEX_SCAN,
		NO_INDEX_SCAN,

		INDEX_JOIN,
		NO_INDEX_JOIN,

		LEADING,

		TASK_ASSIGNMENT, 

		LEGACY_PLAN,
		COST_BASED_JOIN,
		NO_COST_BASED_JOIN,
		COST_BASED_JOIN_DRIVING,
		NO_COST_BASED_JOIN_DRIVING,

		OPTIMIZER_FAILURE_POINT,

		TABLE_ROW_COUNT,

		END_ID
	};
};

struct SQLTableInfo {
	typedef uint32_t Id;

	struct IdInfo {
		IdInfo();

		bool isEmpty() const;
		bool operator==(const IdInfo &another) const;
		UTIL_OBJECT_CODER_MEMBERS(
				id_,
				UTIL_OBJECT_CODER_ENUM(
						type_, SQLType::TABLE_CONTAINER, SQLType::Coder()),
				dbId_,
				partitionId_,
				containerId_,
				schemaVersionId_,
				UTIL_OBJECT_CODER_OPTIONAL(subContainerId_, -1),
				UTIL_OBJECT_CODER_OPTIONAL(partitioningVersionId_, -1),
				UTIL_OBJECT_CODER_OPTIONAL(approxSize_, -1));

		Id id_;
		SQLType::TableType type_;
		DatabaseId dbId_;
		PartitionId partitionId_;
		ContainerId containerId_;
		SchemaVersionId schemaVersionId_;
		int32_t subContainerId_;
		int64_t partitioningVersionId_;
		bool isNodeExpansion_;
		int64_t approxSize_;
	};

	struct SubInfo;
	struct PartitioningInfo;
	struct SQLIndexInfo;
	struct BasicIndexInfoList;

	typedef util::Vector<SubInfo> SubInfoList;
	typedef util::Vector<SQLIndexInfo> IndexInfoList;
	typedef util::Vector<uint8_t> NullStatisticsList;

	typedef std::pair<TupleList::TupleColumnType, util::String> SQLColumnInfo;
	typedef util::Vector<SQLColumnInfo> SQLColumnInfoList;
	typedef util::Vector<uint8_t> NoSQLColumnInfoList;
	typedef util::Vector<uint8_t> NoSQLColumnOptionList;

	explicit SQLTableInfo(util::StackAllocator &alloc);

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(dbName_, tableName_, idInfo_);

	util::String dbName_;
	util::String tableName_;
	bool hasRowKey_;
	bool writable_;
	bool isView_;
	bool isExpirable_;

	IdInfo idInfo_;
	SQLColumnInfoList columnInfoList_;
	const PartitioningInfo *partitioning_;
	const IndexInfoList *indexInfoList_;
	const BasicIndexInfoList *basicIndexInfoList_;

	NoSQLColumnInfoList nosqlColumnInfoList_;
	NoSQLColumnOptionList nosqlColumnOptionList_;
	util::String sqlString_;

	util::Vector<uint64_t> cardinalityList_;
	util::Vector<uint8_t> nullsStats_;
};

struct SQLTableInfo::SubInfo {
	SubInfo();

	UTIL_OBJECT_CODER_MEMBERS(
			partitionId_, containerId_, schemaVersionId_, approxSize_);

	PartitionId partitionId_;
	ContainerId containerId_;
	SchemaVersionId schemaVersionId_;
	int64_t nodeAffinity_;
	int64_t approxSize_;
};

struct SQLTableInfo::PartitioningInfo {
	typedef util::Vector<int64_t> CondensedPartitionIdList;
	typedef std::pair<int64_t, int64_t> IntervalEntry;
	typedef util::Vector<IntervalEntry> IntervalList;

	explicit PartitioningInfo(util::StackAllocator &alloc);

	void copy(const PartitioningInfo &partitioningInfo);

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(
			partitioningType_,
			partitioningColumnId_,
			subPartitioningColumnId_, 
			subInfoList_,
			tableName_,
			isCaseSensitive_);

	util::StackAllocator &alloc_;

	uint8_t partitioningType_;
	int32_t partitioningColumnId_;
	int32_t subPartitioningColumnId_;
	SubInfoList subInfoList_;
	util::String tableName_;
	bool isCaseSensitive_;

	uint32_t partitioningCount_;
	uint32_t clusterPartitionCount_;
	int64_t intervalValue_;
	CondensedPartitionIdList nodeAffinityList_;
	IntervalList availableList_;
};

struct SQLTableInfo::SQLIndexInfo {

	explicit SQLIndexInfo(util::StackAllocator &alloc);

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(columns_);

	util::Vector<ColumnId> columns_;
};

struct SQLTableInfo::BasicIndexInfoList {
public:




	explicit BasicIndexInfoList(util::StackAllocator &alloc);

	void assign(util::StackAllocator &alloc, const IndexInfoList &src);

	const util::Vector<ColumnId>& getFirstColumns() const;

	bool isIndexed(ColumnId firstColumnId) const;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(columns_);

private:
	util::Vector<ColumnId> columns_;
};

class SQLTableInfoList {
public:
	typedef SyntaxTree::QualifiedName QualifiedName;
	typedef SQLTableInfo::BasicIndexInfoList BasicIndexInfoList;
	typedef int32_t NodeId;

	SQLTableInfoList(
			util::StackAllocator &alloc, SQLIndexStatsCache *indexStats);

	uint32_t size() const;
	typedef util::Vector<SQLTableInfo::SubInfo> SubInfoList;

	const SQLTableInfo& add(const SQLTableInfo &info);

	const SQLTableInfo* prepareMeta(
			const QualifiedName &name, const SQLTableInfo::IdInfo &coreIdInfo,
			DatabaseId dbId, uint32_t partitionCount);

	void setDefaultDBName(const char8_t *dbName);
	const char8_t* resolveDBName(const QualifiedName &name) const;
	static bool resolveDBCaseSensitive(const QualifiedName &name);

	const SQLTableInfo& resolve(SQLTableInfo::Id id) const;
	const SQLTableInfo& resolve(const QualifiedName &name) const;
	const SQLTableInfo& resolve(
			const char8_t *dbName, const char8_t *tableName,
			bool dbCaseSensitive, bool tableCaseSensitive) const;

	const SQLTableInfo* find(const QualifiedName &name) const;
	const SQLTableInfo* find(
			const char8_t *dbName, const char8_t *tableName,
			bool dbCaseSensitive, bool tableCaseSensitive) const;

	const BasicIndexInfoList* getIndexInfoList(
			const SQLTableInfo::IdInfo &idInfo) const;

	SQLIndexStatsCache& getIndexStats() const;

private:
	typedef util::Vector<SQLTableInfo> List;

	util::StackAllocator& getAllocator();

	List list_;
	util::String *defaultDBName_;
	SQLIndexStatsCache *indexStats_;
};


class SQLHint::Coder {
public:
	const char8_t* operator()(Id id) const;
	bool operator()(const char8_t *name, Id &id) const;

private:
	struct NameTable;
	static const NameTable nameTable_;
};

class SQLHintValue {
public:
	explicit SQLHintValue(SQLHint::Id id);

	SQLHint::Id getId() const;

	int64_t getInt64() const;
	void setInt64(int64_t value);

	double getDouble() const;
	void setDouble(double value);

private:
	SQLHint::Id hintId_;
	int64_t int64Value_;
	double doubleValue_;
};

class SQLPreparedPlan {
public:
	class Node;

	struct Constants;
	struct TotalProfile;
	struct Profile;
	struct OptProfile;

	typedef int64_t ProfileKey;
	typedef util::Vector<Node> NodeList;
	typedef util::Vector<TupleValue> ValueList;
	typedef util::Vector<TupleList::TupleColumnType> ValueTypeList;

	explicit SQLPreparedPlan(util::StackAllocator &alloc);
	~SQLPreparedPlan();
	util::StackAllocator& getAllocator() const;

	Node& append(SQLType::Id type);

	Node& back();
	const Node& back() const;

	static bool isDisabledNode(const Node &node);
	static bool isDisabledExpr(const SyntaxTree::Expr &expr);

	static SQLType::Id getDisabledNodeType();
	static SQLType::Id getDisabledExprType();

	void removeEmptyNodes();
	void removeEmptyColumns();

	void validateReference(const SQLTableInfoList *tableInfoList) const;

	void validateSrcId(const SQLTableInfoList *tableInfoList) const;

	void setUpHintInfo(const SyntaxTree::ExprList* hintList, SQLTableInfoList &tableInfoList);

	void dump(std::ostream &os, bool asJson = true) const;
	void dumpToFile(const char8_t *name, bool asJson = true) const;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(
			nodeList_, parameterList_,
			UTIL_OBJECT_CODER_OPTIONAL(currentTimeRequired_, false));

	NodeList nodeList_;
	ValueList parameterList_;
	bool currentTimeRequired_;

	const SQLHintInfo *hintInfo_; 
	SQLPlanningVersion planningVersion_;
};

class SQLPreparedPlan::Node {
public:
	typedef uint32_t Id;
	typedef SQLType::Id Type;
	typedef SyntaxTree::QualifiedName QualifiedName;
	typedef SyntaxTree::Expr Expr;
	typedef util::Vector<Id> IdList;
	typedef util::Vector<Expr> ExprList;
	typedef util::Vector<uint8_t> NoSQLColumnInfoList;
	typedef SQLType::AggregationPhase AggregationPhase;
	typedef SQLType::JoinType JoinType;
	typedef SQLType::UnionType UnionType;
	typedef int32_t CommandOptionFlag;
	typedef SyntaxTree::CommandType CommandType;

	typedef SQLTableInfo::SubInfo SubInfo;
	typedef SQLTableInfo::PartitioningInfo PartitioningInfo;
	typedef util::Vector<SubInfo> SubInfoList;
	typedef util::Vector<int32_t> InsertColumnMap;

	struct Config {
		static const char8_t UNSPECIFIED_COLUMN_NAME[];
		static const CommandOptionFlag CMD_OPT_SCAN_INDEX;
		static const CommandOptionFlag CMD_OPT_JOIN_LEADING_LEFT;
		static const CommandOptionFlag CMD_OPT_JOIN_LEADING_RIGHT;
		static const CommandOptionFlag CMD_OPT_SCAN_EXPIRABLE;
		static const CommandOptionFlag CMD_OPT_JOIN_NO_INDEX;
		static const CommandOptionFlag CMD_OPT_JOIN_USE_INDEX;
		static const CommandOptionFlag CMD_OPT_SCAN_MULTI_INDEX;
		static const CommandOptionFlag CMD_OPT_WINDOW_SORTED;
		static const CommandOptionFlag CMD_OPT_GROUP_RANGE;
		static const CommandOptionFlag CMD_OPT_JOIN_DRIVING_NONE_LEFT;
		static const CommandOptionFlag CMD_OPT_JOIN_DRIVING_SOME;
	};

	explicit Node(util::StackAllocator &alloc);
	util::StackAllocator& getAllocator() const;

	Node& getInput(SQLPreparedPlan &plan, size_t index);
	const Node& getInput(const SQLPreparedPlan &plan, size_t index) const;

	void setName(const util::String &table, const util::String *db, int64_t nsId);
	void getColumnNameList(util::Vector<util::String> &nameList) const;

	void remapColumnId(
			const util::Vector<const util::Vector<ColumnId>*> &allColumnMap,
			ExprList &exprList);
	void remapColumnId(
			const util::Vector<const util::Vector<ColumnId>*> &allColumnMap,
			Expr *&expr);

	void validateReference(
			const SQLPreparedPlan &plan,
			const SQLTableInfoList *tableInfoList) const;
	void validateReference(
			const SQLPreparedPlan &plan, const SQLTableInfoList *tableInfoList,
			const Id *idRef, const Expr &expr) const;

	uint32_t getInputTableCount() const;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;

	UTIL_OBJECT_CODER_MEMBERS(
			id_,
			UTIL_OBJECT_CODER_ENUM(type_, SQLType::Coder()),
			qName_,
			inputList_,
			predList_,
			outputList_,
			UTIL_OBJECT_CODER_OPTIONAL(tableIdInfo_, SQLTableInfo::IdInfo()),
			indexInfoList_,
			UTIL_OBJECT_CODER_OPTIONAL(offset_, 0),
			UTIL_OBJECT_CODER_OPTIONAL(limit_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(subOffset_, 0),
			UTIL_OBJECT_CODER_OPTIONAL(subLimit_, -1),
			UTIL_OBJECT_CODER_ENUM(
					aggPhase_, AggregationPhase(), SQLType::Coder()),
			UTIL_OBJECT_CODER_ENUM(
					joinType_, JoinType(), SQLType::Coder()),
			UTIL_OBJECT_CODER_ENUM(
					unionType_, UnionType(), SQLType::Coder()),
			UTIL_OBJECT_CODER_OPTIONAL(cmdOptionFlag_, 0),
			updateSetList_,
			createTableOpt_,
			createIndexOpt_,
			partitioningInfo_,
			UTIL_OBJECT_CODER_ENUM(commandType_, SyntaxTree::CMD_NONE),
			cmdOptionList_,
			insertColumnMap_,
			insertSetList_,
			profile_);

	Id id_;

	Type type_;
	const QualifiedName *qName_;

	/**
		@note SCANは常に入力なし、SELECTは0-2入力、その他は1入力以上
	 */
	IdList inputList_;

	/**
		@note
			- AGGREGATE: (グルーピング対象カラムの列)
			- SORT: (ソート対象カラムの列(ソート順序指定あり))
			- JOIN: (メインのON条件: EQ|LT|GT|LE|GE), (WHERE条件)[, (他のON条件)]
			- SELECT: [(WHERE条件)[, (他のON条件)]]
			- SCAN: (WHERE条件)
	 */
	ExprList predList_;

	/**
		@note
			- SQLProcessorで使用されるのはAGGREGATE、JOIN、SCAN、SELECTのみ
			- 上記以外に、SORT、UNIONにおいても使用
	 */
	ExprList outputList_;

	SQLTableInfo::IdInfo tableIdInfo_;
	const SQLTableInfo::BasicIndexInfoList *indexInfoList_;

	int64_t offset_;
	int64_t limit_;
	int64_t subOffset_;
	int64_t subLimit_;

	AggregationPhase aggPhase_;

	/**
		@note JOIN、SELECTでのみ有効
	 */
	JoinType joinType_;

	UnionType unionType_;
	CommandOptionFlag cmdOptionFlag_;

	ExprList *updateSetList_;

	/**
		@note DDLの場合、セット
		@note DMLの場合、パーティショニングカラム等を特定するためにセット。プロセッサ内で内部解析
	 */
	const SyntaxTree::CreateTableOption *createTableOpt_;
	const SyntaxTree::CreateIndexOption *createIndexOpt_;

	/**
		@note DMLの対象がラージの場合のサブコンテナ情報
	 */
	const PartitioningInfo *partitioningInfo_;
	/**
		@note コマンド種別. DDL特定
	 */
	CommandType commandType_;
	const SyntaxTree::ExprList *cmdOptionList_;
	const InsertColumnMap *insertColumnMap_;
	const SyntaxTree::ExprList *insertSetList_;
	const NoSQLColumnInfoList *nosqlTypeList_;
	const NoSQLColumnInfoList *nosqlColumnOptionList_;

	TotalProfile *profile_;
	ProfileKey profileKey_;
};

struct SQLPreparedPlan::Constants {
	enum StringConstant {
		STR_REORDER_JOIN,
		STR_REVERSED_COST,

		STR_TREE_WEIGHT,
		STR_FILTERED_TREE_WEIGHT,
		STR_NODE_DEGREE,

		STR_FILTER,
		STR_FILTER_LEVEL,
		STR_FILTER_DEGREE,
		STR_FILTER_WEAKNESS,

		STR_EDGE,
		STR_EDGE_LEVEL,
		STR_EDGE_DEGREE,
		STR_EDGE_WEAKNESS,

		STR_NONE,
		STR_TABLE,
		STR_INDEX,
		STR_SCHEMA,
		STR_SYNTAX,

		STR_HINT,

		END_STR
	};

	static const util::NameCoderEntry<StringConstant> CONSTANT_LIST[];
	static const util::NameCoder<StringConstant, END_STR> CONSTANT_CODER;
};

struct SQLPreparedPlan::TotalProfile {
	TotalProfile();

	static bool clearIfEmpty(TotalProfile *&profile);

	UTIL_OBJECT_CODER_MEMBERS(plan_);

	Profile *plan_;
};

struct SQLPreparedPlan::Profile {
	Profile();

	static bool clearIfEmpty(Profile *&profile);

	UTIL_OBJECT_CODER_MEMBERS(optimization_);

	OptProfile *optimization_;
};

struct SQLPreparedPlan::OptProfile {
	struct Join;
	struct JoinCost;
	struct JoinNode;
	struct JoinTree;
	struct JoinDriving;
	struct JoinDrivingInput;

	typedef int32_t JoinOrdinal;
	typedef util::Vector<JoinOrdinal> JoinOrdinalList;

	typedef SyntaxTree::QualifiedName QualifiedName;

	OptProfile();

	template<typename T>
	util::Vector<T>*& getEntryList();

	static bool clearIfEmpty(OptProfile *&profile);

	template<typename T>
	static bool isListEmpty(const util::Vector<T> *list);

	UTIL_OBJECT_CODER_MEMBERS(joinReordering_, joinDriving_);

	util::Vector<Join> *joinReordering_;
	util::Vector<JoinDriving> *joinDriving_;
};

struct SQLPreparedPlan::OptProfile::Join {
	Join();

	UTIL_OBJECT_CODER_MEMBERS(
			nodes_, tree_, candidates_,
			UTIL_OBJECT_CODER_OPTIONAL(reordered_, false),
			UTIL_OBJECT_CODER_OPTIONAL(tableCostAffected_, false),
			UTIL_OBJECT_CODER_OPTIONAL(costBased_, true));

	util::Vector<JoinNode> *nodes_;
	JoinTree *tree_;

	util::Vector<JoinTree> *candidates_;
	bool reordered_;
	bool tableCostAffected_;
	bool costBased_;

	ProfileKey profileKey_;
};

struct SQLPreparedPlan::OptProfile::JoinNode {
	JoinNode();

	UTIL_OBJECT_CODER_MEMBERS(
			ordinal_,
			qName_,
			UTIL_OBJECT_CODER_OPTIONAL(approxSize_, -1));

	JoinOrdinal ordinal_;
	QualifiedName *qName_;
	int64_t approxSize_;
};

struct SQLPreparedPlan::OptProfile::JoinTree {
	JoinTree();

	UTIL_OBJECT_CODER_MEMBERS(
			UTIL_OBJECT_CODER_OPTIONAL(ordinal_, -1),
			left_,
			right_,
			UTIL_OBJECT_CODER_ENUM(
					criterion_, Constants::END_STR, Constants::CONSTANT_CODER),
			cost_,
			best_,
			other_,
			target_);

	JoinOrdinal ordinal_;
	JoinTree *left_;
	JoinTree *right_;
	Constants::StringConstant criterion_;
	JoinCost *cost_;

	JoinTree *best_;
	util::Vector<JoinTree> *other_;
	JoinOrdinalList *target_;
};

struct SQLPreparedPlan::OptProfile::JoinCost {
	JoinCost();

	UTIL_OBJECT_CODER_MEMBERS(
			UTIL_OBJECT_CODER_OPTIONAL(filterLevel_, 0U),
			UTIL_OBJECT_CODER_OPTIONAL(filterDegree_, 0U),
			UTIL_OBJECT_CODER_OPTIONAL(filterWeakness_, 0U),
			UTIL_OBJECT_CODER_OPTIONAL(edgeLevel_, 0U),
			UTIL_OBJECT_CODER_OPTIONAL(edgeDegree_, 0U),
			UTIL_OBJECT_CODER_OPTIONAL(edgeWeakness_, 0U),
			UTIL_OBJECT_CODER_OPTIONAL(approxSize_, -1));

	uint32_t filterLevel_; 
	uint32_t filterDegree_; 
	uint64_t filterWeakness_; 

	uint32_t edgeLevel_; 
	uint32_t edgeDegree_; 
	uint64_t edgeWeakness_; 

	int64_t approxSize_; 
};

struct SQLPreparedPlan::OptProfile::JoinDriving {
	JoinDriving();

	UTIL_OBJECT_CODER_MEMBERS(driving_, inner_, left_, right_);

	JoinDrivingInput *driving_;
	JoinDrivingInput *inner_;

	JoinDrivingInput *left_;
	JoinDrivingInput *right_;

	ProfileKey profileKey_;
};

struct SQLPreparedPlan::OptProfile::JoinDrivingInput {
	JoinDrivingInput();

	UTIL_OBJECT_CODER_MEMBERS(
			qName_,
			cost_,
			tableScanCost_,
			indexScanCost_,
			UTIL_OBJECT_CODER_ENUM(
					criterion_, Constants::END_STR, Constants::CONSTANT_CODER),
			UTIL_OBJECT_CODER_OPTIONAL(indexed_, true),
			UTIL_OBJECT_CODER_OPTIONAL(reducible_, true));

	QualifiedName *qName_;
	JoinCost *cost_;
	JoinCost *tableScanCost_;
	JoinCost *indexScanCost_;
	Constants::StringConstant criterion_;
	bool indexed_;
	bool reducible_;
};

class SQLHintInfo {
public:
	typedef SQLPreparedPlan::Node PlanNode;
	typedef SQLPreparedPlan::Node::Id PlanNodeId;
	typedef SQLPreparedPlan::Constants::StringConstant StringConstant;
	typedef uint32_t HintTableId;
	typedef SyntaxTree::Expr Expr;
	typedef SyntaxTree::ExprList ExprList;
	typedef util::Vector<const Expr*> HintExprArray;
	typedef util::Map<SQLHint::Id, HintExprArray*> HintExprMap;
	typedef std::pair<util::String, util::String> StatisticalHintKey;
	typedef util::Map<StatisticalHintKey, util::Vector<SQLHintValue> > StatisticalHintMap;
	typedef util::Vector<PlanNodeId> PlanNodeIdList;
	typedef util::Vector<HintTableId> HintTableIdList;

	typedef SQLTableInfo::Id TableInfoId; 

	static const HintTableId START_HINT_TABLEID;
	static const HintTableId UNDEF_HINT_TABLEID;

	enum ReportLevel {
		REPORT_NONE = 0,
		REPORT_ERROR,
		REPORT_WARNING,
		REPORT_INFO
	};

	struct SymbolElement {
		SymbolElement();

		StringConstant value_;
	};

	struct SymbolList {
		enum {
			SYMBOL_LIST_SIZE = 3
		};

		StringConstant getValue(size_t pos) const;
		void setValue(size_t pos, StringConstant value);

		SymbolElement elems_[SYMBOL_LIST_SIZE];
	};

	struct HintTableIdMapValue {
		HintTableIdMapValue(HintTableId id, const util::String &origName);

		const util::String origName_;
		const HintTableId id_;
	};
	typedef util::Map<util::String, HintTableIdMapValue> HintTableIdMap;

	struct TableInfoIdMapValue {
		TableInfoIdMapValue(TableInfoId id, const util::String &origName);

		const util::String origName_;
		const TableInfoId id_;
	};
	typedef util::Map<util::String, TableInfoIdMapValue> TableInfoIdMap;

	struct HintTableIdListKey {
		explicit HintTableIdListKey(util::StackAllocator &alloc);
		bool operator<(const HintTableIdListKey &another) const;
		void push_back(HintTableId value);
		void clear();
		void reserve(size_t size);

		util::StackAllocator &alloc_;
		HintTableIdList idList_;
	};

	struct ParsedHint {
		explicit ParsedHint(util::StackAllocator &alloc);

		util::StackAllocator &alloc_;
		SQLHint::Id hintId_;
		const Expr* hintExpr_;
		HintTableIdList hintTableIdList_;
		SymbolList symbolList_;
		bool isTree_;

		ParsedHint& operator=(const ParsedHint &another);
	};

	struct SimpleHint {
		SQLHint::Id hintId_;
		const Expr* hintExpr_;
	};

	typedef util::MultiMap<HintTableIdListKey, ParsedHint> JoinHintMap;
	typedef util::Map<TableInfoId, SimpleHint> ScanHintMap;
	typedef std::pair<HintTableId, HintTableId> JoinMethodHintKey;
	typedef util::Map<JoinMethodHintKey, SimpleHint> JoinMethodHintMap;

	explicit SQLHintInfo(util::StackAllocator &alloc);
	~SQLHintInfo();

	bool hasHint(SQLHint::Id) const;
	const HintExprArray* getHintList(SQLHint::Id) const;

	void getHintValueList(SQLHint::Id id, util::Vector<TupleValue> &valueList) const;

	bool findJoinHintList(
			const util::Vector<const util::String*> &nodeNameList,
			SQLHint::Id filter,
			util::Vector<SQLHintInfo::ParsedHint> &hintList) const;

	const SQLHintValue* findStatisticalHint(
			SQLHint::Id hintId, const char8_t *tableName) const;

	bool findScanHint(const TableInfoId tableInfoId, const SimpleHint* &scanHint) const;

	bool findJoinMethodHint(
			const util::Vector<const util::String*> &nodeNameList,
			SQLHint::Id filter, const SimpleHint* &scanHint) const;

	bool findJoinMethodHint(
			const JoinMethodHintKey &key, SQLHint::Id filter,
			const SimpleHint* &scanHint) const;
	const SQLHintValue* findStatisticalHint(
			SQLHint::Id hintId, const char8_t *tableName);

	const util::Vector<ParsedHint>& getSymbolHintList() const;

	void setReportLevel(ReportLevel flag);

	bool checkReportLevel(ReportLevel type) const;

	void reportError(const char8_t* message) const;
	void reportWarning(const char8_t* message) const;
	void reportInfo(const char8_t* message) const;

	void makeHintMap(const SyntaxTree::ExprList *hintList);

	void makeJoinHintMap(const SQLPreparedPlan &plan);

	void insertHintTableIdMap(const util::String& origName, HintTableId id);
	void insertTableInfoIdMap(const util::String& origName, TableInfoId id);

	HintTableId getHintTableId(const util::String& name) const;

	void dumpList(std::ostream &os, const HintTableIdList &target) const;

private:
	void parseStatisticalHint(SQLHint::Id hintId, const Expr &hintExpr);
	void parseSymbolHint(SQLHint::Id hintId, const Expr *hintExpr);

	HintTableId getHintTableId(const Expr &hintArgExpr) const;
	TableInfoId getTableInfoId(const Expr &hintArgExpr) const;

	bool resolveHintTableArg(
			const Expr &oneHintExpr,
			HintTableIdList &hintTableIdList, bool &isTree);

	bool resolveHintTableTreeArg(
			const Expr &startExpr,
			HintTableIdList &hintTableIdList);

	bool resolveScanHintArg(
			const Expr &oneHintExpr, TableInfoId &tableInfoId);

	bool resolveJoinMethodHintArg(
			const Expr &oneHintExpr, JoinMethodHintKey &hintKey);

	void setScanHintMap(
			SQLHint::Id hintId, const Expr *hintExpr, const TableInfoId tableInfoId);

	void setJoinMethodHintMap(
			SQLHint::Id hintId, const Expr *hintExpr, const JoinMethodHintKey &key);

	void setJoinHintMap(
			SQLHint::Id hintId, const Expr *hintExpr,
			const HintTableIdList &hintTableIdList, bool isTree);

	void getExprToken(const Expr &expr, util::String &exprToken) const;

	bool checkHintArguments(SQLHint::Id hintId, const Expr &hintExpr) const;

	void checkExclusiveHints(SQLHint::Id hintId1, SQLHint::Id hintId2) const;

	bool checkArgCount(
			const Expr &hintExpr,
			size_t count, bool isMinimum) const;

	bool checkArgSQLOpType(
			const Expr &hintExpr,
			SQLType::Id type) const;

	bool checkAllArgsValueType(
			const Expr &hintExpr,
			TupleList::TupleColumnType type,
			int64_t minValue = std::numeric_limits<int64_t>::min(),
			int64_t maxValue = std::numeric_limits<int64_t>::max()) const;

	bool checkArgValueType(
			const Expr &hintExpr, size_t pos,
			TupleList::TupleColumnType type,
			int64_t minValue = std::numeric_limits<int64_t>::min(),
			int64_t maxValue = std::numeric_limits<int64_t>::max()) const;

	bool checkValueRange(int64_t value, int64_t min, int64_t max) const;

	bool checkArgIsTable(
			const Expr &hintExpr, size_t pos, bool forSymbol = false) const;

	bool checkLeadingTreeArgumentsType(const Expr &expr) const;

	util::StackAllocator &alloc_;
	HintExprMap hintExprMap_;
	StatisticalHintMap statisticalHintMap_;
	HintTableIdMap hintTableIdMap_;
	JoinHintMap joinHintMap_;
	TableInfoIdMap tableInfoIdMap_;
	ScanHintMap scanHintMap_;
	JoinMethodHintMap joinMethodHintMap_;
	util::Vector<ParsedHint> symbolHintList_;
	ReportLevel reportLevel_;
};


class SQLCompiler {
private:
	template<typename V> struct IsUInt;

public:
	class Meta;

	struct JoinNode;
	struct JoinEdge;
	struct JoinOperation;
	class JoinScore;

	class SubqueryStack;
	template<bool Const = true> class ExprArgsIterator;
	class PlanNodeRef;
	class ExprRef;
	class ExprHint;
	class QualifiedNameFormatter;
	class SubPlanHasher;
	class SubPlanEq;
	class NarrowingKey;
	class NarrowingKeyTable;
	class TableNarrowingList;
	class JoinExpansionInfo;
	class JoinExpansionBuildInfo;
	class IntegralPartitioner;
	class OptimizationContext;
	class OptimizationUnit;
	class OptimizationOption;
	class CompileOption;
	class ProcessorUtils;
	class Profiler;
	class ProfilerManager;
	class InternalTool; 

	class GenRAContext;
	class GenOverContext;

	class ReorderJoin;
	class SelectJoinDriving;

	typedef SQLTableInfo TableInfo;
	typedef SQLTableInfoList TableInfoList;

	typedef SyntaxTree::QualifiedName QualifiedName;
	typedef SyntaxTree::Select Select;
	typedef SQLPreparedPlan Plan;

	typedef uint32_t JoinNodeId;
	typedef uint32_t JoinEdgeId;
	typedef std::pair<JoinNodeId, JoinNodeId> JoinScoreKey;
	typedef util::Map<JoinScoreKey, JoinScore> JoinScoreMap;

	typedef int32_t OptimizationFlags;

	enum JoinScoreOpLevel {
		JOIN_SCORE_OP_START = 0,	
		JOIN_SCORE_OP_LV1,			
		JOIN_SCORE_OP_LV2,			
		JOIN_SCORE_OP_LV3,			
		JOIN_SCORE_OP_LV4,			
		JOIN_SCORE_OP_END
	};

	enum OptimizationUnitType {
		OPT_START,
		OPT_MERGE_SUB_PLAN = OPT_START,
		OPT_MINIMIZE_PROJECTION,
		OPT_PUSH_DOWN_PREDICATE,
		OPT_PUSH_DOWN_AGGREGATE,
		OPT_PUSH_DOWN_JOIN,
		OPT_PUSH_DOWN_LIMIT,
		OPT_REMOVE_REDUNDANCY,
		OPT_REMOVE_INPUT_REDUNDANCY,
		OPT_REMOVE_PARTITION_COND_REDUNDANCY,
		OPT_ASSIGN_JOIN_PRIMARY_COND,
		OPT_MAKE_SCAN_COST_HINTS,
		OPT_MAKE_JOIN_PUSH_DOWN_HINTS,
		OPT_MAKE_INDEX_JOIN,
		OPT_FACTORIZE_PREDICATE,
		OPT_REORDER_JOIN,
		OPT_SIMPLIFY_SUBQUERY,
		OPT_END
	};

	static const JoinNodeId UNDEF_JOIN_NODEID;
	static const Plan::Node::Id UNDEF_PLAN_NODEID;

	explicit SQLCompiler(
			TupleValue::VarContext &varCxt, const CompileOption *option = NULL);
	~SQLCompiler();

	void setTableInfoList(const TableInfoList *infoList);

	void setParameterTypeList(Plan::ValueTypeList *parameterTypeList);

	void setInputSql(const char* sql);

	void setMetaDataQueryFlag(bool isMetaDataQuery);
	bool isMetaDataQuery();

	void setExplainType(SyntaxTree::ExplainType explainType);

	void setQueryStartTime(int64_t currentTime);

	void setPlanSizeLimitRatio(size_t ratio);
	void setIndexScanEnabled(bool enabled);
	void setMultiIndexScanEnabled(bool enabled);

	void compile(const Select &in, Plan &out, SQLConnectionControl *control = NULL);

	bool isRecompileOnBindRequired() const;
	void bindParameterTypes(Plan &plan);

	static bool reducePartitionedTargetByTQL(
			util::StackAllocator &alloc, const TableInfo &tableInfo,
			const Query &query, bool &uncovered,
			util::Vector<int64_t> *affinityList,
			util::Vector<uint32_t> *subList);
	static FullContainerKey* predicateToContainerKeyByTQL(
			util::StackAllocator &alloc, TransactionContext &txn,
			DataStoreV4 &dataStore, const Query &query, DatabaseId dbId,
			ContainerId metaContainerId, PartitionId partitionCount,
			util::String &dbNameStr, bool &fullReduced,
			PartitionId &reducedPartitionId);

	static bool isDBOnlyMatched(
			const QualifiedName &name1, const QualifiedName &name2);
	static bool isTableOnlyMatched(
			const QualifiedName &name1, const QualifiedName &name2);
	static bool isNameOnlyMatched(
			const QualifiedName &name1, const QualifiedName &name2);

	static int32_t compareStringWithCase(
			const char8_t *str1, const char8_t *str2, bool caseSensitive);

private:
	class CompilerSource;

	class CompilerInitializer {
	public:
		class End {
		public:
			explicit End(CompilerInitializer &init);
		};

		explicit CompilerInitializer(const CompileOption *option);

		operator bool();
		const SQLCompiler* operator->();

		ProfilerManager& profilerManager();

		void close();

	private:
		struct InitializerConstants {
			static CompileOption INVALID_OPTION;
		};

		const CompileOption *baseOption_;
	};

	typedef TupleList::TupleColumnType ColumnType;
	typedef TupleColumnTypeUtils ColumnTypeUtils;

	typedef SQLType::AggregationPhase AggregationPhase;

	typedef SyntaxTree::SourceId SourceId;
	typedef SyntaxTree::Set Set;
	typedef SyntaxTree::Table Table;
	typedef SyntaxTree::Expr Expr;

	typedef SyntaxTree::ExprList ExprList;
	typedef SyntaxTree::SelectList SelectList;

	typedef Plan::Node PlanNode;
	typedef Plan::NodeList PlanNodeList;

	typedef PlanNode::Type Type;
	typedef PlanNode::Id PlanNodeId;
	typedef PlanNode::ExprList PlanExprList;

	typedef util::Set<util::String> StringSet;
	typedef std::pair<PlanNodeId, Expr> PendingColumn;
	typedef util::Vector<PendingColumn> PendingColumnList;

	typedef util::Vector<SQLTableInfo::IdInfo> TableInfoIdList;

	typedef std::pair<PlanNodeId, PlanNodeId> JoinExpansionEntry;
	typedef util::Vector<JoinExpansionEntry> JoinExpansionList;
	typedef util::Vector< std::pair<uint64_t, uint32_t> > JoinMatchCountList;

	typedef std::pair<uint64_t, uint32_t> JoinExpansionScore;
	typedef util::Vector<JoinExpansionScore> JoinExpansionScoreList;

	typedef std::pair<JoinExpansionScore, PlanNodeId> JoinExpansionScoredId;
	typedef util::Vector<JoinExpansionScoredId> JoinExpansionScoreIdList;

	typedef std::list< Expr, util::StdAllocator<
			Expr, util::StackAllocator> > TempColumnExprList;

	typedef SQLPreparedPlan::Node::InsertColumnMap InsertColumnMap;

	typedef util::Vector<ExprRef> ExprRefList;

	enum Mode {
		MODE_DEFAULT = 0,
		MODE_NORMAL_SELECT,
		MODE_AGGR_SELECT,
		MODE_GROUP_SELECT,
		MODE_FROM,
		MODE_ON,
		MODE_WHERE,
		MODE_GROUPBY,
		MODE_HAVING,
		MODE_ORDERBY,
		MODE_LIMIT,
		MODE_OFFSET
	};

	enum {
		JOIN_INPUT_COUNT = 2,
		JOIN_LEFT_INPUT = 0,
		JOIN_RIGHT_INPUT = 1
	};

	enum {
		FUNCTION_SCALAR = 0,
		FUNCTION_AGGR,
		FUNCTION_WINDOW,
		FUNCTION_PSEUDO_WINDOW
	};

	typedef PlanNode::IdList JoinNodeIdList[JOIN_INPUT_COUNT];
	typedef util::Vector<uint32_t> JoinUnifyingUnitList[JOIN_INPUT_COUNT];
	typedef bool JoinBoolList[JOIN_INPUT_COUNT];
	typedef size_t JoinSizeList[JOIN_INPUT_COUNT];

	typedef NarrowingKeyTable JoinKeyTableList[JOIN_INPUT_COUNT];
	typedef util::Vector<uint32_t> JoinKeyIdList[JOIN_INPUT_COUNT];
	typedef util::Vector<bool> JoinInputUsageList[JOIN_INPUT_COUNT];

	typedef std::pair<SQLPreparedPlan::ProfileKey, PlanNodeId> KeyToNodeId;
	typedef util::Vector<KeyToNodeId> KeyToNodeIdList;

	static const ColumnId REPLACED_AGGR_COLUMN_ID;

	static const uint64_t JOIN_SCORE_MAX;
	static const uint64_t DEFAULT_MAX_INPUT_COUNT;
	static const uint64_t DEFAULT_MAX_EXPANSION_COUNT;

	static const SQLPlanningVersion LEGACY_JOIN_REORDERING_VERSION;
	static const SQLPlanningVersion LEGACY_JOIN_DRIVING_VERSION;

	void genDistributedPlan(Plan &plan, bool costCheckOnly);

	void genUnionAllShallow(
			const util::Vector<PlanNodeId> &inputIdList,
			PlanNodeId replaceNodeId, Plan &plan);

	void genSelect(
			const Select &in, bool innerGenSet, PlanNodeId &productedNodeId,
			Plan &plan, bool removeAutoGen = false);
	void genResult(Plan &plan);

	void genInsert(const Select &in, Plan &plan);
	void genUpdate(const Select &in, Plan &plan);
	void genDelete(const Select &in, Plan &plan);
	void genDDL(const Select &in, Plan &plan);

	void checkLimit(Plan &plan);

	void validateCreateTableOption(const SyntaxTree::CreateTableOption &option);
	void validateScanOptionForVirtual(const PlanExprList &scanOptionList, bool isVirtual);
	void validateCreateIndexOptionForVirtual(
			SyntaxTree::QualifiedName &indexName,
			SyntaxTree::CreateIndexOption &option, Plan::ValueTypeList *typeList);

	int32_t makeInsertColumnMap(const SQLTableInfo &tableInfo,
			ExprList *insertList, InsertColumnMap &columnMap, util::Vector<TupleList::TupleColumnType> &columnTypeList);

	void setTargetTableInfo(const char *tableName, bool isCaseSensitive,
			const SQLTableInfo &tableInfo, SQLPreparedPlan::Node &node);

	size_t validateWindowFunctionOccurrence(
			const Select &select,
			size_t &genuineWindowExprCount, size_t &pseudoWindowExprCount);
	bool findWindowExpr(const ExprList &exprList, bool resolved, const Expr* &foundExpr);
	bool findWindowExpr(const Expr &expr, bool resolved, const Expr* &foundExpr);
	bool isRankingWindowExpr(const Expr &expr);

	uint32_t calcFunctionCategory(const Expr &expr, bool resolved);

	static bool isRangeGroupNode(const PlanNode &node);
	static bool isWindowNode(const PlanNode &node);

	void checkNestedAggrWindowExpr(
			const Expr &expr, bool resolved,
			size_t &genuineWindowCount, size_t &pseudoWindowCount,
			size_t &aggrCount);

	void checkNestedAggrWindowExpr(const ExprList &exprList, bool resolved);
	void genFrom(const Select &select, const Expr* &whereExpr, Plan &plan);

	void genWhere(GenRAContext &cxt, Plan &plan);
	void genGroupBy(GenRAContext &cxt, ExprRef &havingExprRef, Plan &plan);
	void genHaving(GenRAContext &cxt, const ExprRef &havingExprRef, Plan &plan);

	bool genRangeGroup(GenRAContext &cxt, ExprRef &havingExprRef, Plan &plan);

	bool genRangeGroupIdNode(
			Plan &plan, const Select &select, int32_t &rangeGroupIdPos);
	static bool isRangeGroupPredicate(const ExprList *groupByList);

	void adjustRangeGroupOutput(
			Plan &plan, PlanNode &node, const Expr &keyExpr);
	void adjustRangeGroupOutput(Expr &outExpr, const Expr &keyExpr);

	Expr resolveRangeGroupPredicate(
			const Plan &plan, const Expr &whereCond, const Expr &keyExpr,
			const Expr &keyExprInCond, const Expr &optionExpr);
	Expr makeRangeGroupIdExpr(Plan &plan, const Expr &optionExpr);
	Expr makeRangeGroupPredicate(
			const Expr &keyExpr, Type fillType, const TupleValue &interval,
			const TupleValue &lower, const TupleValue &upper, int64_t limit);

	void getRangeGroupOptions(
			const Expr &optionExpr, Expr *&inKeyExpr, Type &fillType,
			int64_t &baseInterval, util::DateTime::FieldType &unit,
			int64_t &baseOffset, util::TimeZone &timeZone);
	bool findRangeGroupBoundary(
			const Expr &expr, const Expr &keyExpr, TupleValue &value,
			bool forLower);
	util::TimeZone resolveRangeGroupTimeZone(const TupleValue &specified);
	Expr* makeRangeGroupConst(const TupleValue &value);

	bool findDistinctAggregation(const ExprList *exprList);
	bool findDistinctAggregation(const Expr *expr);

	bool genSplittedSelectProjection(
			GenRAContext &cxt, const ExprList &inSelectList,
			Plan &plan, bool leaveInternalIdColumn = false);

	class ColumnExprId {
	public:
		SourceId srcId_;
		uint32_t subqueryLevel_;
		SyntaxTree::QualifiedName *qName_;

		ColumnExprId(): srcId_(-1), subqueryLevel_(-1), qName_(NULL) {}

		ColumnExprId(SourceId srcId, uint32_t subqueryLevel, SyntaxTree::QualifiedName *qName) :
		srcId_(srcId), subqueryLevel_(subqueryLevel), qName_(qName) {}

		bool operator==(const ColumnExprId &another) const {
			if (srcId_ == another.srcId_ && subqueryLevel_ == another.subqueryLevel_) {
				if (qName_ && qName_->name_ && another.qName_ && another.qName_->name_) {
					return isNameOnlyMatched(*qName_, *another.qName_);
				}
				else if (qName_ && another.qName_) {
					return (!(qName_->name_) && !(another.qName_->name_));
				}
				else if (!qName_ && !another.qName_) {
					return true;
				}
			}
			return false;
		}

		bool operator<(const ColumnExprId &another) const {
			if (srcId_ != another.srcId_) {
				return (srcId_ < another.srcId_);
			}
			else if (subqueryLevel_ != another.subqueryLevel_) {
				return (subqueryLevel_ < another.subqueryLevel_);
			}
			else if (qName_ && qName_->name_ && another.qName_ && another.qName_->name_) {
				return compareStringWithCase(
					qName_->name_->c_str(), another.qName_->name_->c_str(),
					(qName_->nameCaseSensitive_ || another.qName_->nameCaseSensitive_)) < 0;
			}
			else if (qName_ && another.qName_) {
					return (qName_->name_ < another.qName_->name_);
			}
			return (qName_ < another.qName_);
		}
	};

	void genOver(GenRAContext &cxt, Plan &plan);
	void genPseudoWindowFunc(GenRAContext &cxt, Plan &plan);
	void genAggrFunc(GenRAContext &cxt, Plan &plan);
	void genLastOutput(GenRAContext &cxt, Plan &plan);
	void appendUniqueExpr(const PlanExprList &appendlist, PlanExprList &outList);

	void dumpPlanNode(Plan &plan);
	void dumpExprList(const PlanExprList &node);

	void trimExprList(size_t len, PlanExprList &list);
	void hideSelectListName(GenRAContext &cxt, SQLPreparedPlan::Node &inputNode, Plan &plan);
	void restoreSelectListName(GenRAContext &cxt, PlanExprList &restoreList, Plan &plan);
	void genOrderBy(GenRAContext &cxt, ExprRef &innerTopExprRef, Plan &plan);
	void genLimitOffset(
			const Expr *limitExpr, const Expr *offsetExpr, Plan &plan);

	void genSelectProjection(
			GenRAContext &cxt, Plan &plan, bool removeAutoGen = false);

	void genSelectProjection(
			const Select &select, const size_t basePlanSize,
			bool inSubquery, PlanExprList &selectList,
			ExprList *&windowExprList, size_t &windowExprCount,
			PlanNodeId &productedNodeId, Plan &plan,
			bool removeAutoGen = false);

	void genWindowOverClauseOption(
			GenRAContext &cxt, GenOverContext &overCxt, Plan &plan);
	void genWindowSortPlan(
			GenRAContext &cxt, GenOverContext &overCxt, Plan &plan);
	void genWindowMainPlan(
			GenRAContext &cxt, GenOverContext &overCxt, Plan &plan);
	void genFrameOptionExpr(
			GenOverContext& overCxt, PlanExprList& tmpFrameOptionList,
			Plan& plan);

	TupleValue getWindowFrameBoundaryValue(
			const SyntaxTree::WindowFrameBoundary &boundary, Plan &plan);
	int64_t genFrameRangeValue(const Expr *inExpr, Plan &plan);

	void checkWindowFrameFunctionType(const Expr &windowExpr);

	static void checkWindowFrameValues(
			const TupleValue &startValue, const TupleValue &finishValue,
			int64_t flags, const Expr *inExpr);
	static void checkWindowFrameBoundaryOptions(
			int64_t flags, const TupleValue &startValue,
			const TupleValue &finishValue, const Expr *inExpr);

	static void checkWindowFrameFlags(int64_t flags, const Expr *inExpr);

	static void checkWindowFrameOrdering(
			const PlanExprList &orderByList, int64_t flags,
			const TupleValue &startValue, const TupleValue &finishValue,
			const Expr *inExpr);

	static int64_t windowFrameModeToFlag(
			SyntaxTree::WindowFrameOption::FrameMode mode);

	static int64_t getWindowFrameOrderingFlag(const PlanExprList &orderByList);
	static int64_t windowFrameTypeToFlag(SQLType::WindowFrameType type);

	void preparePseudoWindowOption(
			GenRAContext &cxt, GenOverContext &overCxt, Plan &plan);

	void genPseudoWindowResult(
			GenRAContext &cxt, GenOverContext &overCxt, Plan &plan);

	void genSelectList(
			const Select &select, Plan &plan, PlanExprList &selectList,
			Mode mode, const PlanNodeId *productedNodeId);

	void genPromotedProjection(
			Plan &plan, const util::Vector<ColumnType> &columnTypeList);

	void genSubquery(const Expr &inExpr, Plan &plan, Mode mode, Type type);

	void genSubquerySet(
			const Set &set, Type type, PlanNodeId outerBaseNodeId,
			PlanExprList &selectList, PlanNodeId &productedNodeId,
			ExprRef &innerIdExprRef, ExprRef &innerTopExprRef,
			const Select* &lastSelect, bool &needTocompensate, Plan &plan);
	void genSubqueryUnionAll(
			const Set &set, Type type, PlanNodeId outerBaseNodeId,
			PlanExprList &selectList, PlanNodeId &productedNodeId,
			ExprRef &innerIdExprRef, ExprRef &innerTopExprRef,
			const Select* &lastSelect, bool &needTocompensate, Plan &plan);
	void genSubquerySelect(
			const Select &select, Type type, PlanNodeId outerBaseNodeId,
			PlanExprList &selectList, PlanNodeId &productedNodeId,
			ExprRef &innerIdExprRef, ExprRef &innerTopExprRef,
			bool &needTocompensate, Plan &plan);
	void genEmptyRootNode(Plan &plan);

	void genTable(const Expr &inExpr, const Expr* &whereExpr, Plan &plan);

	bool isMetadataQuery();
	static void unescape(util::String &str, char escape);

	void genUnionAll(const Set &set, const util::String *tableName, Plan &plan);
	void genSet(const Set &set, const util::String *tableName, Plan &plan);
	void genJoin(
			const Expr &joinExpr, const util::String *tableName,
			const Expr* &whereExpr, const bool isTop,
			StringSet &aliases, Plan &plan);
	void genJoinCondition(const Plan &plan,
		TempColumnExprList& baseColList, TempColumnExprList& anotherColList,
		PlanExprList &usingExprList, Expr &onExpr, PlanExprList &outColList);
	void genTableOrSubquery(const Expr &table, const Expr* &whereExpr,
			const bool isTop, StringSet &aliases, Plan &plan);

	void prepareInnerJoin(const Plan &plan, const Expr *whereExpr,
			SQLPreparedPlan::Node &node);
	void validateGroupBy(
			const PlanExprList &groupByList, const PlanExprList &selectList);
	bool validateColumnExpr(const Expr &expr, const PlanExprList &groupByList);
	void validateSelectList(const PlanExprList &selectList);
	bool findAggrExpr(const ExprList &exprList, bool resolved, bool exceptWindowFunc = false);
	bool findAggrExpr(const Expr &expr, bool resolved, bool exceptWindowFunc = false);
	bool findSubqueryExpr(const ExprList &exprList);
	bool findSubqueryExpr(const Expr *expr);
	const Expr* findFirstColumnExpr(const Expr &expr);
	bool checkExistAggrExpr(const ExprList &exprList, bool resolved);

	Expr genExpr(const Expr &inExpr, Plan &plan, Mode mode);
	PlanExprList genExprList(
			const ExprList &inExprList, Plan &plan, Mode mode,
			const PlanNodeId *productedNodeId, bool wildcardEnabled = false);
	PlanExprList genColumnExprList(
			const ExprList &inExprList, Plan &plan, Mode mode,
			const PlanNodeId *productedNodeId);

	PlanNodeRef toRef(Plan &plan, const PlanNode *node = NULL);
	ExprRef toRef(Plan &plan, const Expr &expr, const PlanNode *node = NULL);

	void expandSubquery(const Expr &inExpr, Plan &plan, Mode mode);
	Expr genScalarExpr(
			const Expr &inExpr, const Plan &plan, Mode mode, bool aggregated,
			const PlanNodeId *productedNodeId);
	Expr genScalarExpr(
			const Expr &inExpr, const Plan &plan, Mode mode, bool aggregated,
			const PlanNodeId *productedNodeId, ExprHint &exprHint,
			Type parentExprType);
	void setUpExprAttributes(
			Expr &outExpr, const Expr *inExpr, ColumnType columnType,
			Mode mode, AggregationPhase phase);
	static void setUpColumnExprAttributes(Expr &outExpr, bool aggregated);

	PlanExprList genAllColumnExpr(
			const Plan &plan, Mode mode, const QualifiedName *name);
	Expr genColumnExpr(const Plan &plan, uint32_t inputId, ColumnId columnId);
	Expr genColumnExprBase(
			const Plan *plan, const PlanNode *node, uint32_t inputId,
			ColumnId columnId, const Expr *baseExpr);

	Expr genSubqueryRef(const Expr &inExpr, Mode mode);
	Expr genConstExpr(
			const Plan &plan, const TupleValue &value, Mode mode,
			const char8_t *name = NULL);
	Expr genConstExprBase(
			const Plan *plan, const TupleValue &value, Mode mode,
			const char8_t *name = NULL);
	Expr genTupleIdExpr(const Plan &plan, uint32_t inputId, Mode mode);
	Expr genCastExpr(
			const Plan &plan, ColumnType columnType, Expr *inExpr,
			Mode mode, bool shallow, bool withAttributes);
	Expr genOpExpr(
			const Plan &plan, Type exprType, Expr *left, Expr *right,
			Mode mode, bool shallow,
			AggregationPhase phase = SQLType::AGG_PHASE_ALL_PIPE);
	Expr genOpExprBase(
			const Plan *plan, Type exprType, Expr *left, Expr *right,
			Mode mode, bool shallow,
			AggregationPhase phase = SQLType::AGG_PHASE_ALL_PIPE);
	Expr genExprWithArgs(
			const Plan &plan, Type exprType, const ExprList *argList,
			Mode mode, bool shallow,
			AggregationPhase phase = SQLType::AGG_PHASE_ALL_PIPE);
	Expr genExprWithArgs(
			const Plan *plan, Type exprType, const ExprList *argList,
			Mode mode, bool shallow,
			AggregationPhase phase = SQLType::AGG_PHASE_ALL_PIPE);
	int64_t genLimitOffsetValue(const Expr *inExpr, Plan &plan, Mode mode);

	void reduceExpr(Expr &expr, bool strict);
	void validateAggrExpr(const Expr &inExpr, Mode mode);

	void minusOpToBinary(const Plan &plan, Expr &expr, Mode mode);
	void inListToOrTree(const Plan &plan, Expr &expr, Mode mode);
	void exprListToTree(
			const Plan *plan, Type type, ExprList &list, Mode mode,
			Expr *&outExpr);
	void betweenToAndTree(const Plan &plan, Expr &expr, Mode mode);

	void splitAggrWindowExprList(GenRAContext &cxt, const ExprList &inExprList, const Plan &plan);
	Expr genReplacedAggrExpr(const Plan &plan, const Expr *inExpr);

	bool splitSubqueryCond(
			const Expr &inExpr, Expr *&scalarExpr, Expr *&subExpr);
	bool splitJoinPrimaryCond(
			const Plan &plan, const Expr &srcWhere, const Expr &srcOn,
			Expr *&primaryCond, Expr *&destWhere, Expr *&destOn,
			SQLType::JoinType joinType, const Expr *orgPrimaryCond = NULL,
			bool complex = false);
	bool splitJoinPrimaryCondSub(
			const Plan &plan, Expr &target, Expr *&primaryCond,
			uint32_t maxScore, bool complex, Mode mode, bool removable);

	static uint32_t getJoinPrimaryCondScore(
			const Expr &expr, const uint32_t *maxScore, bool complex);

	static bool getJoinCondColumns(
			const PlanNode &node,
			util::Vector< std::pair<ColumnId, ColumnId> > &columnIdPairList);
	static void getJoinCondColumns(
			const Expr &expr,
			util::Vector< std::pair<ColumnId, ColumnId> > &columnIdPairList);

	bool splitExprList(
			const PlanExprList &inExprList, AggregationPhase inPhase,
			PlanExprList &outExprList1, AggregationPhase &outPhase1,
			PlanExprList &outExprList2, AggregationPhase &outPhase2,
			const PlanExprList *groupExprList, PlanExprList *groupExprList2);
	bool splitExprListSub(
			const Expr &inExpr, PlanExprList &outExprList,
			const Expr *&outExpr, uint32_t parentLevel, size_t groupCount,
			bool aggregating, bool lastTop,
			util::Vector<uint32_t> *inputOffsetList);

	static void makeSplittingInputOffsetList(
			const PlanExprList &exprList, util::Vector<uint32_t> &offsetList);
	static void makeSplittingInputSizeList(
			const Expr &expr, util::Vector<uint32_t> &sizeList);
	void setSplittedExprListType(
			PlanExprList &exprList, AggregationPhase &phase);

	void applySubqueryId(const Expr &idRefExpr, Expr &expr);

	PlanExprList tableInfoToOutput(
			const Plan &plan, const TableInfo &info, Mode mode,
			int32_t subContainerId = 0);
	PlanExprList inputListToOutput(
			const Plan &plan, bool withAutoGenerated = true);
	PlanExprList inputListToOutput(
			const Plan &plan, const PlanNode &node,
			const uint32_t *inputId, bool withAutoGenerated = true);
	PlanExprList mergeOutput(
			const Plan &plan, const PlanNode *node = NULL,
			bool unionMismatchIgnorable = false);

	void refreshAllColumnTypes(
			Plan &plan, bool binding, bool *insertOrUpdateFound);
	void refreshColumnTypes(
			Plan &plan, PlanNode &node, PlanExprList &exprList, bool forOutput,
			bool binding, bool unionMismatchIgnorable = false);
	void refreshColumnTypes(
			Plan &plan, PlanNode &node, Expr &expr, bool forOutput,
			bool aggregated, bool binding, AggregationPhase phase,
			const PlanExprList *outputList, const ColumnId *outputColumnId);

	ColumnType followAdvanceColumnType(
			Plan &plan, PlanNode &node, const Expr &expr,
			bool forOutput, bool binding, const PlanExprList *outputList,
			const ColumnId *outputColumnId);
	ColumnType getAggregationFollowingColumnType(
			Type nodeType, const PlanExprList &outputList, ColumnId columnId,
			bool forOutput, bool binding, AggregationPhase phase,
			bool forWindow);
	Mode getModeForColumnTypeRefresh(Type type, bool forWindow);

	void checkModificationColumnConversions(
			const Plan &plan, bool insertOrUpdateFound);
	static bool isForInsertOrUpdate(const PlanNode &node);

	void finalizeScanCostHints(Plan &plan, bool tableCostAffected);

	void applyPlanningOption(Plan &plan);
	static void applyPlanningVersion(
			const SQLPlanningVersion &src, SQLPlanningVersion &dest);

	void applyScanOption(Plan &plan);
	void applyScanOption(
			PlanNode::CommandOptionFlag &optionFlag, const PlanNode &node,
			const SQLHintInfo &hint);

	void applyJoinOption(Plan &plan);
	void applyJoinOption(
			const Plan &plan, const PlanNode &node,
			PlanNode::CommandOptionFlag &optionFlag, const SQLHintInfo &hint);

	void relocateOptimizationProfile(Plan &plan);
	template<typename T>
	bool relocateOptimizationProfile(
			Plan &plan, const KeyToNodeIdList &keyToNodeIdList,
			util::Vector<T> *&entryList);
	template<typename T>
	void addOptimizationProfile(
			Plan *plan, PlanNode *node, T *profile);
	SQLPreparedPlan::OptProfile& prepareOptimizationProfile(
			Plan *plan, PlanNode *node);
	SQLPreparedPlan::ProfileKey prepareProfileKey(PlanNode &node);

	void checkOptimizationUnitSupport(OptimizationUnitType type);
	bool isOptimizationUnitEnabled(OptimizationUnitType type);
	bool isOptimizationUnitCheckOnly(OptimizationUnitType type);

	static OptimizationFlags getDefaultOptimizationFlags();
	static OptimizationOption makeJoinPushDownHintsOption();

	void optimize(Plan &plan);

	bool mergeSubPlan(Plan &plan);
	bool minimizeProjection(Plan &plan, bool subqueryAware);
	bool pushDownPredicate(Plan &plan, bool subqueryAware);
	bool pushDownPredicateSub(
			Plan &plan, util::Vector<bool> *visitedList, bool subqueryAware);
	bool pushDownAggregate(Plan &plan);
	bool pushDownJoin(Plan &plan);
	bool pushDownLimit(Plan &plan);
	bool removeRedundancy(Plan &plan);
	bool removeInputRedundancy(Plan &plan);
	bool removeAggregationRedundancy(Plan &plan);
	bool removePartitionCondRedundancy(Plan &plan);
	bool assignJoinPrimaryCond(Plan &plan, bool &complexFound);
	bool makeScanCostHints(Plan &plan);
	bool makeJoinPushDownHints(Plan &plan);
	bool selectJoinDriving(Plan &plan, bool checkOnly);
	bool selectJoinDrivingLegacy(Plan &plan, bool checkOnly);
	bool factorizePredicate(Plan &plan);
	bool reorderJoin(Plan &plan, bool &tableCostAffected);

	typedef util::Map<uint32_t, uint32_t> JoinGroupMapStubType;

	void makeNodeNameList(
			const Plan &plan, const util::Vector<PlanNodeId> &nodeIdList,
			util::Vector<const util::String*> &nodeNameList) const;

	static bool reorderJoinSub(
			util::StackAllocator &alloc,
			const util::Vector<JoinNode> &joinNodeList,
			const util::Vector<JoinEdge> &joinEdgeList,
			const util::Vector<JoinOperation> &srcJoinOpList,
			const util::Vector<SQLHintInfo::ParsedHint> &joinHintList,
			util::Vector<JoinOperation> &destJoinOpList, bool legacy,
			bool profiling);

	static bool reorderJoinSubLegacy(
			util::StackAllocator &alloc,
			const util::Vector<JoinNode> &joinNodeList,
			const util::Vector<JoinEdge> &joinEdgeList,
			const util::Vector<JoinOperation> &srcJoinOpList,
			const util::Vector<SQLHintInfo::ParsedHint> &joinHintList,
			util::Vector<JoinOperation> &destJoinOpList, bool profiling);

	static void assignJoinGroupId(
			util::StackAllocator &alloc,
			const JoinScoreKey &key, JoinScore &joinScore,
			util::Map<JoinNodeId, size_t> &joinGroupMap,
			util::Vector< util::Vector<JoinScore*> > &joinGroupList);

	static void dumpJoinScoreList(
			std::ostream &os, const util::Deque<JoinScore*>& joinScoreList);

	void makeJoinHintMap(Plan &plan);

	void addJoinReorderingProfile(
			Plan &plan, const util::Vector<PlanNodeId> &nodeIdList,
			const util::Vector<PlanNodeId> &joinIdList,
			const util::Vector<JoinNode> &joinNodeList,
			const util::Vector<JoinOperation> &joinOpList);
	void profileJoinTables(
			const Plan &plan, const util::Vector<PlanNodeId> &nodeIdList,
			const util::Vector<JoinNode> &joinNodeList,
			SQLPreparedPlan::OptProfile::Join &profile) const;
	void dumpJoinTables(
			std::ostream &os, const Plan &plan,
			const util::Vector<PlanNodeId> &nodeIdList) const;
	void dumpJoinGraph(
			std::ostream &os,
			const util::Vector<JoinEdge> &joinEdgeList,
			const util::Vector<JoinOperation> &joinOpList) const;
	const char8_t* getJoinTableName(const PlanNode &node) const;

	bool makeJoinGraph(
			const Plan &plan, PlanNodeId &tailNodeId,
			util::Vector<bool> &acceptedRefList,
			util::Vector<PlanNodeId> &nodeIdList,
			util::Vector<PlanNodeId> &joinIdList,
			util::Vector<JoinNode> &joinNodeList,
			util::Vector<JoinEdge> &joinEdgeList,
			util::Vector<JoinOperation> &joinOpList, bool legacy);

	bool makeJoinNodes(
			const Plan &plan, const PlanNode &node,
			const util::Vector<uint32_t> &nodeRefList,
			util::Vector<bool> &acceptedRefList,
			util::Map<PlanNodeId, JoinNodeId> &nodeIdMap,
			util::Vector<PlanNodeId> &nodeIdList,
			util::Vector<PlanNodeId> &joinIdList,
			util::Vector<JoinNode> &joinNodeList,
			util::Vector<JoinOperation> &joinOpList, bool top);
	static void makeJoinEdges(
			const Plan &plan, const PlanNode &node, const Expr &expr,
			const util::Map<PlanNodeId, JoinNodeId> &nodeIdMap,
			util::Vector<JoinNode> &joinNodeList,
			util::Vector<JoinEdge> &joinEdgeList);

	void makeJoinNodesCost(
			const Plan &plan, const PlanNode &node,
			util::Vector<int64_t> &approxSizeList);
	int64_t resolveJoinNodeApproxSize(
			const PlanNode &node, const SQLHintInfo *hintInfo);

	static void makeJoinEdgesSub(
			const Plan &plan, const PlanNode &node, const Expr &expr,
			const util::Map<PlanNodeId, JoinNodeId> &nodeIdMap,
			util::Vector<JoinEdge> &joinEdgeList, bool negative);
	static void mergeJoinEdges(
			util::Vector<JoinEdge> &joinEdgeList,
			size_t orStartPos, size_t orEndPos);
	static void makeJoinEdgeElement(
			const Plan &plan, const PlanNode &node, const Expr &expr,
			const util::Map<PlanNodeId, JoinNodeId> &nodeIdMap,
			util::Vector<JoinEdge> &joinEdgeList, bool negative);
	static void normalizeJoinEdge(JoinEdge &edge);
	static size_t mergeJoinEdgeInput(
			const util::Vector<JoinEdge> &joinEdgeList, size_t startPos,
			JoinEdge *destEdge);
	static void listJoinEdgeColumn(
			const Plan &plan, const PlanNode &node, const Expr &expr,
			const util::Map<PlanNodeId, JoinNodeId> &nodeIdMap,
			util::Vector<JoinEdge> &joinEdgeList);
	static bool followJoinColumnInput(
			const Plan &plan, const PlanNode &node, const Expr &expr,
			const util::Map<PlanNodeId, JoinNodeId> &nodeIdMap,
			PlanNodeId &resultNodeId, ColumnId *resultColumnId);
	static const PlanNode* findJoinPredNode(
			const Plan &plan, PlanNodeId joinNodeId,
			util::Vector<PlanNodeId> &predIdList);

	void applyJoinOrder(
			Plan &plan, const util::Vector<PlanNodeId> &nodeIdList,
			const util::Vector<PlanNodeId> &joinIdList,
			const util::Vector<JoinNode> &joinNodeList,
			const util::Vector<JoinOperation> &joinOpList);

	PlanNodeId applyJoinOrderSub(
			Plan &plan, const util::Vector<PlanNodeId> &nodeIdList,
			const util::Vector<JoinNode> &joinNodeList,
			const util::Vector<JoinOperation> &joinOpList,
			util::Vector< std::pair<PlanNodeId, uint32_t> > &wholeColumnList,
			util::Vector<PlanNodeId> &restJoinIdList, size_t joinOpIndex,
			size_t &midPos);
	size_t resolveTopJoinOpIndex(
			const util::Vector<JoinNode> &joinNodeList,
			const util::Vector<JoinOperation> &joinOpList);
	Expr remapJoinColumn(
			const util::Vector<const util::Vector<
					std::pair<uint32_t, ColumnId> >*> &columnMap,
			const Expr &expr);

	bool pullUpJoinPredicate(
			Plan &plan, PlanNode &node,
			const util::Map<PlanNodeId, JoinNodeId> &nodeIdMap);
	Expr pullUpJoinColumn(
			const Plan &plan, const PlanNode &node, uint32_t inputId,
			const Expr &expr);

	uint64_t getMaxInputCount(const Plan &plan);
	uint64_t getMaxExpansionCount(const Plan &plan);
	int64_t getMaxGeneratedRows(const Plan &plan);

	bool isLegacyJoinReordering(const Plan &plan);
	bool isLegacyJoinDriving(const Plan &plan);
	static bool isLegacyPlanning(
			const Plan &plan, const SQLPlanningVersion &legacyVersion);
	SQLPlanningVersion getPlanningVersionHint(const SQLHintInfo *hintInfo);

	bool getSingleHintValue(
			const SQLHintInfo *hintInfo, SQLHint::Id hint, uint64_t defaultValue,
			uint64_t &value);
	size_t getMultipleHintValues(
			const SQLHintInfo *hintInfo, SQLHint::Id hint, uint64_t *valueList,
			size_t valueCount, size_t minValueCount);

	void initializeMinimizingTargets(
			const Plan &plan, PlanNodeId &resultId, bool &unionFound,
			util::Vector<util::Vector<bool>*> &retainList,
			util::Vector<util::Vector<bool>*> &modList);
	void retainMinimizingTargetsFromResult(
			const Plan &plan, PlanNodeId resultId,
			util::Vector<util::Vector<bool>*> &retainList);
	void fixMinimizingTargetNodes(
			const Plan &plan,
			util::Vector<util::Vector<bool>*> &retainList,
			util::Vector<util::Vector<bool>*> &modList,
			bool subqueryAware);
	void balanceMinimizingTargets(
			const Plan &plan, bool unionFound,
			util::Vector<util::Vector<bool>*> &retainList,
			util::Vector<util::Vector<bool>*> &modList);
	bool rewriteMinimizingTargets(
			Plan &plan,
			const util::Vector<util::Vector<bool>*> &retainList,
			const util::Vector<util::Vector<bool>*> &modList,
			bool subqueryAware);

	bool checkAndAcceptJoinExpansion(
			const Plan &plan, const PlanNode &node,
			const util::Vector< std::pair<int64_t, bool> > &nodeLimitList,
			JoinNodeIdList &joinInputList, JoinBoolList &unionList,
			JoinExpansionInfo &expansionInfo);
	void cleanUpJoinExpansionInput(
			Plan &plan, const PlanNode &srcNode, PlanNodeId srcNodeId,
			util::Vector<uint32_t> &nodeRefList,
			JoinNodeIdList &joinInputList, const JoinBoolList &unionList,
			const JoinExpansionInfo &expansionInfo);
	void makeJoinExpansionMetaInput(
			Plan &plan, SQLType::JoinType joinType,
			const util::Vector<uint32_t> &nodeRefList,
			const JoinNodeIdList &joinInputList, JoinNodeIdList &metaInputList,
			const JoinExpansionInfo &expansionInfo);
	void makeJoinExpansionInputUnification(
			Plan &plan, SQLType::JoinType joinType,
			const JoinNodeIdList &joinInputList, JoinNodeIdList &metaInputList,
			PlanNode::IdList &unifiedIdList,
			const JoinExpansionInfo &expansionInfo);
	void makeJoinExpansionEntries(
			Plan &plan, const PlanNode &srcNode, PlanNodeId srcNodeId,
			const JoinNodeIdList &joinInputList,
			const JoinNodeIdList &metaInputList,
			const PlanNode::IdList &unifiedIdList,
			const JoinExpansionInfo &expansionInfo);

	bool findJoinExpansion(
			const Plan &plan, const PlanNode &node,
			JoinNodeIdList &joinInputList, JoinExpansionInfo &expansionInfo);

	bool makeJoinNarrowingKeyTables(
			const Plan &plan, const PlanNode &node,
			const JoinNodeIdList &joinInputList,
			const TableNarrowingList &tableNarrowingList,
			JoinKeyTableList &keyTableList, bool &joinCondNarrowable);
	static bool splitJoinByReachableUnit(
			JoinKeyTableList &keyTableList, JoinKeyIdList &joinKeyIdList,
			JoinUnifyingUnitList &unifyingUnitList,
			JoinInputUsageList &inputUsageList, JoinBoolList &singleUnitList);

	bool tryMakeJoinAllUnitDividableExpansion(
			SQLType::JoinType joinType,
			PlanNode::CommandOptionFlag cmdOptionFlag,
			const JoinNodeIdList &joinInputList,
			JoinKeyTableList &keyTableList, const JoinKeyIdList &joinKeyIdList,
			const JoinUnifyingUnitList &unifyingUnitList,
			const JoinBoolList &singleUnitList, uint32_t expansionLimit,
			JoinExpansionInfo *expansionInfo);
	void makeJoinAllUnitNonDividableExpansion(
			const JoinNodeIdList &joinInputList,
			const JoinKeyIdList &joinKeyIdList,
			const JoinUnifyingUnitList &unifyingUnitList,
			JoinExpansionInfo &expansionInfo);
	static void removeJoinExpansionMatchedInput(
			const JoinInputUsageList &inputUsageList,
			JoinNodeIdList &joinInputList, JoinExpansionInfo &expansionInfo);

	static bool makeJoinUnitDividableExpansion(
			const JoinBoolList &dividableList,
			const JoinNodeIdList &joinInputList,
			JoinKeyTableList &keyTableList, const JoinKeyIdList &joinKeyIdList,
			const JoinUnifyingUnitList &unifyingUnitList,
			const JoinBoolList *drivingOptList, uint32_t unitIndex,
			uint32_t unitLimit, bool fullMatch,
			JoinKeyIdList &joinSubKeyIdList,
			JoinMatchCountList &matchCountList,
			util::Vector<uint32_t> &subLimitList,
			JoinExpansionBuildInfo *buildInfo);
	static bool getJoinUnitDividableExpansionLimit(
			const JoinSizeList &unitSizeList, uint64_t baseUnitLimit,
			bool fullMatch, const JoinBoolList &dividableList,
			const JoinBoolList *drivingOptList, uint32_t &dividingSide,
			uint64_t &sideLimit, uint64_t &unitLimit);

	static void makeJoinUnitExpansion(
			const JoinNodeIdList &joinInputList,
			const JoinKeyIdList &joinKeyIdList,
			const JoinSizeList &keyIdStartList,
			const JoinSizeList &keyIdEndList,
			JoinExpansionBuildInfo &buildInfo);

	void makeJoinAllUnitLimitList(
			JoinKeyTableList &keyTableList, const JoinKeyIdList &joinKeyIdList,
			const JoinUnifyingUnitList &unifyingUnitList,
			uint32_t expansionLimit, util::Vector<uint32_t> &limitList,
			util::Vector<bool> &fullMatchList);
	static void arrangeJoinExpansionLimit(
			JoinMatchCountList &matchCountList,
			util::Vector<uint32_t> &unitLimitList, uint64_t totalLimit);

	static bool checkJoinUnitDividable(
			SQLType::JoinType joinType, const JoinBoolList &singleUnitList,
			JoinBoolList &dividableList);
	static bool getJoinDividableList(
			SQLType::JoinType joinType, JoinBoolList &dividableList);
	static const JoinBoolList* getJoinUnitDrivingOptionList(
			PlanNode::CommandOptionFlag cmdOptionFlag,
			const JoinBoolList &dividableList, JoinBoolList &baseJoinOptList);

	static uint32_t getJoinSmallerSide(const JoinNodeIdList &joinInputList);
	static uint32_t getJoinSmallerSide(const JoinKeyTableList &keyTableList);
	static uint32_t getJoinAnotherSide(uint32_t srcSide);

	static size_t checkIteratorForwardStep(uint64_t src, ptrdiff_t remaining);

	bool resolveNarrowingKey(
			const Plan &plan, const PlanNode &node, ColumnId columnId,
			const TableNarrowingList &tableNarrowingList, NarrowingKey &key);
	TableNarrowingList& prepareTableNarrowingList(const Plan &plan);

	uint32_t getAcceptableJoinExpansionCount(const Plan &plan);
	void acceptJoinExpansion(uint32_t extraCount, bool reached);

	void makeJoinExpansionScoreIdList(
			const Plan &plan, JoinExpansionScoreList &scoreList,
			JoinExpansionScoreIdList &scoreIdList);
	void makeJoinExpansionScoreList(
			const Plan &plan, JoinExpansionScoreList &scoreList);

	static bool isJoinExpansionUnmatchInputRequired(
			uint32_t joinInputId, SQLType::JoinType joinType);
	static bool isJoinExpansionMetaInputRequired(
			uint32_t joinInputId, SQLType::JoinType joinType,
			const JoinNodeIdList &joinInputList, bool &metaOnly);
	static size_t getJoinExpansionMetaUnificationCount(
			uint32_t joinInputId, SQLType::JoinType joinType,
			const JoinNodeIdList &joinInputList,
			const JoinNodeIdList &metaInputList);
	static bool getJoinExpansionTarget(
			SQLType::JoinType joinType, const JoinNodeIdList &joinInputList,
			const JoinNodeIdList &metaInputList,
			const PlanNode::IdList &unifiedIdList,
			const JoinExpansionInfo &expansionInfo, size_t ordinal,
			JoinExpansionEntry &entry);

	bool simplifySubquery(Plan &plan);

	bool listRelatedSubqueries(
			const Plan &plan, PlanNode::IdList &pendingIdList,
			util::Vector< std::pair<PlanNodeId, bool> > &subqueryJoinList,
			util::Set<PlanNodeId> &rewroteJoinIdSet,
			util::Set<PlanNodeId> &visitedNodeIdSet);
	void listFilteringSubqueryColumns(
			const Expr &expr, uint32_t inputId, bool forOutput,
			const util::Vector<ColumnId> &foldedColumnList,
			util::Vector<bool> &columnVisitedList,
			util::Vector< std::pair<PlanNodeId, bool> > &subqueryJoinList);
	uint32_t getNodeInputId(
			const PlanNode &node, PlanNodeId inNodeId);
	bool findOutputColumn(
			const PlanNode &node, uint32_t inputId, ColumnId inColumnId,
			ColumnId &outColumnId, bool reverse);

	static bool isFoldAggregationNode(const PlanNode &node);
	static bool isFoldExprType(Type type);
	static bool isNegativeFoldType(Type foldType);
	static bool isMatchingFoldType(Type foldType);

	bool getSubqueryCorrelation(
			const Plan &plan, const PlanNode &subqueryJoinNode,
			PlanNodeId &insidePredId, PlanNode::IdList &bothSideJoinList,
			util::Vector<ColumnId> &correlatedColumnList,
			ExprList &correlationList, Expr *&foldExpr);
	bool isSimpleCorrelatedSubqueryNode(
			const PlanNode &node, const PlanNode *foldNode,
			bool neCorrelated);
	bool mergeCorrelatedOutput(
			const Plan &plan, const PlanNode &node, PlanNodeId outsideId,
			ColumnId outsideIdColumn, Expr **foldExpr, bool visited,
			util::Vector<ColumnId> &correlatedColumnList,
			ExprList &correlationList);
	bool isSubqueryIdJoinNode(
			const Plan &plan, const PlanNode &node, ColumnId outsideIdColumn,
			const util::Vector<ColumnId> &correlatedColumnList);
	std::pair<ColumnId, ColumnId> makeCorrelatedInPositions(
			const Plan &plan, const PlanNode &node,
			const util::Vector<ColumnId> &correlatedColumnList);

	bool getSubqueryPredicateCorrelation(
			const Expr &expr, ColumnId outsideIdColumn,
			const util::Vector<ColumnId> &correlatedColumnList,
			const std::pair<ColumnId, ColumnId> &inPos,
			ExprList *correlationList);
	bool getSubqueryOutputCorrelation(
			const Expr &expr,
			const util::Vector<ColumnId> &correlatedColumnList,
			const std::pair<ColumnId, ColumnId> &inPos, Expr **foldExpr);

	bool findSubqueryOutsideIdRef(
			const Expr &expr, ColumnId outsideIdColumn,
			const util::Vector<ColumnId> &correlatedColumnList,
			const std::pair<ColumnId, ColumnId> &inPos);
	bool findSubquerySingleSideExpr(
			const Expr &inExpr, Expr **outExpr,
			const util::Vector<ColumnId> &correlatedColumnList,
			const std::pair<ColumnId, ColumnId> &inPos, int32_t &forOutside);

	bool rewriteSubquery(
			Plan &plan, PlanNodeId subqueryJoinId, bool filterable,
			util::Set<PlanNodeId> &rewroteJoinIdSet);
	void rewriteSimpleSubquery(
			Plan &plan, PlanNodeId subqueryJoinId, bool filterable,
			bool unique, const Expr &foldExpr, const ExprList &correlationList,
			const util::Vector<ColumnId> &outsideCorrelationList,
			const util::Vector<ColumnId> &insideCorrelationList,
			const PlanNode::IdList &insidePredPath);

	void setUpSimpleSubqueryGroup(
			Plan &plan, PlanNode &node, const Expr &foldExpr,
			bool withValue, bool withNE, bool neReducing, bool preGrouped,
			PlanNodeId preFoldId, const ExprList &correlationList,
			const util::Vector<ColumnId> &insideCorrelationList,
			bool compensating);
	bool setUpSimpleSubqueryCompensationGroup(
			Plan &plan, const Expr &foldExpr,
			const PlanNode::IdList &insidePredPath, size_t correlationCount,
			PlanNodeId &outNodeId);
	void setUpSimpleSubqueryJoin(
			Plan &plan, PlanNode &node, const Expr &foldExpr,
			bool filtering, bool withValue, bool withNE,
			bool forResult, const PlanExprList &subqueryOutputList,
			size_t outsideColumnCount, const ExprList &correlationList,
			const util::Vector<ColumnId> &outsideCorrelationList,
			bool compensating);
	Expr makeSimpleSubqueryJoinResult(
			const Plan &plan, const PlanNode &node,
			const ColumnId (&baseColumnIdList)[2],
			const Expr &foldExpr, bool filtering, bool withNE,
			bool compensating, Expr *nullCondExpr);

	bool pushDownSubquery(
			Plan &plan, PlanNodeId subqueryJoinId, PlanNodeId outsideId,
			bool filterable, const PlanNode::IdList &bothSideJoinList,
			const util::Vector<ColumnId> &correlatedColumnList,
			util::Set<PlanNodeId> &rewroteJoinIdSet);
	void pushDownCorrelatedColumns(
			Plan &plan, PlanNodeId outsideId, PlanNodeId insidePredId,
			const ExprList &correlationList,
			util::Vector<ColumnId> &outsideCorrelationList,
			util::Vector<ColumnId> &insideCorrelationList);

	void disableSubqueryOutsideColumns(
			Plan &plan, PlanNode &node, PlanNodeId outsideId,
			ColumnId insideColumnId, bool &typeRefreshRequired,
			util::Vector<bool> &fullDisabledList,
			util::Vector<PlanNodeId> &pendingLimitOffsetIdList);
	bool removeDisabledColumnRefPredicate(
			Plan &plan, PlanNode &node, const Expr &inExpr, Expr *&outExpr);
	bool removeDisabledColumnRefOutput(
			Plan &plan, PlanNode &node, const Expr &inExpr, Expr *&outExpr);
	bool findDisabledColumnRef(
			const Plan &plan, const PlanNode &node, const Expr &expr,
			bool *columnFound);
	void disableSubqueryLimitOffset(Plan &plan, PlanNodeId nodeId);

	void removeEmptyNodes(OptimizationContext &cxt, Plan &plan);
	void removeEmptyColumns(OptimizationContext &cxt, Plan &plan);

	Expr genDisabledExpr();
	static bool isDisabledExpr(const Expr &expr);
	static Type getDisabledExprType();

	static uint64_t applyRatio(uint64_t denom, uint64_t ratio);

	void makeNodeRefList(
			const Plan &plan, util::Vector<uint32_t> &nodeRefList);
	void findAllInput(
			const Plan &plan, const PlanNode &node,
			util::Set<PlanNodeId> &allInput);

	void checkReference(
			const Plan &plan, const PlanNode &node,
			const PlanExprList &exprList,
			util::Vector<util::Vector<bool>*> &checkList, bool forOutput);
	void checkReference(
			const PlanNode &node, const Expr &expr,
			util::Vector<util::Vector<bool>*> &checkList);
	bool isSameOutputRequired(const Plan &plan, const PlanNode &node);

	void disableNode(Plan &plan, PlanNodeId nodeId);
	void setNodeAndRefDisabled(
			Plan &plan, PlanNodeId nodeId,
			util::Vector<uint32_t> &nodeRefList);
	static void setDisabledNodeType(PlanNode &node);

	static bool isDisabledNode(const PlanNode &node);
	static Type getDisabledNodeType();

	bool factorizeExpr(Expr &inExpr, Expr *&outExpr, ExprList *andList);
	bool factorizeOrExpr(Expr &inExpr, Expr *&outExpr, ExprList *andList);

	bool splitJoinPushDownCond(
			const Plan &plan, Mode mode, const Expr &inExpr, Expr *&outExpr,
			Expr *&condExpr, uint32_t inputId, SQLType::JoinType joinType,
			bool complex);
	bool splitJoinPushDownCondSub(
			const Plan &plan, Mode mode, const Expr &inExpr, Expr *&outExpr,
			Expr *&condExpr, uint32_t inputId, bool complex);

	static bool isSubqueryResultPredicateNode(
			const Plan &plan, const PlanNode &node);

	static bool findSingleInputExpr(
			const Expr &expr, uint32_t &inputId, bool *multiFound = NULL);
	static bool isSingleInputExpr(const Expr &expr, uint32_t inputId);

	static bool isNodeAggregated(const PlanNode &node);
	static bool isNodeAggregated(
			const PlanExprList &exprList, SQLType::Id type, bool forWindow);
	static uint32_t getNodeAggregationLevel(const PlanExprList &exprList);

	void assignReducedScanCostHints(Plan &plan);
	bool applyScanCostHints(const Plan &srcPlan, Plan &destPlan);

	bool findJoinPushDownHintingTarget(const Plan &plan);
	bool applyJoinPushDownHints(const Plan &srcPlan, Plan &destPlan);
	static PlanNode::CommandOptionFlag getJoinDrivingOptionMask();

	bool isNarrowingJoinInput(
			const Plan &plan, const PlanNode &node, uint32_t inputId);
	bool isNarrowingNode(const PlanNode &node);
	static bool isNarrowingPredicate(
			const Expr &expr, const uint32_t *inputId);

	void reduceTablePartitionCond(
			const Plan &plan, Expr &expr, const TableInfo &tableInfo,
			const util::Vector<uint32_t> &affinityRevList,
			int32_t subContainerId);
	const Expr* reduceTablePartitionCondSub(
			const Expr &expr, const TableInfo &tableInfo,
			const util::Vector<uint32_t> &affinityRevList,
			int32_t subContainerId);

	bool findIndexedPredicate(
			const Plan &plan, const PlanNode &node, uint32_t inputId,
			bool withUnionScan, bool *unique);
	bool findIndexedPredicateSub(
			const Plan &plan, const PlanNode &node, uint32_t inputId,
			const uint32_t *unionInputId, bool *unique);
	bool findIndexedPredicateSub(
			const Plan &plan, const PlanNode &node,
			const SQLTableInfo::BasicIndexInfoList &indexInfoList,
			uint32_t inputId, const uint32_t *unionInputId, const Expr &expr,
			bool &firstUnmatched, bool *unique);

	void dereferenceAndAppendExpr(
			Plan &plan, PlanNodeRef &node, uint32_t inputId, const Expr &inExpr,
			Expr *&refExpr, util::Vector<uint32_t> &nodeRefList);
	bool isDereferenceableNode(
			const PlanNode &inNode, const PlanExprList *outputList,
			AggregationPhase phase, bool multiInput);

	bool isDereferenceableExprList(const PlanExprList &exprList);
	bool isDereferenceableExpr(const Expr &expr);
	bool isDereferenceableExprType(Type exprType);

	bool isInputDereferenceable(
			const Plan &plan, const PlanNode &node, uint32_t inputId,
			const PlanExprList &exprList, bool forOutput);
	bool isInputDereferenceable(
			const Plan &plan, const PlanNode &node, const uint32_t *inputId,
			const Expr &expr, bool columnRefOnly);

	void dereferenceExprList(
			const Plan &plan, const PlanNode &node, uint32_t inputId,
			const PlanExprList &inExprList, PlanExprList &outExprList,
			AggregationPhase &phase,
			const util::Map<uint32_t, uint32_t> *inputIdMap,
			const PlanNode *inputNode = NULL);
	void dereferenceExpr(
			const Plan &plan, const PlanNode &node, uint32_t inputId,
			const Expr &inExpr, Expr *&outExpr,
			const util::Map<uint32_t, uint32_t> *inputIdMap,
			const PlanNode *inputNode = NULL);
	void replaceExprInput(Expr &expr, uint32_t inputId);

	void copyPlan(const Plan &src, Plan &dest);
	void copyNode(const PlanNode &src, PlanNode &dest);

	void copyExprList(
			const PlanExprList &src, PlanExprList &dest, bool shallow);
	void copyExpr(const Expr &src, Expr &dest);

	void mergeAndOp(
			Expr *inExpr1, Expr *inExpr2,
			const Plan &plan, Mode mode, Expr *&outExpr);
	void mergeAndOpBase(
			Expr *inExpr1, Expr *inExpr2,
			const Plan *plan, Mode mode, Expr *&outExpr);
	void mergeOrOp(
			Expr *inExpr1, Expr *inExpr2, Mode mode, Expr *&outExpr);

	static bool mergeLimit(
			int64_t limit1, int64_t limit2, int64_t offset1, int64_t *merged);

	void makeNodeLimitList(
			const Plan &plan,
			util::Vector< std::pair<int64_t, bool> > &nodeLimitList);
	static bool isNodeLimitUnchangeable(const PlanNode &node);

	const TableInfo* findTable(const QualifiedName &name);
	const TableInfo& resolveTable(const QualifiedName &name);

	size_t resolveColumn(
			const Plan &plan, const PlanNode &node,
			const QualifiedName &name, Expr &out, Mode mode,
			const PlanNodeId *productedNodeId, bool *tableFoundRef,
			const PendingColumn **pendingColumnRef);
	size_t resolveColumnDeep(
			const Plan &plan, const PlanNode &node, const QualifiedName &name,
			Expr &out, Mode mode, bool *tableFoundRef, bool *errorRef,
			const PendingColumn **pendingColumnRef);

	const Expr* followResolvedColumn(
			const Plan &plan, const PlanNode &srcNode,
			const PlanNode &destNode, const Expr &expr, bool strict);
	Expr genResolvedColumn(
			const Plan &plan, const PlanNode &node, uint32_t inputId,
			ColumnId columnId, const PendingColumn **pendingColumnRef);

	void applyPendingColumnList(Plan &plan);
	void applyPendingColumn(
			Plan &plan, PlanNode &node, const PendingColumn &pendingColumn);
	void clearPendingColumnList(size_t orgSize);

	Type resolveFunction(const Expr &expr);
	bool filterFunction(Type &type, size_t argCount);
	void arrangeFunctionArgs(Type type, Expr &expr);

	uint32_t genSourceId();

	static const PlanExprList& getOutput(const Plan &in);

	static ColumnType getResultType(
			const Expr &expr, const Expr *baseExpr, ColumnType columnType,
			Mode mode, size_t index = 0,
			AggregationPhase phase = SQLType::AGG_PHASE_ALL_PIPE);
	static bool isInputNullable(const PlanNode &planNode, uint32_t inputId);
	static bool isConstantEvaluable(const Expr &expr);
	static bool isConstantOnExecution(const Expr &expr);
	static uint32_t getAggregationLevel(const Expr &expr);

	static size_t getArgCount(const Expr &expr);
	static Type getAggregationType(Type exprType, bool distinct);
	static ColumnType setColumnTypeNullable(ColumnType type, bool nullable);

	static bool isTableMatched(
			const QualifiedName &name1, const QualifiedName &name2);

	static TupleValue makeBoolValue(bool value);
	static bool isTrueExpr(const Expr &expr);

	TupleValue::VarContext& getVarContext();
	const CompileOption& getCompileOption();

	static bool isIndexScanEnabledDefault();

	const TableInfoList& getTableInfoList() const;
	SubqueryStack& getSubqueryStack();

	static util::AllocUniquePtr<SubqueryStack>::ReturnType makeSubqueryStack(
			util::StackAllocator &alloc);
	static util::AllocUniquePtr<
			OptimizationOption>::ReturnType makeOptimizationOption(
			util::StackAllocator &alloc, const OptimizationOption &src);

	static util::StackAllocator& varContextToAllocator(
			TupleValue::VarContext &varCxt);
	template<typename T> static T* emptyPtr();

	util::StackAllocator &alloc_;

	TupleValue::VarContext &varCxt_;
	CompilerInitializer init_;
	const TableInfoList *tableInfoList_;
	util::AllocUniquePtr<SubqueryStack> subqueryStack_;
	uint32_t lastSrcId_;
	PendingColumnList pendingColumnList_;
	TableInfoIdList metaTableInfoIdList_;
	Plan::ValueTypeList *parameterTypeList_;
	const char8_t *inputSql_;
	uint32_t subqueryLevel_; 
	SyntaxTree::ExplainType explainType_;
	util::DateTime queryStartTime_;

	size_t planSizeLimitRatio_;
	uint64_t generatedScanCount_;
	uint64_t expandedJoinCount_;
	TableNarrowingList *tableNarrowingList_;

	const CompileOption *compileOption_;
	util::AllocUniquePtr<OptimizationOption> optimizationOption_;

	bool currentTimeRequired_;
	bool isMetaDataQuery_;
	bool indexScanEnabled_;
	bool multiIndexScanEnabled_;
	bool expansionLimitReached_;
	bool predicatePushDownApplied_;
	bool substrCompatible_;
	bool varianceCompatible_;
	bool experimentalFuncEnabled_;
	bool recompileOnBindRequired_;
	bool neverReduceExpr_;

	Profiler *profiler_;
	SQLPreparedPlan::ProfileKey nextProfileKey_;
	CompilerInitializer::End initEnd_;
};

template<typename V>
struct SQLCompiler::IsUInt {
	enum {
		VALUE = (std::numeric_limits<V>::is_integer &&
				!std::numeric_limits<V>::is_signed)
	};
};

class SQLCompiler::Meta {
public:
	enum SystemTableType {
		DRIVER_TABLES,
		DRIVER_COLUMNS,
		DRIVER_PRIMARY_KEYS,
		DRIVER_TYPE_INFO,
		DRIVER_INDEX_INFO,
		DRIVER_CLUSTER_INFO,
		DRIVER_TABLE_TYPES,

		END_SYSTEM_TABLE
	};

	enum StringConstant {
		STR_AUTO_INCREMENT,
		STR_BUFFER_LENGTH,
		STR_CASE_SENSITIVE,
		STR_CHAR_OCTET_LENGTH,
		STR_COLUMN_DEF,
		STR_COLUMN_NAME,
		STR_COLUMN_SIZE,
		STR_CREATE_PARAMS,
		STR_DATA_TYPE,
		STR_DECIMAL_DIGITS,
		STR_FIXED_PREC_SCALE,
		STR_IS_AUTOINCREMENT,
		STR_IS_GENERATEDCOLUMN,
		STR_IS_NULLABLE,
		STR_KEY_SEQ,
		STR_LITERAL_PREFIX,
		STR_LITERAL_SUFFIX,
		STR_LOCAL_TYPE_NAME,
		STR_MINIMUM_SCALE,
		STR_MAXIMUM_SCALE,
		STR_NULLABLE,
		STR_NUM_PREC_RADIX,
		STR_ORDINAL_POSITION,
		STR_PK_NAME,
		STR_PRECISION,
		STR_REF_GENERATION,
		STR_REMARKS,
		STR_SCOPE_CATALOG,
		STR_SCOPE_SCHEMA,
		STR_SCOPE_TABLE,
		STR_SEARCHABLE,
		STR_SELF_REFERENCING_COL_NAME,
		STR_SOURCE_DATA_TYPE,
		STR_SQL_DATA_TYPE,
		STR_SQL_DATETIME_SUB,
		STR_TABLE,
		STR_TABLE_CAT,
		STR_TABLE_SCHEM,
		STR_TABLE_NAME,
		STR_TABLE_TYPE,
		STR_TYPE_CAT,
		STR_TYPE_SCHEM,
		STR_TYPE_NAME,
		STR_UNSIGNED_ATTRIBUTE,
		STR_YES,
		STR_NO,
		STR_UNKNOWN,
		STR_EMPTY,
		STR_NON_UNIQUE,
		STR_INDEX_QUALIFIER,
		STR_INDEX_NAME,
		STR_TYPE,
		STR_ASC_OR_DESC,
		STR_CARDINALITY,
		STR_PAGES,
		STR_FILTER_CONDITION,
		STR_DATABASE_MAJOR_VERSION,
		STR_DATABASE_MINOR_VERSION,
		STR_EXTRA_NAME_CHARACTERS,
		STR_VIEW,

		END_STR
	};

	explicit Meta(SQLCompiler &base);

	static bool isNodeExpansion(ContainerId containerId);

	static bool isSystemTable(const QualifiedName &name);
	static bool isSystemTable(const char *name);
	static const char8_t* getTableNameSystemPart(const QualifiedName &name);

	static SQLTableInfo::IdInfo setSystemTableColumnInfo(
			util::StackAllocator &alloc, const QualifiedName &tableName,
			SQLTableInfo &tableInfo, bool metaVisible, bool internalMode,
			bool forDriver, bool failOnError = true);

	static void getCoreMetaTableName(
			util::StackAllocator &alloc, const SQLTableInfo::IdInfo &idInfo,
			QualifiedName &name);
	static void getMetaColumnInfo(
			util::StackAllocator &alloc, const SQLTableInfo::IdInfo &idInfo,
			bool forCore, SQLTableInfo::SQLColumnInfoList &columnInfoList,
			int8_t metaNamingType);

	void genSystemTable(
			const Expr &table, const Expr *&whereExpr, Plan &plan);

private:
	struct SystemTableInfo;
	class SystemTableInfoList;

	template<typename T> struct RefColumnEntry {
		T metaColumn_;
		StringConstant name_;
	};

	typedef std::pair<ColumnType, StringConstant> ColumnDefinition;
	typedef void (Meta::*TableGenFunc)(
			const Expr*&, Plan&, const SystemTableInfo&);

	static const char8_t SYSTEM_NAME_SEPARATOR[];

	static const char8_t DRIVER_TABLES_NAME[];
	static const char8_t DRIVER_COLUMNS_NAME[];
	static const char8_t DRIVER_PRIMARY_KEYS_NAME[];
	static const char8_t DRIVER_TYPE_INFO_NAME[];
	static const char8_t DRIVER_INDEX_INFO_NAME[];
	static const char8_t DRIVER_CLUSTER_INFO_NAME[];
	static const char8_t DRIVER_TABLE_TYPES_NAME[];

	static const util::NameCoderEntry<StringConstant> CONSTANT_LIST[];
	static const util::NameCoder<StringConstant, END_STR> CONSTANT_CODER;

	static const ColumnDefinition DRIVER_TABLES_COLUMN_LIST[];
	static const ColumnDefinition DRIVER_COLUMNS_COLUMN_LIST[];
	static const ColumnDefinition DRIVER_PRIMARY_KEYS_COLUMN_LIST[];
	static const ColumnDefinition DRIVER_TYPE_INFO_COLUMN_LIST[];
	static const ColumnDefinition DRIVER_INDEX_INFO_COLUMN_LIST[];
	static const ColumnDefinition DRIVER_CLUSTER_INFO_COLUMN_LIST[];
	static const ColumnDefinition DRIVER_TABLE_TYPES_COLUMN_LIST[];

	static const SystemTableInfoList SYSTEM_TABLE_INFO_LIST;

	static bool isTableMatched(
			const QualifiedName &qName, const char8_t *name);

	void appendMetaTableInfoId(
			const Plan &plan, const PlanNode &node, const Expr& exprWhere,
			SQLTableInfo::Id tableInfoId, const Plan::ValueList *parameterList,
			bool &placeholderAffected);
	static bool findCommonColumnId(
			const Plan &plan, const PlanNode &node,
			SQLTableInfo::Id tableInfoId, const MetaContainerInfo &metaInfo,
			uint32_t &partitionColumn, uint32_t &tableNameColumn,
			uint32_t &tableIdColumn, uint32_t &partitionNameColumn);

	void genDriverTables(
			const Expr *&whereExpr, Plan &plan, const SystemTableInfo &info);
	void genDriverColumns(
			const Expr *&whereExpr, Plan &plan, const SystemTableInfo &info);
	void genDriverPrimaryKeys(
			const Expr *&whereExpr, Plan &plan, const SystemTableInfo &info);
	void genDriverIndexInfo(
			const Expr *&whereExpr, Plan &plan, const SystemTableInfo &info);
	void genDriverTypeInfo(
			const Expr *&whereExpr, Plan &plan, const SystemTableInfo &info);
	void genDriverTableTypes(
			const Expr* &whereExpr, Plan &plan, const SystemTableInfo &info);
	void genDriverClusterInfo(
			const Expr *&whereExpr, Plan &plan, const SystemTableInfo &info);
	void genMetaTableGeneral(
			const Expr *&whereExpr, Plan &plan, const SystemTableInfo &info);

	PlanNode& genMetaScan(Plan &plan, const SystemTableInfo &info);
	PlanNode& genMetaSelect(Plan &plan);
	PlanNode* genMetaWhere(
			Plan &plan, const Expr *&whereExpr, const PlanNode &scanNode);

	void addDBNamePred(
			Plan &plan, const SystemTableInfo &info,
			const PlanExprList &scanOutputList, const PlanNode &scanNode,
			PlanNode &projNode);

	Expr genDBNameCondExpr(
			const Plan &plan, const util::String &dbName,
			const Expr &dbColumnExpr);

	template<typename T>
	void addPrimaryKeyPred(
			Plan &plan, T metaColumn, const PlanExprList &scanOutputList,
			PlanNode &projNode);

	template<typename T>
	void addContainerAttrPred(
			Plan &plan, T metaColumn, const PlanExprList &scanOutputList,
			PlanNode &projNode);

	static void setSystemColumnInfoList(
			util::StackAllocator &alloc, const SystemTableInfo &info,
			SQLTableInfo::SQLColumnInfoList &columnInfoList);

	void genAllRefColumns(
			const Plan &plan, const SystemTableInfo &info,
			const PlanExprList &scanOutputList, PlanExprList &destOutputList);

	template<typename T>
	Expr genRefColumn(
			const Plan &plan, const PlanExprList &scanOutputList,
			T metaColumn, StringConstant name);
	Expr genRefColumnById(
			const Plan &plan, const PlanExprList &scanOutputList,
			ColumnId metaColumnId, const char8_t *name);

	Expr genEmptyColumn(
			const Plan &plan, ColumnType baseType, StringConstant name);
	Expr genConstColumn(
			const Plan &plan, const TupleValue &value, StringConstant name);

	QualifiedName* genColumnName(StringConstant name);
	TupleValue genStr(StringConstant constant);

	static ColumnDefinition defineColumn(
			ColumnType type, StringConstant constant);
	static const char8_t* str(StringConstant constant);

	static const SystemTableInfoList& getSystemTableInfoList();

	SQLCompiler &base_;
	util::StackAllocator &alloc_;
};

struct SQLCompiler::Meta::SystemTableInfo {
	SystemTableInfo();

	SQLTableInfo::IdInfo getMetaIdInfo(bool forCore) const;

	SystemTableType type_;
	const char8_t *name_;
	const ColumnDefinition *columnList_;
	size_t columnCount_;
	ContainerId metaId_;
	ContainerId coreMetaId_;
	TableGenFunc tableGenFunc_;
	bool forDriver_;
	int8_t metaNamingType_;
};

class SQLCompiler::Meta::SystemTableInfoList {
public:
	SystemTableInfoList();

	bool findInfo(
			const QualifiedName &name, SystemTableInfo &info,
			bool internalMode) const;

private:
	template<typename T, size_t N> void addDriverInfo(
			SystemTableType type, const char8_t *name,
			const ColumnDefinition (&columnList)[N], T coreMetaType,
			TableGenFunc tableGenFunc);

	template<typename T, size_t N> void addInfo(
			SystemTableType type, const char8_t *name,
			const ColumnDefinition (&columnList)[N], T coreMetaType,
			TableGenFunc tableGenFunc, bool forDriver);

	SystemTableInfo entryList_[END_SYSTEM_TABLE];
};

struct SQLCompiler::JoinNode {
	explicit JoinNode(util::StackAllocator &alloc);

	util::Vector<JoinEdgeId> edgeIdList_;

	int64_t approxSize_;
	util::Vector<uint64_t> cardinalityList_;
};

struct SQLCompiler::JoinEdge {
	JoinEdge();

	void dump(std::ostream &os) const;

	Type type_;

	JoinNodeId nodeIdList_[JOIN_INPUT_COUNT];

	ColumnId columnIdList_[JOIN_INPUT_COUNT];

	uint64_t weakness_;

	bool simple_;
};

struct SQLCompiler::JoinOperation {
	JoinOperation();

	bool operationFrom(size_t nodeCount, JoinNodeId nodeId, size_t &opIndex);

	void dump(std::ostream &os) const;

	SQLType::JoinType type_;

	JoinNodeId nodeIdList_[JOIN_INPUT_COUNT];

	static const uint64_t HINT_LEADING_NONE = 0;
	static const uint64_t HINT_LEADING_LIST = 1; 
	static const uint64_t HINT_LEADING_TREE = 2; 
	uint64_t hintType_;

	SQLPreparedPlan::OptProfile::Join *profile_;
	bool tableCostAffected_;
};

class SQLCompiler::JoinScore {
public:
	static const uint64_t DEFAULT_TABLE_ROW_APPROX_SIZE;

	explicit JoinScore(util::StackAllocator &alloc);
	JoinScore(util::StackAllocator &alloc, JoinNodeId id);
	JoinScore(util::StackAllocator &alloc, JoinNodeId id1, JoinNodeId id2);

	void applyAnd(const JoinEdge &edge);

	JoinScore applyAnd(const JoinScore &another) const;

	void setDegree(size_t degree);

	size_t getDegree() const;		
	uint32_t getLevel() const;
	uint64_t getCount() const;

	void setApproxTotalCount(uint64_t approxSize);
	uint64_t getApproxTotalCount() const;

	void setApproxFilteredCount(uint64_t approxSize);
	uint64_t getApproxFilteredCount() const;

	void setJoinNodeId(JoinNodeId id1, JoinNodeId id2 = UNDEF_JOIN_NODEID);
	std::pair<JoinNodeId, JoinNodeId> getJoinNodeId() const;

	void setScoreNodeId(JoinNodeId id1, JoinNodeId id2 = UNDEF_JOIN_NODEID);
	std::pair<JoinNodeId, JoinNodeId> getScoreNodeId() const;

	bool operator==(const JoinScore &another) const;
	bool operator<(const JoinScore &another) const;
	bool operator>(const JoinScore &another) const;
	JoinScore& operator=(const JoinScore &another);

	static bool greater(const JoinScore* arg1, const JoinScore* arg2);

	static uint64_t multiplyWithUpperLimit(uint64_t val1, uint64_t val2);
	void dump(std::ostream &os) const;
	void dumpScore(std::ostream &os) const;
private:
	uint32_t getOpLevel(SQLType::Id opType) const;

	util::StackAllocator &alloc_;
	util::Vector<const JoinEdge*> edgeList_;

	JoinNodeId nodeIdList_[JOIN_INPUT_COUNT];		
	JoinNodeId scoreNodeIdList_[JOIN_INPUT_COUNT];	
	size_t degree_;		
	uint32_t opLevel_;	
	uint64_t opCount_;	
	uint64_t approxTotalCount_;
	uint64_t approxFilteredCount_;
	util::Vector<uint64_t> cardinalityList_;
};

class SQLCompiler::SubqueryStack {
public:
	class Scope;

	explicit SubqueryStack(util::StackAllocator &alloc);

	ColumnId addExpr(const Expr &inExpr, Mode mode);

private:
	typedef util::Vector<const Expr*> Base;

	Base base_;
	size_t scopeLevel_;
};

class SQLCompiler::SubqueryStack::Scope {
public:
	explicit Scope(SQLCompiler &base);
	~Scope();

	void applyBefore(const Plan &plan, Expr &expr);
	void applyAfter(
			const Plan &plan, Expr &expr, Mode mode, const bool aggregated);

	bool hasSubquery() const;
	const Expr* takeExpr();

private:
	typedef util::Vector<ColumnId> OffsetList;

	void applyBefore(const OffsetList &offsetList, Expr &expr);

	SQLCompiler &base_;
	SubqueryStack &stack_;
	size_t pos_;
};

template<bool Const>
class SQLCompiler::ExprArgsIterator {
public:
	typedef typename util::Conditional<Const, const Expr, Expr>::Type& Ref;
	typedef typename util::Conditional<Const, const Expr*, Expr*>::Type Ptr;

	ExprArgsIterator(Ref expr);

	bool exists() const;
	Ref get() const;
	void next();

private:
	Ref expr_;
	Ptr next_;
	size_t phase_;
	ExprList::iterator it_;
};

class SQLCompiler::PlanNodeRef {
public:
	PlanNodeRef();
	PlanNodeRef(Plan &plan, const PlanNode *node = NULL);

	Plan* getPlan() const;

	PlanNode& operator*() const;
	PlanNode* operator->() const;
	PlanNode* get() const;

private:
	static PlanNode::Id toNodeId(const Plan &plan, const PlanNode *node);

	Plan *plan_;
	PlanNode::Id nodeId_;
};

class SQLCompiler::ExprRef {
public:
	ExprRef();
	ExprRef(
			SQLCompiler &compiler, Plan &plan, const Expr &expr,
			const PlanNode *node);

	Expr& operator*() const;
	Expr* operator->() const;
	Expr* get() const;

	Expr generate(const PlanNode *node = NULL) const;

	void update(const PlanNode *node = NULL);

private:
	static ColumnId toColumnId(const PlanNode &node, const Expr &expr);

	const Expr* followColumn(
			const PlanNode &node, uint32_t &inputId, ColumnId &columnId,
			size_t depth) const;

	SQLCompiler *compiler_;
	PlanNodeRef nodeRef_;
	ColumnId columnId_;
	Expr *dupExpr_;
};

class SQLCompiler::GenRAContext {
public:
	GenRAContext(util::StackAllocator &alloc, const Select &select)
	: alloc_(alloc), select_(select), basePlanSize_(0), type_(SQLType::Id()),
	  selectList_(alloc), selectExprRefList_(alloc),
	  selectTopColumnId_(UNDEF_COLUMNID),
	  productedNodeId_(UNDEF_PLAN_NODEID), whereExpr_(NULL),
	  outExprList_(alloc), aggrExprList_(alloc), aggrExprCount_(0),
	  windowExprList_(alloc), genuineWindowExprCount_(0), pseudoWindowExprCount_(0),
	  otherTopPos_(-1), otherColumnCount_(0),
	  inSubQuery_(false)
	{};

	util::StackAllocator &alloc_;
	const Select &select_;
	size_t basePlanSize_;
	Type type_;
	PlanExprList selectList_;
	ExprRefList selectExprRefList_;
	ColumnId selectTopColumnId_;
	PlanNodeId productedNodeId_;
	const Expr *whereExpr_;
	ExprRef innerIdExprRef_;
	ExprList outExprList_;
	ExprList aggrExprList_;
	size_t aggrExprCount_;
	ExprList windowExprList_;
	size_t genuineWindowExprCount_;
	size_t pseudoWindowExprCount_;
	size_t otherTopPos_;
	size_t otherColumnCount_;
	bool inSubQuery_;
	PlanNodeRef scalarWhereNodeRef_;

private:
	GenRAContext(const GenRAContext&);
	GenRAContext& operator=(const GenRAContext&);
};

class SQLCompiler::GenOverContext {
public:
	explicit GenOverContext(util::StackAllocator &alloc)
	: windowExpr_(NULL), windowExprPos_(0),
	  windowList_(alloc), windowPartitionByList_(alloc),
	  windowOrderByList_(alloc), addColumnExprList_(alloc),
	  replaceColumnIndexList_(alloc),
	  sortNodePredSize_(0), baseOutputListSize_(0),
	  orderByStartPos_(0)
	{};

	const Expr *windowExpr_;
	size_t windowExprPos_;
	PlanExprList windowList_;
	PlanExprList windowPartitionByList_;
	PlanExprList windowOrderByList_;
	PlanExprList addColumnExprList_;
	util::Vector<int32_t> replaceColumnIndexList_;
	size_t sortNodePredSize_;
	size_t baseOutputListSize_;
	size_t orderByStartPos_;
};


class SQLCompiler::ExprHint {
public:
	ExprHint();

	bool isNoIndex() const;
	void setNoIndex(bool enabled);

private:
	bool noIndex_;
};

class SQLCompiler::QualifiedNameFormatter {
public:
	static QualifiedNameFormatter asColumn(const QualifiedName &qName);
	static QualifiedNameFormatter asTable(const QualifiedName &qName);

	void format(std::ostream &os) const;

private:
	QualifiedNameFormatter(const QualifiedName &qName, bool forColumn);

	const QualifiedName &qName_;
	bool forColumn_;
};

std::ostream& operator<<(
		std::ostream &os,
		const SQLCompiler::QualifiedNameFormatter &formatter);

class SQLCompiler::SubPlanHasher {
public:
	uint32_t operator()(
			const PlanNode &value, const uint32_t *base = NULL) const;
	uint32_t operator()(
			const Expr &value, const uint32_t *base = NULL) const;
	uint32_t operator()(
			const SQLTableInfo::IdInfo &value,
			const uint32_t *base = NULL) const;
	uint32_t operator()(
			const QualifiedName &value, const uint32_t *base = NULL) const;

private:
	uint32_t operator()(
			const TupleValue &value, const uint32_t *base = NULL) const;
	uint32_t operator()(
			const util::String &value, const uint32_t *base = NULL) const;
	uint32_t operator()(
			int64_t value, const uint32_t *base = NULL) const;

	template<typename T>
	uint32_t operator()(
			const T *value, const uint32_t *base = NULL) const;

	template<typename T, typename Alloc>
	uint32_t operator()(
			const std::vector<T, Alloc> &value,
			const uint32_t *base = NULL) const;

	template<typename T>
	uint32_t operator()(
			const T &value,
			const typename util::EnableIf<
					IsUInt<T>::VALUE, uint32_t>::Type *base = NULL) const;
};

class SQLCompiler::SubPlanEq {
public:
	bool operator()(const PlanNode &value1, const PlanNode &value2) const;
	bool operator()(const Expr &value1, const Expr &value2) const;
	bool operator()(
			const SQLTableInfo::IdInfo &value1,
			const SQLTableInfo::IdInfo &value2) const;
	bool operator()(
			const QualifiedName &value1, const QualifiedName &value2) const;

private:
	bool operator()(const TupleValue &value1, const TupleValue &value2) const;
	bool operator()(const util::String &value1, const util::String &value2) const;
	bool operator()(int64_t value1, int64_t value2) const;

	template<typename T>
	bool operator()(const T *value1, const T *value2) const;

	template<typename T, typename Alloc>
	bool operator()(
			const std::vector<T, Alloc> &value1,
			const std::vector<T, Alloc> &value2) const;

	template<typename T>
	bool operator()(
			const T &value1,
			const typename util::EnableIf<
					IsUInt<T>::VALUE, T>::Type &value2) const;
};

class SQLCompiler::NarrowingKey {
public:
	typedef std::pair<int64_t, int64_t> LongRange;

	NarrowingKey();

	bool overlapsWith(const NarrowingKey &another) const;
	void mergeWith(const NarrowingKey &another);

	bool isFull() const;
	bool isNone() const;

	void setNone();
	void setRange(const LongRange &longRange);
	void setHash(uint32_t hashIndex, uint32_t hashCount);

private:
	friend class NarrowingKeyTable;

	static LongRange getRangeFull();
	static LongRange getRangeNone();

	bool isRangeFull() const;
	bool isRangeNone() const;
	bool isHashFull() const;

	bool overlapsWithRange(const NarrowingKey &another) const;
	bool overlapsWithHash(const NarrowingKey &another) const;

	LongRange longRange_;
	uint32_t hashIndex_;
	uint32_t hashCount_;
};

class SQLCompiler::NarrowingKeyTable {
public:
	typedef util::Vector<uint32_t> KeyIdList;

	explicit NarrowingKeyTable(util::StackAllocator &alloc);

	uint32_t getSize() const;

	bool isNone() const;

	void addKey(uint32_t columnIndex, const NarrowingKey &key);

	uint32_t getMatchCount(
			const NarrowingKeyTable &srcTable, uint32_t srcId);

	bool findAll(
			NarrowingKeyTable &srcTable, size_t srcOffset, KeyIdList &srcList,
			KeyIdList &destList, bool shallow);

private:
	typedef std::pair<int64_t, uint32_t> IndexElement;
	typedef util::Vector<IndexElement> IndexElementList;
	typedef util::Vector<NarrowingKey> KeyList;

	struct IndexEntry {
		explicit IndexEntry(util::StackAllocator &alloc);

		IndexElementList elements_;
		int64_t baseValue_;
		bool forRange_;
	};

	typedef util::Vector<IndexEntry> IndexList;

	struct ColumnEntry {
		explicit ColumnEntry(util::StackAllocator &alloc);

		KeyList keyList_;
		KeyIdList fullList_;
		IndexList indexList_;
	};

	typedef util::Vector<ColumnEntry> ColumnList;

	bool find(
			const NarrowingKeyTable &src, uint32_t srcId,
			KeyIdList &idList, util::Vector<bool> &visitedList,
			size_t offset, bool &exact) const;

	static bool matchIndex(
			const KeyList &keyList, const NarrowingKey &key,
			const IndexEntry &indexEntry, bool merging,
			KeyIdList &idList, util::Vector<bool> &visitedList);
	static void matchKeyList(
			const KeyList &keyList, const NarrowingKey &key, bool merging,
			KeyIdList &idList, util::Vector<bool> &visitedList);
	static void matchKey(
			const NarrowingKey &srcKey, const NarrowingKey &destKey,
			uint32_t destId, bool merging, KeyIdList &idList,
			util::Vector<bool> &visitedList);

	static void finishMerging(
			KeyIdList &idList, util::Vector<bool> &visitedList,
			size_t offset, size_t orgSize);

	static void clearVisits(
			const KeyIdList &idList, size_t offset,
			util::Vector<bool> &visitedList);
	static void setVisits(
			KeyIdList::const_iterator beginIt, KeyIdList::const_iterator endIt,
			util::Vector<bool> &visitedList, bool visited);

	void prepareVisits();
	void buildIndex();
	static int64_t makeColumnScore(const ColumnEntry &columnEntry);
	static bool isNoneColumn(const ColumnEntry &columnEntry);

	IndexEntry& resolveIndex(
			ColumnEntry &columnEntry, const NarrowingKey &key, bool forRange);
	static bool findIndex(
			const ColumnEntry &columnEntry, const NarrowingKey &key,
			bool forRange, IndexList::const_iterator &it);
	static int64_t getIndexBaseValue(const NarrowingKey &key, bool forRange);

	util::StackAllocator &alloc_;
	ColumnList columnList_;
	uint32_t size_;
	bool indexBuilt_;

	IndexElementList columnScoreList_;
	util::Vector<bool> visitedList_;
};

class SQLCompiler::TableNarrowingList {
public:
	explicit TableNarrowingList(util::StackAllocator &alloc);

	static TableNarrowingList* tryDuplicate(const TableNarrowingList *src);

	void put(const TableInfo &tableInfo);
	bool contains(TableInfo::Id id) const;
	NarrowingKey resolve(
			const TableInfo::IdInfo &idInfo, ColumnId columnId) const;

private:
	struct Entry;

	typedef util::Vector<Entry> EntryList;
	typedef util::Vector<NarrowingKey> KeyList;

	util::StackAllocator &alloc_;
	EntryList entryList_;
	util::Vector<uint32_t> affinityRevList_;
};

class SQLCompiler::JoinExpansionInfo {
public:
	JoinExpansionInfo();

	bool isEstimationOnly() const;

	void reserve(const JoinExpansionInfo &info);
	void clear();

	template<typename T>
	static void tryAppend(util::Vector<T> *list, const T &element);
	template<typename T, typename It>
	static void tryAppend(util::Vector<T> *list, It beginIt, It endIt);

	template<typename T> static void tryClear(util::Vector<T> *list);

	PlanNode::IdList *unificationList_;
	util::Vector<uint32_t> *unifyingUnitList_;
	JoinExpansionList *expansionList_;

	size_t unificationCount_;
	size_t unifyingUnitCount_;
	size_t expansionCount_;

	uint32_t extraCount_;
	bool limitReached_;
};

class SQLCompiler::JoinExpansionBuildInfo {
public:
	JoinExpansionBuildInfo(
			util::StackAllocator &alloc, JoinExpansionInfo &expansionInfo);

	JoinExpansionInfo &expansionInfo_;
	PlanNode::IdList idList_;
	util::Map<PlanNode::IdList, uint32_t> unifyingMap_;
};

struct SQLCompiler::TableNarrowingList::Entry {
	explicit Entry(util::StackAllocator &alloc);

	KeyList keyList_;
	KeyList subKeyList_;
	ColumnId columnId_;
	ColumnId subColumnId_;
	bool assigned_;
};

class SQLCompiler::IntegralPartitioner {
public:
	typedef uint64_t ValueType;

	IntegralPartitioner(ValueType totalAmount, ValueType groupCount);

	ValueType getGroupAmount() const;

	ValueType popGroup();
	ValueType popGroupLimited(ValueType amount);
	ValueType popGroupFull(ValueType amount);

private:
	ValueType totalAmount_;
	ValueType groupCount_;
};

class SQLCompiler::OptimizationContext {
public:
	OptimizationContext(SQLCompiler &compiler, Plan &plan);

	SQLCompiler& getCompiler();
	Plan& getPlan();

private:
	SQLCompiler &compiler_;
	Plan &plan_;
};

class SQLCompiler::OptimizationUnit {
public:
	static const util::NameCoderEntry<OptimizationUnitType> TYPE_LIST[];
	static const util::NameCoder<OptimizationUnitType, OPT_END> TYPE_CODER;

	OptimizationUnit(OptimizationContext &cxt, OptimizationUnitType type);
	~OptimizationUnit();

	bool operator()();
	bool operator()(bool optimized);

	bool isCheckOnly() const;

private:
	OptimizationContext &cxt_;
	OptimizationUnitType type_;
};

class SQLCompiler::OptimizationOption {
public:
	explicit OptimizationOption(ProfilerManager *manager);

	bool isSupported(OptimizationUnitType type) const;
	bool isEnabled(OptimizationUnitType type) const;
	bool isCheckOnly(OptimizationUnitType type) const;

	bool isSomeEnabled() const;
	OptimizationFlags getEnablementFlags() const;

	bool isSomeCheckOnly() const;

	void supportFull();
	void support(OptimizationUnitType type, bool enabled);
	void setCheckOnly(OptimizationUnitType type);

private:
	static OptimizationFlags getFullFlags();
	static bool isFlagOn(OptimizationFlags flags, OptimizationUnitType type);
	static void setFlag(
			OptimizationFlags &flags, OptimizationUnitType type, bool value);

	OptimizationFlags supportFlags_;
	OptimizationFlags enablementFlags_;
	OptimizationFlags checkOnlyFlags_;
};

class SQLCompiler::CompileOption {
public:
	explicit CompileOption(const CompilerSource *src = NULL);

	void setTimeZone(const util::TimeZone &timeZone);
	const util::TimeZone& getTimeZone() const;

	void setPlanningVersion(const SQLPlanningVersion &version);
	const SQLPlanningVersion& getPlanningVersion() const;

	void setCostBasedJoin(bool value);
	bool isCostBasedJoin() const;

	void setCostBasedJoinDriving(bool value);
	bool isCostBasedJoinDriving() const;

	void setIndexStatsRequester(SQLIndexStatsCache::RequesterHolder *value);
	SQLIndexStatsCache::RequesterHolder* getIndexStatsRequester() const;

	static const CompileOption& getEmptyOption();

	static const SQLCompiler* findBaseCompiler(
			const CompileOption *option, CompilerInitializer&);

private:
	static const CompileOption EMPTY_OPTION;

	util::TimeZone timeZone_;

	SQLPlanningVersion planningVersion_;
	bool costBasedJoin_;
	bool costBasedJoinDriving_;

	SQLIndexStatsCache::RequesterHolder *indexStatsRequester_;

	const SQLCompiler *baseCompiler_;
};

/**
	@brief SQLProcessorの機能に依存するユーティリティ
	@note SQLProcessorにおいて実装
 */
class SQLCompiler::ProcessorUtils {
public:
	static bool predicateToMetaTarget(
			TupleValue::VarContext &varCxt, const Expr &expr,
			uint32_t partitionIdColumn, uint32_t containerNameColumn,
			uint32_t containerIdColumn, uint32_t partitionNameColumn,
			PartitionId partitionCount, PartitionId &partitionId,
			const Plan::ValueList *parameterList, bool &placeholderAffected);
	static FullContainerKey* predicateToContainerKey(
			TupleValue::VarContext &varCxt, TransactionContext &txn,
			DataStoreV4 &dataStore, const Query &query, DatabaseId dbId,
			ContainerId metaContainerId, PartitionId partitionCount,
			util::String &dbNameStr, bool &fullReduced,
			PartitionId &reducedPartitionId);

	static Expr tqlToPredExpr(
			util::StackAllocator &alloc, const Query &query);

	static bool reducePartitionedTarget(
			TupleValue::VarContext &varCxt, const TableInfo &tableInfo,
			const Expr &expr, bool &uncovered,
			util::Vector<int64_t> *affinityList,
			util::Vector<uint32_t> *subList,
			const Plan::ValueList *parameterList, bool &placeholderAffected,
			util::Set<int64_t> &unmatchAffinitySet);

	static bool isReducibleTablePartitionCondition(
			const Expr &expr, const TableInfo &tableInfo,
			const util::Vector<uint32_t> &affinityRevList,
			int32_t subContainerId);

	static bool getTablePartitionKeyList(
			const TableInfo &tableInfo,
			const util::Vector<uint32_t> &affinityRevList,
			util::Vector<NarrowingKey> &keyList,
			util::Vector<NarrowingKey> &subKeyList);
	static void getTablePartitionAffinityRevList(
			const TableInfo &tableInfo,
			util::Vector<uint32_t> &affinityRevList);

	static bool isRangeGroupSupportedType(ColumnType type);
	static void resolveRangeGroupInterval(
			util::StackAllocator &alloc, ColumnType type, int64_t baseInterval,
			int64_t baseOffset, util::DateTime::FieldType unit,
			const util::TimeZone &timeZone, TupleValue &inerval,
			TupleValue &offset);
	static bool findRangeGroupBoundary(
			util::StackAllocator &alloc, ColumnType keyType,
			const TupleValue &base, bool forLower, bool inclusive,
			TupleValue &boundary);
	static bool adjustRangeGroupBoundary(
			util::StackAllocator &alloc, TupleValue &lower, TupleValue &upper,
			const TupleValue &interval, const TupleValue &offset);
	static TupleValue mergeRangeGroupBoundary(
			const TupleValue &base1, const TupleValue &base2, bool forLower);

	static bool findAdjustedRangeValues(
			util::StackAllocator &alloc, const TableInfo &tableInfo,
			const util::Vector<TupleValue> *parameterList,
			const Expr &expr, uint32_t &columnId,
			std::pair<TupleValue, TupleValue> &valuePair,
			std::pair<bool, bool> &inclusivePair);

	static TupleValue makeTimestampValue(
			util::StackAllocator &alloc, const util::PreciseDateTime &src);

	static TupleValue evalConstExpr(
			TupleValue::VarContext &varCxt, const Expr &expr,
			const CompileOption &option);
	static void checkExprArgs(
			TupleValue::VarContext &varCxt, const Expr &expr,
			const CompileOption &option);
	static bool checkArgCount(
			Type exprType, size_t argCount, AggregationPhase phase);

	static bool isConstEvaluable(Type exprType);
	static bool isInternalFunction(Type exprType);
	static bool isExperimentalFunction(Type exprType);
	static bool isWindowExprType(
			Type type, bool &windowOnly, bool &pseudoWindow);
	static bool isExplicitOrderingAggregation(Type exprType);

	static ColumnType getResultType(
			Type exprType, size_t index, AggregationPhase phase,
			const util::Vector<ColumnType> &argTypeList, bool grouping);
	static size_t getResultCount(Type exprType, AggregationPhase phase);
	static ColumnType filterColumnType(ColumnType type);
	static bool updateArgType(
			Type exprType, util::Vector<ColumnType> &argTypeList);
	static bool findConstantArgType(
			Type exprType, size_t startIndex, size_t &index,
			ColumnType &argType);

	static TupleValue convertType(
			TupleValue::VarContext &varCxt, const TupleValue &src,
			ColumnType type, bool implicit);

	static ColumnType findPromotionType(
			ColumnType type1, ColumnType type2);
	static ColumnType findConversionType(
			ColumnType src, ColumnType desired,
			bool implicit, bool evaluating);

	static ColumnType getTableColumnType(
			const TableInfo &info, const ColumnId columnId,
			const bool *withNullsStatsRef);
	static bool isInputNullsStatsEnabled();

	static bool toTupleColumnType(
			uint8_t src, bool nullable, ColumnType &dest, bool failOnUnknown);
	static int32_t toSQLColumnType(ColumnType type);

	static Type swapCompOp(Type type);
	static Type negateCompOp(Type type);
	static Type getLogicalOp(Type type, bool negative);
};

class SQLCompiler::Profiler {
public:
	struct Stats {
		Stats();

		UTIL_OBJECT_CODER_MEMBERS(
				singleInputPredExtraction_,
				complexJoinPrimaryCond_);

		int64_t singleInputPredExtraction_;
		int64_t complexJoinPrimaryCond_;
	};

	struct Target;
	struct OptimizationUnitResult;

	explicit Profiler(util::StackAllocator &alloc);

	static Profiler* tryDuplicate(const Profiler *src);

	void setPlan(const Plan &plan);
	void setTableInfoList(const SQLTableInfoList &list);

	void startOptimization(OptimizationUnitType type);
	void endOptimization(bool optimized);

	void setTarget(const Target &target);

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(
			stats_, plan_, tableInfoList_, optimizationResult_);

public:
	util::StackAllocator &alloc_;
	Stats stats_;
	Plan plan_;
	util::Vector<SQLTableInfo> tableInfoList_;
	util::Vector<OptimizationUnitResult> optimizationResult_;

private:
	util::Stopwatch watch_;
	Target *target_;
};

struct SQLCompiler::Profiler::Target {
	Target();

	bool isEmpty() const;

	UTIL_OBJECT_CODER_MEMBERS(
			UTIL_OBJECT_CODER_OPTIONAL(optimize_, false));

	bool optimize_;
};

struct SQLCompiler::Profiler::OptimizationUnitResult {
	OptimizationUnitResult();

	UTIL_OBJECT_CODER_MEMBERS(
			UTIL_OBJECT_CODER_ENUM(type_, OptimizationUnit::TYPE_CODER),
			elapsedNanos_,
			optimized_);

	OptimizationUnitType type_;
	uint64_t elapsedNanos_;
	bool optimized_;
};

class SQLCompiler::ProfilerManager {
public:
	ProfilerManager();
	static ProfilerManager& getInstance();

	static bool isEnabled();

	Profiler::Target getTarget();
	void setTarget(const Profiler::Target &target);

	OptimizationFlags getOptimizationFlags();
	void setOptimizationFlags(OptimizationFlags flags, bool forDefault);

	Profiler* tryCreateProfiler(util::StackAllocator &alloc);
	Profiler* getProfiler(util::StackAllocator &alloc, uint32_t index);
	void addProfiler(const Profiler &profiler);

private:
	typedef std::vector<uint8_t> Entry;
	typedef std::list<Entry> EntryList;

	ProfilerManager(const ProfilerManager&);
	ProfilerManager& operator=(const ProfilerManager&);

	static ProfilerManager instance_;
	static const uint32_t MAX_PROFILER_COUNT;

	util::Atomic<OptimizationFlags> optimizationFlags_;
	util::Mutex mutex_;
	Profiler::Target target_;
	EntryList entryList_;
};

class SQLCompiler::CompilerSource {
public:
	explicit CompilerSource(const SQLCompiler *baseCompiler);

	static const SQLCompiler* findBaseCompiler(const CompilerSource *src);

private:
	const SQLCompiler *baseCompiler_;
};

struct JDBCDatabaseMetadata {


	static const int32_t TYPE_NO_NULLS; 
	static const int32_t TYPE_NULLABLE; 
	static const int32_t TYPE_NULLABLE_UNKNOWN; 


	static const int32_t TYPE_PRED_NONE; 
	static const int32_t TYPE_PRED_CHAR; 
	static const int32_t TYPE_PRED_BASIC; 
	static const int32_t TYPE_SEARCHABLE; 


	static const int16_t TABLE_INDEX_STATISTIC; 
	static const int16_t TABLE_INDEX_CLUSTERED; 
	static const int16_t TABLE_INDEX_HASHED; 
	static const int16_t TABLE_INDEX_OTHER; 
};


inline bool SQLCompiler::JoinScore::operator==(const JoinScore &another) const {
	return (approxFilteredCount_ == another.approxFilteredCount_) &&
			(opLevel_ == another.opLevel_) &&
			(opCount_ ==  another.opCount_) &&
			(approxTotalCount_ == another.approxTotalCount_);
}

inline bool SQLCompiler::JoinScore::operator<(const JoinScore &another) const {
	if (approxFilteredCount_ == another.approxFilteredCount_) {
		if (opLevel_ == another.opLevel_) {
			if (opCount_ ==  another.opCount_) {
				if (approxTotalCount_ == another.approxTotalCount_) {
					if (scoreNodeIdList_[0] == another.scoreNodeIdList_[0]) {
						return scoreNodeIdList_[1] < another.scoreNodeIdList_[1];
					}
					else {
						return scoreNodeIdList_[0] < another.scoreNodeIdList_[0];
					}
				}
				else {
					return approxTotalCount_ > another.approxTotalCount_;
				}
			}
			else {
				return opCount_ < another.opCount_;
			}
		}
		else {
			return opLevel_ < another.opLevel_;
		}
	}
	else {
		return approxFilteredCount_ > another.approxFilteredCount_;
	}
}

inline bool SQLCompiler::JoinScore::operator>(const JoinScore &another) const {
	if (approxFilteredCount_ == another.approxFilteredCount_) {
		if (opLevel_ == another.opLevel_) {
			if (opCount_ ==  another.opCount_) {
				if (approxTotalCount_ == another.approxTotalCount_) {
					if (scoreNodeIdList_[0] == another.scoreNodeIdList_[0]) {
						return (scoreNodeIdList_[1] < another.scoreNodeIdList_[1]);
					}
					else {
						return (scoreNodeIdList_[0] < another.scoreNodeIdList_[0]);
					}
				}
				else {
					return approxTotalCount_ < another.approxTotalCount_;
				}
			}
			else {
				return (opCount_ > another.opCount_);
			}
		}
		else {
			return (opLevel_ > another.opLevel_);
		}
	}
	else {
		return approxFilteredCount_ < another.approxFilteredCount_;
	}
}

inline bool SQLCompiler::JoinScore::greater(
		const JoinScore *arg1, const JoinScore *arg2) {
	assert(arg1);
	assert(arg2);
	return *arg1 > *arg2;
}


inline SQLCompiler::JoinScore& SQLCompiler::JoinScore::operator=(const JoinScore &another) {
	if (this != &another) {
		edgeList_.assign(another.edgeList_.begin(), another.edgeList_.end());
		nodeIdList_[0] = another.nodeIdList_[0];
		nodeIdList_[1] = another.nodeIdList_[1];
		scoreNodeIdList_[0] = another.scoreNodeIdList_[0];
		scoreNodeIdList_[1] = another.scoreNodeIdList_[1];
		degree_ = another.degree_;
		opLevel_ = another.opLevel_;
		opCount_ = another.opCount_;
		approxTotalCount_ = another.approxTotalCount_;
		approxFilteredCount_ = another.approxFilteredCount_;
		cardinalityList_.assign(
				another.cardinalityList_.begin(), another.cardinalityList_.end());
	}
	return *this;
}

inline uint64_t SQLCompiler::JoinScore::multiplyWithUpperLimit(uint64_t val1, uint64_t val2) {
	if (val1 != 0) {
		static const uint64_t MAX = std::numeric_limits<uint64_t>::max();
		return (val2 < (MAX / val1)) ? val1 * val2 : MAX;
	}
	else {
		return 0;
	}
}

inline void SQLCompiler::JoinScore::setApproxTotalCount(uint64_t approxSize) {
	approxTotalCount_ = approxSize;
}


inline uint64_t SQLCompiler::JoinScore::getApproxTotalCount() const {
	return (approxTotalCount_ == 0) ? DEFAULT_TABLE_ROW_APPROX_SIZE : approxTotalCount_;
}

inline void SQLCompiler::JoinScore::setApproxFilteredCount(uint64_t approxSize) {
	approxFilteredCount_ = approxSize;
}


inline uint64_t SQLCompiler::JoinScore::getApproxFilteredCount() const {
	return (approxFilteredCount_ == 0) ? DEFAULT_TABLE_ROW_APPROX_SIZE : approxFilteredCount_;
}

inline void SQLCompiler::JoinScore::setDegree(size_t degree) {
	degree_ = degree;
}

inline size_t SQLCompiler::JoinScore::getDegree() const {
	return degree_;
}

inline uint32_t SQLCompiler::JoinScore::getLevel() const {
	return opLevel_;
}

inline uint64_t SQLCompiler::JoinScore::getCount() const {
	return opCount_;
}

inline void SQLCompiler::JoinScore::setJoinNodeId(JoinNodeId id1, JoinNodeId id2) {
	nodeIdList_[0] = id1;
	nodeIdList_[1] = id2;
}
inline std::pair<SQLCompiler::JoinNodeId, SQLCompiler::JoinNodeId> SQLCompiler::JoinScore::getJoinNodeId() const {
	return std::make_pair(nodeIdList_[0], nodeIdList_[1]);
}

inline void SQLCompiler::JoinScore::setScoreNodeId(JoinNodeId id1, JoinNodeId id2) {
	scoreNodeIdList_[0] = id1;
	scoreNodeIdList_[1] = id2;
}
inline std::pair<SQLCompiler::JoinNodeId, SQLCompiler::JoinNodeId> SQLCompiler::JoinScore::getScoreNodeId() const {
	return std::make_pair(scoreNodeIdList_[0], scoreNodeIdList_[1]);
}
#endif
