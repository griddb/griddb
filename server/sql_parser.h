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
/*!
 * @file sql_parser.h
 * @brief SQLパーサ関連クラスの定義
 */

#ifndef SQL_PARSER_H_
#define SQL_PARSER_H_

#include "util/allocator.h"
#include "util/code.h"
#include "util/container.h"
#include "util/trace.h"
#include "sql_tuple.h"
#include "sql_type.h"
#include "sql_common.h" 

#include <list>
#include <vector>

typedef util::StackAllocator SQLAllocator;
typedef util::VariableSizeAllocator<> SQLVarSizeAllocator;

class SQLExecution;
struct SQLExpandViewContext;
class SQLConnectionControl;
class SQLParserContext;
struct OptionParam;

struct DDLWithParameter {
	class Coder;

	enum Id {
		START_ID,
		EXPIRATION_TIME,
		EXPIRATION_TIME_UNIT,
		EXPIRATION_DIVISION_COUNT,
		EXPIRATION_TYPE,
		DATA_AFFINITY,
		DATA_AFFINITY_POLICY,
		INTERVAL_WORKER_GROUP,
		INTERVAL_WORKER_GROUP_POSITION,
		END_ID
	};
};

class DDLWithParameter::Coder {
public:
	const char8_t* operator()(Id id) const;
	bool operator()(const char8_t *name, Id &id) const;

private:
	struct NameTable;
	static const NameTable nameTable_;
};


struct SQLToken {
	int32_t id_; 
	uint32_t size_; 
	const char *value_; 
	uint32_t pos_;

	inline void reset() {
		id_ = 0;
		size_ = 0;
		value_ = NULL;
		pos_ = 0;
	}
};

class SyntaxTree {
public:
	class QualifiedName;
	class Expr;
	class Table;
	class Select;
	class Set;
	struct CreateTableOption;
	struct CreateIndexOption;
	struct TableColumn;
	struct PartitioningOption;
	struct ColumnInfo;
	struct TypeInfo;
	struct WindowOption;
	struct WindowFrameOption;
	struct WindowFrameBoundary;

	typedef TupleList::TupleColumnType ColumnType;
	typedef int32_t SourceId;

	typedef std::vector< SyntaxTree::Expr*, util::StdAllocator<
			SyntaxTree::Expr*, void> > ExprList;
	typedef std::vector< SyntaxTree::Select*, util::StdAllocator<
			SyntaxTree::Select*, void> > SelectList;
	typedef std::vector< SyntaxTree::QualifiedName*, util::StdAllocator<
			SyntaxTree::QualifiedName*, void> > QualifiedNameList;
	typedef std::vector< SyntaxTree::TableColumn*, util::StdAllocator<
			SyntaxTree::Select*, void> > TableColumnList;
	typedef std::vector< SyntaxTree::ColumnInfo*, util::StdAllocator<
			SyntaxTree::Select*, void> > ColumnInfoList;

	typedef util::Map<int32_t, TupleValue> DDLWithParameterMap;

	typedef std::pair< std::string, bool > ValueStrToBoolMapEntry;
	typedef std::map< std::string, bool > ValueStrToBoolMap;
	static const ValueStrToBoolMap::value_type VALUESTR_TO_BOOL_LIST[];
	static const ValueStrToBoolMap VALUESTR_TO_BOOL_MAP;

	static const uint32_t UNDEF_INPUTID;

	static const TupleList::TupleColumnType NULL_VALUE_RAW_DATA;

	static const int64_t SQL_TOKEN_COUNT_LIMIT;
	static const int64_t SQL_EXPR_TREE_DEPTH_LIMIT;
	static const int64_t SQL_VIEW_NESTING_LEVEL_LIMIT;

	template< typename T >
	static T* duplicateList(SQLAllocator &alloc, const T *src);

	template< typename T >
	static void deleteList(SQLAllocator &alloc, T *src);
	static void deleteTree(QualifiedName *name);
	static void deleteTree(Select *select);
	static void deleteTree(Set *set);
	static void deleteTree(Expr *expr);

	template< typename T >
	static void dumpList(std::ostream &os, const T *src, const char* name);

	static CreateTableOption* makeCreateTableOption(
		SQLAllocator &alloc, SyntaxTree::TableColumnList *columnList,
		SyntaxTree::ExprList *tableConstraintList,
		SyntaxTree::PartitioningOption *option,
		SyntaxTree::ExprList *createOptList, int32_t noErr,
		util::String *extensionName, bool isVirtual, bool isTimeSeries);

	static CreateTableOption* makeAlterTableAddColumnOption(
		SQLAllocator &alloc, SyntaxTree::TableColumnList *columnList);

	static void checkTableConstraint(
		SQLAllocator &alloc, SyntaxTree::TableColumnList *&columnList,
		SyntaxTree::ExprList *constList);

	static CreateIndexOption* makeCreateIndexOption(
		SQLAllocator &alloc, SyntaxTree::ExprList *columnList,
		int32_t ifNotExist, CreateIndexOption *usingOption);

	static TableColumn* makeCreateTableColumn(
		SQLAllocator &alloc, SQLToken *name, TupleList::TupleColumnType type,
		SyntaxTree::ColumnInfo *colInfo);
	static TableColumn* makeCreateTableColumn(
		SQLAllocator &alloc, SQLToken *name,
		SyntaxTree::ColumnInfo *colInfo);

	static ExprList* makeRangeGroupOption(
			SQLAllocator &alloc, SyntaxTree::Expr *baseKey,
			SyntaxTree::Expr *fillExpr, int64_t interval, int64_t intervalUnit,
			int64_t offset, int64_t timeZone, bool withTimeZone);

	static bool resolveRangeGroupOffset(
			const SQLToken &token, int64_t &offset);
	static bool resolveRangeGroupTimeZone(
			SQLAllocator &alloc, const SQLToken &token, int64_t &timeZone);

	static TupleList::TupleColumnType toColumnType(
			SQLAllocator &alloc, const SQLToken &type, const int64_t *num1,
			const int64_t *num2);
	static TupleList::TupleColumnType toColumnType(
			const char8_t *name, const int64_t *num1,const int64_t *num2);
	static TupleList::TupleColumnType determineType(const char *zIn);

	static bool checkPartitioningIntervalTimeField(
			util::DateTime::FieldType type);
	static bool checkGroupIntervalTimeField(util::DateTime::FieldType type);

	static int32_t gsDequote(SQLAllocator &alloc, util::String &str);
	static void tokenToString(SQLAllocator &alloc, const SQLToken &token,
			bool dequote, util::String* &out, bool &isQuoted);
	static util::String* tokenToString(
			SQLAllocator &alloc, const SQLToken &token, bool dequote);

	static bool toSignedValue(
			const SQLToken &token, bool minus, int64_t &value);

	static void countLineAndColumnFromToken(
			const char* srcSqlStr, const SQLToken &token, size_t &line, size_t &column);

	static void countLineAndColumn(
			const char* srcSqlStr, const Expr* expr, size_t &line, size_t &column);

	static bool isEmptyToken(const SQLToken &token);
	static SQLToken makeEmptyToken();

	static Expr* makeConst(SQLAllocator& alloc, const TupleValue &value);

	static TupleValue makeNanoTimestampValue(
			SQLAllocator& alloc, const NanoTimestamp &ts);
	static TupleValue makeStringValue(
			SQLAllocator& alloc, const char* str, size_t strLen);

	static TupleValue makeBlobValue(
			TupleValue::VarContext& cxt, const void* src, size_t byteLen);

	static void calcPositionInfo(const char *top, const char *cursor,
			int32_t &line, int32_t &charPos);

	enum SQLCommandType {
		COMMAND_SELECT,
		COMMAND_DML,
		COMMAND_DDL,
		COMMAND_NONE,
		COMMAND_DCL
	};

	enum CommandType {
		CMD_NONE,
		CMD_SELECT,
		CMD_INSERT,
		CMD_UPDATE,
		CMD_DELETE,

		CMD_CREATE_DATABASE,
		CMD_DROP_DATABASE,
		CMD_CREATE_TABLE,
		CMD_DROP_TABLE,
		CMD_CREATE_INDEX,
		CMD_DROP_INDEX,
		CMD_ALTER_TABLE_DROP_PARTITION,   
		CMD_ALTER_TABLE_ADD_COLUMN,       
		CMD_ALTER_TABLE_RENAME_COLUMN,    
		CMD_CREATE_VIEW,  
		CMD_DROP_VIEW,    

		CMD_CREATE_USER,
		CMD_CREATE_ROLE, 
		CMD_DROP_USER,
		CMD_SET_PASSWORD,
		CMD_GRANT,
		CMD_REVOKE,

		CMD_BEGIN,
		CMD_COMMIT,
		CMD_ROLLBACK
	};

	enum ExplainType {
		EXPLAIN_NONE = 0,
		EXPLAIN_PLAN,
		EXPLAIN_ANALYZE
	};

	enum AggrOpt {
		AGGR_OPT_ALL = 0,
		AGGR_OPT_DISTINCT
	};

	enum SetOp {
		SET_OP_NONE = 0,
		SET_OP_UNION_ALL,
		SET_OP_UNION,
		SET_OP_INTERSECT,
		SET_OP_EXCEPT
	};

	enum ResolveType {
		RESOLVETYPE_DEFAULT = 0,
		RESOLVETYPE_IGNORE,
		RESOLVETYPE_REPLACE
	};

	enum FieldType {
		FIELD_YEAR = 1,
		FIELD_MONTH,
		FIELD_DAY,
		FIELD_WEEK,
		FIELD_HOUR,
		FIELD_MINUTE,
		FIELD_SECOND,
		FIELD_MILLISECOND
	};

	enum TablePartitionType {
		TABLE_PARTITION_TYPE_UNDEF = 0,
		TABLE_PARTITION_TYPE_HASH,
		TABLE_PARTITION_TYPE_RANGE,
		TABLE_PARTITION_TYPE_RANGE_HASH
	};

	template<typename T>
	static bool isRangePartitioningType(T typeVal) {
		TablePartitionType type = static_cast<TablePartitionType>(typeVal);
		return (type == TABLE_PARTITION_TYPE_RANGE
				|| type ==TABLE_PARTITION_TYPE_RANGE_HASH);
	}

	template<typename T>
	static bool isIncludeHashPartitioningType(T typeVal) {
		TablePartitionType type = static_cast<TablePartitionType>(typeVal);
		return (type == TABLE_PARTITION_TYPE_HASH
				|| type ==TABLE_PARTITION_TYPE_RANGE_HASH);
	}

	class QualifiedName {
	public:
		static const int64_t TOP_NS_ID = 0;

		explicit QualifiedName(SQLAllocator &alloc);
		~QualifiedName();

		static QualifiedName* makeQualifiedName(SQLAllocator &alloc);
		static QualifiedName* makeQualifiedName(
				SQLAllocator &alloc, int64_t nsId,
				const char8_t *db, const char8_t *table, const char8_t *name);
		static QualifiedName* makeQualifiedName(
				SQLAllocator& alloc, int64_t nsId,
				SQLToken* db, SQLToken* table, SQLToken* name);
		QualifiedName *duplicate() const;

		void dump(std::ostream &os);

		SQLAllocator &alloc_;

		int64_t nsId_; 
		util::String *db_; 
		util::String *table_; 
		util::String *name_; 

		bool dbCaseSensitive_;
		bool tableCaseSensitive_;
		bool nameCaseSensitive_;

		UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
		UTIL_OBJECT_CODER_MEMBERS(
				UTIL_OBJECT_CODER_OPTIONAL(nsId_, 0),
				db_, table_, name_,
				UTIL_OBJECT_CODER_OPTIONAL(dbCaseSensitive_, false),
				UTIL_OBJECT_CODER_OPTIONAL(tableCaseSensitive_, false),
				UTIL_OBJECT_CODER_OPTIONAL(nameCaseSensitive_, false));
	};

	class Select {
	public:
		explicit Select(SQLAllocator &alloc);
		~Select();

		static Select* makeSelect(SQLAllocator &alloc, CommandType commandType);
		static Select* makeSelect(
				SQLAllocator &alloc, CommandType commandType,
				ExprList* selectList, AggrOpt selectOpt,
				Expr* fromExpr, Expr* whereExpr,
				ExprList* groupByList, Expr* having,
				ExprList* orderByList, ExprList* limitList,
				ExprList* hintList);
		Select* duplicate(SQLAllocator &alloc) const;

		void dump(std::ostream &os);

		bool isDDL();
		int32_t calcCommandType();
		CommandType getCommandType();
		bool isDropOnlyCommand();

		const char* dumpCommandType();
		int32_t dumpCategoryType();

		SQLAllocator& alloc_;
		CommandType cmdType_; 
		ExprList* selectList_; 
		AggrOpt selectOpt_; 
		Expr* from_; 
		Expr* where_; 
		ExprList* groupByList_; 
		Expr* having_; 
		ExprList* orderByList_; 
		ExprList* limitList_; 
		ExprList* hintList_; 
		QualifiedName* targetName_;
		ExprList* updateSetList_; 
		ExprList* insertList_; 
		Set* insertSet_; 
		ExprList* valueList_; 
		ExprList* valueChildList_; 
		CreateTableOption* createTableOpt_;
		CreateIndexOption* createIndexOpt_;
		ExprList* cmdOptionList_; 
		int32_t cmdOptionValue_;

		Select* parent_; 

		UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
		UTIL_OBJECT_CODER_MEMBERS(
				UTIL_OBJECT_CODER_ENUM(cmdType_),
				selectList_,
				UTIL_OBJECT_CODER_ENUM(selectOpt_),
				from_, where_, groupByList_, having_, orderByList_,
				limitList_, hintList_,
				targetName_, updateSetList_,
				insertList_, insertSet_,
				valueList_, valueChildList_,
				createTableOpt_, createIndexOpt_,
				cmdOptionList_, cmdOptionValue_);
	};


	class Set {
	public:
		explicit Set(SQLAllocator &alloc);
		~Set();

		static Set* makeSet(SQLAllocator &alloc);
		static Set* makeSet(SQLAllocator &alloc, SetOp type,
							Set* leftSet, Select* oneselect, SelectList* unionAllList);
		static Set* makeSet(SQLAllocator &alloc, SetOp type,
							Select* left);
		Set* duplicate(SQLAllocator &alloc) const;
		void dump(std::ostream &os);

		SQLAllocator& alloc_;
		SetOp type_; 
		Set* left_; 
		Select* right_; 
		SelectList* unionAllList_; 

		UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
		UTIL_OBJECT_CODER_MEMBERS(
				UTIL_OBJECT_CODER_ENUM(type_), left_, right_, unionAllList_);
	};

	class Expr {
	public:
		explicit Expr(SQLAllocator &alloc);
		~Expr();

		static Expr* makeExpr(SQLAllocator &alloc, SQLType::Id op);
		static Expr* makeUnaryExpr(
				SQLParserContext &context, SQLType::Id op, Expr* expr);
		static Expr* makeBinaryExpr(
				SQLParserContext &context, SQLType::Id op, Expr* left, Expr* right);
		static Expr* makeTable(
				SQLAllocator &alloc, QualifiedName* qName, Set* subQuery);
		Expr* duplicate(SQLAllocator &alloc) const;

		Expr(const Expr &another);
		Expr& operator=(const Expr &another);

		void dump(std::ostream &os);
		static const char* op2str(int32_t op);

		SQLAllocator& alloc_;
		SourceId srcId_; 
		SQLType::Id op_; 

		QualifiedName* qName_;
		util::String* aliasName_; 
		TupleList::TupleColumnType columnType_; 
		TupleValue value_; 
		Expr* left_; 
		Expr* right_; 
		Expr* mid_; 
		ExprList* next_; 
		Set* subQuery_; 
		bool sortAscending_; 
		AggrOpt aggrOpts_; 
		SQLType::JoinType joinOp_; 
		int32_t placeHolderCount_; 

		WindowOption* windowOpt_; 

		uint32_t inputId_;
		ColumnId columnId_;
		uint32_t subqueryLevel_;
		uint32_t aggrNestLevel_;
		bool constantEvaluable_;
		bool autoGenerated_;

		uint32_t placeHolderPos_;
		uint32_t treeHeight_;

		SQLToken startToken_; 
		SQLToken endToken_; 

		UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
		UTIL_OBJECT_CODER_MEMBERS(
				UTIL_OBJECT_CODER_OPTIONAL(srcId_, -1),
				UTIL_OBJECT_CODER_ENUM(op_, SQLType::Coder()),
				qName_, aliasName_,
				UTIL_OBJECT_CODER_ENUM(
						columnType_, TupleList::TupleColumnType(),
						TupleList::TupleColumnTypeCoder()),
				value_,
				left_, right_, mid_, next_,
				subQuery_,
				windowOpt_,
				UTIL_OBJECT_CODER_OPTIONAL(sortAscending_, true),
				UTIL_OBJECT_CODER_ENUM(aggrOpts_, AggrOpt()),
				UTIL_OBJECT_CODER_ENUM(
						joinOp_, SQLType::JoinType(), SQLType::Coder()),
				UTIL_OBJECT_CODER_OPTIONAL(placeHolderCount_, 0),
				UTIL_OBJECT_CODER_OPTIONAL(inputId_, UNDEF_INPUTID),
				UTIL_OBJECT_CODER_OPTIONAL(columnId_, UNDEF_COLUMNID),
				UTIL_OBJECT_CODER_OPTIONAL(subqueryLevel_, 0U),
				UTIL_OBJECT_CODER_OPTIONAL(aggrNestLevel_, 0U),
				UTIL_OBJECT_CODER_OPTIONAL(constantEvaluable_, false),
				UTIL_OBJECT_CODER_OPTIONAL(autoGenerated_, false));
	};

	struct TableColumn {
		QualifiedName* qName_;
		ColumnType type_;
		int32_t option_;
		Expr* virtualColumnOption_; 

		UTIL_OBJECT_CODER_MEMBERS(qName_, type_, option_, virtualColumnOption_);
	};

	struct WindowOption {
		explicit WindowOption(SQLAllocator &alloc);

		WindowOption(const WindowOption &another);
		WindowOption& operator=(const WindowOption &another);

		void dump(std::ostream &os);

		SQLAllocator& alloc_;
		ExprList* partitionByList_;
		ExprList* orderByList_;
		WindowFrameOption* frameOption_;

		UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
		UTIL_OBJECT_CODER_MEMBERS(
				partitionByList_, orderByList_, frameOption_);
	};

	struct WindowFrameOption {		
		enum FrameMode {
			FRAME_MODE_NONE = 0,
			FRAME_MODE_ROW,
			FRAME_MODE_RANGE
		};

		WindowFrameOption();

		void dump(std::ostream &os);

		FrameMode frameMode_;
		WindowFrameBoundary* frameStartBoundary_;
		WindowFrameBoundary* frameFinishBoundary_;

		UTIL_OBJECT_CODER_MEMBERS(
				UTIL_OBJECT_CODER_ENUM(frameMode_),
				frameStartBoundary_, frameFinishBoundary_);
	};

	struct WindowFrameBoundary {	
		enum FrameBoundaryType {
			BOUNDARY_NONE = 0,
			BOUNDARY_UNBOUNDED_PRECEDING,
			BOUNDARY_UNBOUNDED_FOLLOWING,
			BOUNDARY_CURRENT_ROW,
			BOUNDARY_N_PRECEDING,
			BOUNDARY_N_FOLLOWING
		};

		explicit WindowFrameBoundary(SQLAllocator &alloc);

		WindowFrameBoundary(const WindowFrameBoundary &another);
		WindowFrameBoundary& operator=(const WindowFrameBoundary &another);

		void dump(std::ostream &os);

		SQLAllocator& alloc_;
		FrameBoundaryType boundaryType_;
		Expr* boundaryValueExpr_;
		int64_t boundaryLongValue_;
		int64_t boundaryTimeUnit_;

		UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
		UTIL_OBJECT_CODER_MEMBERS(
				UTIL_OBJECT_CODER_ENUM(boundaryType_),
				boundaryValueExpr_, boundaryLongValue_, boundaryTimeUnit_);
	};

	struct PartitioningOption {
		PartitioningOption() :
				partitionColumn_(NULL), subPartitionColumn_(NULL),
				partitionCount_(0),
				partitionType_(TABLE_PARTITION_TYPE_UNDEF),
				optInterval_(0), optIntervalUnit_(-1) {};

		QualifiedName* partitionColumn_;
		QualifiedName* subPartitionColumn_;
		int32_t partitionCount_;
		TablePartitionType partitionType_;
		int64_t optInterval_;
		int64_t optIntervalUnit_;

		UTIL_OBJECT_CODER_MEMBERS(
				partitionColumn_, subPartitionColumn_, partitionCount_,
				UTIL_OBJECT_CODER_ENUM(partitionType_),
				optInterval_, optIntervalUnit_);
	};


	typedef uint8_t ColumnOption;
	static const ColumnOption COLUMN_OPT_PRIMARY_KEY = 1 << 0; 
	static const ColumnOption COLUMN_OPT_VIRTUAL     = 1 << 1; 
	static const ColumnOption COLUMN_OPT_NOT_NULL    = 1 << 2; 
	static const ColumnOption COLUMN_OPT_NULLABLE    = 1 << 3; 

	struct ColumnInfo {
		ColumnInfo();

		bool isPrimaryKey() { return (option_ & COLUMN_OPT_PRIMARY_KEY) != 0; }
		bool isVirtual() { return (option_ & COLUMN_OPT_VIRTUAL) != 0; }
		bool hasNotNullConstraint() { return (option_ & COLUMN_OPT_NOT_NULL) != 0; }

		void setPrimaryKey() {
			option_ |= COLUMN_OPT_PRIMARY_KEY;
			option_ |= COLUMN_OPT_NOT_NULL;
		}
		QualifiedName *columnName_; 
		ColumnType type_; 
		ColumnOption option_; 
		Expr* virtualColumnOption_; 

		UTIL_OBJECT_CODER_MEMBERS(
				columnName_, type_,
				option_, virtualColumnOption_);
	};

	struct TablePartitionInfo {
		void dump(std::ostream &os);

		u8string option_; 

		UTIL_OBJECT_CODER_MEMBERS(option_);
	};

	struct CreateTableOption {
		explicit CreateTableOption(SQLAllocator &alloc);
		void dump(std::ostream &os);

		SQLAllocator& alloc_;
		bool ifNotExists_;					
		int32_t columnNum_;					
		ColumnInfoList* columnInfoList_;	
		ExprList* tableConstraintList_;		
		TablePartitionType partitionType_;	
		QualifiedName* partitionColumn_;	
		QualifiedName* subPartitionColumn_;	
		int64_t optInterval_;				
		int64_t optIntervalUnit_;			
		DDLWithParameterMap* propertyMap_;	
		TablePartitionInfo* partitionInfoList_;	
		int32_t partitionInfoNum_;				
		util::String* optionString_;		
		util::String* extensionName_;		
		ExprList* extensionOptionList_;		
		bool isVirtual_;
		bool isTimeSeries_;

		bool isPartitioning() const {
			return (partitionType_ != TABLE_PARTITION_TYPE_UNDEF);
		}
		bool isTimeSeries() const {
			return isTimeSeries_;
		}

		UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
		UTIL_OBJECT_CODER_MEMBERS(isTimeSeries_,
				ifNotExists_, columnNum_, columnInfoList_,
				tableConstraintList_,
				UTIL_OBJECT_CODER_ENUM(partitionType_),
				partitionColumn_, subPartitionColumn_,
				optInterval_, optIntervalUnit_,
				partitionInfoList_, partitionInfoNum_, optionString_,
				extensionName_, extensionOptionList_, isVirtual_,
				isTimeSeries_);
	};

	struct CreateIndexOption {
		CreateIndexOption();
		void dump(std::ostream &os);

		QualifiedNameList* columnNameList_;
		bool ifNotExists_; 
		util::String* extensionName_; 
		ExprList* extensionOptionList_; 
		OptionParam *optionParams_;

		UTIL_OBJECT_CODER_MEMBERS(
				columnNameList_, ifNotExists_,
				extensionName_, extensionOptionList_);
	};
};

class SQLPlanningVersion {
public:
	SQLPlanningVersion();
	SQLPlanningVersion(int32_t major, int32_t minor);

	bool isEmpty() const;

	int32_t getMajor() const;
	int32_t getMinor() const;

	void parse(const char8_t *str);

	int32_t compareTo(int32_t major, int32_t minor) const;
	int32_t compareTo(const SQLPlanningVersion &another) const;

private:
	int32_t major_;
	int32_t minor_;
};

namespace lemon_SQLParser{ class SQLParser; };


/*!
	@brief 解析情報
*/
struct SQLParsedInfo {
	typedef SyntaxTree::Expr Expr;

	/*!
		@brief バインド情報
		@note prepare時に生成、bind時にBindInfoSetと対応してpPlanをiPlanに変更する
	*/
	struct BindInfo {

	/*!
		@brief コンストラクタ
		@param [in] varName 変数名
		@param [in] Expr エクスプレッション
	*/
		BindInfo(util::String &varName, Expr *bindExpr) :
				varName_(varName), bindExpr_(bindExpr) {}

		util::String &varName_;
		Expr *bindExpr_;
	};

	/*!
		@brief コンストラクタ
		@param [in] alloc アロケータ
	*/
	SQLParsedInfo(util::StackAllocator &alloc) : alloc_(alloc),
			syntaxTreeList_(alloc), tableList_(alloc), bindList_(alloc),
			inputSql_(NULL),
			placeHolderCount_(0), explainType_(SyntaxTree::EXPLAIN_NONE),
			pragmaType_(SQLPragma::PRAGMA_NONE), pragmaValue_(NULL),
			createForceView_(false)
			{}
	/*!
		@brief デストラクタ
	*/
	~SQLParsedInfo() {}

	/*!
		@brief 解析済みコマンド数の取得
	*/
	int32_t getCommandNum() {
		return static_cast<int32_t>(syntaxTreeList_.size());
	}

	void clear() {
		syntaxTreeList_.clear();
		tableList_.clear();
	};

	util::StackAllocator &alloc_;

	util::Vector<SyntaxTree::Select*> syntaxTreeList_;

	util::Vector<SyntaxTree::Expr*> tableList_;

	util::Vector<BindInfo> bindList_;

	const char* inputSql_;

	int32_t placeHolderCount_;
	SyntaxTree::ExplainType explainType_;

	SQLPragma::PragmaType pragmaType_;
	util::String* pragmaValue_;
	bool createForceView_;
};


/*!
 * @brief SQLParserContext object
 */
struct SQLTableInfo;
class SQLTableInfoList;
class SQLParserContext {
	friend class lemon_SQLParser::SQLParser;

public:
	explicit SQLParserContext(SQLAllocator &alloc);
	void reset();

	void beginParse(SyntaxTree::ExplainType explainType);
	void finishParse();
	void beginTransaction(int32_t type);
	void commitTransaction();
	void rollbackTransaction();

	virtual ~SQLParserContext();

	void dumpSyntaxTree(std::ostream &os);

	enum ParseState{
		PARSESTATE_SELECTION,
		PARSESTATE_CONDITION,
		PARSESTATE_END
	};

	void setError(const char* errMsg);
	void setTopSelect(SyntaxTree::Select *topSelect) {
		topSelect_ = topSelect;
	}
	void setCurrentSelect(SyntaxTree::Select *currentSelect) {
		currentSelect_ = currentSelect;
	}
	void setHintSelect(const SyntaxTree::Select *hintSelect) {
		hintSelect_ = hintSelect;
	}

	void setPragma(
			SQLToken &key1, SQLToken &key2, SQLToken &key3,
			SQLToken &value, int32_t minusFlag);

	SQLAllocator & getSQLAllocator() { return alloc_; }
	SQLAllocator & getDefaultAllocator() { return alloc_; }
	TupleValue::VarContext & getVarContext() { return cxt_; }
	const char* getInputSql() { return inputSql_; }

	SyntaxTree::Select* getTopSelect() {
		return topSelect_;
	}
	SyntaxTree::Select* getCurrentSelect() {
		return currentSelect_;
	}
	const SyntaxTree::Select* getHintSelect() const {
		return hintSelect_;
	}

	int64_t checkAndGetViewSelectStr(SyntaxTree::Expr* &table);

	const char* getViewSelect(SyntaxTree::Expr &expr);

	bool parseViewSelect(
			const char *viewSelectStr, SQLParsedInfo& viewParsedInfo);

	void checkViewCircularReference(SyntaxTree::Expr* expr);

	int64_t allocateNewNsId();

	void contractCondition();

	SQLAllocator& alloc_;
	TupleValue::VarContext cxt_;

	util::XArray<SyntaxTree::Select*> childrenList_;
	util::Vector<SyntaxTree::Expr*> tableList_;
	SyntaxTree::Select* topSelect_;
	SyntaxTree::Select* currentSelect_;
	const SyntaxTree::Select* hintSelect_;

	ParseState parseState_;

	int32_t placeHolderCount_; 
	SyntaxTree::ExplainType explainType_;
	int64_t exprTreeDepthLimit_;

	const char* inputSql_;

	SQLPragma::PragmaType pragmaType_;
	util::String* pragmaValue_;

	util::String* errorMsg_;
	bool isSetError_;

	void getTableInfo(SyntaxTree::Expr *expr, bool viewAllowed);

	SQLExpandViewContext* expandViewCxt_;
	SQLExecution* sqlExecution_;
	bool expandView_;
	bool createForceView_;
	uint32_t viewDepth_;
	util::Exception savedException_;

	int64_t viewNsId_;
	int64_t maxViewNsId_;
	util::String* viewExpandError_;
};



class GenSyntaxTree {
public:
	GenSyntaxTree(SQLAllocator &alloc);
	~GenSyntaxTree();
	void parse(const char* sqlCommandList, SyntaxTree::Select* &treeTop, SQLToken &last);
	void parseAll(
			SQLExpandViewContext *expandViewCxt,
			SQLExecution *execution,
			util::String& sqlCommandList, uint32_t viewDepth,
			int64_t viewNsId, int64_t maxViewNsId, bool expandView);

private:
	SQLAllocator& alloc_;
	lemon_SQLParser::SQLParser* parser_;
	SQLParserContext parserCxt_;
	int64_t tokenCountLimit_;
};

class SQLIndexStatsCache {
public:
	struct Key;
	struct Value;
	struct Option;

	class RequesterHolder;

	typedef util::Vector<Key> KeyList;

	SQLIndexStatsCache(
			SQLVariableSizeGlobalAllocator &globalVarAlloc,
			const Option &option);
	~SQLIndexStatsCache();

	bool find(const Key &key, Value &value) const;

	void put(const Key &key, uint64_t requester, const Value &value);
	void putMissed(const Key &key, uint64_t requester);

	bool checkKeyResolved(uint64_t requester);
	PartitionId getMissedKeys(
			util::StackAllocator &alloc, uint64_t requester,
			PartitionId startPartitionId, KeyList &keyList);

	uint64_t addRequester();
	void removeRequester(uint64_t requester);

private:
	struct Body;

	SQLIndexStatsCache(const SQLIndexStatsCache &another);
	SQLIndexStatsCache& operator=(const SQLIndexStatsCache &another);

	Body *body_;
};

struct SQLIndexStatsCache::Key {
public:
	Key(
			PartitionId partitionId, ContainerId containerId, uint32_t column,
			const TupleValue &lower, const TupleValue &upper,
			bool lowerInclusive, bool upperInclusive);

	bool operator<(const Key &another) const;
	int32_t compare(const Key &another) const;

	PartitionId partitionId_;
	ContainerId containerId_;
	uint32_t column_;
	TupleValue lower_;
	TupleValue upper_;
	bool lowerInclusive_;
	bool upperInclusive_;
};

struct SQLIndexStatsCache::Value {
	Value();

	bool isEmpty() const;

	int64_t approxSize_;
};

struct SQLIndexStatsCache::Option {
	Option();

	int64_t expirationMillis_;
	size_t freeElementLimit_;
};

class SQLIndexStatsCache::RequesterHolder {
public:
	RequesterHolder();
	~RequesterHolder();

	void assign(SQLIndexStatsCache &cache);
	void reset();

	const uint64_t* get();

private:
	RequesterHolder(const RequesterHolder &another);
	RequesterHolder& operator=(const RequesterHolder &another);

	SQLIndexStatsCache *cache_;
	uint64_t requester_;
};

#endif
