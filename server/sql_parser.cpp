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
 * @brief SQLパーサ関連クラスの実装
 */



#ifndef SQL_PARSER_ENABLE_WINDOW_FUNCTION
#define SQL_PARSER_ENABLE_WINDOW_FUNCTION 1
#endif

#include "sql_parser.h"
#include "sql_internal_parser.h"
#include "sql_lexer.h"
#include "sql_processor.h" 

#include "sql_execution.h"
#include "sql_compiler.h" 
#include "sql_utils.h"



const int64_t SyntaxTree::SQL_TOKEN_COUNT_LIMIT = 100000;
const int64_t SyntaxTree::SQL_EXPR_TREE_DEPTH_LIMIT = 3000;
const int64_t SyntaxTree::SQL_VIEW_NESTING_LEVEL_LIMIT = 10;

const uint32_t SyntaxTree::UNDEF_INPUTID =
		std::numeric_limits<uint32_t>::max();

const TupleList::TupleColumnType SyntaxTree::NULL_VALUE_RAW_DATA = TupleList::TYPE_NULL;

const SyntaxTree::ValueStrToBoolMap::value_type
SyntaxTree::VALUESTR_TO_BOOL_LIST[] = {
	SyntaxTree::ValueStrToBoolMap::value_type("true", true),
	SyntaxTree::ValueStrToBoolMap::value_type("false", false),
	SyntaxTree::ValueStrToBoolMap::value_type("on", true),
	SyntaxTree::ValueStrToBoolMap::value_type("off", false),
	SyntaxTree::ValueStrToBoolMap::value_type("yes", true),
	SyntaxTree::ValueStrToBoolMap::value_type("no", false)
};

const SyntaxTree::ValueStrToBoolMap SyntaxTree::VALUESTR_TO_BOOL_MAP(
		VALUESTR_TO_BOOL_LIST + 0, VALUESTR_TO_BOOL_LIST +
				sizeof VALUESTR_TO_BOOL_LIST / sizeof VALUESTR_TO_BOOL_LIST[0]);


struct DDLWithParameter::Coder::NameTable {
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

const DDLWithParameter::Coder::NameTable DDLWithParameter::Coder::nameTable_;

DDLWithParameter::Coder::NameTable::NameTable() : entryEnd_(entryList_) {
	std::fill(
			idList_, idList_ + TYPE_COUNT, static_cast<const char8_t*>(NULL));

#define SQL_DDL_WITH_PARAM_CODER_NAME_TABLE_ADD(id) add(id, #id)
	SQL_DDL_WITH_PARAM_CODER_NAME_TABLE_ADD(START_ID);
	SQL_DDL_WITH_PARAM_CODER_NAME_TABLE_ADD(EXPIRATION_TIME);
	SQL_DDL_WITH_PARAM_CODER_NAME_TABLE_ADD(EXPIRATION_TIME_UNIT);
	SQL_DDL_WITH_PARAM_CODER_NAME_TABLE_ADD(EXPIRATION_DIVISION_COUNT);
	SQL_DDL_WITH_PARAM_CODER_NAME_TABLE_ADD(EXPIRATION_TYPE);
	
	SQL_DDL_WITH_PARAM_CODER_NAME_TABLE_ADD(DATA_AFFINITY);
	SQL_DDL_WITH_PARAM_CODER_NAME_TABLE_ADD(DATA_AFFINITY_POLICY);
	SQL_DDL_WITH_PARAM_CODER_NAME_TABLE_ADD(INTERVAL_WORKER_GROUP);
	SQL_DDL_WITH_PARAM_CODER_NAME_TABLE_ADD(INTERVAL_WORKER_GROUP_POSITION);

#undef SQL_DDL_WITH_PARAM_CODER_NAME_TABLE_ADD

	std::sort(entryList_, entryEnd_, EntryPred());
}

DDLWithParameter::Coder::NameTable::~NameTable() {
	for (size_t pos = 0; pos < TYPE_COUNT; ++pos) {
		idList_[pos] = NULL;
	}
}

const char8_t* DDLWithParameter::Coder::NameTable::toString(Id id) const {
	if (id < 0 || static_cast<ptrdiff_t>(id) >= TYPE_COUNT) {
		return NULL;
	}

	return idList_[id];
}

bool DDLWithParameter::Coder::NameTable::find(const char8_t *name, Id &id) const {
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

void DDLWithParameter::Coder::NameTable::add(Id id, const char8_t *name) {
	if (id < 0 || static_cast<ptrdiff_t>(id) >= TYPE_COUNT ||
			entryEnd_ - entryList_ >= TYPE_COUNT) {
		assert(false);
		return;
	}
	idList_[id] = name;

	entryEnd_->first = name;
	entryEnd_->second = id;
	entryEnd_++;
}

bool DDLWithParameter::Coder::NameTable::EntryPred::operator()(
		const Entry &entry1, const Entry &entry2) const {
	return SQLProcessor::ValueUtils::strICmp(entry1.first, entry2.first) < 0;
}

const char8_t* DDLWithParameter::Coder::operator()(Id id) const {
	return nameTable_.toString(id);
}

bool DDLWithParameter::Coder::operator()(const char8_t *name, Id &id) const {
	return nameTable_.find(name, id);
}

SyntaxTree::Select::Select(SQLAllocator &alloc) :
		alloc_(alloc), cmdType_(CMD_NONE), selectList_(NULL),
		selectOpt_(AGGR_OPT_ALL),
		from_(NULL), where_(NULL), groupByList_(NULL), having_(NULL),
		orderByList_(NULL), limitList_(NULL), hintList_(NULL),
		targetName_(NULL), updateSetList_(NULL),
		insertList_(NULL), insertSet_(NULL),
		valueList_(NULL), valueChildList_(NULL),
		createTableOpt_(NULL), createIndexOpt_(NULL),
		cmdOptionList_(NULL), cmdOptionValue_(0),
		parent_(NULL) {
}

SyntaxTree::Select::~Select() {
}

SyntaxTree::Select* SyntaxTree::Select::makeSelect(SQLAllocator &alloc, CommandType commandType) {
	Select* select = ALLOC_NEW(alloc) Select(alloc);
	select->cmdType_ = commandType;
	return select;
}

SyntaxTree::Select* SyntaxTree::Select::makeSelect(SQLAllocator &alloc, CommandType commandType,
		ExprList *selectList, AggrOpt selectOpt,
		Expr *fromExpr, Expr *whereExpr,
		ExprList *groupByList, Expr *having,
		ExprList *orderByList, ExprList *limitList, ExprList* hintList) {
	Select* select = ALLOC_NEW(alloc) Select(alloc);
	select->cmdType_ = commandType;
	select->selectList_ = selectList;
	select->selectOpt_ = selectOpt;
	select->from_ = fromExpr;
	select->where_ = whereExpr;
	select->groupByList_ = groupByList;
	select->having_ = having;
	select->orderByList_ = orderByList;
	select->limitList_ = limitList;
	select->hintList_ = hintList;
	return select;
}

SyntaxTree::Select* SyntaxTree::Select::duplicate(SQLAllocator &alloc) const {
	Select* select = NULL;
	try {
		select = ALLOC_NEW(alloc) Select(alloc);
		select->cmdType_ = select->cmdType_;
		if (selectList_) {
			select->selectList_ = selectList_;
		}
		select->selectOpt_ = selectOpt_;
		if (from_) {
			select->from_ = from_->duplicate(alloc);
		}
		if (where_) {
			select->where_ = where_->duplicate(alloc);
		}
		if (groupByList_) {
			select->groupByList_ = SyntaxTree::duplicateList<ExprList>(alloc, groupByList_);
		}
		if (having_) {
			select->having_ = having_->duplicate(alloc);
		}
		if (orderByList_) {
			select->orderByList_ = SyntaxTree::duplicateList<ExprList>(alloc, orderByList_);
		}
		if (limitList_) {
			select->limitList_ = SyntaxTree::duplicateList<ExprList>(alloc, limitList_);
		}
		if (targetName_) {
			select->targetName_ = targetName_->duplicate();
		}
		if (updateSetList_) {
			select->updateSetList_ = SyntaxTree::duplicateList<ExprList>(alloc, updateSetList_);
		}
		if (insertList_) {
			select->insertList_ = SyntaxTree::duplicateList<ExprList>(alloc, insertList_);
		}
		if (insertSet_) {
			select->insertSet_ = insertSet_->duplicate(alloc);
		}
		if (valueList_) {
			select->valueList_ = SyntaxTree::duplicateList<ExprList>(alloc, valueList_);
		}
		if (valueChildList_) {
			select->valueChildList_ = SyntaxTree::duplicateList<ExprList>(alloc, valueChildList_);
		}
		if (cmdOptionList_) {
			select->cmdOptionList_ = SyntaxTree::duplicateList<ExprList>(alloc, cmdOptionList_);
		}
		select->cmdOptionValue_ = select->cmdOptionValue_;
		if (parent_) {
			select->parent_ = parent_->duplicate(alloc);
		}
	}
	catch (std::exception &e) {
		ALLOC_DELETE(alloc, select);
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
	return select;
}

void SyntaxTree::CreateTableOption::dump(std::ostream &os) {
	os << "{\"CreateTableOption\":{\"IfNotExists\":" << (ifNotExists_ ? "true" : "false");
	os << ", \"IsTimeSeries\":" << (isTimeSeries_ ? "true" : "false");
	os << ", \"IsVirtual\":" << (isVirtual_ ? "true" : "false");
	os << ", \"ColumnNum\":" << columnNum_;
	if (columnInfoList_) {
		os << ", \"ColumnInfoList\":[";
		ColumnInfoList::iterator itr = columnInfoList_->begin();
		for (; itr != columnInfoList_->end(); ++itr) {
			if (itr != columnInfoList_->begin()) {
				os << ", ";
			}
			os << "{\"type\":" << (*itr)->type_;
			os << ", \"columnName\":\"" << (*itr)->columnName_->name_->c_str() << "\"";
			os << ", \"columnNameCaseSensitive\":\"" <<
					((*itr)->columnName_->nameCaseSensitive_ ? "true" : "false") << "\"";
			os << ", \"option\":" << static_cast<uint32_t>((*itr)->option_);
			os << "}";
		}
		os << "]";
	}
	os << ", \"TablePartitionType\":" << static_cast<int32_t>(partitionType_);
	if (partitionColumn_) {
		os << ", \"TablePartitionColumn\":\"" << partitionColumn_->name_->c_str() << "\"";
	}
	os << ", \"TablePartitionCount\":" << partitionInfoNum_;
	if (partitionInfoList_) {
		os << ", \"TablePartitionInfo\":\"";
		partitionInfoList_->dump(os);
	}
	if (optionString_) {
		os << ", \"OptionString\":\"" << optionString_->c_str() << "\"";
	}
	if (propertyMap_) {
		os << ", \"PropertyMap\":[";
		DDLWithParameterMap::iterator itr = propertyMap_->begin();
		for (; itr != propertyMap_->end(); ++itr) {
			if (itr != propertyMap_->begin()) {
				os << ", ";
			}
			os << "{\"key\":" << itr->first;
			const TupleValue &val = itr->second;
			if (val.getType() == TupleList::TYPE_LONG) {
				os << ", \"value\":" << val.get<int64_t>();
			}
			else if (val.getType() == TupleList::TYPE_STRING) {
				std::string str(static_cast<const char*>(val.varData()),
						val.varSize());
				os << ", \"value\":\"" << str.c_str() << "\"";
			}
			else {
				os << ", \"valueType\":" << val.getType();
			}
			os << "}";
		}
		os << "]";
	}
	os << "}}";
}

void SyntaxTree::TablePartitionInfo::dump(std::ostream &os) {
	os << "{\"TablePartitionInfo\":{\"option\":\"" << option_.c_str() << "\"}}";
}

void SyntaxTree::CreateIndexOption::dump(std::ostream &os) {
	os << "{\"CreateIndexOption\":{\"IfNotExists\":" <<
			(ifNotExists_ ? "true" : "false");
	for (size_t i = 0; i < columnNameList_->size(); i++) {
		if ((*columnNameList_)[i] && (*columnNameList_)[i]->name_) {
			os << ", \"columnName_\":\"" << (*columnNameList_)[i]->name_->c_str() << "\"";
		}
	}
	os << "}}";
}

bool SyntaxTree::Select::isDDL() {
	switch (cmdType_) {
	case CMD_CREATE_DATABASE:
	case CMD_DROP_DATABASE:
	case CMD_CREATE_TABLE:
	case CMD_DROP_TABLE:
	case CMD_CREATE_VIEW: 
	case CMD_DROP_VIEW: 
	case CMD_CREATE_INDEX:
	case CMD_DROP_INDEX:
	case CMD_ALTER_TABLE_DROP_PARTITION:  
	case CMD_ALTER_TABLE_ADD_COLUMN:  
	case CMD_ALTER_TABLE_RENAME_COLUMN:  
	case CMD_CREATE_USER:
	case CMD_DROP_USER:
	case CMD_SET_PASSWORD:
	case CMD_GRANT:
	case CMD_REVOKE:
		return true;
	default:
		return false;
	}
	return false;
}

int32_t SyntaxTree::Select::calcCommandType() {
	switch (cmdType_) {
	case CMD_NONE:
		return COMMAND_NONE;
	case CMD_SELECT:
		return COMMAND_SELECT;
	case CMD_INSERT:
	case CMD_UPDATE:
	case CMD_DELETE:
		return COMMAND_DML;
	case CMD_CREATE_DATABASE:
	case CMD_DROP_DATABASE:
	case CMD_CREATE_TABLE:
	case CMD_DROP_TABLE:
	case CMD_CREATE_VIEW: 
	case CMD_DROP_VIEW: 
	case CMD_CREATE_INDEX:
	case CMD_DROP_INDEX:
	case CMD_ALTER_TABLE_DROP_PARTITION:  
	case CMD_ALTER_TABLE_ADD_COLUMN:  
	case CMD_ALTER_TABLE_RENAME_COLUMN:  
	case CMD_CREATE_USER:
	case CMD_DROP_USER:
	case CMD_SET_PASSWORD:
	case CMD_GRANT:
	case CMD_REVOKE:
		return COMMAND_DDL;
	case CMD_BEGIN:
	case CMD_COMMIT:
	case CMD_ROLLBACK:
	case CMD_CREATE_ROLE:
		return COMMAND_NONE;
	default:
		assert(false);
		return COMMAND_NONE;
	}
	return COMMAND_NONE;
}

bool SyntaxTree::Select::isDropOnlyCommand() {
	switch (cmdType_) {
	case CMD_SELECT:
	case CMD_DELETE:
	case CMD_DROP_TABLE:
	case CMD_DROP_VIEW:
	case CMD_DROP_INDEX:
	case CMD_ALTER_TABLE_DROP_PARTITION:
		return true;
	default:
		return false;
	}
}

SyntaxTree::CommandType SyntaxTree::Select::getCommandType() {
	return cmdType_;
}


const char* SyntaxTree::Select::dumpCommandType() {
	switch (cmdType_) {
	case CMD_NONE:
		return "NONE";
	case CMD_SELECT:
		return "SELECT";
	case CMD_INSERT:
		return "INSERT";
	case CMD_UPDATE:
		return "UPDATE";
	case CMD_DELETE:
		return "DELETE";
	case CMD_CREATE_DATABASE:
		return "CREATE_DATABASE";
	case CMD_DROP_DATABASE:
		return "DROP_DATABASE";
	case CMD_CREATE_TABLE:
		return "CREATE_TABLE";
	case CMD_DROP_TABLE:
		return "DROP_TABLE";
	case CMD_CREATE_VIEW: 
		return "CREATE_VIEW";
	case CMD_DROP_VIEW: 
		return "DROP_VIEW";
	case CMD_CREATE_INDEX:
		return "CREATE_INDEX";
	case CMD_DROP_INDEX:
		return "DROP_INDEX";
	case CMD_ALTER_TABLE_DROP_PARTITION:  
	case CMD_ALTER_TABLE_ADD_COLUMN:  
	case CMD_ALTER_TABLE_RENAME_COLUMN:  
		return "ALTER_TABLE";
	case CMD_CREATE_USER:
		return "CREATE_USER";
	case CMD_DROP_USER:
		return "DROP_USER";
	case CMD_SET_PASSWORD:
		return "SET_PASSWORD";
	case CMD_GRANT:
		return "GRANT";
	case CMD_REVOKE:
		return "REVOKE";
	case CMD_BEGIN:
	case CMD_COMMIT:
	case CMD_ROLLBACK:
		return "NONE";
	case CMD_CREATE_ROLE:
		return "CREATE_ROLE";
	default:
		return "NONE";
	}
	return "NONE";
}


int32_t SyntaxTree::Select::dumpCategoryType() {
	switch (cmdType_) {
	case CMD_NONE:
		return COMMAND_NONE;
	case CMD_SELECT:
		return COMMAND_SELECT;
	case CMD_INSERT:
	case CMD_UPDATE:
	case CMD_DELETE:
		return COMMAND_DML;
	case CMD_CREATE_DATABASE:
	case CMD_DROP_DATABASE:
	case CMD_CREATE_TABLE:
	case CMD_DROP_TABLE:
	case CMD_CREATE_VIEW: 
	case CMD_DROP_VIEW: 
	case CMD_CREATE_INDEX:
	case CMD_DROP_INDEX:
	case CMD_ALTER_TABLE_DROP_PARTITION:  
	case CMD_ALTER_TABLE_ADD_COLUMN:  
	case CMD_ALTER_TABLE_RENAME_COLUMN:  
	case CMD_CREATE_USER:
	case CMD_DROP_USER:
	case CMD_CREATE_ROLE:
		return COMMAND_DDL;
	case CMD_SET_PASSWORD:
	case CMD_GRANT:
	case CMD_REVOKE:
		return COMMAND_DCL;
	case CMD_BEGIN:
	case CMD_COMMIT:
	case CMD_ROLLBACK:
		return COMMAND_NONE;
	default:
		return COMMAND_NONE;
	}
	return COMMAND_NONE;
}


void SyntaxTree::Select::dump(std::ostream &os) {
	os << "{\"Select\":{\"cmdType\":" << static_cast<int32_t>(cmdType_); 
	if (selectList_) {
		SyntaxTree::dumpList<ExprList>(os, selectList_, "selectList");
	}
	os << ", \"selectOpt\":" << static_cast<int32_t>(selectOpt_);
	if (from_) {
		os << ", \"fromExpr\":";
		from_->dump(os);
	}
	if (where_) {
		os << ", \"whereExpr\":";
		where_->dump(os);
	}
	if (groupByList_) {
		SyntaxTree::dumpList<ExprList>(os, groupByList_, "groupByList");
	}
	if (having_) {
		os << ", \"havingExpr\":";
		having_->dump(os);
	}
	if (orderByList_) {
		SyntaxTree::dumpList<ExprList>(os, orderByList_, "orderByList");
	}
	if (limitList_) {
		SyntaxTree::dumpList<ExprList>(os, limitList_, "limitList");
	}
	if (targetName_) {
		os << ", \"targetName\":";
		targetName_->dump(os);
	}
	if (updateSetList_) {
		SyntaxTree::dumpList<ExprList>(os, updateSetList_, "updateSetList");
	}
	if (insertList_) {
		SyntaxTree::dumpList<ExprList>(os, insertList_, "insertList");
	}
	if (insertSet_) {
		os << ", \"insertSelectSet\":";
		insertSet_->dump(os);
	}
	if (valueList_) {
		SyntaxTree::dumpList<ExprList>(os, valueList_, "valueList");
	}
	if (valueChildList_) {
		SyntaxTree::dumpList<ExprList>(os, valueChildList_, "valueChildList");
	}
	if (createTableOpt_) {
		os << ", \"createTableOpt\":";
		createTableOpt_->dump(os);
	}
	if (createIndexOpt_) {
		os << ", \"createIndexOpt\":";
		createIndexOpt_->dump(os);
	}
	if (cmdOptionList_) {
		SyntaxTree::dumpList<ExprList>(os, cmdOptionList_, "cmdOptionList");
	}
	os << ", \"cmdOptionValue\":" << cmdOptionValue_;
	if (parent_) {
		os << ", \"parent\":\"exist\"";
	}
	os << "}}";
}


SyntaxTree::QualifiedName::QualifiedName(SQLAllocator &alloc) :
		alloc_(alloc),
		nsId_(TOP_NS_ID),
		db_(NULL),
		table_(NULL),
		name_(NULL),
		dbCaseSensitive_(false),
		tableCaseSensitive_(false),
		nameCaseSensitive_(false) {
}

SyntaxTree::QualifiedName::~QualifiedName() {
}

SyntaxTree::QualifiedName* SyntaxTree::QualifiedName::makeQualifiedName(SQLAllocator &alloc) {
	QualifiedName* qName = ALLOC_NEW(alloc) QualifiedName(alloc);
	return qName;
}

SyntaxTree::QualifiedName* SyntaxTree::QualifiedName::makeQualifiedName(
		SQLAllocator &alloc, int64_t nsId,
		const char8_t *db, const char8_t *table, const char8_t *name) {
	QualifiedName* qName = ALLOC_NEW(alloc) QualifiedName(alloc);
	if (db) {
		qName->db_ = ALLOC_NEW(alloc) util::String(db, alloc);
	}
	if (table) {
		qName->table_ = ALLOC_NEW(alloc) util::String(table, alloc);
	}
	if (name) {
		qName->name_ = ALLOC_NEW(alloc) util::String(name, alloc);
	}
	qName->nsId_ = nsId;
	return qName;
}

SyntaxTree::QualifiedName* SyntaxTree::QualifiedName::makeQualifiedName(
		SQLAllocator& alloc, int64_t nsId,
		SQLToken* db, SQLToken* table, SQLToken* name) {
	QualifiedName* qName = ALLOC_NEW(alloc) QualifiedName(alloc);
	if (db && db->size_ > 0) {
		tokenToString(alloc, *db, true, qName->db_, qName->dbCaseSensitive_);
	}
	if (table && table->size_ > 0) {
		tokenToString(alloc, *table, true, qName->table_, qName->tableCaseSensitive_);
	}
	if (name && name->size_ > 0) {
		tokenToString(alloc, *name, true, qName->name_, qName->nameCaseSensitive_);
	}
	qName->nsId_ = nsId;
	return qName;
}

SyntaxTree::QualifiedName* SyntaxTree::QualifiedName::duplicate() const {
	QualifiedName* qName = NULL;
	try {
		qName = ALLOC_NEW(alloc_) QualifiedName(alloc_);
		qName->nsId_ = nsId_;
		if (db_) {
			qName->db_ = ALLOC_NEW(alloc_) util::String(db_->c_str(), alloc_);
		}
		if (table_) {
			qName->table_ = ALLOC_NEW(alloc_) util::String(table_->c_str(), alloc_);
		}
		if (name_) {
			qName->name_ = ALLOC_NEW(alloc_) util::String(name_->c_str(), alloc_);
		}
		qName->dbCaseSensitive_ = dbCaseSensitive_;
		qName->tableCaseSensitive_ = tableCaseSensitive_;
		qName->nameCaseSensitive_ = nameCaseSensitive_;
	}
	catch (std::exception &e) {
		ALLOC_DELETE(alloc_, qName);
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
	return qName;
}

void SyntaxTree::QualifiedName::dump(std::ostream &os) {
	os << "{\"QualifiedName\":" << util::ObjectFormatter()(*this) << "}";
}


SyntaxTree::Set::Set(SQLAllocator &alloc) :
		alloc_(alloc), type_(SET_OP_NONE), left_(NULL), right_(NULL),
		unionAllList_(NULL) {
}

SyntaxTree::Set::~Set() {
}

SyntaxTree::Set* SyntaxTree::Set::makeSet(SQLAllocator &alloc) {
	Set* set = ALLOC_NEW(alloc) Set(alloc);
	return set;
}

SyntaxTree::Set* SyntaxTree::Set::makeSet(
		SQLAllocator &alloc, SetOp type, Select* oneselect) {
	Set* set = ALLOC_NEW(alloc) Set(alloc);
	set->type_ = type;

	set->right_ = oneselect;

	return set;
}

SyntaxTree::Set* SyntaxTree::Set::makeSet(
		SQLAllocator &alloc,
		SetOp type, Set* left, Select* oneselect,
		SelectList* unionAllList) {
	Set* set = ALLOC_NEW(alloc) Set(alloc);

	set->type_ = type;
	set->left_ = left;
	set->right_ = oneselect;
	set->unionAllList_ = unionAllList;
	return set;
}

SyntaxTree::Set* SyntaxTree::Set::duplicate(SQLAllocator &alloc) const {
	Set* set = NULL;
	try {
		set = ALLOC_NEW(alloc) Set(alloc);
		set->type_ = type_;
		if (left_) {
			set->left_ = left_->duplicate(alloc);
		}
		if (right_) {
			set->right_ = right_->duplicate(alloc);
		}
		if (unionAllList_) {
			set->unionAllList_ = SyntaxTree::duplicateList<SelectList>(alloc, unionAllList_);
		}
	}
	catch (std::exception &e) {
		ALLOC_DELETE(alloc, set);
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
	return set;
}

void SyntaxTree::Set::dump(std::ostream &os) {
	os << "{\"Set\":{";
	os << "\"type\":" << static_cast<int32_t>(type_);
	if (left_) {
		os << ", \"left\":";
		left_->dump(os);
	}
	if (right_) {
		os << ", \"right\":";
		right_->dump(os);
	}
	if (unionAllList_) {
		SyntaxTree::dumpList<SelectList>(os, unionAllList_, "unionAllList");
	}
	os << "}}";
}


SyntaxTree::Expr::Expr(SQLAllocator &alloc) :
		alloc_(alloc), srcId_(-1), op_(SQLType::Id()),
		qName_(NULL), aliasName_(NULL),
		columnType_(TupleList::TYPE_NULL),
		left_(NULL), right_(NULL), mid_(NULL), next_(NULL),
		subQuery_(NULL), sortAscending_(true),
		aggrOpts_(AggrOpt()), joinOp_(SQLType::JoinType()),
		placeHolderCount_(0),
		windowOpt_(NULL),
		inputId_(UNDEF_INPUTID), columnId_(UNDEF_COLUMNID),
		subqueryLevel_(0), aggrNestLevel_(0),
		constantEvaluable_(false), autoGenerated_(false),
		placeHolderPos_(0), treeHeight_(0) {
	startToken_.reset();
	endToken_.reset();
}

SyntaxTree::Expr::~Expr() {
}

SyntaxTree::Expr::Expr(const Expr &another) :
		alloc_(another.alloc_), srcId_(-1), op_(SQLType::Id()),
		qName_(NULL), aliasName_(NULL),
		columnType_(TupleList::TYPE_NULL),
		left_(NULL), right_(NULL), mid_(NULL), next_(NULL),
		subQuery_(NULL), sortAscending_(true),
		aggrOpts_(AggrOpt()), joinOp_(SQLType::JoinType()),
		placeHolderCount_(0),
		windowOpt_(NULL),
		inputId_(UNDEF_INPUTID), columnId_(UNDEF_COLUMNID),
		subqueryLevel_(0), aggrNestLevel_(0),
		constantEvaluable_(false), autoGenerated_(false),
		placeHolderPos_(0), treeHeight_(0) {
	*this = another;
}

SyntaxTree::Expr& SyntaxTree::Expr::operator=(const Expr &another) {
	srcId_ = another.srcId_;
	op_ = another.op_;
	qName_ = another.qName_;
	aliasName_ = another.aliasName_;
	columnType_ = another.columnType_;
	value_ = another.value_;
	left_ = another.left_;
	right_ = another.right_;
	mid_ = another.mid_;
	next_ = another.next_;
	subQuery_ = another.subQuery_;
	sortAscending_ = another.sortAscending_;
	aggrOpts_ = another.aggrOpts_;
	joinOp_ = another.joinOp_;
	placeHolderCount_ = another.placeHolderCount_;
	windowOpt_ = another.windowOpt_;
	inputId_ = another.inputId_;
	columnId_ = another.columnId_;
	subqueryLevel_ = another.subqueryLevel_;
	aggrNestLevel_ = another.aggrNestLevel_;
	constantEvaluable_ = another.constantEvaluable_;
	autoGenerated_ = another.autoGenerated_;
	treeHeight_ = another.treeHeight_;
	startToken_ = another.startToken_;
	endToken_ = another.endToken_;

	return *this;
}

SyntaxTree::Expr* SyntaxTree::Expr::makeExpr(SQLAllocator &alloc, SQLType::Id op) {
	Expr* expr = ALLOC_NEW(alloc) Expr(alloc);
	expr->op_ = op;

	return expr;
}

SyntaxTree::Expr* SyntaxTree::Expr::makeUnaryExpr(
		SQLParserContext &context, SQLType::Id op, Expr* arg) {

	assert(arg);
	SQLAllocator &alloc = context.getSQLAllocator();
	Expr* expr = ALLOC_NEW(alloc) Expr(alloc);
	expr->op_ = op;
	expr->left_ = arg;

	expr->treeHeight_ = arg->treeHeight_ + 1;
	if (expr->treeHeight_ > context.exprTreeDepthLimit_) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				"Expression tree is too large (maximum depth " <<
				context.exprTreeDepthLimit_ << ")");
	}

	expr->startToken_ = arg->startToken_;
	return expr;
}

SyntaxTree::Expr* SyntaxTree::Expr::makeBinaryExpr(
		SQLParserContext &context, SQLType::Id op, Expr* left, Expr* right) {
	assert(left);
	assert(right);
	SQLAllocator &alloc = context.getSQLAllocator();
	Expr* expr = ALLOC_NEW(alloc) Expr(alloc);
	expr->op_ = op;
	expr->left_ = left;
	expr->right_ = right;

	expr->treeHeight_ = (left->treeHeight_ > right->treeHeight_) ?
			left->treeHeight_ : right->treeHeight_;
	++expr->treeHeight_;
	if (expr->treeHeight_ > context.exprTreeDepthLimit_) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				"Expression tree is too large (maximum depth " <<
				context.exprTreeDepthLimit_ << ")");
	}

	expr->startToken_ = left->startToken_;
	expr->endToken_ = right->startToken_;

	return expr;
}

SyntaxTree::Expr* SyntaxTree::Expr::makeTable(SQLAllocator &alloc,
												QualifiedName* qName, Set* subQuery) {
	Expr* table = ALLOC_NEW(alloc) Expr(alloc);
	table->op_ = SQLType::EXPR_TABLE;
	if (qName) {
		table->qName_ = qName->duplicate();
	}
	else {
		table->qName_ = NULL;
	}
	table->subQuery_ = subQuery;

	return table;
}

SyntaxTree::Expr* SyntaxTree::makeConst(
		SQLAllocator& alloc, const TupleValue &value) {
	Expr *expr = Expr::makeExpr(alloc, SQLType::EXPR_CONSTANT);
	expr->value_ = value;
	return expr;
}

TupleValue SyntaxTree::makeNanoTimestampValue(
		SQLAllocator& alloc, const NanoTimestamp &ts) {
	return TupleNanoTimestamp(ALLOC_NEW(alloc) NanoTimestamp(ts));
}

TupleValue SyntaxTree::makeStringValue(
		SQLAllocator& alloc, const char* str, size_t strLen) {
	TupleValue::StackAllocSingleVarBuilder singleVarBuilder(
		alloc, TupleList::TYPE_STRING, strLen);
	singleVarBuilder.append(str, strLen);
	return singleVarBuilder.build();
}

TupleValue SyntaxTree::makeBlobValue(
		TupleValue::VarContext& cxt, const void* src, size_t byteLen) {
	TupleValue::StackAllocLobBuilder lobBuilder(
		cxt, TupleList::TYPE_BLOB, byteLen);
	lobBuilder.append(src, byteLen);
	return lobBuilder.build();
}

void SyntaxTree::calcPositionInfo(
		const char *top, const char *target, int32_t &line, int32_t &charPos) {
	line = 0;
	charPos = -1;
	if (top > target) {
		return;
	}
	const char *cursor = top;
	const char *lineTop = top;
	for (; cursor < target; ++cursor) {
		if (*cursor == '\n') {
			++line;
			lineTop = cursor + sizeof(char);
		}
	}
	charPos = static_cast<int32_t>((target - lineTop) / sizeof(char));
}

SyntaxTree::Expr* SyntaxTree::Expr::duplicate(SQLAllocator &alloc) const {
	Expr* expr = NULL;
	try {
		expr = ALLOC_NEW(alloc) Expr(alloc);
		expr->srcId_ = srcId_;
		expr->op_ = op_;
		if (qName_) {
			expr->qName_ = qName_->duplicate();
		}
		if (aliasName_) {
			expr->aliasName_ = ALLOC_NEW(alloc) util::String(aliasName_->c_str(), alloc);
		}
		expr->value_ = value_;
		if (left_) {
			expr->left_ = left_->duplicate(alloc);
		}
		if (right_) {
			expr->right_ = right_->duplicate(alloc);
		}
		if (mid_) {
			expr->mid_ = mid_->duplicate(alloc);
		}
		if (next_) {
			expr->next_ = SyntaxTree::duplicateList<ExprList>(alloc, next_);
		}
		if (subQuery_) {
			expr->subQuery_ = subQuery_->duplicate(alloc);
		}
		expr->sortAscending_ = sortAscending_;
		expr->aggrOpts_ = aggrOpts_;
		expr->joinOp_ = joinOp_;
		expr->placeHolderCount_ = placeHolderCount_;
		expr->windowOpt_ = windowOpt_;
		expr->treeHeight_ = treeHeight_;
	}
	catch (std::exception &e) {
		ALLOC_DELETE(alloc, expr);
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
	return expr;
}


void SyntaxTree::Expr::dump(std::ostream &os) {
	os << "{\"Expr\":{\"op\":\"" << SyntaxTree::Expr::op2str(op_) << "\"";
	if (qName_) {
		os << ", \"qName\":";
		qName_->dump(os);
	}
	if (aliasName_) {
		os << ", \"aliasName\":\"" << aliasName_->c_str() << "\"";
	}
	if (value_.getType() != TupleList::TYPE_NULL) {
		switch (value_.getType()) {
		case TupleList::TYPE_NULL:
			break;
		case TupleList::TYPE_BYTE: {
			int8_t val;
			memcpy(&val, value_.fixedData(), sizeof(val));
			os << ", \"value_byte\":" << val;
			break;
		}
		case TupleList::TYPE_SHORT: {
			int16_t val;
			memcpy(&val, value_.fixedData(), sizeof(val));
			os << ", \"value_short\":" << val;
			break;
		}
		case TupleList::TYPE_INTEGER: {
			int32_t val;
			memcpy(&val, value_.fixedData(), sizeof(val));
			os << ", \"value_int\":" << val;
			break;
		}
		case TupleList::TYPE_BOOL: {
			int8_t val;
			memcpy(&val, value_.fixedData(), sizeof(val));
			os << ", \"value_bool\":\"" << ((val != 0) ? "TRUE" : "FALSE") << "\"";
			break;
		}
		case TupleList::TYPE_LONG:
		case TupleList::TYPE_TIMESTAMP: {
			int64_t val = value_.get<int64_t>();
			os << ", \"value_long\":" << val;
			break;
		}
		case TupleList::TYPE_MICRO_TIMESTAMP: {
			MicroTimestamp val = value_.get<MicroTimestamp>();
			os << ", \"value_micro_timestamp\":" << val.value_;
			break;
		}
		case TupleList::TYPE_NANO_TIMESTAMP: {
			const NanoTimestamp &val = value_.get<TupleNanoTimestamp>();
			os << ", \"value_micro_timestamp\":" <<
					static_cast<uint32_t>(val.getHigh()) << " " <<
					val.getLow();
			break;
		}
		case TupleList::TYPE_FLOAT: {
			float val;
			memcpy(&val, value_.fixedData(), sizeof(val));
			os << ", \"value_float\":" << val;
			break;
		}
		case TupleList::TYPE_DOUBLE: {
			double val = value_.get<double>();
			os << ", \"value_double\":" << val;
			break;
		}
		case TupleList::TYPE_ANY:
			os << ", \"value\":\"TYPE_ANY\"";
			break;
		case TupleList::TYPE_STRING: {
			const char* c_str = static_cast<const char*>(value_.varData());
			std::string str(c_str, value_.varSize());
			os << ", \"value_str\":\"" << str.c_str() << "\"";
			break;
		}
		case TupleList::TYPE_GEOMETRY:
			os << ", \"value\":\"TYPE_GEOMETRY\"";
			break;
		case TupleList::TYPE_BLOB:
			os << ", \"value_blob\":{\"count\":";
			{
				const char temp = 0;
				TupleValue::LobReader reader(value_);
				const uint64_t count = reader.getPartCount();
				os << count;
				os << ", \"hex_value\":\"";
				const void *data;
				size_t size;
				if (reader.next(data, size)) {
					util::XArray<char> buffer(alloc_);
					buffer.assign(size * 2 + 1, temp);
					const char* in = static_cast<const char*>(data);
					size_t strSize = util::HexConverter::encode(buffer.data(), in, size, false);
					util::String hexStr(buffer.data(), strSize, alloc_);
					os << hexStr.c_str();
				}
				os << "\"}";
			}
			break;
		default:
			os << ", \"value\":\"TYPE_UNKNOWN\"";
			assert(false);
			break;
		}
	}
	if (subQuery_) {
		os << ", \"subQuery\":";
		subQuery_->dump(os);
	}
	if (!sortAscending_) {
		os << ", \"sortOrder\":\"DESC\"";
	}
	if (aggrOpts_ != 0) {
		os << ", \"aggrateOpt\":" << static_cast<int32_t>(aggrOpts_);
	}
	if (joinOp_ > 0) {
		os << ", \"joinOp\":" << static_cast<int32_t>(joinOp_);
	}
	if (placeHolderCount_ > 0) {
		os << ", \"placeHolderCount\":" << placeHolderCount_;
	}
	if (windowOpt_) {
		os << ", \"windowOption\":";
		windowOpt_->dump(os);
	}
	if (left_) {
		os << ", \"left\":";
		left_->dump(os);
	}
	if (right_) {
		os << ", \"right\":";
		right_->dump(os);
	}
	if (mid_) {
		os << ", \"mid\":";
		mid_->dump(os);
	}
	if (next_) {
		os << ", \"next\":[";
		ExprList::iterator itr = next_->begin();
		for (; itr != next_->end(); ++itr) {
			if (itr != next_->begin()) {
				os << ", ";
			}
			if (*itr) {
				(*itr)->dump(os);
			}
			else {
				os << "{\"Expr\":{}}";
			}
		}
		os << "]";
	}
	os << "}}";
}

const char* SyntaxTree::Expr::op2str(int32_t op) {
	return SQLType::Coder()(static_cast<SQLType::Id>(op), "UNKNOWN_OP");
}


template< typename T>
T* SyntaxTree::duplicateList(SQLAllocator &alloc, const T *src) {
	T* targetList = NULL;
	if (src) {
		try {
			targetList = ALLOC_NEW(alloc) T(alloc);
			targetList->resize(src->size());
			typename T::const_iterator itr = src->begin();
			for (; itr != src->end(); ++itr) {
				if (*itr) {
					targetList->push_back((*itr)->duplicate(alloc));
				}
				else {
					targetList->push_back(NULL);
				}
			}
		}
		catch (std::exception &e) {
			ALLOC_DELETE(alloc, targetList);
			GS_RETHROW_USER_OR_SYSTEM(e, "");
		}
	}
	return targetList;
}

template< typename T >
void SyntaxTree::deleteList(SQLAllocator &alloc, T *src) {
	if (src == NULL) {
		return;
	}

	typename T::iterator itr = src->begin();
	for (; itr != src->end(); ++itr) {
		deleteTree(*itr);
	}

	ALLOC_DELETE(alloc, src);
}

void SyntaxTree::deleteTree(QualifiedName *name) {
	if (name == NULL) {
		return;
	}

	SQLAllocator &alloc = name->alloc_;

	ALLOC_DELETE(alloc, name->db_);
	ALLOC_DELETE(alloc, name->table_);
	ALLOC_DELETE(alloc, name->name_);

	ALLOC_DELETE(alloc, name);
}

void SyntaxTree::deleteTree(Select *select) {
	if (select == NULL) {
		return;
	}

	SQLAllocator &alloc = select->alloc_;

	deleteList(alloc, select->selectList_);
	deleteTree(select->from_);
	deleteTree(select->where_);
	deleteList(alloc, select->groupByList_);
	deleteTree(select->having_);
	deleteList(alloc, select->orderByList_);
	deleteList(alloc, select->limitList_);
	deleteTree(select->targetName_);
	deleteList(alloc, select->updateSetList_);
	deleteList(alloc, select->insertList_);
	deleteTree(select->insertSet_);
	deleteList(alloc, select->valueList_);
	deleteList(alloc, select->valueChildList_);
	ALLOC_DELETE(select->alloc_, select->createTableOpt_);
	ALLOC_DELETE(select->alloc_, select->createIndexOpt_);
	deleteList(alloc, select->cmdOptionList_);

	ALLOC_DELETE(alloc, select);
}

void SyntaxTree::deleteTree(Set *set) {
	if (set == NULL) {
		return;
	}

	SQLAllocator &alloc = set->alloc_;

	deleteTree(set->left_);
	deleteTree(set->right_);
	deleteList(alloc, set->unionAllList_);

	ALLOC_DELETE(alloc, set);
}

void SyntaxTree::deleteTree(Expr *expr) {
	if (expr == NULL) {
		return;
	}

	SQLAllocator &alloc = expr->alloc_;

	deleteTree(expr->qName_);
	ALLOC_DELETE(alloc, expr->aliasName_);
	deleteTree(expr->left_);
	deleteTree(expr->right_);
	deleteTree(expr->mid_);
	deleteList(alloc, expr->next_);
	deleteTree(expr->subQuery_);

	ALLOC_DELETE(alloc, expr);
}

template< typename T >
void SyntaxTree::dumpList(std::ostream &os, const T *src, const char* name) {
	if (src) {
		os << ", \"" << name << "\":[";
		typename T::const_iterator itr = src->begin();
		for (; itr != src->end(); ++itr) {
			if (itr != src->begin()) {
				os << ", " << std::endl;
			}
			if (*itr) {
				(*itr)->dump(os);
			}
			else {
				os << "{\"null\":{}}";
			}
		}
		os << "]";
	}
}

SyntaxTree::ColumnInfo::ColumnInfo():
		columnName_(NULL), type_(0),
		option_(0), virtualColumnOption_(0) {
}

SyntaxTree::CreateTableOption::CreateTableOption(SQLAllocator &alloc):
		alloc_(alloc),
		ifNotExists_(false), columnNum_(0), columnInfoList_(NULL),
		tableConstraintList_(NULL),
		partitionType_(TABLE_PARTITION_TYPE_UNDEF),
		partitionColumn_(NULL), subPartitionColumn_(NULL),
		optInterval_(0), optIntervalUnit_(-1), propertyMap_(NULL),
		partitionInfoList_(NULL), partitionInfoNum_(0),
		optionString_(NULL),
		extensionName_(NULL), extensionOptionList_(NULL),
		isVirtual_(false), isTimeSeries_(false) {
}

SyntaxTree::CreateTableOption* SyntaxTree::makeCreateTableOption(
		SQLAllocator &alloc, SyntaxTree::TableColumnList *columnList,
		SyntaxTree::ExprList *tableConstraintList,
		SyntaxTree::PartitioningOption *option,
		SyntaxTree::ExprList *createOptList, int noErr,
		util::String* extensionName, bool isVirtual, bool isTimeSeries) {

	CreateTableOption *dest = ALLOC_NEW(alloc) CreateTableOption(alloc);
	dest->ifNotExists_ = (noErr != 0);

	dest->columnNum_ = static_cast<int32_t>(columnList->size());
	dest->columnInfoList_ = ALLOC_NEW(alloc) ColumnInfoList(alloc);
	dest->columnInfoList_->reserve(columnList->size());
	for (size_t i = 0; i < columnList->size(); i++) {
		ColumnInfo* destInfo = ALLOC_NEW(alloc) ColumnInfo;
		TableColumn &srcInfo = *(*columnList)[i];
		destInfo->columnName_ = srcInfo.qName_;
		destInfo->type_ = srcInfo.type_;
		destInfo->option_ = static_cast<ColumnOption>(srcInfo.option_);
		destInfo->virtualColumnOption_ = srcInfo.virtualColumnOption_;
		dest->columnInfoList_->push_back(destInfo);
	}
	dest->tableConstraintList_ = tableConstraintList;

	if (option == NULL) {
		dest->partitionType_ = TABLE_PARTITION_TYPE_UNDEF;
		dest->partitionColumn_ = NULL;
		dest->partitionInfoNum_ = 0;
		dest->optInterval_ = 0;
		dest->subPartitionColumn_ = NULL;
		dest->optIntervalUnit_ = -1;
	}
	else {
		dest->partitionType_ = option->partitionType_;
		dest->partitionColumn_ = option->partitionColumn_;
		dest->partitionInfoNum_ = option->partitionCount_;
		dest->optInterval_ = option->optInterval_;
		dest->subPartitionColumn_ = option->subPartitionColumn_;
		dest->optIntervalUnit_ = option->optIntervalUnit_;
	}
	dest->partitionInfoList_ = NULL;
	if (createOptList) {
		if (dest->propertyMap_ == NULL) {
			dest->propertyMap_ = ALLOC_NEW(alloc) DDLWithParameterMap(DDLWithParameterMap::key_compare(), alloc);
		}
		std::pair<DDLWithParameterMap::iterator, bool> result;
		ExprList::iterator itr = createOptList->begin();
		for ( ; itr != createOptList->end(); ++itr) {
			DDLWithParameter::Coder withParamCoder;
			DDLWithParameter::Id withParamId;
			if (withParamCoder((*itr)->aliasName_->c_str(), withParamId)) {
				result = dest->propertyMap_->insert(
						std::make_pair(withParamId, (*itr)->value_));
				if (!result.second) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
							"Parameter (" << (*itr)->aliasName_->c_str() <<
							") was specified more than once");
				}
			}
			else {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
						"Parameter (" << (*itr)->aliasName_->c_str() <<
						") is not supported");
			}
		}
	}

	if (extensionName != NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				"Virtual table is not supported");
	}
	dest->extensionName_ = extensionName;
	dest->extensionOptionList_ = NULL;
	dest->isVirtual_ = isVirtual;
	dest->isTimeSeries_ = isTimeSeries;

	return dest;
}

SyntaxTree::TableColumn* SyntaxTree::makeCreateTableColumn(
	SQLAllocator &alloc, SQLToken *name,
	SyntaxTree::ColumnInfo *colInfo) {
	TableColumn *column = ALLOC_NEW(alloc) TableColumn;
	do {
		util::String *colName = NULL;
		bool isQuoted = false;
		tokenToString(alloc, *name, true, colName, isQuoted);
		if (!colName) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
								"Table name must not be empty");
		}
		column->qName_ = ALLOC_NEW(alloc) QualifiedName(alloc);
		column->qName_->name_ = colName;
		column->qName_->nameCaseSensitive_ = isQuoted;

		column->type_ = TupleList::TYPE_NULL;

		if (colInfo) {
			column->option_ = colInfo->option_;
			column->virtualColumnOption_ = colInfo->virtualColumnOption_;
			if ((column->option_ & COLUMN_OPT_VIRTUAL) != 0) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
						"Virtual column is not supported");
			}
			if ((column->option_ & COLUMN_OPT_PRIMARY_KEY) != 0) {
				column->option_ |= COLUMN_OPT_NOT_NULL;
			}
		}
		else {
			column->option_ = 0;
			column->virtualColumnOption_ = NULL;
		}

		return column;
	}
	while (false);

	ALLOC_DELETE(alloc, column);

	return NULL;
}

SyntaxTree::CreateTableOption* SyntaxTree::makeAlterTableAddColumnOption(
		SQLAllocator &alloc, SyntaxTree::TableColumnList *columnList) {

	CreateTableOption *dest = ALLOC_NEW(alloc) CreateTableOption(alloc);
	dest->ifNotExists_ = false;

	dest->columnNum_ = static_cast<int32_t>(columnList->size());
	dest->columnInfoList_ = ALLOC_NEW(alloc) ColumnInfoList(alloc);
	dest->columnInfoList_->reserve(columnList->size());
	for (size_t i = 0; i < columnList->size(); i++) {
		ColumnInfo* destInfo = ALLOC_NEW(alloc) ColumnInfo;
		TableColumn &srcInfo = *(*columnList)[i];
		destInfo->columnName_ = srcInfo.qName_;
		destInfo->type_ = srcInfo.type_;
		destInfo->option_ = static_cast<ColumnOption>(srcInfo.option_);
		destInfo->virtualColumnOption_ = srcInfo.virtualColumnOption_;
		dest->columnInfoList_->push_back(destInfo);
	}
	dest->tableConstraintList_ = NULL;

	dest->partitionType_ = TABLE_PARTITION_TYPE_UNDEF;
	dest->partitionColumn_ = NULL;
	dest->partitionInfoNum_ = 0;
	dest->optInterval_ = 0;
	dest->subPartitionColumn_ = NULL;
	dest->optIntervalUnit_ = -1;

	dest->partitionInfoList_ = NULL;

	dest->extensionName_ = NULL;
	dest->extensionOptionList_ = NULL;
	dest->isVirtual_ = false;
	dest->isTimeSeries_ = false;

	return dest;
}

void SyntaxTree::checkTableConstraint(
		SQLAllocator &alloc, TableColumnList *&columnList,
		ExprList *constList) {
	UNUSED_VARIABLE(alloc);
	UNUSED_VARIABLE(columnList);

	if (constList) {
		ExprList::iterator itr = constList->begin();
		for (; itr != constList->end(); ++itr) {
			Expr* constraint = *itr;
			if (constraint->op_ != SQLType::EXPR_COLUMN
				|| constraint->next_ == NULL) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
						"Invalid constraint");
			}
		}
	}
}

SyntaxTree::CreateIndexOption::CreateIndexOption() :
	columnNameList_(NULL), ifNotExists_(false),
	extensionName_(NULL), extensionOptionList_(NULL), optionParams_(NULL) {
}

SyntaxTree::CreateIndexOption* SyntaxTree::makeCreateIndexOption(
		SQLAllocator &alloc, SyntaxTree::ExprList *columnList,
		int32_t ifNotExist, CreateIndexOption *option) {
	for (size_t i = 0; i < columnList->size(); i++) {
		if ((*columnList)[i] == NULL
				|| (*columnList)[i]->qName_ == NULL
				|| (*columnList)[i]->qName_->name_ == NULL) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
					"Just one column must be specified for index");
		}
	}
	CreateIndexOption *dest = ALLOC_NEW(alloc) CreateIndexOption;
	dest->columnNameList_ = ALLOC_NEW(alloc) QualifiedNameList(alloc);
	for (size_t i = 0; i < columnList->size(); i++) {
		dest->columnNameList_->push_back((*columnList)[i]->qName_);
	}
	dest->ifNotExists_ = (ifNotExist != 0);
	if (option) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				"Syntax error");
	}
	return dest;
}

SyntaxTree::TableColumn* SyntaxTree::makeCreateTableColumn(
	SQLAllocator &alloc, SQLToken *name, TupleList::TupleColumnType type,
	SyntaxTree::ColumnInfo *colInfo) {
	TableColumn *column = ALLOC_NEW(alloc) TableColumn;
	do {
		util::String *colName = NULL;
		bool isQuoted = false;
		tokenToString(alloc, *name, true, colName, isQuoted);
		if (!colName) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
								"Table name must not be empty");
		}
		column->qName_ = ALLOC_NEW(alloc) QualifiedName(alloc);
		column->qName_->name_ = colName;
		column->qName_->nameCaseSensitive_ = isQuoted;
		column->type_ = type;
		if (colInfo) {
			column->option_ = colInfo->option_;
			column->virtualColumnOption_ = colInfo->virtualColumnOption_;
			if ((column->option_ & COLUMN_OPT_VIRTUAL) != 0) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
						"Virtual column is not supported");
			}
			if ((column->option_ & COLUMN_OPT_PRIMARY_KEY) != 0) {
				column->option_ |= COLUMN_OPT_NOT_NULL;
			}
		}
		else {
			column->option_ = 0;
			column->virtualColumnOption_ = NULL;
		}
		return column;
	}
	while (false);

	ALLOC_DELETE(alloc, column);

	return NULL;
}

SyntaxTree::ExprList* SyntaxTree::makeRangeGroupOption(
		SQLAllocator &alloc, SyntaxTree::Expr *baseKey,
		SyntaxTree::Expr *fillExpr, int64_t interval, int64_t intervalUnit,
		int64_t offset, int64_t timeZone, bool withTimeZone) {

	SyntaxTree::Expr *key = Expr::makeExpr(alloc, SQLType::EXPR_RANGE_GROUP);
	{
		key->next_ = ALLOC_NEW(alloc) ExprList(alloc);
		key->next_->push_back(baseKey);
		key->next_->push_back(fillExpr);
		key->next_->push_back(makeConst(alloc, TupleValue(interval)));
		key->next_->push_back(makeConst(alloc, TupleValue(intervalUnit)));
		key->next_->push_back(makeConst(alloc, TupleValue(offset)));
		key->next_->push_back(makeConst(
				alloc, (withTimeZone ? TupleValue(timeZone) : TupleValue())));
	}

	ExprList *option = ALLOC_NEW(alloc) ExprList(alloc);
	option->push_back(key);

	return option;
}

bool SyntaxTree::resolveRangeGroupOffset(
		const SQLToken &token, int64_t &offset) {
	if(!SQLProcessor::ValueUtils::toLong(
			token.value_, token.size_, offset)) {
		return false;
	}
	if (offset < 0) {
		return false;
	}
	return true;
}

bool SyntaxTree::resolveRangeGroupTimeZone(
		SQLAllocator &alloc, const SQLToken &token, int64_t &timeZone) {
	timeZone = 0;

	bool isQuoted = false;
	util::String *tokenStr = NULL;
	tokenToString(alloc, token, true, tokenStr, isQuoted);

	util::TimeZone zone;
	const bool throwOnError = false;
	if (!zone.parse(tokenStr->c_str(), tokenStr->size(), throwOnError)) {
		return false;
	}
	timeZone = zone.getOffsetMillis();
	return true;
}

TupleList::TupleColumnType SyntaxTree::toColumnType(
		SQLAllocator &alloc, const SQLToken &type, const int64_t *num1,
		const int64_t *num2) {
	bool isQuoted = false;
	util::String *typeStr = NULL;
	tokenToString(alloc, type, false, typeStr, isQuoted);

	TupleList::TupleColumnType resolvedType;
	if (typeStr) {
		resolvedType = toColumnType(typeStr->c_str(), num1, num2);
		ALLOC_DELETE(alloc, typeStr);
	}
	else {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				"Column type name must not be empty");
	}
	return resolvedType;
}

TupleList::TupleColumnType SyntaxTree::toColumnType(
		const char8_t *name, const int64_t *num1, const int64_t *num2) {
	if (util::stricmp(name, "NULL") == 0) {
		return TupleList::TYPE_NULL;
	}
	else if (util::stricmp(name, "BYTE") == 0) {
		return TupleList::TYPE_BYTE;
	}
	else if (util::stricmp(name, "SHORT") == 0) {
		return TupleList::TYPE_SHORT;
	}
	else if (util::stricmp(name, "INTEGER") == 0) {
		return TupleList::TYPE_INTEGER;
	}
	else if (util::stricmp(name, "LONG") == 0) {
		return TupleList::TYPE_LONG;
	}
	else if (util::stricmp(name, "FLOAT") == 0) {
		return TupleList::TYPE_FLOAT;
	}
	else if (util::stricmp(name, "DOUBLE") == 0) {
		return TupleList::TYPE_DOUBLE;
	}
	else if (util::stricmp(name, "TIMESTAMP") == 0) {
		if (num2 != NULL) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
					"Too many precision arguments for TIMESTAMP column type");
		}
		if (num1 != NULL) {
			switch (*num1) {
			case 3:
				break;
			case 6:
				return TupleList::TYPE_MICRO_TIMESTAMP;
			case 9:
				return TupleList::TYPE_NANO_TIMESTAMP;
			default:
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
						"Unsupported precision number for TIMESTAMP column type ("
						"number=" << *num1 << ")");
			}
		}
		return TupleList::TYPE_TIMESTAMP;
	}
	else if (util::stricmp(name, "BOOL") == 0) {
		return TupleList::TYPE_BOOL;
	}
	else if (util::stricmp(name, "NUMERIC") == 0) {
		return TupleList::TYPE_NUMERIC;
	}
	else if (util::stricmp(name, "STRING") == 0) {
		return TupleList::TYPE_STRING;
	}
	else if (util::stricmp(name, "BLOB") == 0) {
		return TupleList::TYPE_BLOB;
	}
	else if (util::stricmp(name, "REAL") == 0) {
		return TupleList::TYPE_DOUBLE;
	}
	else if (util::stricmp(name, "TINYINT") == 0) {
		return TupleList::TYPE_BYTE;
	}
	else if (util::stricmp(name, "SMALLINT") == 0) {
		return TupleList::TYPE_SHORT;
	}
	else if (util::stricmp(name, "BIGINT") == 0) {
		return TupleList::TYPE_LONG;
	}

	TupleList::TupleColumnType aff = determineType(name);
	if (aff == TupleList::TYPE_NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				"Unknown type (name=" << name << ")");
	}
	return aff;
}


/*
   カラムタイプ文字列をスキャンして以下のルールにより型を決定する
   - 下の表の部分文字列にマッチした場合に対応する型とする
   - 複数回マッチする場合は、上の物が優先される(例えば、BLOBINTならTYPE_INTEGER)

   Substring     | TupleList::ColumnType
   --------------------------------
   'INT'         | TYPE_INTEGER
   'CHAR'        | TYPE_STRING
   'CLOB'        | TYPE_STRING
   'TEXT'        | TYPE_STRING
   'BLOB'        | TYPE_BLOB
   'REAL'        | TYPE_DOUBLE
   'DOUB'        | TYPE_DOUBLE
   'FLOA'        | TYPE_FLOAT
**
** If none of the substrings in the above table are found,
** GS_AFF_NUMERIC is returned.
*/
TupleList::TupleColumnType SyntaxTree::determineType(const char *zIn) {
	uint32_t h = 0;
	TupleList::TupleColumnType aff = TupleList::TYPE_NULL;

	if (zIn == 0) return aff;
	while (zIn[0]) {
		h = (h << 8) + SQLLexer::gsUpperToLower[(*zIn) & 0xff];
		zIn++;
		if (h == (('c'<<24)+('h'<<16)+('a'<<8)+'r')) { /* CHAR */
			aff = TupleList::TYPE_STRING;
		}
		else if (h == (('c'<<24)+('l'<<16)+('o'<<8)+'b')) { /* CLOB */
			aff = TupleList::TYPE_STRING;
		}
		else if (h == (('t'<<24)+('e'<<16)+('x'<<8)+'t')) { /* TEXT */
			aff = TupleList::TYPE_STRING;
		}
		else if (h == (('b'<<24)+('l'<<16)+('o'<<8)+'b') /* BLOB */
				 && (aff == TupleList::TYPE_NULL
						 || aff == TupleList::TYPE_FLOAT
						 || aff == TupleList::TYPE_DOUBLE)) {
			aff = TupleList::TYPE_BLOB;
		}
		else if (h == (('r'<<24)+('e'<<16)+('a'<<8)+'l') /* REAL */
				&& (aff == TupleList::TYPE_NULL
						|| aff == TupleList::TYPE_FLOAT)) {
			aff = TupleList::TYPE_DOUBLE;
		}
		else if (h == (('d'<<24)+('o'<<16)+('u'<<8)+'b') /* DOUB */
				&& (aff == TupleList::TYPE_NULL
						|| aff == TupleList::TYPE_FLOAT)) {
			aff = TupleList::TYPE_DOUBLE;
		}
		else if (h == (('f'<<24)+('l'<<16)+('o'<<8)+'a') /* FLOA */
				&& aff == TupleList::TYPE_NULL) {
			aff = TupleList::TYPE_FLOAT;
		}
		else if ((h & 0x00FFFFFF) == (('i'<<16)+('n'<<8)+'t')) { /* INT */
			aff = TupleList::TYPE_INTEGER;
			break;
		}
	}
	return aff;
}

bool SyntaxTree::checkPartitioningIntervalTimeField(
		util::DateTime::FieldType type) {
	switch(type) {
	case util::DateTime::FIELD_DAY_OF_MONTH:
	case util::DateTime::FIELD_HOUR:
		return true;
	default:
		return false;
	}
}

bool SyntaxTree::checkGroupIntervalTimeField(util::DateTime::FieldType type) {
	switch(type) {
	case util::DateTime::FIELD_DAY_OF_MONTH:
	case util::DateTime::FIELD_HOUR:
	case util::DateTime::FIELD_MINUTE:
	case util::DateTime::FIELD_SECOND:
	case util::DateTime::FIELD_MILLISECOND:
	case util::DateTime::FIELD_MICROSECOND:
	case util::DateTime::FIELD_NANOSECOND:
		return true;
	default:
		return false;
	}
}

void SyntaxTree::countLineAndColumn(
		const char8_t *srcSqlStr, const Expr* expr, size_t &line,
		size_t &column) {
	line = 0;
	column = 0;
	if (srcSqlStr != NULL && expr != NULL && expr->startToken_.value_[0]) {
		countLineAndColumnFromToken(srcSqlStr, expr->startToken_, line, column);
	}
}

void SyntaxTree::countLineAndColumnFromToken(
		const char8_t *srcSqlStr, const SQLToken &token, size_t &line,
		size_t &column) {
	line = 1;
	column = 0;

	if (isEmptyToken(token)) {
		return;
	}

	for (const char* cur = srcSqlStr; cur < token.value_; ++cur) {
		if (*cur == '\n') {
			++line;
			column = 0;
		}
		else {
			++column;
		}
	}
}

bool SyntaxTree::isEmptyToken(const SQLToken &token) {
	return (token.value_ == NULL);
}

SQLToken SyntaxTree::makeEmptyToken() {
	SQLToken token;
	token.reset();
	return token;
}

/*!
	@brief SQLTokenからutil::String*へ変換
	@param [in] alloc
	@param [in] token
	@param [in] dequote dequote処理を行う場合true
	@return 結果が0文字TokenならNULLを返す。それ以外ならTokenからutil::String*を生成して返す
*/
void SyntaxTree::tokenToString(
		SQLAllocator &alloc, const SQLToken &token, bool dequote,
		util::String* &out, bool &isQuoted) {
	out = NULL;
	isQuoted = false;
	util::String* str = NULL;
	if (token.size_ > 0) {
		str = ALLOC_NEW(alloc) util::String(token.value_, token.size_, alloc);
		if (dequote) {
			int32_t result = gsDequote(alloc, *str);
			isQuoted = (result != -1);
		}
		if (str->size() == 0) {
			ALLOC_DELETE(alloc, str);
			out = NULL;
			return;
		}
	}
	out = str;
}

bool SyntaxTree::toSignedValue(
		const SQLToken &token, bool minus, int64_t &value) {
	if (!SQLProcessor::ValueUtils::toLong(token.value_, token.size_, value)) {
		return false;
	}

	if (minus) {
		if (value >= std::numeric_limits<int64_t>::max()) {
			return false;
		}
		value = -value;
	}

	return true;
}

/*!
	@brief SQLTokenからutil::String*へ変換(簡易版: 引用符付かどうかの情報が不要な場合)
	@param [in] alloc
	@param [in] token
	@return 結果が0文字TokenならNULLを返す。それ以外ならTokenからutil::String*を生成して返す
*/
util::String* SyntaxTree::tokenToString(
		SQLAllocator &alloc, const SQLToken &token, bool dequote) {

	util::String *out = NULL;
	bool isQuoted = false;
	tokenToString(alloc, token, dequote, out, isQuoted);
	return out;
}


/*
	SQLスタイルのクォートされた文字列からクォート文字を除去する
	変換が必要な場合は、新たなutil::Stringを作ってそれを返す。
	文字列がクォート文字から始まらない場合、何もしない

	戻り値: クォート除去処理が行われなかった場合-1, それ以外は
	クォート除去後の(終端文字を含まない)文字列長
*/
int32_t SyntaxTree::gsDequote(SQLAllocator &alloc, util::String &str) {
	char quote;
	int32_t i, j;
	if (str.size() == 0) {
		return -1;
	}
	const char *src = str.c_str();
	quote = src[0];
	switch (quote) {
	case '\'':
		break;
	case '"':
		break;
	case '`': 
		break;
	case '[': 
		quote = ']';
		break;
	default:
		return -1;
	}
	util::XArray<char> buffer(alloc);
	char temp = 0;
	buffer.assign(str.size(), temp);

	char *dest = buffer.data();
	for (i=1, j=0;; i++) {
		assert(src[i]);
		if (src[i] == quote) {
			if (src[i+1] == quote) {
				dest[j++] = quote;
				i++;
			}
			else {
				break;
			}
		}
		else {
			dest[j++] = src[i];
		}
	}
	dest[j] = 0;
	str.assign(dest, j);

	return j;
}


SQLPlanningVersion::SQLPlanningVersion() :
		major_(-1),
		minor_(-1) {
}

SQLPlanningVersion::SQLPlanningVersion(int32_t major, int32_t minor) :
		major_(major),
		minor_(minor) {
}

bool SQLPlanningVersion::isEmpty() const {
	return major_ < 0;
}

int32_t SQLPlanningVersion::getMajor() const {
	return major_;
}

int32_t SQLPlanningVersion::getMinor() const {
	return minor_;
}

void SQLPlanningVersion::parse(const char8_t *str) {
	const char8_t sep = '.';
	const size_t len = strlen(str);

	const size_t elemCount = 3;
	int32_t elemList[elemCount] = { 0 };
	size_t nextElemPos = 0;
	bool failed = false;

	const char8_t *const end = str + len;
	const char8_t *lastElemStart = str;
	for (const char8_t *it = str;; ++it) {
		if (it != end && *it != sep) {
			continue;
		}

		int64_t elem;
		if (nextElemPos >= elemCount ||
				!SQLProcessor::ValueUtils::toLong(
						lastElemStart, (it - lastElemStart), elem) ||
				elem < 0 || elem > std::numeric_limits<int32_t>::max()) {
			failed = true;
			break;
		}

		elemList[nextElemPos] = static_cast<int32_t>(elem);
		if (it == end) {
			break;
		}
		lastElemStart = it + 1;
		nextElemPos++;
	}

	if (failed) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				"Failed to parse planning version (value=" << str << ")");
	}

	major_ = elemList[0];
	minor_ = elemList[1];
}

int32_t SQLPlanningVersion::compareTo(int32_t major, int32_t minor) const {
	return compareTo(SQLPlanningVersion(major, minor));
}

int32_t SQLPlanningVersion::compareTo(const SQLPlanningVersion &another) const {
	if (isEmpty() || another.isEmpty()) {
		return (isEmpty() ? 1 : 0) - (another.isEmpty() ? 1 : 0);
	}

	if (major_ != another.major_) {
		return (major_ - another.major_);
	}

	return (minor_ - another.minor_);
}

GenSyntaxTree::GenSyntaxTree(SQLAllocator &alloc)
: alloc_(alloc), parser_(NULL), parserCxt_(alloc),
  tokenCountLimit_(SyntaxTree::SQL_TOKEN_COUNT_LIMIT)
{
	try {
		parser_ = ALLOC_NEW(alloc_) lemon_SQLParser::SQLParser();
	}
	catch (std::exception &) {
		ALLOC_DELETE(alloc_, parser_);
		parser_ = NULL;
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
							"Create SQLParser failed.");
	}
}

GenSyntaxTree::~GenSyntaxTree() {
	ALLOC_DELETE(alloc_, parser_);
}

void GenSyntaxTree::parse(const char* sql_str, SyntaxTree::Select* &treeTop, SQLToken &last) {
	treeTop = NULL;
	parserCxt_.reset();
	if (sql_str) {
		int32_t ret;
		int64_t tokenCount = 0;
		bool inHint = false; 
		for (;;) {
			ret = SQLLexer::gsGetToken(sql_str, last, inHint);
			if (ret <= 0) {
				last.size_ = 0;
				break;
			}
			if (last.id_ == TK_ILLEGAL) {
				break;
			}
			if (last.id_ != TK_SPACE) {
				parser_->Execute(last.id_, last, &parserCxt_);
				if (parserCxt_.isSetError_) {
					GS_THROW_USER_ERROR(
						GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, parserCxt_.errorMsg_->c_str());
				}
				if (SQLParserContext::PARSESTATE_END == parserCxt_.parseState_) {
					break;
				}
				++tokenCount;
				if (tokenCount > tokenCountLimit_) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
						"Too many tokens (>" << tokenCountLimit_ << ")");
				}
			}
			sql_str = last.value_ + last.size_;
		}
		parser_->Execute(0, last, &parserCxt_);
		if (last.size_ > 0) {
			if (parserCxt_.isSetError_
				|| (parserCxt_.topSelect_ == NULL
					&& parserCxt_.pragmaType_ == SQLPragma::PRAGMA_NONE)) {
				std::string str2 = "Syntax error: ";
				str2 += sql_str;
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
									str2.c_str());
			}
			treeTop = parserCxt_.topSelect_;
		}
		else {
		}
		return;
	}
	else {
		GS_THROW_USER_ERROR(
			GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, "Syntax error: Query was empty");
	}
}


void GenSyntaxTree::parseAll(
		SQLExpandViewContext *expandViewCxt, SQLExecution *execution,
		util::String& sqlCommandList, uint32_t viewDepth,
		int64_t viewNsId, int64_t maxViewNsId, bool expandView) {
	SQLConnectionControl *control = expandViewCxt->control_;
	assert(control);
	SQLParsedInfo &parsedInfo = *expandViewCxt->parsedInfo_;
	std::string value;
	if (control->getEnv(SQLPragma::PRAGMA_INTERNAL_PARSER_TOKEN_COUNT_LIMIT, value)) {
		int64_t intVal = 0;
		bool ret = SQLProcessor::ValueUtils::toLong(value.c_str(), value.size(), intVal);
		if (ret && intVal > 20) {
			tokenCountLimit_ = intVal;
		}
		else {
			GS_THROW_USER_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				"Invalid pragma value: internal.parser.token_count_limit="
				<< value.c_str());
		}
	}
	int64_t exprTreeDepthLimit = SyntaxTree::SQL_EXPR_TREE_DEPTH_LIMIT;
	if (control->getEnv(SQLPragma::PRAGMA_INTERNAL_PARSER_EXPR_TREE_DEPTH_LIMIT, value)) {
		int64_t intVal = 0;
		bool ret = SQLProcessor::ValueUtils::toLong(value.c_str(), value.size(), intVal);
		if (ret && intVal > 100) {
			exprTreeDepthLimit = intVal;
		}
		else {
			GS_THROW_USER_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				"Invalid pragma value: internal.parser.expr_tree_depth_limit="
				<< value.c_str());
		}
	}
	size_t commandLen = sqlCommandList.size();
	size_t semiPos = sqlCommandList.find_last_of(";");
	if (semiPos == std::string::npos ||
		((semiPos + 1) != commandLen &&
		sqlCommandList.find_last_not_of(" ", semiPos + 1) != std::string::npos)) {
		sqlCommandList.append("\n;"); 
	}
	util::String sql(alloc_);
	sql = sqlCommandList;

	const char *sql_str = sql.c_str();

	SQLToken lastToken;
	lastToken.reset();
	SyntaxTree::Select* treeTop = NULL;

	parserCxt_.reset();
	parserCxt_.inputSql_ = sql_str;
	parserCxt_.placeHolderCount_ = 0; 
	parserCxt_.exprTreeDepthLimit_ = exprTreeDepthLimit;
	parserCxt_.expandViewCxt_ = expandViewCxt;
	parserCxt_.sqlExecution_ = execution;
	parserCxt_.viewDepth_ = viewDepth;
	parserCxt_.expandView_ = expandView;
	parserCxt_.viewNsId_ = viewNsId;
	parserCxt_.maxViewNsId_ = maxViewNsId;
	parserCxt_.viewExpandError_ = NULL;
	if (viewDepth == 0) {
		parserCxt_.viewExpandError_ = NULL;
	}
	parsedInfo.pragmaType_ = SQLPragma::PRAGMA_NONE;
	parsedInfo.pragmaValue_ = NULL;
	int32_t totalCommandCount = 0;
	int32_t validCommandCount = 0;
	for (;;) {
		try {
			treeTop = NULL;
			parse(sql_str, treeTop, lastToken);
			++totalCommandCount;

			if (parserCxt_.pragmaType_ != SQLPragma::PRAGMA_NONE) {
				parsedInfo.pragmaType_ = parserCxt_.pragmaType_;
				assert(parserCxt_.pragmaValue_);
				parsedInfo.pragmaValue_ = parserCxt_.pragmaValue_;
				std::string pragmaValue(parserCxt_.pragmaValue_->c_str());
				control->setEnv(parserCxt_.pragmaType_, pragmaValue);
				++validCommandCount;
			}
			else if (treeTop) {
				parsedInfo.syntaxTreeList_.push_back(treeTop);
				parsedInfo.tableList_.reserve(
						parsedInfo.tableList_.size()
						+ parserCxt_.tableList_.size());
				parsedInfo.tableList_.insert(
						parsedInfo.tableList_.end(),
						parserCxt_.tableList_.begin(),
						parserCxt_.tableList_.end());

				parsedInfo.placeHolderCount_ = parserCxt_.placeHolderCount_;
				parsedInfo.explainType_ = parserCxt_.explainType_;
				parsedInfo.inputSql_ = parserCxt_.inputSql_;

				++validCommandCount;
				parsedInfo.createForceView_ = parserCxt_.createForceView_;
				if ((!parserCxt_.createForceView_) &&
						parserCxt_.viewDepth_ == 0 &&
						parserCxt_.viewExpandError_) {
					if (parserCxt_.savedException_.isEmpty()) {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
								"NoSQL get container failed: " <<
								parserCxt_.viewExpandError_->c_str());
					}
					else {
						try {
							throw parserCxt_.savedException_;
						}
						catch(util::Exception &e) {
							GS_RETHROW_USER_ERROR(e, "");
						}
						catch(...) {
							GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
						}
					}
				}
			}
			else {
			}
			if ((lastToken.pos_ + lastToken.size_) == commandLen) {
				break;
			}

			if (lastToken.size_ > 0) {
			}
			else {
			}
		}
		catch (std::exception &e) {
			GS_RETHROW_USER_OR_SYSTEM(e, "Parse SQL failed, reason = "
					<< GS_EXCEPTION_MESSAGE(e));
			break;
		}
		if (lastToken.size_ > 0) {
			sql_str = lastToken.value_ + lastToken.size_;
		}
		else {
			lastToken.pos_ = 0;
			break;
		}
		if (!*sql_str) {
			break;
		}
	}
	if (validCommandCount == 0) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				"Syntax error: Query was empty");
	}
	else if (validCommandCount > 1) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_UNSUPPORTED,
				"Multiple SQL statement is not supported");
	}
}


SQLParserContext::SQLParserContext(SQLAllocator &alloc)
: alloc_(alloc), childrenList_(alloc), tableList_(alloc),
  topSelect_(NULL), currentSelect_(NULL), hintSelect_(NULL),
  parseState_(PARSESTATE_SELECTION),
  placeHolderCount_(0), explainType_(SyntaxTree::EXPLAIN_NONE),
  exprTreeDepthLimit_(SyntaxTree::SQL_EXPR_TREE_DEPTH_LIMIT),
  inputSql_(NULL),
  pragmaType_(SQLPragma::PRAGMA_NONE), pragmaValue_(NULL),
  errorMsg_(NULL), isSetError_(false) 
  , expandViewCxt_(NULL), sqlExecution_(NULL)
  , expandView_(false), createForceView_(false)
  , viewDepth_(0), viewNsId_(0), maxViewNsId_(0)
  , viewExpandError_(NULL)
{
	cxt_.setStackAllocator(&alloc);
}

SQLParserContext::~SQLParserContext() {
}

void SQLParserContext::dumpSyntaxTree(std::ostream &os) {
	if (topSelect_) {
		topSelect_->dump(os);
	}
	else {
		os << "(empty)" << std::endl;
	}
}

void SQLParserContext::setError(const char* errMsg) {
	ALLOC_DELETE(alloc_, topSelect_);
	topSelect_ = NULL;
	errorMsg_ = ALLOC_NEW(alloc_) util::String(errMsg, alloc_);
	isSetError_ = true;
}

void SQLParserContext::setPragma(
	SQLToken &key1, SQLToken &key2, SQLToken &key3, SQLToken &value, int32_t valueType) {
	do {
		if (key1.size_ == 0 || key2.size_ == 0 || key3.size_ == 0 || value.size_ == 0) {
			break;
		}
		try {
			util::String *keyStr = NULL;
			bool isQuoted = false;
			SyntaxTree::tokenToString(alloc_, key1, false, keyStr, isQuoted);
			keyStr->reserve(keyStr->size() + key2.size_ + key3.size_ + 2);
			keyStr->append(".");
			keyStr->append(key2.value_, key2.size_);
			keyStr->append(".");
			keyStr->append(key3.value_, key3.size_);

			util::String *valueStr = NULL;
			SyntaxTree::tokenToString(alloc_, value, false, valueStr, isQuoted);
			if (valueStr == NULL) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
						"Illegal value");
			}
			if (valueType == 2) {
				valueStr->insert(0, "-");
			}

			bool asBool = false;
			if (SQLProcessor::ValueUtils::strICmp(
					"internal.compiler.execute_as_meta_data_query",
					keyStr->c_str()) == 0) {
				pragmaType_ = SQLPragma::PRAGMA_INTERNAL_COMPILER_EXECUTE_AS_META_DATA_QUERY;
				asBool = true;
			}
			else if (SQLProcessor::ValueUtils::strICmp(
					"internal.compiler.scan_index",
					keyStr->c_str()) == 0) {
				pragmaType_ = SQLPragma::PRAGMA_INTERNAL_COMPILER_SCAN_INDEX;
				asBool = true;
			}
			else if (SQLProcessor::ValueUtils::strICmp(
					"internal.compiler.meta_table_visible",
					keyStr->c_str()) == 0) {
				pragmaType_ = SQLPragma::PRAGMA_INTERNAL_COMPILER_META_TABLE_VISIBLE;
				asBool = true;
			}
			else if (SQLProcessor::ValueUtils::strICmp(
					"internal.compiler.internal_meta_table_visible",
					keyStr->c_str()) == 0) {
				pragmaType_ = SQLPragma::PRAGMA_INTERNAL_COMPILER_INTERNAL_META_TABLE_VISIBLE;
				asBool = true;
			}
			else if (SQLProcessor::ValueUtils::strICmp(
					"internal.compiler.driver_meta_table_visible",
					keyStr->c_str()) == 0) {
				pragmaType_ = SQLPragma::PRAGMA_INTERNAL_COMPILER_DRIVER_META_TABLE_VISIBLE;
				asBool = true;
			}
			else if (SQLProcessor::ValueUtils::strICmp(
					"internal.compiler.substr_compatible",
					keyStr->c_str()) == 0) {
				pragmaType_ = SQLPragma::PRAGMA_INTERNAL_COMPILER_SUBSTR_COMPATIBLE;
				asBool = true;
			}
			else if (SQLProcessor::ValueUtils::strICmp(
					"internal.compiler.variance_compatible",
					keyStr->c_str()) == 0) {
				pragmaType_ = SQLPragma::PRAGMA_INTERNAL_COMPILER_VARIANCE_COMPATIBLE;
				asBool = true;
			}
			else if (SQLProcessor::ValueUtils::strICmp(
					"internal.compiler.experimental_functions",
					keyStr->c_str()) == 0) {
				pragmaType_ = SQLPragma::PRAGMA_INTERNAL_COMPILER_EXPERIMENTAL_FUNCTIONS;
				asBool = true;
			}
			else if (SQLProcessor::ValueUtils::strICmp(
					"internal.parser.token_count_limit",
					keyStr->c_str()) == 0) {
				pragmaType_ = SQLPragma::PRAGMA_INTERNAL_PARSER_TOKEN_COUNT_LIMIT;
			}
			else if (SQLProcessor::ValueUtils::strICmp(
					"internal.parser.expr_tree_depth_limit",
					keyStr->c_str()) == 0) {
				pragmaType_ = SQLPragma::PRAGMA_INTERNAL_PARSER_EXPR_TREE_DEPTH_LIMIT;
			}
			else if (SQLProcessor::ValueUtils::strICmp(
					"plan.distributed.strategy",
					keyStr->c_str()) == 0) {
				pragmaType_ = SQLPragma::PRAGMA_PLAN_DISTRIBUTED_STRATEGY;
			}
			else if (SQLProcessor::ValueUtils::strICmp(
					"experimental.show.system",
					keyStr->c_str()) == 0) {
				pragmaType_ = SQLPragma::PRAGMA_EXPERIMENTAL_SHOW_SYSTEM;
			}
			else if (SQLProcessor::ValueUtils::strICmp(
					"experimental.scan.multi_index",
					keyStr->c_str()) == 0) {
				pragmaType_ = SQLPragma::PRAGMA_EXPERIMENTAL_SCAN_MULTI_INDEX;
				asBool = true;
			}
			else if (SQLProcessor::ValueUtils::strICmp(
					"internal.resultset.timeout",
					keyStr->c_str()) == 0) {
				pragmaType_ = SQLPragma::PRAGMA_RESULTSET_TIMEOUT_INTERVAL;
			}
			else {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
						"Unknown pragma key: " << keyStr->c_str());
			}

			if (asBool) {
				bool boolVal = false;
				if (valueType == 0) {
					util::XArray<char> valueArray(alloc_);
					const size_t valueSize = valueStr->size();
					valueArray.assign(valueStr->c_str(),
							valueStr->c_str() + valueSize + 1);
					SQLProcessor::ValueUtils::toLower(valueArray.data(), valueSize);
					SyntaxTree::ValueStrToBoolMap::const_iterator itr =
							SyntaxTree::VALUESTR_TO_BOOL_MAP.find(valueArray.data());
					if (itr != SyntaxTree::VALUESTR_TO_BOOL_MAP.end()) {
						boolVal = itr->second;
					}
					else {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
								"Illegal value: " << valueStr->c_str());
					}
				}
				else {
					int64_t intVal = 0;
					bool ret = SQLProcessor::ValueUtils::toLong(valueStr->c_str(), valueStr->size(), intVal);
					if (ret) {
						boolVal = (intVal != 0);
					}
					else {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
								"Illegal value: " << valueStr->c_str());
					}
				}
				if (boolVal) {
					pragmaValue_ = ALLOC_NEW(alloc_) util::String(SQLPragma::VALUE_TRUE, alloc_);
				}
				else {
					pragmaValue_ = ALLOC_NEW(alloc_) util::String(SQLPragma::VALUE_FALSE, alloc_);
				}
			}
			else {
				pragmaValue_ = valueStr;
			}
		}
		catch (std::exception &e) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
					"Pragma set failed, reason = "
					<< GS_EXCEPTION_MESSAGE(e));
		}
		return;
	}
	while (false);

}


void SQLParserContext::reset() {
	ALLOC_DELETE(alloc_, topSelect_);
	cxt_.setStackAllocator(&alloc_);
	childrenList_.clear();
	tableList_.clear();
	topSelect_ = NULL;
	currentSelect_ = NULL; 
	parseState_ = PARSESTATE_SELECTION;
	explainType_ = SyntaxTree::EXPLAIN_NONE;
	exprTreeDepthLimit_ = SyntaxTree::SQL_EXPR_TREE_DEPTH_LIMIT;
	pragmaType_ = SQLPragma::PRAGMA_NONE;
	pragmaValue_ = NULL;
	errorMsg_ = NULL;
	isSetError_ = false;
}

void SQLParserContext::beginParse(SyntaxTree::ExplainType explainType) {
	explainType_ = explainType;
}

void SQLParserContext::finishParse() {
	parseState_ = PARSESTATE_END;
}


void SQLParserContext::beginTransaction(int32_t type) {
	static_cast<void>(type); 
	assert(!topSelect_);
	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(alloc_, SyntaxTree::CMD_BEGIN);
	setTopSelect(select);
}


void SQLParserContext::commitTransaction() {
	assert(!topSelect_);
	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(alloc_, SyntaxTree::CMD_COMMIT);
	setTopSelect(select);
}


void SQLParserContext::rollbackTransaction() {
	assert(!topSelect_);
	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(alloc_, SyntaxTree::CMD_ROLLBACK);
	setTopSelect(select);
}

const char* SQLParserContext::getViewSelect(SyntaxTree::Expr &expr) {
	assert(sqlExecution_);
	const SQLTableInfo* sqlTableInfo = sqlExecution_->getTableInfo(
			*expandViewCxt_, &expr, viewDepth_, true); 
	if (sqlTableInfo && sqlTableInfo->sqlString_.size() > 0) {
		return sqlTableInfo->sqlString_.c_str();
	}
	return NULL;
}

void SQLParserContext::checkViewCircularReference(SyntaxTree::Expr* expr) {

	util::StackAllocator &alloc = expandViewCxt_->getAllocator();
	util::Set<util::String> &viewSet = *expandViewCxt_->viewSet_;
	std::pair<util::Set<util::String>::iterator, bool> outResult;
	if (viewDepth_ == 0) {
		viewSet.clear();
		return;
	}
	if (!expr) {
		return;
	}
	const SyntaxTree::QualifiedName *srcName = expr->qName_;
	if (!srcName) {
		return;
	}
	const util::String *tableName = srcName->table_;
	if (!tableName) {
		return;
	}
	if (srcName->tableCaseSensitive_) {
		const util::String &normalizeTableName =
				normalizeName(alloc, tableName->c_str());
		outResult = viewSet.insert(normalizeTableName);
	}
	else {
		util::String currentTableNameStr(tableName->c_str(), alloc);
		outResult = viewSet.insert(currentTableNameStr);
	}
	if (!outResult.second) {
		util::NormalOStringStream ss;
		util::Set<util::String>::iterator itr = viewSet.begin();
		for (; itr != viewSet.end(); ++itr) {
			if (itr != viewSet.begin()) {
				ss << ", ";
			}
			ss << itr->c_str();
		}
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				"Circular reference detected: related views=(" <<
				ss.str().c_str() << ")");
	}
}

bool SQLParserContext::parseViewSelect(
		const char *viewSelectStr, SQLParsedInfo& viewParsedInfo) {
	assert(sqlExecution_);

	++viewDepth_;
	if (viewDepth_ > SyntaxTree::SQL_VIEW_NESTING_LEVEL_LIMIT) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				"Maximum view nesting level exceeded (maximum depth " <<
				SyntaxTree::SQL_VIEW_NESTING_LEVEL_LIMIT << ")");
	}
	util::String viewSelect(alloc_);
	viewSelect = viewSelectStr;
	bool result = sqlExecution_->parseViewSelect(
			*expandViewCxt_, viewParsedInfo, viewDepth_, viewNsId_, maxViewNsId_,
			viewSelect);
	--viewDepth_;
	return result;
}

int64_t SQLParserContext::allocateNewNsId() {
	++maxViewNsId_;
	return maxViewNsId_;
}

int64_t SQLParserContext::checkAndGetViewSelectStr(SyntaxTree::Expr* &table) {
	const char *viewSelectStr = NULL;
	expandView_ = true;
	int64_t currentNsId = viewNsId_;
	int64_t newNsId = 0;
	try {
		viewSelectStr = getViewSelect(*table);
	}
	catch (util::Exception &e) {
		viewExpandError_ = ALLOC_NEW(alloc_) util::String("", alloc_);
		savedException_ = e;
	}
	catch (std::exception &e) {
		viewExpandError_ =
				ALLOC_NEW(alloc_) util::String(e.what(), alloc_);
	}
	if (viewSelectStr) {
		viewNsId_ = allocateNewNsId();
		newNsId = viewNsId_;
		checkViewCircularReference(table);
		SQLParsedInfo viewParsedInfo(alloc_);
		parseViewSelect(viewSelectStr, viewParsedInfo);
		if (viewParsedInfo.syntaxTreeList_.size() > 0) {
			SyntaxTree::Expr *viewSelectExpr = SyntaxTree::Expr::makeExpr(
					alloc_, SQLType::EXPR_SELECTION);
			viewSelectExpr->subQuery_ = SyntaxTree::Set::makeSet(
					alloc_, SyntaxTree::SET_OP_NONE,
					viewParsedInfo.syntaxTreeList_[0]);
			table = viewSelectExpr;
		}
		viewNsId_ = currentNsId;
		return newNsId;
	}
	return 0;
}


SyntaxTree::WindowOption::WindowOption(SQLAllocator &alloc):
		alloc_(alloc),
		partitionByList_(NULL),
		orderByList_(NULL),
		frameOption_(NULL)
{
}

SyntaxTree::WindowOption::WindowOption(const WindowOption &another) :
		alloc_(another.alloc_), 
		partitionByList_(NULL),
		orderByList_(NULL),
		frameOption_(NULL)
{
	*this = another;
}

SyntaxTree::WindowOption& SyntaxTree::WindowOption::operator=(const WindowOption &another) {
	partitionByList_ = another.partitionByList_;
	orderByList_ = another.orderByList_;
	frameOption_ = another.frameOption_;

	return *this;
}


void SyntaxTree::WindowOption::dump(std::ostream &os) {
	os << "{\"WindowOption\":{";
	if (partitionByList_) {
		os << ", \"partitionByList\":[";
		ExprList::iterator itr = partitionByList_->begin();
		for (; itr != partitionByList_->end(); ++itr) {
			if (itr != partitionByList_->begin()) {
				os << ", ";
			}
			if (*itr) {
				(*itr)->dump(os);
			}
			else {
				os << "{\"Expr\":{}}";
			}
		}
		os << "]";
	}
	if (orderByList_) {
		os << ", \"orderByList\":[";
		ExprList::iterator itr = orderByList_->begin();
		for (; itr != orderByList_->end(); ++itr) {
			if (itr != orderByList_->begin()) {
				os << ", ";
			}
			if (*itr) {
				(*itr)->dump(os);
			}
			else {
				os << "{\"Expr\":{}}";
			}
		}
		os << "]";
	}
	if (frameOption_) {
		frameOption_->dump(os);
	}
	os << "}}";
}

SyntaxTree::WindowFrameOption::WindowFrameOption() :
		frameMode_(FRAME_MODE_NONE),
		frameStartBoundary_(NULL),
		frameFinishBoundary_(NULL) {
}

void SyntaxTree::WindowFrameOption::dump(std::ostream &os) {
	os << "{\"WindowFrameOption\":{";
	os << ", \"frameMode:" << static_cast<int32_t>(frameMode_);
	if (frameStartBoundary_) {
		os << ", \"frameStart\":{";
		frameStartBoundary_->dump(os);
		os << "}";
	}
	if (frameFinishBoundary_) {
		os << ", \"frameFinish\":{";
		frameFinishBoundary_->dump(os);
		os << "}";
	}
	os << "}}";
}

SyntaxTree::WindowFrameBoundary::WindowFrameBoundary(SQLAllocator &alloc):
		alloc_(alloc),
		boundaryType_(BOUNDARY_NONE),
		boundaryValueExpr_(NULL),
		boundaryLongValue_(-1),
		boundaryTimeUnit_(-1)
{
}

SyntaxTree::WindowFrameBoundary::WindowFrameBoundary(const WindowFrameBoundary &another) :
		alloc_(another.alloc_), 
		boundaryType_(BOUNDARY_NONE),
		boundaryValueExpr_(NULL),
		boundaryLongValue_(-1),
		boundaryTimeUnit_(-1)
{
	*this = another;
}

SyntaxTree::WindowFrameBoundary& SyntaxTree::WindowFrameBoundary::operator=(const WindowFrameBoundary &another) {
	boundaryType_ = another.boundaryType_;
	boundaryValueExpr_ = another.boundaryValueExpr_;
	boundaryLongValue_ = another.boundaryLongValue_;
	boundaryTimeUnit_ = another.boundaryTimeUnit_;

	return *this;
}

void SyntaxTree::WindowFrameBoundary::dump(std::ostream &os) {
	os << "{\"WindowFrameBoundary\":{";
	os << "\"frameBoundaryType:" << static_cast<int32_t>(boundaryType_);
	os << ", \"boundaryValueExpr\":{";
	if (boundaryValueExpr_) {
		if (boundaryValueExpr_) {
			boundaryValueExpr_->dump(os);
		}
	}
	os << "}";
	os << ", \"boundaryLongValue\":" << boundaryLongValue_;
	os << ", \"boundaryTimeUnit\":" << boundaryTimeUnit_;
}


struct SQLIndexStatsCache::Body {
	struct Entry;
	struct RequesterKey;

	struct ValueHolder;
	struct KeyHolder;

	typedef util::AllocMap<Key, Entry> KeyMap;
	typedef KeyMap::iterator KeyIterator;

	typedef util::AllocMap<RequesterKey, KeyIterator> RequesterMap;

	typedef util::StdAllocator<KeyIterator, void> KeyRefAlloc;
	typedef std::list<KeyIterator, KeyRefAlloc> KeyRefList;

	typedef KeyRefList::iterator KeyRefIterator;

	explicit Body(
			SQLVariableSizeGlobalAllocator &globalVarAlloc,
			const Option &option);
	~Body();

	static Body* create(
			SQLVariableSizeGlobalAllocator &globalVarAlloc,
			const Option &option);
	static void reset(Body *&body);

	bool findSub(const Key &key, Value &value);
	void putSub(const Key &key, uint64_t requester, const Value &value);

	bool checkKeyResolvedSub(uint64_t requester);
	PartitionId getMissedKeysSub(
			util::StackAllocator &alloc, uint64_t requester,
			PartitionId startPartitionId, KeyList &keyList);

	uint64_t addRequesterSub();
	void removeRequesterSub(uint64_t requester);

	void adjustFreeList();

	void clear();
	void eraseKeyDirect(const KeyIterator &it);

	static int32_t compareValue(const TupleValue &v1, const TupleValue &v2);
	static int32_t compareBool(bool v1, bool v2);

	template<typename T>
	static int32_t compareIntegral(const T &v1, const T &v2);

	SQLVariableSizeGlobalAllocator &globalVarAlloc_;
	SQLVarSizeAllocator varAlloc_;
	util::Mutex lock_;
	Option option_;

	util::Atomic<uint64_t> nextRequester_;

	KeyMap keyMap_;
	RequesterMap requesterMap_;
	KeyRefList freeList_;
};

struct SQLIndexStatsCache::Body::Entry {
	explicit Entry(const KeyRefIterator &freeIt);

	Value value_;
	uint64_t requesterCount_;
	KeyRefIterator freeIt_;
};

struct SQLIndexStatsCache::Body::RequesterKey {
	RequesterKey(uint64_t requester, PartitionId partitionId, const Key *key);

	bool operator<(const RequesterKey &another) const;
	int32_t compare(const RequesterKey &another) const;

	uint64_t requester_;
	PartitionId partitionId_;
	const Key *key_;
};

struct SQLIndexStatsCache::Body::ValueHolder {
public:
	explicit ValueHolder(SQLVarSizeAllocator &varAlloc);
	~ValueHolder();

	TupleValue& get();

	void attach(const TupleValue &value);
	void detach();

	void duplicate(const TupleValue &value);
	void destroy();

private:
	ValueHolder(const ValueHolder &another);
	ValueHolder& operator=(const ValueHolder &another);

	SQLVarSizeAllocator &varAlloc_;
	TupleValue target_;
};

struct SQLIndexStatsCache::Body::KeyHolder {
public:
	explicit KeyHolder(SQLVarSizeAllocator &varAlloc);
	~KeyHolder();

	Key& get();

	void attach(const Key &key);
	void detach();

	void duplicate(const Key &key);
	void destroy();

	static Key duplicateBy(util::StackAllocator &alloc, const Key &src);

private:
	KeyHolder(const KeyHolder &another);
	KeyHolder& operator=(const KeyHolder &another);

	SQLVarSizeAllocator &varAlloc_;
	util::LocalUniquePtr<Key> target_;
	ValueHolder lowerValueHolder_;
	ValueHolder upperValueHolder_;
};


SQLIndexStatsCache::SQLIndexStatsCache(
		SQLVariableSizeGlobalAllocator &globalVarAlloc,
		const Option &option) :
		body_(Body::create(globalVarAlloc, option)) {
}

SQLIndexStatsCache::~SQLIndexStatsCache() {
	Body::reset(body_);
}

bool SQLIndexStatsCache::find(const Key &key, Value &value) const {
	return body_->findSub(key, value);
}

void SQLIndexStatsCache::put(
		const Key &key, uint64_t requester, const Value &value) {
	body_->putSub(key, requester, value);
}

void SQLIndexStatsCache::putMissed(const Key &key, uint64_t requester) {
	body_->putSub(key, requester, Value());
}

bool SQLIndexStatsCache::checkKeyResolved(uint64_t requester) {
	return body_->checkKeyResolvedSub(requester);
}

PartitionId SQLIndexStatsCache::getMissedKeys(
		util::StackAllocator &alloc, uint64_t requester,
		PartitionId startPartitionId, KeyList &keyList) {
	return body_->getMissedKeysSub(alloc, requester, startPartitionId, keyList);
}

uint64_t SQLIndexStatsCache::addRequester() {
	return body_->addRequesterSub();
}

void SQLIndexStatsCache::removeRequester(uint64_t requester) {
	body_->removeRequesterSub(requester);
}


SQLIndexStatsCache::Key::Key(
		PartitionId partitionId, ContainerId containerId, uint32_t column,
		const TupleValue &lower, const TupleValue &upper,
		bool lowerInclusive, bool upperInclusive) :
		partitionId_(partitionId),
		containerId_(containerId),
		column_(column),
		lower_(lower),
		upper_(upper),
		lowerInclusive_(lowerInclusive),
		upperInclusive_(upperInclusive) {
}

bool SQLIndexStatsCache::Key::operator<(const Key &another) const {
	return (compare(another) < 0);
}

int32_t SQLIndexStatsCache::Key::compare(const Key &another) const {
	int32_t cmp;
	if ((cmp =
			Body::compareIntegral(partitionId_, another.partitionId_)) != 0) {
		return cmp;
	}

	if ((cmp =
			Body::compareIntegral(containerId_, another.containerId_)) != 0) {
		return cmp;
	}

	if ((cmp = Body::compareIntegral(column_, another.column_)) != 0) {
		return cmp;
	}

	if ((cmp = Body::compareValue(lower_, another.lower_)) != 0) {
		return cmp;
	}

	if ((cmp = Body::compareValue(upper_, another.upper_)) != 0) {
		return cmp;
	}

	if ((cmp = Body::compareBool(
			lowerInclusive_, another.lowerInclusive_)) != 0) {
		return cmp;
	}

	if ((cmp = Body::compareBool(
			upperInclusive_, another.upperInclusive_)) != 0) {
		return cmp;
	}

	return cmp;
}


SQLIndexStatsCache::Value::Value() :
		approxSize_(-1) {
}

bool SQLIndexStatsCache::Value::isEmpty() const {
	return approxSize_ < 0;
}


SQLIndexStatsCache::Option::Option() :
		expirationMillis_(-1),
		freeElementLimit_(100 * 1000) {
}


SQLIndexStatsCache::RequesterHolder::RequesterHolder() :
		cache_(NULL),
		requester_(0) {
}

SQLIndexStatsCache::RequesterHolder::~RequesterHolder() {
	try {
		reset();
	}
	catch (...) {
	}
}

void SQLIndexStatsCache::RequesterHolder::assign(SQLIndexStatsCache &cache) {
	reset();

	const uint64_t nextRequester = cache.addRequester();
	cache_ = &cache;
	requester_ = nextRequester;
}

void SQLIndexStatsCache::RequesterHolder::reset() {
	if (cache_ == NULL) {
		return;
	}

	SQLIndexStatsCache *cache = cache_;
	const uint64_t requester = requester_;

	cache_ = NULL;
	requester_ = 0;

	cache->removeRequester(requester);
}

const uint64_t* SQLIndexStatsCache::RequesterHolder::get() {
	if (cache_ == NULL) {
		return NULL;
	}

	return &requester_;
}


SQLIndexStatsCache::Body::Body(
		SQLVariableSizeGlobalAllocator &globalVarAlloc, const Option &option) :
		globalVarAlloc_(globalVarAlloc),
		varAlloc_(util::AllocatorInfo(
				ALLOCATOR_GROUP_SQL_JOB, "sqlIndexStatsCache")),
		option_(option),
		keyMap_(varAlloc_),
		requesterMap_(varAlloc_),
		freeList_(varAlloc_) {
}

SQLIndexStatsCache::Body::~Body() {
	clear();
}

SQLIndexStatsCache::Body* SQLIndexStatsCache::Body::create(
		SQLVariableSizeGlobalAllocator &globalVarAlloc, const Option &option) {
	return ALLOC_NEW(globalVarAlloc) Body(globalVarAlloc, option);
}

void SQLIndexStatsCache::Body::reset(Body *&body) {
	if (body != NULL) {
		ALLOC_DELETE(body->globalVarAlloc_, body);
	}
	body = NULL;
}

bool SQLIndexStatsCache::Body::findSub(const Key &key, Value &value) {
	util::LockGuard<util::Mutex> guard(lock_);

	KeyIterator it = keyMap_.find(key);
	if (it == keyMap_.end() || it->second.value_.isEmpty()) {
		value = Value();
		return false;
	}
	value = it->second.value_;
	return true;
}

void SQLIndexStatsCache::Body::putSub(
		const Key &key, uint64_t requester, const Value &value) {
	util::LockGuard<util::Mutex> guard(lock_);

	KeyIterator it = keyMap_.find(key);

	if (it == keyMap_.end()) {
		KeyHolder keyHolder(varAlloc_);
		keyHolder.duplicate(key);
		keyMap_.insert(std::make_pair(keyHolder.get(), Entry(freeList_.end())));
		keyHolder.detach();
	}
	else {
		KeyRefIterator &freeIt = it->second.freeIt_;
		if (freeIt != freeList_.end()) {
			freeList_.erase(freeIt);
			freeIt = freeList_.end();
		}
	}

	if (it->second.value_.isEmpty() && value.isEmpty()) {
		RequesterKey reqKey(requester, key.partitionId_, &it->first);
		if (requesterMap_.insert(std::make_pair(reqKey, it)).second) {
			it->second.requesterCount_++;
		}
	}
	else if (!value.isEmpty()) {
		it->second.value_ = value;
	}
}

bool SQLIndexStatsCache::Body::checkKeyResolvedSub(uint64_t requester) {
	util::LockGuard<util::Mutex> guard(lock_);

	const RequesterKey reqKey(requester, 0, NULL);
	RequesterMap::iterator it = requesterMap_.lower_bound(reqKey);

	return (it == requesterMap_.end() || it->first.requester_ != requester);
}

PartitionId SQLIndexStatsCache::Body::getMissedKeysSub(
		util::StackAllocator &alloc, uint64_t requester,
		PartitionId startPartitionId, KeyList &keyList) {
	util::LockGuard<util::Mutex> guard(lock_);

	keyList.clear();

	const RequesterKey reqKey(requester, startPartitionId, NULL);
	RequesterMap::iterator it = requesterMap_.lower_bound(reqKey);

	if (it == requesterMap_.end() || it->first.requester_ != requester) {
		return UNDEF_PARTITIONID;
	}

	const PartitionId curPartitionId = it->first.partitionId_;
	const RequesterKey nextKey(requester, curPartitionId + 1, NULL);
	do {
		keyList.push_back(
				KeyHolder::duplicateBy(alloc, it->second->first));
	}
	while (++it != requesterMap_.end() && it->first < nextKey);
	return nextKey.partitionId_;
}

uint64_t SQLIndexStatsCache::Body::addRequesterSub() {
	for (;;) {
		const uint64_t requester = ++nextRequester_;
		if (0 < requester && requester < std::numeric_limits<uint64_t>::max()) {
			return requester;
		}
	}
}

void SQLIndexStatsCache::Body::removeRequesterSub(uint64_t requester) {
	util::LockGuard<util::Mutex> guard(lock_);

	const RequesterKey reqKey(requester, 0, NULL);
	RequesterMap::iterator it = requesterMap_.lower_bound(reqKey);

	const RequesterKey nextKey(requester + 1, 0, NULL);
	for (; it != requesterMap_.end() && it->first < nextKey;) {
		RequesterMap::iterator nextIt = it;
		++nextIt;

		KeyIterator keyIt = it->second;
		uint64_t &requesterCount = keyIt->second.requesterCount_;

		if (--requesterCount == 0) {
			KeyRefIterator &freeIt = keyIt->second.freeIt_;
			if (freeIt == freeList_.end()) {
				freeList_.push_back(keyIt);
				freeIt--;
			}
		}

		requesterMap_.erase(it);
		it = nextIt;
	}

	adjustFreeList();
}

void SQLIndexStatsCache::Body::adjustFreeList() {
	while (freeList_.size() > option_.freeElementLimit_) {
		KeyIterator keyIt = freeList_.front();

		const uint64_t &requesterCount = keyIt->second.requesterCount_;
		if (requesterCount == 0) {
			eraseKeyDirect(keyIt);
		}

		freeList_.pop_front();
	}
}

void SQLIndexStatsCache::Body::clear() {
	for (KeyIterator it = keyMap_.begin(); it != keyMap_.end(); ++it) {
		it->second.freeIt_ = freeList_.end();
	}

	freeList_.clear();
	requesterMap_.clear();

	for (KeyIterator it = keyMap_.begin(); it != keyMap_.end();) {
		KeyIterator nextIt = it;
		++nextIt;

		eraseKeyDirect(it);

		it = nextIt;
	}
}

void SQLIndexStatsCache::Body::eraseKeyDirect(const KeyIterator &it) {
	KeyHolder keyHolder(varAlloc_);
	keyHolder.attach(it->first);

	keyMap_.erase(it);
	keyHolder.destroy();
}

int32_t SQLIndexStatsCache::Body::compareValue(
		const TupleValue &v1, const TupleValue &v2) {
	const bool null1 = (v1.getType() == TupleList::TYPE_NULL);
	const bool null2 = (v1.getType() == TupleList::TYPE_NULL);
	if (!null1 != !null2) {
		return compareBool(null1, null2);
	}

	return SQLProcessor::ValueUtils::orderValue(v1, v2);
}

int32_t SQLIndexStatsCache::Body::compareBool(bool v1, bool v2) {
	return compareIntegral((v1 ? 1 : 0), (v2 ? 1 : 0));
}

template<typename T>
int32_t SQLIndexStatsCache::Body::compareIntegral(const T &v1, const T &v2) {
	if (v1 != v2) {
		return (v1 < v2 ? -1 : 1);
	}
	return 0;
}


SQLIndexStatsCache::Body::Entry::Entry(const KeyRefIterator &freeIt) :
		requesterCount_(0),
		freeIt_(freeIt) {
}


SQLIndexStatsCache::Body::RequesterKey::RequesterKey(
		uint64_t requester, PartitionId partitionId, const Key *key) :
		requester_(requester),
		partitionId_(partitionId),
		key_(key) {
}

bool SQLIndexStatsCache::Body::RequesterKey::operator<(
		const RequesterKey &another) const {
	return (compare(another) < 0);
}

int32_t SQLIndexStatsCache::Body::RequesterKey::compare(
		const RequesterKey &another) const {
	int32_t cmp;
	if ((cmp = compareIntegral(requester_, another.requester_)) != 0) {
		return cmp;
	}

	if ((cmp = compareIntegral(partitionId_, another.partitionId_)) != 0) {
		return cmp;
	}

	if ((cmp = compareIntegral(key_, another.key_)) != 0) {
		return cmp;
	}

	return cmp;
}


SQLIndexStatsCache::Body::ValueHolder::ValueHolder(
		SQLVarSizeAllocator &varAlloc) :
		varAlloc_(varAlloc) {
}

SQLIndexStatsCache::Body::ValueHolder::~ValueHolder() {
	destroy();
}

TupleValue& SQLIndexStatsCache::Body::ValueHolder::get() {
	return target_;
}

void SQLIndexStatsCache::Body::ValueHolder::attach(const TupleValue &value) {
	target_ = value;
}

void SQLIndexStatsCache::Body::ValueHolder::detach() {
	target_ = TupleValue();
}

void SQLIndexStatsCache::Body::ValueHolder::duplicate(const TupleValue &value) {
	destroy();
	target_ = SQLProcessor::ValueUtils::duplicateValue(varAlloc_, value);
}

void SQLIndexStatsCache::Body::ValueHolder::destroy() {
	SQLProcessor::ValueUtils::destroyValue(varAlloc_, target_);
}


SQLIndexStatsCache::Body::KeyHolder::KeyHolder(SQLVarSizeAllocator &varAlloc) :
		varAlloc_(varAlloc),
		lowerValueHolder_(varAlloc),
		upperValueHolder_(varAlloc) {
}

SQLIndexStatsCache::Body::KeyHolder::~KeyHolder() {
	destroy();
}

SQLIndexStatsCache::Key& SQLIndexStatsCache::Body::KeyHolder::get() {
	return *target_;
}

void SQLIndexStatsCache::Body::KeyHolder::attach(const Key &key) {
	target_ = UTIL_MAKE_LOCAL_UNIQUE(target_, Key, key);
	lowerValueHolder_.attach(target_->lower_);
	upperValueHolder_.attach(target_->upper_);
}

void SQLIndexStatsCache::Body::KeyHolder::detach() {
	lowerValueHolder_.detach();
	upperValueHolder_.detach();
	target_.reset();
}

void SQLIndexStatsCache::Body::KeyHolder::duplicate(const Key &key) {
	destroy();

	lowerValueHolder_.duplicate(key.lower_);
	upperValueHolder_.duplicate(key.upper_);

	Key subKey = key;
	subKey.lower_ = lowerValueHolder_.get();
	subKey.upper_ = upperValueHolder_.get();

	attach(subKey);
}

void SQLIndexStatsCache::Body::KeyHolder::destroy() {
	lowerValueHolder_.destroy();
	upperValueHolder_.destroy();
	target_.reset();
}

SQLIndexStatsCache::Key
SQLIndexStatsCache::Body::KeyHolder::duplicateBy(
		util::StackAllocator &alloc, const Key &src) {
	Key dest = src;
	dest.lower_ = SQLProcessor::ValueUtils::duplicateValue(alloc, src.lower_);
	dest.upper_ = SQLProcessor::ValueUtils::duplicateValue(alloc, src.upper_);
	return dest;
}
