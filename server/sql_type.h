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
#ifndef SQL_TYPE_H_
#define SQL_TYPE_H_

#include "util/code.h"

struct SQLType {
	class Coder;

	enum Id {
		START_EXEC,
		EXEC_REFERENCE,
		EXEC_GROUP,
		EXEC_JOIN,
		EXEC_LIMIT,
		EXEC_SCAN,
		EXEC_SELECT,
		EXEC_SORT,
		EXEC_UNION,
		EXEC_DELETE,
		EXEC_INSERT,
		EXEC_UPDATE,
		EXEC_DDL,
		EXEC_RESULT,
		END_EXEC,

		START_EXPR,
		EXPR_ID,
		EXPR_TABLE,
		EXPR_COLUMN,
		EXPR_CONSTANT,
		EXPR_TYPE,
		EXPR_SELECTION,
		EXPR_PRODUCTION,
		EXPR_PROJECTION,
		EXPR_JOIN,
		EXPR_AND,
		EXPR_OR,
		EXPR_BETWEEN,
		EXPR_CASE,
		EXPR_IN,
		EXPR_EXISTS,
		EXPR_ALL_COLUMN,
		EXPR_FUNCTION,
		EXPR_PLACEHOLDER,
		EXPR_LIST,
		EXPR_AGG_FOLLOWING,
		EXPR_AGG_ORDERING,
		EXPR_COND,
		EXPR_TUPLE_COLUMN,
		EXPR_HINT_NO_INDEX,
		EXPR_WINDOW_OPTION,
		EXPR_RANGE_GROUP,
		EXPR_RANGE_GROUP_ID,
		EXPR_RANGE_KEY,
		EXPR_RANGE_KEY_CURRENT,
		EXPR_RANGE_AGG,
		EXPR_RANGE_FILL,
		EXPR_RANGE_FILL_NONE,
		EXPR_RANGE_FILL_NULL,
		EXPR_RANGE_FILL_PREV,
		EXPR_RANGE_FILL_LINEAR,
		EXPR_LINEAR,
		END_EXPR,

		START_AGG,
		AGG_AVG,
		AGG_COUNT_ALL,
		AGG_COUNT_COLUMN,
		AGG_GROUP_CONCAT,
		AGG_LAG,
		AGG_LEAD,
		AGG_MAX,
		AGG_MEDIAN,
		AGG_MIN,
		AGG_PERCENTILE_CONT,
		AGG_ROW_NUMBER,
		AGG_STDDEV,
		AGG_STDDEV0,
		AGG_STDDEV_POP,
		AGG_STDDEV_SAMP,
		AGG_SUM,
		AGG_TOTAL,
		AGG_VAR_POP,
		AGG_VAR_SAMP,
		AGG_VARIANCE,
		AGG_VARIANCE0,
		AGG_DISTINCT_AVG,
		AGG_DISTINCT_COUNT_COLUMN,
		AGG_DISTINCT_GROUP_CONCAT,
		AGG_DISTINCT_STDDEV,
		AGG_DISTINCT_STDDEV0,
		AGG_DISTINCT_STDDEV_POP,
		AGG_DISTINCT_STDDEV_SAMP,
		AGG_DISTINCT_SUM,
		AGG_DISTINCT_TOTAL,
		AGG_DISTINCT_VAR_POP,
		AGG_DISTINCT_VAR_SAMP,
		AGG_DISTINCT_VARIANCE,
		AGG_DISTINCT_VARIANCE0,
		AGG_FIRST,
		AGG_LAST,
		AGG_FOLD_EXISTS,
		AGG_FOLD_NOT_EXISTS,
		AGG_FOLD_IN,
		AGG_FOLD_NOT_IN,
		AGG_FOLD_UPTO_ONE,
		END_AGG,

		START_FUNC,
		FUNC_ABS,
		FUNC_CHAR,
		FUNC_COALESCE,
		FUNC_GLOB,
		FUNC_IFNULL,
		FUNC_INSTR,
		FUNC_HEX,
		FUNC_HEX_TO_DEC,
		FUNC_LENGTH,
		FUNC_LIKE,
		FUNC_LOG,
		FUNC_LOWER,
		FUNC_LTRIM,
		FUNC_MAX,
		FUNC_MIN,
		FUNC_NULLIF,
		FUNC_PRINTF,
		FUNC_QUOTE,
		FUNC_RANDOM,
		FUNC_RANDOMBLOB,
		FUNC_REPLACE,
		FUNC_ROUND,
		FUNC_RTRIM,
		FUNC_SQRT,
		FUNC_SUBSTR,
		FUNC_SUBSTR_WITH_BOUNDS,
		FUNC_TRANSLATE,
		FUNC_TRIM,
		FUNC_TRUNC,
		FUNC_TYPEOF,
		FUNC_UNICODE,
		FUNC_UPPER,
		FUNC_ZEROBLOB,

		FUNC_EXTRACT,
		FUNC_MAKE_TIMESTAMP,
		FUNC_MAKE_TIMESTAMP_BY_DATE,
		FUNC_MAKE_TIMESTAMP_MS,
		FUNC_MAKE_TIMESTAMP_US,
		FUNC_MAKE_TIMESTAMP_NS,
		FUNC_NOW,
		FUNC_STRFTIME,
		FUNC_TO_EPOCH_MS,
		FUNC_TO_TIMESTAMP_MS,
		FUNC_TIMESTAMP,
		FUNC_TIMESTAMP_MS,
		FUNC_TIMESTAMP_US,
		FUNC_TIMESTAMP_NS,
		FUNC_TIMESTAMP_TRUNC,
		FUNC_TIMESTAMP_ADD,
		FUNC_TIMESTAMP_DIFF,
		FUNC_TIMESTAMPADD,
		FUNC_TIMESTAMPDIFF,
		END_FUNC,

		START_OP,
		OP_CONCAT,
		OP_MULTIPLY,
		OP_DIVIDE,
		OP_REMAINDER,
		OP_ADD,
		OP_SUBTRACT,
		OP_SHIFT_LEFT,
		OP_SHIFT_RIGHT,
		OP_BIT_AND,
		OP_BIT_OR,
		OP_LT,
		OP_LE,
		OP_GT,
		OP_GE,
		OP_EQ,
		OP_NE,
		OP_IS,
		OP_IS_NOT,
		OP_CAST,
		OP_IS_NULL,
		OP_IS_NOT_NULL,
		OP_BIT_NOT,
		OP_NOT,
		OP_PLUS,
		OP_MINUS,
		END_OP,

		END_TYPE
	};

	enum JoinType {
		JOIN_INNER,
		JOIN_LEFT_OUTER,
		JOIN_RIGHT_OUTER,
		JOIN_FULL_OUTER,
		JOIN_NATURAL_INNER,
		JOIN_NATURAL_LEFT_OUTER,
		JOIN_NATURAL_RIGHT_OUTER,
		JOIN_NATURAL_FULL_OUTER,

		END_JOIN
	};

	enum OrderDirection {
		DIRECTION_ASC,
		DIRECTION_DESC,

		END_DIRECTION
	};

	enum UnionType {
		UNION_ALL,
		UNION_DISTINCT,
		UNION_INTERSECT,
		UNION_EXCEPT,

		END_UNION
	};

	enum AggregationPhase {
		AGG_PHASE_ALL_PIPE,
		AGG_PHASE_MERGE_PIPE,
		AGG_PHASE_ADVANCE_PIPE,

		AGG_PHASE_MERGE_FINISH,
		AGG_PHASE_ADVANCE_FINISH,

		END_AGG_PHASE
	};

	enum TableType {
		TABLE_CONTAINER,
		TABLE_META,

		END_TABLE
	};

	enum IndexType {
		INDEX_TREE_RANGE,
		INDEX_TREE_EQ,
		INDEX_HASH,

		END_INDEX
	};

	enum WindowFrameType {
		FRAME_ROWS,
		FRAME_START_PRECEDING,
		FRAME_START_FOLLOWING,
		FRAME_FINISH_PRECEDING,
		FRAME_FINISH_FOLLOWING,
		FRAME_ASCENDING
	};

	static bool isNatural(JoinType type);
	static JoinType removeNatural(JoinType type);
};

class SQLType::Coder {
public:
	const char8_t* operator()(Id id, const char8_t *name = NULL) const;
	bool operator()(const char8_t *name, Id &id) const;

	const char8_t* operator()(JoinType type, const char8_t *name = NULL) const;
	bool operator()(const char8_t *name, JoinType &type) const;

	const char8_t* operator()(
			OrderDirection direction, const char8_t *name = NULL) const;
	bool operator()(const char8_t *name, OrderDirection &direction) const;

	const char8_t* operator()(
			UnionType type, const char8_t *name = NULL) const;
	bool operator()(const char8_t *name, UnionType &type) const;

	const char8_t* operator()(
			AggregationPhase phase, const char8_t *name = NULL) const;
	bool operator()(const char8_t *name, AggregationPhase &phase) const;

	const char8_t* operator()(
			TableType type, const char8_t *name = NULL) const;
	bool operator()(const char8_t *name, TableType &type) const;

	const char8_t* operator()(
			IndexType type, const char8_t *name = NULL) const;
	bool operator()(const char8_t *name, IndexType &type) const;

private:
	static util::NameCoder<Id, END_EXEC> makeExecIdCoder(
			const util::NameCoderEntry<Id> (&idList)[END_TYPE]);

	static const util::NameCoderEntry<Id> ID_LIST[];
	static const util::NameCoderEntry<JoinType> JOIN_TYPE_LIST[];
	static const util::NameCoderEntry<OrderDirection> DIRECTION_LIST[];
	static const util::NameCoderEntry<UnionType> UNION_TYPE_LIST[];
	static const util::NameCoderEntry<AggregationPhase> AGG_PHASE_LIST[];
	static const util::NameCoderEntry<TableType> TABLE_TYPE_LIST[];
	static const util::NameCoderEntry<IndexType> INDEX_TYPE_LIST[];

	static const util::NameCoder<Id, END_TYPE> ID_CODER;
	static const util::NameCoder<Id, END_EXEC> EXEC_ID_CODER;
	static const util::NameCoder<JoinType, END_JOIN> JOIN_TYPE_CODER;
	static const util::NameCoder<OrderDirection, END_DIRECTION> DIRECTION_CODER;
	static const util::NameCoder<UnionType, END_UNION> UNION_TYPE_CODER;
	static const util::NameCoder<AggregationPhase, END_AGG_PHASE> AGG_PHASE_CODER;
	static const util::NameCoder<TableType, END_TABLE> TABLE_TYPE_CODER;
	static const util::NameCoder<IndexType, END_INDEX> INDEX_TYPE_CODER;
};

#endif
