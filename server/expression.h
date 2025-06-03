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
	@file
	@brief Definition of Expr
*/


#ifndef EXPR_HPP
#define EXPR_HPP

#include "util/type.h"
#include "util/allocator.h"
#include "util/container.h"
#include "data_type.h"
#include "gs_error.h"
#include "qp_def.h"
#include "schema.h"
#include "value_processor.h"
#include <cstdarg>
#include <stdexcept>
#include "transaction_context.h"

class AggregationMap;
class Expr;
class FunctionMap;
class SelectionMap;
class SpecialIdMap;
class TqlAggregation;
class TqlFunc;
class TqlSelection;
class TqlSpecialId;

enum TrivalentLogicType {
	TRI_TRUE = 1,
	TRI_NULL = 0,
	TRI_FALSE = -1
};

static inline TrivalentLogicType notTrivalentLogic(const TrivalentLogicType input) {
	return static_cast<TrivalentLogicType>(input * -1);
}

/*!
* @brief Token structure
*/
struct Token {
	int id;
	int n;
	const char *z;
	int minusflag;  
};
/*!
*	@brief Represents the order of Rows requested by a query
*/
enum SortOrder { ASC, DESC };
/*!
*	@brief Class for sort expression
*/
typedef struct SortExpr {
	SortOrder order;
	Expr *expr;
	bool nullsLast;
	SortExpr() : order(ASC), expr(NULL), nullsLast(true) {}
	SortExpr *dup(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy);
} SortItem;

/*!
*	@brief Represents the mode of API passthrough
*/
enum QpPassMode {
	QP_PASSMODE_NO_PASS,
	QP_PASSMODE_PASS,
	QP_PASSMODE_PASS_IF_NO_WHERE
};

typedef QP_XArray<Expr *> ExprList;
typedef QP_XArray<SortExpr> SortExprList;

class Value;
class ColumnInfo;
class Query;
class BoolExpr;
class OutputMessageRowStore;

#include "gis_geometry.h"
#include "collection.h"

/*!
*	@brief Interface for Container Row
*/
class ContainerRowWrapper {
public:
	virtual ~ContainerRowWrapper() {}
	virtual const Value *getColumn(uint32_t columnId) = 0;
	const Value *operator[](uint32_t id) {
		return getColumn(id);
	}
	virtual void load(OId oId) = 0;
	virtual RowId getRowId() = 0;
	virtual void getImage(TransactionContext &txn,
		MessageRowStore *messageRowStore, bool isWithRowId) = 0;
};

class BaseContainer;
/*!
*	@brief Class for general expression
*/
class Expr {
	friend class BoolExpr;

public:
	/*!
	*	@brief Represents the type of expression
	*/
	enum Type {
		VALUE,
		EXPR,
		EXPRARRAY,
		FUNCTION,
		AGGREGATION,
		SELECTION,
		COLUMN,
		VARIABLE,
		EXPRLABEL,
		NULLVALUE,
		BOOL_EXPR,
	};
	/*!
	*	@brief Represents the type of Operation
	*/
	enum Operation {
		ADD,
		SUB,
		MUL,
		DIV,
		REM,
		IS,
		ISNOT,
		BITNOT,
		BITAND,
		BITOR,
		LSHIFT,
		RSHIFT,
		NE,
		EQ,
		LT,
		LE,
		GT,
		GE,
		PLUS,
		MINUS,
		BITMINUS,
		BETWEEN,
		NOTBETWEEN,
		NONE
	};

	bool isNumeric() const;
	bool isNumericOfType(ColumnType t) const;
	bool isNumericInteger() const;
	bool isNumericFloat() const;
	bool isBoolean() const;
	bool isString() const;
	bool isGeometry() const;
	bool isTimestamp() const;
	bool isMicroTimestamp() const;
	bool isNanoTimestamp() const;
	bool isTimestampFamily() const;
	bool isArrayValue() const;
	bool isValue() const;
	bool isExprLabel() const;
	bool isExprArray() const;
	bool isAggregation() const;
	bool isSelection() const;
	bool isColumn() const;
	bool isNullValue() const;
	bool isNullable() const;
	QpPassMode getPassMode(TransactionContext &txn);

	Type getType();
	ColumnType resolveValueType() const;
	int getValueAsInt();
	int64_t getValueAsInt64();
	double getValueAsDouble();
	bool getValueAsBool();
	const char *getValueAsString(TransactionContext &txn);
	Geometry *getGeometry();
	const ExprList &getArgList();
	Expr *getArrayElement(
		TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy, size_t idx);
	size_t getArrayLength(
		TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy) const;
	Timestamp getTimeStamp();
	NanoTimestamp getNanoTimestamp();

	virtual Expr *dup(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy);

	/*!
	 * @param Generate expression with null value
	 *
	 * @param txn The transaction context
	 *
	 * @return generated expression
	 */
	static Expr *newNullValue(TransactionContext &txn) {
		return QP_NEW_BY_TXN(txn) Expr(TRI_NULL, txn);
	}

	/*!
	 * @brief Generate expression with boolean value
	 *
	 * @param v value
	 * @param txn The transaction context
	 *
	 * @return generated expression
	 */
	static Expr *newBooleanValue(bool v, TransactionContext &txn) {
		return QP_NEW_BY_TXN(txn) Expr(v, txn);
	}
	/*!
	 * @brief Generate expression with boolean value
	 *
	 * @param v value
	 * @param txn The transaction context
	 *
	 * @return generated expression
	 */
	static Expr *newBooleanValue(TrivalentLogicType v, TransactionContext &txn) {
		bool b = (v == TRI_TRUE);
		return QP_NEW_BY_TXN(txn) Expr(b, txn);
	}

	/*!
	 * @brief  Generate expression with numeric value
	 *
	 * @param v value
	 * @param txn The transaction context
	 *
	 * @return generated expression
	 */
	template <typename V>
	static Expr *newNumericValue(V v, TransactionContext &txn) {
		return QP_NEW_BY_TXN(txn) Expr(v, txn);
	}

	/*!
	 * @brief Generate expression with create blob value
	 *
	 * @return
	 */
	static Expr *newBlobValue(Token &, TransactionContext &) {
		return NULL;
	}

	/*!
	 * @brief Generate expression from string with quote-escaping
	 *
	 * @param s Quoted string
	 * @param txn The transaction context
	 *
	 * @return generated expression
	 */
	static Expr *newStringValueWithUnescape(
		const char *s, TransactionContext &txn) {
		return QP_NEW_BY_TXN(txn) Expr(s, txn, false, true);
	}

	/*!
	 * @brief Generate expression from string
	 *
	 * @param s string
	 * @param txn The transaction context
	 *
	 * @return generated expression
	 */
	static Expr *newStringValue(const char *s, TransactionContext &txn) {
		return QP_NEW_BY_TXN(txn) Expr(s, txn, false, false);
	}

	/*!
	 * @brief Generate expression from string
	 *
	 * @param s string
	 * @param size length of the string
	 * @param txn The transaction context
	 *
	 * @return generated expression
	 */
	static Expr *newStringValue(
		const char *s, size_t size, TransactionContext &txn) {
		if (size == 0 && s == NULL) {
			return QP_NEW_BY_TXN(txn) Expr("", txn);
		}
		else {
			return QP_NEW_BY_TXN(txn) Expr(s, size, txn);  
		}
	}

	/*!
	 * @brief Generate expression of Timestamp value
	 *
	 * @param t timestamp object
	 * @param txn The transaction context
	 *
	 * @return generated expression
	 */
	static Expr *newTimestampValue(
		const util::DateTime &t, TransactionContext &txn) {
		return QP_NEW_BY_TXN(txn) Expr(t.getUnixTime(), txn, true);
	}

	static Expr *newTimestampValue(
			const util::PreciseDateTime &t, TransactionContext &txn,
			ColumnType type) {
		switch (type) {
		case COLUMN_TYPE_MICRO_TIMESTAMP:
			return newTimestampValue(ValueProcessor::getMicroTimestamp(t), txn);
		case COLUMN_TYPE_NANO_TIMESTAMP:
			return newTimestampValue(ValueProcessor::getNanoTimestamp(t), txn);
		default:
			assert(type == COLUMN_TYPE_TIMESTAMP);
			return newTimestampValue(
					ValueProcessor::getTimestamp(t.getBase()), txn);
		}
	}

	/*!
	 * @brief Generate expression of Timestamp value
	 *
	 * @param t timestamp value
	 * @param txn The transaction context
	 *
	 * @return generated expression
	 */
	static Expr *newTimestampValue(Timestamp t, TransactionContext &txn) {
		return QP_NEW_BY_TXN(txn) Expr(t, txn, true);
	}

	static Expr *newTimestampValue(MicroTimestamp t, TransactionContext &txn) {
		return QP_NEW_BY_TXN(txn) Expr(t, txn);
	}

	static Expr *newTimestampValue(NanoTimestamp t, TransactionContext &txn) {
		return QP_NEW_BY_TXN(txn) Expr(t, txn);
	}

	/*!
	 * @brief Generate expression of geometry value
	 *
	 * @param geom Geometry
	 * @param txn The transaction context
	 *
	 * @return generated expression
	 */
	static Expr *newGeometryValue(Geometry *geom, TransactionContext &txn) {
		return QP_NEW_BY_TXN(txn) Expr(geom, txn);
	}

	/*!
	 * @brief Generate expression of expression array
	 *
	 * @param v List of expression
	 * @param txn The transaction context
	 *
	 * @return generated expression
	 */
	static Expr *newExprArray(ExprList &v, TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy) {
		return QP_NEW_BY_TXN(txn) Expr(&v, txn, objectManager, strategy);
	}

	static Expr *newFunctionNode(Token &fnName, ExprList *args,
		TransactionContext &txn, FunctionMap &fmap);
	static Expr *newSelectionNode(Token &fnName, ExprList *args,
		TransactionContext &txn, FunctionMap *fmap, AggregationMap *amap,
		SelectionMap *smap);
	static Expr *newColumnOrIdNode(Token &colName, TransactionContext &txn,
		unsigned int state, Collection *collection = NULL,
		TimeSeries *timeSeries = NULL);
	static Expr *newColumnNode(Token &colName, TransactionContext &txn,
		unsigned int state, Collection *collection = NULL,
		TimeSeries *timeSeries = NULL);
	static Expr *newColumnNode(const char *colName, TransactionContext &txn,
		unsigned int state, Collection *collection = NULL,
		TimeSeries *timeSeries = NULL);
	static Expr *newColumnOrIdNode(Token &colName, TransactionContext &txn,
		unsigned int state, Collection *collection = NULL,
		TimeSeries *timeSeries = NULL, SpecialIdMap *map = NULL);

	/*!
	 * @brief Generate operation node for binary operations
	 *
	 * @param op Operation
	 * @param arg1 Operand 1
	 * @param arg2 Operand 2
	 * @param txn The transaction context
	 *
	 * @return generated expression
	 */
	static Expr *newOperationNode(
		Operation op, Expr *arg1, Expr *arg2, TransactionContext &txn) {
		return QP_NEW_BY_TXN(txn) Expr(op, arg1, arg2, txn);
	}

	/*!
	 * @brief Generate operation node
	 *
	 * @param op Operation
	 * @param args Operands
	 * @param txn The transaction context
	 *
	 * @return generated expression
	 */
	static Expr *newOperationNode(
		Operation op, ExprList &args, TransactionContext &txn) {
		return QP_NEW_BY_TXN(txn) Expr(op, args, txn);
	}
	/*!
	 * @brief Generate expression of a expression label
	 *
	 * @param s string used for label
	 * @param txn The transaction context
	 *
	 * @return generated expression
	 */
	static Expr *newExprLabel(const char *s, TransactionContext &txn) {
		return QP_NEW_BY_TXN(txn) Expr(s, txn, true);
	}
	/*!
	 * @brief Generate expression of array
	 *
	 * @param array Array value
	 * @param txn The transaction context
	 *
	 * @return generated expression
	 */
	static Expr *newArrayValue(Value *array, TransactionContext &txn) {
		return QP_NEW_BY_TXN(txn) Expr(array, txn);
	}

	static char *dequote(util::StackAllocator &alloc, const char *str, bool &isQuote);

	static util::DateTime::ZonedOption newZonedTimeOption(
			const util::DateTime::FieldType *precision,
			const util::TimeZone &zone);
	static bool isTimeRangeValidationStrict(
			util::DateTime::FieldType precision);

	virtual ~Expr();

	/*!
	 * @brief Cast to 32bit integer
	 */
	operator int() {
		return getValueAsInt();
	}
	/*!
	 * @brief Cast to 64bit integer
	 */
	operator int64_t() {
		return getValueAsInt64();
	}
	/*!
	 * @brief Cast to double
	 */
	operator double() {
		return getValueAsDouble();
	}

	/*!
	 * @brief Cast to boolean
	 */
	operator bool() {
		return getValueAsBool();
	}

	/*!
	 * @brief Cast to trivalent logic value
	 */
	TrivalentLogicType castTrivalentLogicValue() {
		if (isNullValue()) {
			return TRI_NULL;
		} else if (getValueAsBool()) {
			return TRI_TRUE;
		} else {
			return TRI_FALSE;
		}
	}

	/*!
	 * @brief Cast to specified numeric value
	 */
	template <typename T>
	T castNumericValue();
	/*!
	 * @brief Cast numeric value to boolean
	 */
	bool castNumericValue();

	virtual Expr *eval(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
		ContainerRowWrapper *column_values, FunctionMap *function_map,
		EvalMode mode);

	void getMappingColumnInfo(
		TransactionContext &txn, std::vector<util::String> &x);
	void getMappingVariableInfo(
		TransactionContext &txn, std::vector<util::String> &x);

	bool equals(Expr &e) {
		return (*this == e);
	}
	virtual bool operator==(const Expr &e) const;
	virtual bool operator!=(const Expr &e) const {
		return !(*this == e);
	}

	TermCondition *toCondition(
			TransactionContext &txn, MapType mapType, Query &queryObj,
			bool notFlag, bool &semiFiltering);

	bool aggregate(TransactionContext &txn, Collection &collection,
		util::XArray<OId> &resultRowIdList, Value &result);

	bool aggregate(TransactionContext &txn, TimeSeries &collection,
		util::XArray<PointRowId> &resultRowIdList, Value &result);

	void getColumnName(TransactionContext &txn, util::String &outName);
	uint32_t getColumnId();
	const ColumnInfo *getColumnInfo();

	void getIndexBitmapAndInfo(
			TransactionContext &txn, BaseContainer &container, Query &queryObj,
			uint32_t &mapBitmap, ColumnInfo *&columnInfo,
			Operation &detectedOp, bool notFlag, bool withPartialMatch);

	void select(TransactionContext &txn, Collection &collection,
		util::XArray<PointRowId> &resultRowIdList, bool isSorted,
		OutputOrder apiOutputOrder, SortExprList *orderByExpr, uint64_t limit,
		uint64_t offset, FunctionMap &function_map, uint64_t &resultNum,
		OutputMessageRowStore *messageRowStore, ResultType &resultType);

	void select(TransactionContext &txn, TimeSeries &timeSeries,
		util::XArray<PointRowId> &resultRowIdList, bool isSorted,
		OutputOrder apiOutputOrder, SortExprList *orderByExpr, uint64_t limit,
		uint64_t offset, FunctionMap &function_map, uint64_t &resultNum,
		OutputMessageRowStore *messageRowStore, ResultType &resultType);

	void select(TransactionContext &txn, TimeSeries &timeSeries,
		BtreeMap::SearchContext &sc, OutputOrder apiOutputOrder, uint64_t limit,
		uint64_t offset, uint64_t &resultNum,
		OutputMessageRowStore *messageRowStore, ResultType &resultType);

	void aggregate(TransactionContext &txn, TimeSeries &timeSeries,
		BtreeMap::SearchContext &sc, ResultSize &resultNum,
		Value &value);

	static void dequote(util::String &str, char quote);
	static void unescape(util::String &str, char escape);

	/*!
	 * @brief Compare expressions as values
	 *
	 * @param txn The transaction context
	 * @param e2 Target value
	 *
	 * @return compare result(-1, 0, 1)
	 */
	int compareAsValue(TransactionContext &txn, Expr *e2, bool nullsLast) {
		if (isValue() && e2->isValue() && !value_->isArray() &&
				!e2->value_->isArray()) {
			Comparator op = ComparatorTable::findComparator(
					value_->getType(), e2->value_->getType());
#ifdef QP_USE_STRING_COMPARATOR
			if (op) {
				return op(
						txn,
						value_->data(), value_->size(),
						e2->value_->data(),
						e2->value_->size());
			}
#else
			if (value_->getType() != COLUMN_TYPE_STRING && op) {
				return op(
						txn,
						value_->data(), value_->size(),
						e2->value_->data(),
						e2->value_->size());
			}
			else if (value_->getType() == COLUMN_TYPE_STRING) {
				size_t size1 = value_->size(), size2 = e2->value_->size();
				size_t cmpSize = size1 > size2 ? size2 : size1;
				int res =
					strncmp(reinterpret_cast<const char *>(value_->data()),
						reinterpret_cast<const char *>(e2->value_->data()),
						cmpSize);
				if (res == 0) {
					res = (size1 == size2) ? 0 : ((size1 > size2) ? 1 : -1);
				}
				return res;
			}
#endif
			else {
				GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Invalid argument type: Cannot compare non-comparable "
					"types");
			}
		}
		else {
			if (isNullValue() && e2->isValue()) {
				return nullsLast ? 1 : -1;
			}
			else if (isValue() && e2->isNullValue()) {
				return nullsLast ? -1 : 1;
			}
			else if (isNullValue() && e2->isNullValue()) {
				return 0;
			}
			else {
				GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Invalid argument type: Cannot compare non-comparable "
					"types");
			}
		}
	}

protected:
	Expr(TransactionContext &txn) : arglist_(NULL) {
		Init();
		value_ = QP_NEW Value();
	}

	Expr *evalSubBinOp(
			TransactionContext &txn, ObjectManagerV4 &objectManager,
			AllocateStrategy &strategy, const ValueOperatorTable &opTable,
			ContainerRowWrapper *column_values, FunctionMap *function_map,
			const char *mark, EvalMode mode);
	Expr *evalSubBinOp(
			TransactionContext &txn, ObjectManagerV4 &objectManager,
			AllocateStrategy &strategy, const ValueCalculatorTable &opTable,
			ContainerRowWrapper *column_values, FunctionMap *function_map,
			const char *mark, EvalMode mode);
	Expr *evalSubBinOp(
			TransactionContext &txn, ObjectManagerV4 &objectManager,
			AllocateStrategy &strategy, const Operation opType,
			ContainerRowWrapper *column_values, FunctionMap *function_map,
			const char *mark, EvalMode mode);

	Expr *evalSubUnaryOpBaseZero(
			TransactionContext &txn, ObjectManagerV4 &objectManager,
			AllocateStrategy &strategy, const ValueCalculatorTable &opTable,
			ContainerRowWrapper *column_values, FunctionMap *function_map,
			const char *mark, EvalMode mode);

	bool isIncludedBy(Expr *outerExpr);
	bool isAntinomy(Expr *testExpr);
	Expr *transposeExpr(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy);

	void Init();

	Expr(bool v, TransactionContext &txn);
	Expr(int8_t v, TransactionContext &txn);
	Expr(int16_t v, TransactionContext &txn);
	Expr(int32_t v, TransactionContext &txn);
	Expr(int64_t v, TransactionContext &txn, bool isTimestamp = false);
	Expr(float v, TransactionContext &txn);
	Expr(double v, TransactionContext &txn);

	Expr(const MicroTimestamp &v, TransactionContext &txn);
	Expr(const NanoTimestamp &v, TransactionContext &txn);

	Expr(const char *s, TransactionContext &txn, bool isLabel = false,
		bool needDequote = false);

	Expr(const char *s, size_t len, TransactionContext &txn,
		bool isLabel = false, bool needDequote = false);

	Expr(Geometry *geom, TransactionContext &txn);

	Expr(Value *array, TransactionContext &txn);

	Expr(ExprList *args, TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy);

	Expr(Operation op, Expr *arg1, Expr *arg2, TransactionContext &txn);
	Expr(Operation op, ExprList &args, TransactionContext &txn);

	Expr(const char *name, TransactionContext &txn, uint32_t columnId,
		ColumnInfo *cInfo);

	Expr(const char *name, TqlFunc *func, ExprList *arg,
		TransactionContext &txn);
	Expr(const char *name, TqlAggregation *func, ExprList *arg,
		TransactionContext &txn);
	Expr(const char *name, TqlSelection *func, ExprList *arg,
		TransactionContext &txn);
	Expr(const char *name, TqlSpecialId *func, TransactionContext &txn);
	Expr(TrivalentLogicType logicType, TransactionContext &txn);

	template<typename T, typename R>
	const char* stringifyValue(TransactionContext &txn);
	template<typename T>
	const char8_t* stringifyTimestamp(TransactionContext &txn);

	virtual bool isValueOrColumn() {
		return isColumn() || isValue();
	}
	virtual ColumnType getColumnType() {
		if (isColumn()) {
			return columnType_;
		}
		else if (isValue()) {
			return value_->getType();
		}
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: getColumnType() is called in invalid "
			"context");
	}

	bool checkValueRange(ColumnType t1, ColumnType t2, const void *value,
		int64_t &i64, double &d) {
		if (t1 <= COLUMN_TYPE_LONG) {
			switch (t2) {
			case COLUMN_TYPE_BYTE:
				i64 = *(reinterpret_cast<const int8_t *>(value));
				break;
			case COLUMN_TYPE_SHORT:
				i64 = *(reinterpret_cast<const int16_t *>(value));
				break;
			case COLUMN_TYPE_INT:
				i64 = *(reinterpret_cast<const int32_t *>(value));
				break;
			case COLUMN_TYPE_LONG:
				i64 = *(reinterpret_cast<const int64_t *>(value));
				break;
			default:
				break;
			}

			switch (t1) {
			case COLUMN_TYPE_BYTE:
				return (i64 >= INT8_MIN && i64 <= INT8_MAX);
			case COLUMN_TYPE_SHORT:
				return (i64 >= INT16_MIN && i64 <= INT16_MAX);
			case COLUMN_TYPE_INT:
				return (i64 >= INT32_MIN && i64 <= INT32_MAX);
			default:
				break;
			}
			return true;
		}
		else {
			switch (t2) {
			case COLUMN_TYPE_BYTE:
				d = *(reinterpret_cast<const int8_t *>(value));
				return true;
			case COLUMN_TYPE_SHORT:
				d = *(reinterpret_cast<const int16_t *>(value));
				return true;
			case COLUMN_TYPE_INT:
				d = *(reinterpret_cast<const int32_t *>(value));
				return true;
			case COLUMN_TYPE_LONG:
				d = static_cast<double>(*(const_cast<int64_t *>(
					reinterpret_cast<const int64_t *>(value))));
				return true;
			case COLUMN_TYPE_FLOAT:
				{
					float t = *(reinterpret_cast<const float *>(value));
					if (util::isNaN(t)) {
						d = std::numeric_limits<double>::quiet_NaN();
					}
					else if (util::isInf(t)) {
						d = (t > 0) ? std::numeric_limits<double>::infinity()
									: -std::numeric_limits<double>::infinity();
					}
					else {
						d = static_cast<double>(t);
					}
				}
				break;
			case COLUMN_TYPE_DOUBLE:
				d = *(reinterpret_cast<const double *>(value));
				break;
			default:
				break;
			}

			return (!util::isFinite(d)) || (d > -std::numeric_limits<float>::max() &&
										 d < std::numeric_limits<float>::max());
		}
	}

	inline const char *os2char(
		TransactionContext &txn, util::NormalOStringStream &os) {
		std::string str = os.str();
		size_t len = str.size() + 1;
		char *allocatedStr =
			reinterpret_cast<char *>(txn.getDefaultAllocator().allocate(len));
		memcpy(allocatedStr, str.c_str(), len);
		return allocatedStr;
	}

	Type type_;

	Value *
		value_;					  
	ExprList *arglist_;			  
	util::XArray<uint8_t> *buf_;  
	char *label_;   
	Operation op_;  
	ColumnType columnType_;   
	ColumnInfo *columnInfo_;  
	uint32_t columnId_;  
	void *functor_;  
	Geometry *geomCache_;  
};

#endif
