/*
	Copyright (c) 2012 TOSHIBA CORPORATION.

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
	@brief Definition and Implementation of TQL Aggregate function
*/

#ifndef AGGREGATION_H_
#define AGGREGATION_H_

#include "util/type.h"
#include "expression.h"
#include "function_map.h"
#include "value_operator.h"
#include <limits>

typedef Operator OperatorTable[11][11];

typedef void (*Calculator)(TransactionContext &txn, uint8_t const *p,
	uint32_t size1, uint8_t const *q, uint32_t size2, Value &value);
extern const Operator gtTable[][11];
extern const Operator ltTable[][11];
extern const Calculator addTable[][11];
extern const Calculator divTable[][11];

/*!
 * @brief TQL Aggregate function base class
 *
 */
class TqlAggregation {
public:
	/*!
	 * @brief Reset the aggregation state
	 *
	 * @param inputType Value type to input
	 */
	virtual void reset(ColumnType inputType) = 0;  

	/*!
	 * @brief Put an aggregate value to the aggregation buffer (for container)
	 * at least called one time.
	 *
	 * @param txn The transaction context
	 * @param input Value to aggregate
	 */
	virtual void putValue(TransactionContext &txn, const Value &input) = 0;

	/*!
	 * @brief Put two values to calculate (for timeSeries):
	 * at least must be called one time.
	 * Default implementation passes inputValue to putValue for container.
	 *
	 * @param txn Transaction context
	 * @param inputValue aggregation value
	 */
	virtual void putValue(
		TransactionContext &txn, const Value &, const Value &inputValue) {
		this->putValue(txn, inputValue);
	}

	/*!
	 * @brief Get the final result
	 *
	 * @param txn The transaction context
	 * @param[out] result Result value object
	 *
	 * @return The result value is valid
	 */
	virtual bool getResult(
		TransactionContext &txn, Value &result) = 0;  

	/*!
	 * @brief Check the value type is acceptable.
	 * Default: Only numerical type is acceptable
	 *
	 * @param type Value type
	 *
	 * @return test result
	 */
	virtual bool isAcceptable(ColumnType type) {
		return (type >= COLUMN_TYPE_BYTE && type <= COLUMN_TYPE_DOUBLE);
	}

	virtual QpPassMode getPassMode() {
		return (apiAggregationType() != AGG_UNSUPPORTED_TYPE)
				   ? QP_PASSMODE_PASS
				   : QP_PASSMODE_NO_PASS;
	}

	virtual AggregationType apiAggregationType() = 0;
	/*!
	 * @brief API passthrough
	 * @param txn
	 * @param timeSeries
	 * @param sc
	 * @param columnId
	 * @param serializedRowList
	 *
	 * @return
	 */
	virtual bool getResultFromAPI(TransactionContext &txn,
		TimeSeries &timeSeries, BtreeMap::SearchContext &sc, uint32_t columnId,
		util::XArray<uint8_t> &serializedRowList) {
		uint64_t resultNum;
		timeSeries.aggregate(txn, sc, columnId, apiAggregationType(), resultNum,
			serializedRowList);
		return resultNum != 0;
	}

	/*!
	 * @brief If function has internal valuables,
	 *        returns a clone object of "this". Otherwise, returns "this"
	 * @return Cloned object or Constant object
	 */
	virtual TqlAggregation *clone(util::StackAllocator &alloc) = 0;

	/*!
	 * @brief If clone() generates a new object, delete that
	 */
	virtual void destruct() = 0;

	virtual ~TqlAggregation() {}
};

/*!
 * @brief TQL COUNT function class
 *
 */
class AggregationCount : public TqlAggregation {
public:
	/*!
	 * @brief Reset the aggregation state
	 *
	 * @param inputType Value type to input
	 */
	virtual void reset(ColumnType inputType) {
		i = 0;
		cType = inputType;
	}

	using TqlAggregation::putValue;
	/*!
	 * @brief Put an aggregate value to the aggregation buffer
	 *
	 * @param input Value to aggregate
	 */
	virtual void putValue(TransactionContext &, const Value &input) {
		i++;
		if (cType != COLUMN_TYPE_WITH_BEGIN && input.getType() != cType) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_INVALID_ARGUMENT,
				"Internal logic error: Values of different type are passed to "
				"count()");
		}
		cType = input.getType();
	}

	/*!
	 * @brief Get the final result
	 *
	 * @param[out] result Result value object
	 *
	 * @return The result value is valid
	 */
	virtual bool getResult(TransactionContext &, Value &result) {
		result.set(i);
		return true;
	}

	/*!
	 * @brief Check the value type is acceptable.
	 *        All values are acceptable
	 *
	 * @return test result
	 */
	virtual bool isAcceptable(ColumnType) {
		return true;
	}

	AggregationType apiAggregationType() {
		return AGG_COUNT;
	}

	/*!
	 * @brief If function has internal valuables,
	 *        returns a clone object of "this". Otherwise, returns "this"
	 * @return Cloned object or Constant object
	 */
	virtual TqlAggregation *clone(util::StackAllocator &alloc) {
		return ALLOC_NEW(alloc) AggregationCount();
	}

	/*!
	 * @brief If clone() generates a new object, delete that
	 */
	virtual void destruct() {
		this->~AggregationCount();
	}

	virtual ~AggregationCount() {}

private:
	int64_t i;
	ColumnType cType;
};

/*!
 * @brief TQL SUM function class
 *
 */
class AggregationSum : public TqlAggregation {
public:
	/*!
	 * @brief Reset the aggregation state
	 *
	 * @param inputType Value type to input
	 */
	virtual void reset(ColumnType inputType) {
		switch (inputType) {
		case COLUMN_TYPE_BOOL:
			value.set(false);
			break;
		case COLUMN_TYPE_BYTE:
		case COLUMN_TYPE_SHORT:
		case COLUMN_TYPE_INT:
		case COLUMN_TYPE_LONG:
			value.set(0);
			break;
		case COLUMN_TYPE_FLOAT:
			value.set(0.0f);
			break;
		case COLUMN_TYPE_DOUBLE:
			value.set(0.0);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_INVALID_ARGUMENT,
				"Internal logic error: Invalid type for aggregation");
		}

		b = false;
	}

	using TqlAggregation::putValue;
	/*!
	 * @brief Put an aggregate value to the aggregation buffer
	 *
	 * @param txn The transaction context
	 * @param input Value to aggregate
	 */
	virtual void putValue(TransactionContext &txn, const Value &input) {
		if (!input.isNumerical()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_INVALID_ARGUMENT,
				"Internal logic error: Invalid value for aggregation");
		}
		ColumnType type = input.getType();
		assert(addTable[type][value.getType()] != NULL);
		addTable[type][value.getType()](
			txn, input.data(), 0, value.data(), 0, value);
		b = true;
	}

	/*!
	 * @brief Get the final result
	 *
	 * @param txn The transaction context
	 * @param[out] result Result value object
	 *
	 * @return The result value is valid
	 */
	virtual bool getResult(TransactionContext &txn, Value &result) {
		result.set(0);
		addTable[result.getType()][value.getType()](
			txn, result.data(), 0, value.data(), 0, result);
		return b;  
	}

	AggregationType apiAggregationType() {
		return AGG_SUM;
	}

	/*!
	 * @brief If function has internal valuables,
	 *        returns a clone object of "this". Otherwise, returns "this"
	 * @return Cloned object or Constant object
	 */
	virtual TqlAggregation *clone(util::StackAllocator &alloc) {
		return ALLOC_NEW(alloc) AggregationSum();
	}

	/*!
	 * @brief If clone() generates a new object, delete that
	 */
	virtual void destruct() {
		this->~AggregationSum();
	}

	virtual ~AggregationSum() {}

private:
	Value value;
	bool b;
};

/*!
 * @brief TQL AVG function class
 *
 */
class AggregationAvg : public TqlAggregation {
public:
	/*!
	 * @brief Reset the aggregation state
	 */
	virtual void reset(ColumnType) {
		v = 0;
		i = 0;
	}

	using TqlAggregation::putValue;
	/*!
	 * @brief Put an aggregate value to the aggregation buffer
	 *
	 * @param input Value to aggregate
	 */
	virtual void putValue(TransactionContext &, const Value &input) {
		if (!input.isNumerical()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_INVALID_ARGUMENT,
				"Internal logic error: Invalid value for aggregation");
		}
		v += input.getDouble();
		i++;
	}
	/*!
	 * @brief Get the final result
	 *
	 * @param[out] result Result value object
	 *
	 * @return The result value is valid
	 */
	virtual bool getResult(TransactionContext &, Value &result) {
		if (i > 0) {
			result.set(v / i);
			return true;
		}
		else {
			return false;
		}
	}

	AggregationType apiAggregationType() {
		return AGG_AVG;
	}

	/*!
	 * @brief If function has internal valuables,
	 *        returns a clone object of "this". Otherwise, returns "this"
	 * @return Cloned object or Constant object
	 */
	virtual TqlAggregation *clone(util::StackAllocator &alloc) {
		return ALLOC_NEW(alloc) AggregationAvg();
	}

	/*!
	 * @brief If clone() generates a new object, delete that
	 */
	virtual void destruct() {
		this->~AggregationAvg();
	}

	virtual ~AggregationAvg() {}

private:
	double v;
	uint32_t i;
};

/*!
 * @brief TQL VARIANCE_SAMP function class
 *
 */
class AggregationVarianceSamp : public TqlAggregation {
public:
	/*!
	 * @brief Reset the aggregation state
	 */
	virtual void reset(ColumnType) {
		sum = 0;
		sqsum = 0;
		i = 0;
	}

	using TqlAggregation::putValue;
	/*!
	 * @brief Put an aggregate value to the aggregation buffer
	 *
	 * @param input Value to aggregate
	 */
	virtual void putValue(TransactionContext &, const Value &input) {
		if (!input.isNumerical()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_INVALID_ARGUMENT,
				"Internal logic error: Invalid value for aggregation");
		}
		double inputValue;
		inputValue = input.getDouble();
		sum += inputValue;
		sqsum += inputValue * inputValue;
		i++;
	}

	/*!
	 * @brief Get the final result
	 *
	 * @param[out] result Result value object
	 *
	 * @return The result value is valid
	 */
	virtual bool getResult(TransactionContext &, Value &result) {
		if (i == 0) {
			return false;
		}
		if (i == 1) {
			result.set(0.0);
			return true;
		}
		else {
			double squ = sqsum - (sum * sum / i);
			result.set(squ / (i - 1));
		}
		return true;
	}

#ifdef QP_DISABLE_VARIANCE_PASSTHROUGH
	virtual AggregationType apiAggregationType() {
		return AGG_UNSUPPORTED_TYPE;
	}
#else
	virtual AggregationType apiAggregationType() {
		return AGG_VARIANCE;
	}
#endif

	/*!
	 * @brief If function has internal valuables,
	 *        returns a clone object of "this". Otherwise, returns "this"
	 * @return Cloned object or Constant object
	 */
	virtual TqlAggregation *clone(util::StackAllocator &alloc) {
		return ALLOC_NEW(alloc) AggregationVarianceSamp();
	}

	/*!
	 * @brief If clone() generates a new object, delete that
	 */
	virtual void destruct() {
		this->~AggregationVarianceSamp();
	}

	virtual ~AggregationVarianceSamp() {}

protected:
	double sqsum, sum;
	uint32_t i;
};

/*!
 * @brief TQL VARIANCE_POP function class
 *
 */
class AggregationVariancePop : public AggregationVarianceSamp {
public:
	/*!
	 * @brief Get the final result
	 *
	 * @param[out] result Result value object
	 *
	 * @return The result value is valid
	 */
	virtual bool getResult(TransactionContext &, Value &result) {
		if (i <= 0) {
			return false;
		}
		else if (i == 1) {
			result.set(0.0);
		}
		else {
			double squ = sqsum - ((sum * sum) / i);
			result.set(squ / i);
		}
		return true;
	}

	virtual AggregationType apiAggregationType() {
		return AGG_UNSUPPORTED_TYPE;
	}

	/*!
	 * @brief If function has internal valuables,
	 *        returns a clone object of "this". Otherwise, returns "this"
	 * @return Cloned object or Constant object
	 */
	virtual TqlAggregation *clone(util::StackAllocator &alloc) {
		return ALLOC_NEW(alloc) AggregationVariancePop();
	}

	/*!
	 * @brief If clone() generates a new object, delete that
	 */
	virtual void destruct() {
		this->~AggregationVariancePop();
	}

	virtual ~AggregationVariancePop() {}
};

typedef AggregationVarianceSamp Aggregation_variance;

/*!
 * @brief TQL STDDEV_SAMP function class
 *
 */
class AggregationStddevSamp : public Aggregation_variance {
public:
	/*!
	 * @brief Get the final result
	 *
	 * @param[out] result Result value object
	 *
	 * @return The result value is valid
	 */
	virtual bool getResult(TransactionContext &, Value &result) {
		if (i <= 0) {
			return false;
		}
		if (i == 1) {
			result.set(0.0);
			return true;
		}
		else {
			double squ = sqsum - ((sum * sum) / i);
			result.set(sqrt(squ / (i - 1)));
		}
		return true;
	}

#ifdef QP_DISABLE_VARIANCE_PASSTHROUGH
	virtual AggregationType apiAggregationType() {
		return AGG_UNSUPPORTED_TYPE;
	}
#else
	virtual AggregationType apiAggregationType() {
		return AGG_STDDEV;
	}
#endif

	/*!
	 * @brief If function has internal valuables,
	 *        returns a clone object of "this". Otherwise, returns "this"
	 * @return Cloned object or Constant object
	 */
	virtual TqlAggregation *clone(util::StackAllocator &alloc) {
		return ALLOC_NEW(alloc) AggregationStddevSamp();
	}

	/*!
	 * @brief If clone() generates a new object, delete that
	 */
	virtual void destruct() {
		this->~AggregationStddevSamp();
	}

	virtual ~AggregationStddevSamp() {}
};

/*!
 * @brief TQL STDDEV_POP function class
 *
 */
class AggregationStddevPop : public Aggregation_variance {
public:
	/*!
	 * @brief Get the final result
	 *
	 * @param[out] result Result value object
	 *
	 * @return The result value is valid
	 */
	virtual bool getResult(TransactionContext &, Value &result) {
		if (i <= 0) {
			return false;
		}
		if (i == 1) {
			result.set(0.0);
		}
		else {
			double squ = sqsum - ((sum * sum) / i);
			result.set(sqrt(squ / i));
		}
		return true;
	}

	virtual AggregationType apiAggregationType() {
		return AGG_UNSUPPORTED_TYPE;
	}

	/*!
	 * @brief If function has internal valuables,
	 *        returns a clone object of "this". Otherwise, returns "this"
	 * @return Cloned object or Constant object
	 */
	virtual TqlAggregation *clone(util::StackAllocator &alloc) {
		return ALLOC_NEW(alloc) AggregationStddevPop();
	}

	/*!
	 * @brief If clone() generates a new object, delete that
	 */
	virtual void destruct() {
		this->~AggregationStddevPop();
	}

	virtual ~AggregationStddevPop() {}
};
typedef AggregationStddevSamp Aggregation_stddev;

/*!
 * @brief TQL MAX/MIN function base class
 *
 */
class AggregationMaxMinBase : public TqlAggregation {
public:
	/*!
	 * @brief Reset the aggregation state
	 *
	 * @param inputType Value type to input
	 */
	virtual void reset(ColumnType inputType) {
		resultFlag = false;
		type = inputType;
	}

	using TqlAggregation::putValue;
	/*!
	 * @brief Put an aggregate value to the aggregation buffer
	 *
	 * @param txn The transaction context
	 * @param input Value to aggregate
	 */
	virtual void putValue(TransactionContext &txn, const Value &input) {
		if (type == COLUMN_TYPE_TIMESTAMP) {
			if (resultFlag) {
				if ((*opTable)[type][type](txn, input.data(), 0,
						reinterpret_cast<uint8_t *>(&tsValue), 0) == true) {
					tsValue = input.getTimestamp();
				}
			}
			else {
				tsValue = input.getTimestamp();
				resultFlag = true;
			}
		}
		else {
			if (!input.isNumerical()) {
				GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_INVALID_ARGUMENT,
					"Internal logic error: Invalid value for aggregation");
			}

			if (resultFlag) {
				assert(opTable[type][value.getType()] != NULL);
				if ((*opTable)[type][value.getType()](
						txn, input.data(), 0, value.data(), 0) == true) {
					value.set(0);
					addTable[type][value.getType()](
						txn, input.data(), 0, value.data(), 0, value);
				}
			}
			else {
				value.set(0);
				assert(opTable[type][value.getType()] != NULL);
				addTable[input.getType()][value.getType()](
					txn, input.data(), 0, value.data(), 0, value);
				resultFlag = true;
			}
		}
	}

	/*!
	 * @brief Get the final result
	 *
	 * @param txn The transaction context
	 * @param[out] result Result value object
	 *
	 * @return The result value is valid
	 */
	virtual bool getResult(TransactionContext &txn, Value &result) {
		if (!resultFlag) {
			return false;
		}

		switch (type) {
		case COLUMN_TYPE_TIMESTAMP:
			result.setTimestamp(tsValue);
			break;
		default: {
			Value zero(0);
			addTable[value.getType()][COLUMN_TYPE_INT](
				txn, value.data(), 0, zero.data(), 0, result);
		} break;
		}
		return true;
	}

	/*!
	 * @brief Check the value type is acceptable. Timestamp is added with
	 * default implemantation.
	 *
	 * @param type Value type
	 *
	 * @return test result
	 */
	virtual bool isAcceptable(ColumnType type) {
		return (type >= COLUMN_TYPE_BYTE && type <= COLUMN_TYPE_DOUBLE) ||
			   type == COLUMN_TYPE_TIMESTAMP;
	}

	virtual ~AggregationMaxMinBase() {}

protected:
	Value value;
	Timestamp tsValue;
	bool resultFlag;
	ColumnType type;
	const OperatorTable *opTable;
};

/*!
 * @brief TQL MAX function class
 *
 */
class AggregationMax : public AggregationMaxMinBase {
public:
	/*!
	 * @brief constructor setups opTable
	 */
	AggregationMax() {
		opTable = reinterpret_cast<const OperatorTable *>(&gtTable);
	}
	virtual AggregationType apiAggregationType() {
		return AGG_MAX;
	}

	/*!
	 * @brief If function has internal valuables,
	 *        returns a clone object of "this". Otherwise, returns "this"
	 * @return Cloned object or Constant object
	 */
	virtual TqlAggregation *clone(util::StackAllocator &alloc) {
		return ALLOC_NEW(alloc) AggregationMax();
	}

	/*!
	 * @brief If clone() generates a new object, delete that
	 */
	virtual void destruct() {
		this->~AggregationMax();
	}

	virtual ~AggregationMax() {}
};

/*!
 * @brief TQL MIN function class
 *
 */
class AggregationMin : public AggregationMaxMinBase {
public:
	/*!
	 * @brief constructor setups opTable
	 */
	AggregationMin() {
		opTable = reinterpret_cast<const OperatorTable *>(&ltTable);
	}
	virtual AggregationType apiAggregationType() {
		return AGG_MIN;
	}

	/*!
	 * @brief If function has internal valuables,
	 *        returns a clone object of "this". Otherwise, returns "this"
	 * @return Cloned object or Constant object
	 */
	virtual TqlAggregation *clone(util::StackAllocator &alloc) {
		return ALLOC_NEW(alloc) AggregationMin();
	}

	/*!
	 * @brief If clone() generates a new object, delete that
	 */
	virtual void destruct() {
		this->~AggregationMin();
	}

	virtual ~AggregationMin() {}
};

/*!
 * @brief TQL TIME_AVG function class
 *
 */
class AggregationTimeAvg : public TqlAggregation {
public:
	/*!
	 * @brief Reset the aggregation state
	 */
	virtual void reset(ColumnType) {
		beforeTs = UNDEF_TIMESTAMP;
		beforeValue = 0;
		beforeIntTs = UNDEF_TIMESTAMP;
		weight = 0;
		weightedSum = 0;
		isFirstData = true;
		numCount = 0;
	}

	/*!
	 * @brief (TIME_AVG cannot use with collection)
	 *
	 */
	virtual void putValue(TransactionContext &, const Value &) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_AGGREGATION_ABUSE,
			"Aggregation for timeSeries is used to collection.");
	}

	/*!
	 * @brief Put an aggregate value to the aggregation buffer
	 *
	 * @param inputKey Timestamp
	 * @param inputValue Value to aggregate
	 */
	virtual void putValue(
		TransactionContext &, const Value &inputKey, const Value &inputValue) {
		if (inputKey.getType() != COLUMN_TYPE_TIMESTAMP) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_INVALID_ARGUMENT,
				"Internal logic error: Aggregation key must be a timestamp.");
		}
		if (!inputValue.isNumerical()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_INVALID_ARGUMENT,
				"Internal logic error: Aggregation value must be a numeric "
				"value.");
		}
		numCount++;

		double v = inputValue.getDouble();

		if (isFirstData) {
			beforeTs = *(reinterpret_cast<const Timestamp *>(inputKey.data()));
			beforeIntTs = beforeTs;
			beforeValue = v;
			isFirstData = false;
		}
		else {
			Timestamp nextTs =
				*(reinterpret_cast<const Timestamp *>(inputKey.data()));
			Timestamp intTs = (nextTs - beforeTs) / 2 + beforeTs;  
			double currentWeight = static_cast<double>(
				intTs - beforeIntTs);  
			weightedSum += beforeValue * currentWeight;
			beforeTs = nextTs;
			beforeIntTs = intTs;
			beforeValue = v;
			weight += currentWeight;
		}
	}

	/*!
	 * @brief Get the final result
	 *
	 * @param[out] result Result value object
	 *
	 * @return The result value is valid
	 */
	virtual bool getResult(TransactionContext &, Value &result) {
		if (numCount > 0) {
			if (beforeIntTs == beforeTs) {
				result.set(beforeValue);
			}
			else {
				double currentWeight =
					static_cast<double>(beforeTs - beforeIntTs);  
				weight += currentWeight;
				weightedSum += beforeValue * currentWeight;
				result.set(weightedSum / weight);
			}
			return true;
		}
		else {
			return false;
		}
	}
	virtual AggregationType apiAggregationType() {
		return AGG_TIME_AVG;
	}

	/*!
	 * @brief If function has internal valuables,
	 *        returns a clone object of "this". Otherwise, returns "this"
	 * @return Cloned object or Constant object
	 */
	virtual TqlAggregation *clone(util::StackAllocator &alloc) {
		return ALLOC_NEW(alloc) AggregationTimeAvg();
	}

	/*!
	 * @brief If clone() generates a new object, delete that
	 */
	virtual void destruct() {
		this->~AggregationTimeAvg();
	}

	virtual ~AggregationTimeAvg() {}

private:
	Timestamp beforeTs, beforeIntTs;
	double beforeValue, weight, weightedSum;
	bool isFirstData;
	int numCount;
};

/*!
 * @brief Hash based map for aggregation
 */
class AggregationMap : public OpenHash<TqlAggregation> {
public:
	static AggregationMap *getInstance() {
		return &map_;
	}

private:
	static AggregationMap map_;

public:
	/*!
	 * @brief Constructor
	 *
	 * @return generated aggregation map
	 */
	AggregationMap() : OpenHash<TqlAggregation>() {
		RegisterFunction<AggregationSum>("SUM");
		RegisterFunction<AggregationCount>("COUNT");
		RegisterFunction<AggregationAvg>("AVG");
		RegisterFunction<AggregationMax>("MAX");
		RegisterFunction<AggregationMin>("MIN");
		RegisterFunction<Aggregation_stddev>("STDDEV");
		RegisterFunction<Aggregation_variance>("VARIANCE");
		RegisterFunction<AggregationStddevSamp>("STDDEV_SAMP");
		RegisterFunction<AggregationVarianceSamp>("VARIANCE_SAMP");
		RegisterFunction<AggregationStddevPop>("STDDEV_POP");
		RegisterFunction<AggregationVariancePop>("VARIANCE_POP");
		RegisterFunction<AggregationTimeAvg>("TIME_AVG");
	}
};

#endif /* AGGREGATION_H_ */
