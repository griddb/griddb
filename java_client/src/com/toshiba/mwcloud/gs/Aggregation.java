/*
   Copyright (c) 2017 TOSHIBA Digital Solutions Corporation

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.toshiba.mwcloud.gs;

/**
 * Represents the methods of aggregation operation on a set of Rows or their specific Columns.
 *
 * <p>Available only to {@link TimeSeries} in the current version. </p>
 *
 * <p>In a weighted operation, a weighted value is determined based on a key value.
 * In a weighted operation on a {@link TimeSeries}, a weighted value is obtained
 * by calculating half the time span between the adjacent Rows before and after
 * a Row in terms of the specific unit. However, if a Row has only one adjacent Row,
 * the time span from the adjacent Row is considered, and if no adjacent Rows exist,
 * 1 (sec.) is used as a weighted value. </p>
 *
 * <p>If an overflow occurs in an internal operation, -INF or INF is returned
 * for floating point operation, and the value "undefined" is returned
 * for integer operation. And if NaN is given as an operand for floating-point operation,
 * NaN is returned. </p>
 */
public enum Aggregation {

	/**
	 * An operation to obtain a minimum value.
	 *
	 * <p>Available only to Columns of numeric and time types allowing magnitude comparison.
	 * The type of a returned value is the same as that of a specified Column.
	 * If no target Row exists, the aggregation result will not be stored
	 * in a result object. </p>
	 */
	MINIMUM,

	/**
	 * An operation to obtain a maximum value.
	 *
	 * <p>Available only to Columns of numeric and time types allowing magnitude comparison.
	 * The type of a returned value is the same as that of a specified Column.
	 * If no target Row exists, the aggregation result will not be stored in a result object.
	 * </p>
	 */
	MAXIMUM,

	/**
	 * An operation to obtain a sum total.
	 *
	 * <p>Available only to numeric-type Columns. The type of a returned value is LONG
	 * if a specified Column is of integer type, and DOUBLE if it is of floating-point type.
	 * If no target Row exists, the aggregation result will not be stored in a result object.</p>
	 */
	TOTAL,

	/**
	 * An operation to obtain an average.
	 *
	 * <p>Available only to numeric-type Columns. The type of a returned value is
	 * always DOUBLE. If no target Row exists, the aggregation result will
	 * not be stored in a result object.</p>
	 */
	AVERAGE,

	/**
	 * An operation to obtain a variance.
	 *
	 * <p>Available only to numeric-type Columns. The type of a returned value is always DOUBLE.
	 * If no target Row exists, the aggregation result will not be stored in a result object. </p>
	 */
	VARIANCE,

	/**
	 * An operation to obtain a standard deviation.
	 *
	 * <p>Available only to numeric-type Columns. The type of a returned value is always DOUBLE.
	 * If no target Row exists, the aggregation result will not be stored in a result object. </p>
	 */
	STANDARD_DEVIATION,

	/**
	 * An operation to obtain the number of samples, i.e., the number of Rows.
	 *
	 * <p>Available to any kinds of Columns. The type of a returned value is always LONG.
	 * If no target Row exists, the aggregation result will not be stored in a result object. </p>
	 */
	COUNT,

	/**
	 * An operation to obtain a weighted average.
	 *
	 * <p>The weighted average is calculated by dividing the sum of products of
	 * sample values and their respective weighted values by the sum of weighted values.
	 * For the method of calculating a weighted value, see the description of {@link Aggregation}. </p>
	 *
	 * <p>This operation is only available to numeric-type Columns. The type of
	 * a returned value is always DOUBLE. If no target Row exists,
	 * the aggregation result will not be stored in a result object. </p>
	 */
	WEIGHTED_AVERAGE

}
