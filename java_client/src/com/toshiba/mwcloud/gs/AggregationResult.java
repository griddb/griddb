/*
   Copyright (c) 2012 TOSHIBA CORPORATION.

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

import java.util.Date;

/**
 * Stores the result of an aggregation operation.
 *
 * <p>Stores the result returned by an aggregation Query or
 * {@link TimeSeries#aggregate(Date, Date, String, Aggregation)}.
 * A floating-point-type result can be obtained from an operation
 * on a numeric-type Column, and a higher-precision result can be obtained
 * from an operation on a numeric-type Column with a small number of
 * significant digits.</p>
 *
 * <p>The type of the stored result depends on the type of aggregation operation and
 * the type of the target Columns. For specific rules, see {@link Aggregation} or
 * the TQL specifications.</p>
 *
 * <p>The type of obtaining value depends on the stored type.
 * DOUBLE type and LONG type are only available when a result is of numeric type,
 * and TIMESTAMP type when a result is of TIMESTAMP type.</p>
 */
public interface AggregationResult {

	/**
	 * Obtains the result of aggregating numeric-type values in DOUBLE type ({@link Double}).
	 *
	 * <p>It returns {@code null} if a result is not of numeric type. If a result is not of DOUBLE type,
	 * it returns the value converted to DOUBLE. </p>
	 *
	 * @return Result of aggregating DOUBLE type ({@link Double}).
	 * If the type of result is different, returns {@code null}.
	 */
	public Double getDouble();

	/**
	 * Obtains the result of aggregating numeric-type values in LONG type ({@link Long}).
	 *
	 * <p>It returns {@code null} if a result is not of numeric type.
	 * If a result is not of LONG type, it returns the value converted to LONG.</p>
	 *
	 * @return Result of aggregating LONG type ({@link Long}).
	 * If the type of result is different, returns {@code null}.
	 */
	public Long getLong();

	/**
	 * Obtains the result of aggregating time-type values in TIMESTAMP type ({@link Date}).
	 *
	 * <p>It returns {@code null} if a result is not of TIMESTAMP type. </p>
	 *
	 * @return Result of aggregating TIMESTAMP type ({@link Date}).
	 * If the type of result is different, returns {@code null}.
	 */
	public Date getTimestamp();

}
