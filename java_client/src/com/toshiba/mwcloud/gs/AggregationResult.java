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

import java.sql.Timestamp;
import java.util.Date;

/**
 * <div lang="ja">
 * 集計演算の結果を保持します。
 *
 * <p>集計演算に関するクエリの実行もしくは
 * {@link TimeSeries#aggregate(Date, Date, String, Aggregation)}
 * により取得できる、集計演算の結果を保持します。
 * 整数型カラムに対する演算結果を浮動小数点型として、また、有効桁数の
 * 少ない数値型のカラムに対する演算結果をより桁数の多い数値型として
 * 受け取ることができます。</p>
 *
 * <p>保持する型は、集計演算の種別や集計対象のカラムの型によって決定されます。
 * 具体的な規則は{@link Aggregation}またはTQLの仕様を参照してください。</p>
 *
 * <p>取り出しできる型は、保持されている型によって決まります。
 * 保持されている型が数値型の場合はDOUBLE型またはLONG型、TIMESTAMP型の
 * 場合はTIMESTAMP型の値としてのみ取り出しできます。</p>
 * </div><div lang="en">
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
 * </div>
 */
public interface AggregationResult {

	/**
	 * <div lang="ja">
	 * 数値型の集計値をDOUBLE型({@link Double})として取得します。
	 *
	 * <p>数値型以外の値を保持している場合、{@code null}を返します。
	 * DOUBLE型以外の数値を保持している場合、DOUBLE型に変換したものを返します。</p>
	 *
	 * @return DOUBLE型({@link Double})の集計値。結果が数値型以外の場合は
	 * {@code null}
	 * </div><div lang="en">
	 * Obtains the result of aggregating numeric-type values in DOUBLE type ({@link Double}).
	 *
	 * <p>It returns {@code null} if a result is not of numeric type. If a result is not of DOUBLE type,
	 * it returns the value converted to DOUBLE. </p>
	 *
	 * @return Result of aggregating DOUBLE type ({@link Double}).
	 * If the type of result is different, returns {@code null}.
	 * </div>
	 */
	public Double getDouble();

	/**
	 * <div lang="ja">
	 * 数値型の集計値をLONG型({@link Long})として取得します。
	 *
	 * <p>数値型以外の値を保持している場合、{@code null}を返します。
	 * LONG型以外の数値を保持している場合、LONG型に変換したものを返します。</p>
	 *
	 * @return LONG型({@link Double})の集計値。結果が数値型以外の場合は
	 * {@code null}
	 * </div><div lang="en">
	 * Obtains the result of aggregating numeric-type values in LONG type ({@link Long}).
	 *
	 * <p>It returns {@code null} if a result is not of numeric type.
	 * If a result is not of LONG type, it returns the value converted to LONG.</p>
	 *
	 * @return Result of aggregating LONG type ({@link Long}).
	 * If the type of result is different, returns {@code null}.
	 * </div>
	 */
	public Long getLong();

	/**
	 * <div lang="ja">
	 * 時刻型の集計値を通常精度のTIMESTAMP型({@link Date})で取得します。
	 *
	 * <p>TIMESTAMP型以外の値を保持している場合、{@code null}を
	 * 返します。</p>
	 *
	 * <p>高精度のTIMESTAMP値を保持している場合、通常精度の値に変換したものを
	 * 返します。</p>
	 *
	 * @return 通常精度のTIMESTAMP型({@link Date})の集計値。結果がTIMESTAMP型
	 * 以外の場合は{@code null}
	 * </div><div lang="en">
	 * Retrieves the result of aggregating time-type values as normal -precision TIMESTAMP type ({@link Date}) values.
	 *
	 * <p>It returns {@code null} if a result is not of TIMESTAMP type. </p>
	 *
	 * <p>If high-precision TIMESTAMP values are retained, they are converted to 
	 * normal-precision values and then returned.</p>
	 *
	 * @return Result of aggregating normal-precision TIMESTAMP type ({@link Date}) values.
	 * If the type of result is different, returns {@code null}.
	 * </div>
	 */
	public Date getTimestamp();

	/**
	 * <div lang="ja">
	 * 時刻型の集計値を高精度のTIMESTAMP型({@link Timestamp})で取得します。
	 *
	 * <p>TIMESTAMP型以外の値を保持している場合、{@code null}を
	 * 返します。</p>
	 *
	 * <p>通常精度のTIMESTAMP値を保持している場合、高精度の値に変換したものを
	 * 返します。</p>
	 *
	 * @return 高精度のTIMESTAMP型({@link Timestamp})の集計値。結果がTIMESTAMP型
	 * 以外の場合は{@code null}
	 * @since 5.3
	 * </div><div lang="en">
	 * Retrieves the result of aggregating time-type values as high-precision TIMESTAMP type ({@link Timestamp}) values.
	 *
	 * <p>If non-TIMESTAMP type values are retained, 
	 * {@code null} is returned.</p>
	 *
	 * <p>If normal-precision TIMESTAMP values are retained, 
	 * they are converted to high-precision values and then returned.</p>
	 *
	 * @return Result of aggregating high-precision TIMESTAMP type ({@link Timestamp}) values.
	 * @since 5.3
	 * </div>
	 */
	public Timestamp getPreciseTimestamp();

}
