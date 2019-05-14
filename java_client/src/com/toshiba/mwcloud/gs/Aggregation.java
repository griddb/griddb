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
 * <div lang="ja">
 * ロウ集合またはその特定のカラムに対する、集計演算の方法を示します。
 *
 * <p>現バージョンでは、{@link TimeSeries}に対してのみ使用できます。</p>
 *
 * <p>重み付きの演算の場合、キーの値に基づき重み付け値を決定します。
 * {@link TimeSeries}に対する重み付きの演算の場合、前後それぞれの
 * 時刻のロウとの中間時刻間の期間を特定の単位で換算したものを、
 * 重み付け値として使用します。
 * ただし、前後いずれかの時刻のロウのみが存在しない場合は、存在しないロウの
 * 代わりに重み付け対象のロウを用いて求めた重み付け値を使用します。
 * 前後いずれの時刻のロウも存在しない場合は、重み付け値として{@code 1}
 * (単位は前後いずれかのロウが存在する場合と同一)を使用します。</p>
 *
 * <p>演算の内部処理にてオーバーフローが発生した場合、浮動小数点数型では
 * 負または正の無限大、整数型では未定義の値が求まります。また、浮動小数点数型にて
 * 演算対象に非数(NaN)が含まれていた場合、非数が求まります。</p>
 * </div><div lang="en">
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
 * </div>
 */
public enum Aggregation {

	/**
	 * <div lang="ja">
	 * 最小値を求める演算です。
	 *
	 * <p>大小比較できる型、すなわち数値型や時刻型のカラムに対してのみ使用できます。
	 * 演算結果の型は、対象のカラムと同一の型となります。
	 * 対象となるロウが1つも存在しない場合、演算結果は設定されません。</p>
	 * </div><div lang="en">
	 * An operation to obtain a minimum value.
	 *
	 * <p>Available only to Columns of numeric and time types allowing magnitude comparison.
	 * The type of a returned value is the same as that of a specified Column.
	 * If no target Row exists, the aggregation result will not be stored
	 * in a result object. </p>
	 * </div>
	 */
	MINIMUM,

	/**
	 * <div lang="ja">
	 * 最大値を求める演算です。
	 *
	 * <p>大小比較できる型、すなわち数値型や時刻型のカラムに対してのみ使用できます。
	 * 演算結果の型は、対象のカラムと同一の型となります。
	 * 対象となるロウが1つも存在しない場合、演算結果は設定されません。</p>
	 * </div><div lang="en">
	 * An operation to obtain a maximum value.
	 *
	 * <p>Available only to Columns of numeric and time types allowing magnitude comparison.
	 * The type of a returned value is the same as that of a specified Column.
	 * If no target Row exists, the aggregation result will not be stored in a result object.
	 * </p>
	 * </div>
	 */
	MAXIMUM,

	/**
	 * <div lang="ja">
	 * 合計を求める演算です。
	 *
	 * <p>数値型のカラムに対してのみ使用できます。
	 * 演算結果の型は、対象のカラムが整数型の場合LONG、
	 * 浮動小数点型の場合 DOUBLEとなります。
	 * 対象となるロウが1つも存在しない場合、演算結果は設定されません。</p>
	 * </div><div lang="en">
	 * An operation to obtain a sum total.
	 *
	 * <p>Available only to numeric-type Columns. The type of a returned value is LONG
	 * if a specified Column is of integer type, and DOUBLE if it is of floating-point type.
	 * If no target Row exists, the aggregation result will not be stored in a result object.</p>
	 * </div>
	 */
	TOTAL,

	/**
	 * <div lang="ja">
	 * 平均を求める演算です。
	 *
	 * <p>数値型のカラムに対してのみ使用できます。
	 * 演算結果の型は常にDOUBLEとなります。
	 * 対象となるロウが1つも存在しない場合、演算結果は設定されません。</p>
	 * </div><div lang="en">
	 * An operation to obtain an average.
	 *
	 * <p>Available only to numeric-type Columns. The type of a returned value is
	 * always DOUBLE. If no target Row exists, the aggregation result will
	 * not be stored in a result object.</p>
	 * </div>
	 */
	AVERAGE,

	/**
	 * <div lang="ja">
	 * 分散を求める演算です。
	 *
	 * <p>数値型のカラムに対してのみ使用できます。
	 * 演算結果の型は常にDOUBLEとなります。
	 * 対象となるロウが1つも存在しない場合、演算結果は設定されません。</p>
	 * </div><div lang="en">
	 * An operation to obtain a variance.
	 *
	 * <p>Available only to numeric-type Columns. The type of a returned value is always DOUBLE.
	 * If no target Row exists, the aggregation result will not be stored in a result object. </p>
	 * </div>
	 */
	VARIANCE,

	/**
	 * <div lang="ja">
	 * 標準偏差を求める演算です。
	 *
	 * <p>数値型のカラムに対してのみ使用できます。
	 * 演算結果の型は常にDOUBLEとなります。
	 * 対象となるロウが1つも存在しない場合、演算結果は設定されません。</p>
	 * </div><div lang="en">
	 * An operation to obtain a standard deviation.
	 *
	 * <p>Available only to numeric-type Columns. The type of a returned value is always DOUBLE.
	 * If no target Row exists, the aggregation result will not be stored in a result object. </p>
	 * </div>
	 */
	STANDARD_DEVIATION,

	/**
	 * <div lang="ja">
	 * 標本数、すなわち対象ロウの個数を求める演算です。
	 *
	 * <p>任意のカラムに対して使用できます。
	 * 演算結果の型は常にLONGとなります。
	 * 対象となるロウが1つも存在しない場合、演算結果の値は{@code 0}となります。</p>
	 * </div><div lang="en">
	 * An operation to obtain the number of samples, i.e., the number of Rows.
	 *
	 * <p>Available to any kinds of Columns. The type of a returned value is always LONG.
	 * If no target Row exists, the aggregation result will not be stored in a result object. </p>
	 * </div>
	 */
	COUNT,

	/**
	 * <div lang="ja">
	 * 重み付きで平均を求める演算です。
	 *
	 * <p>各標本値と重み付け値との積の合計を、各重み付け値の合計で割ることにより
	 * 求めます。重み付け値の計算方法は、{@link Aggregation}の説明を
	 * 参照してください。</p>
	 *
	 * <p>この演算は、数値型のカラムに対してのみ使用できます。
	 * 演算結果の型は常にDOUBLEとなります。
	 * 対象となるロウが1つも存在しない場合、演算結果は設定されません。</p>
	 * </div><div lang="en">
	 * An operation to obtain a weighted average.
	 *
	 * <p>The weighted average is calculated by dividing the sum of products of
	 * sample values and their respective weighted values by the sum of weighted values.
	 * For the method of calculating a weighted value, see the description of {@link Aggregation}. </p>
	 *
	 * <p>This operation is only available to numeric-type Columns. The type of
	 * a returned value is always DOUBLE. If no target Row exists,
	 * the aggregation result will not be stored in a result object. </p>
	 * </div>
	 */
	WEIGHTED_AVERAGE

}
