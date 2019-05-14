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
 * ロウの補間方法の種別を表します。
 *
 * <p>時系列ロウの補間機能で使用されます。</p>
 * </div><div lang="en">
 * Represents the type of interpolation of Rows.
 *
 * <p>It is used by the function of interpolating TimeSeries Rows.</p>
 * </div>
 */
public enum InterpolationMode {

	/**
	 * <div lang="ja">
	 * カラムに応じて線形補間または直前ロウの値による補間を行うことを示します。
	 *
	 * <p>補間機能にて指定されたカラムについては、補間対象時刻の
	 * 前後時刻のロウの値により線形補間を行います。対象とするカラムの型は
	 * 数値型でなければなりません。</p>
	 *
	 * <p>補間機能にて特に指定されていないカラムについては、補間対象時刻と
	 * 隣接する直前の時刻のロウの値を補間値として使用します。対象とするカラムの
	 * 型に制限はありません。</p>
	 * </div><div lang="en">
	 * Indicates performing linear interpolation or interpolation with the value of
	 * a previous Row on Columns.
	 *
	 * <p>The Column specified the interpolation function is linearly interpolated
	 * with the Row values before and after the target time of interpolation.
	 * The target Column must be of numeric type. </p>
	 *
	 * <p>The Columns not specified as interpolation targets are interpolated
	 * with adjacent Row values just before the target time of interpolation.
	 * Those Columns can be of any type. </p>
	 * </div>
	 */
	LINEAR_OR_PREVIOUS,

	/**
	 * <div lang="ja">
	 * 空の値を補間値として用いることを示します。
	 *
	 * <p>ロウキーを除くすべてのロウフィールドについて、{@link Container}にて
	 * 定義されている空の値を補間値として用いることを示します。</p>
	 * </div><div lang="en">
	 * Indicates using an empty value as an interpolated value.
	 *
	 * <p>It indicates that an empty value defined in {@link Container} is used as
	 * an interpolated value for all Row fields except Row keys.</p>
	 * </div>
	 */
	EMPTY

}
