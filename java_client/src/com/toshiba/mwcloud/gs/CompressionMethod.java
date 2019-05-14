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
 * 圧縮方式の種別を表します。
 *
 * <p>時系列圧縮設定を行う際に使用します。</p>
 * </div><div lang="en">
 * Represents types of compression methods.
 *
 * <p>These types are used when setting a time series compression.</p>
 * </div>
 */
public enum CompressionMethod {

	/**
	 * <div lang="ja">
	 * 無圧縮であることを示します。
	 * </div><div lang="en">
	 * Represents no compression.
	 * </div>
	 */
	NO,

	/**
	 * <div lang="ja">
	 * 誤差なし間引き圧縮方式であることを示します。
	 *
	 * <p>誤差なし間引き圧縮では、直前及び直後に登録したロウと同じデータを
	 * 持つロウは省かれます。
	 * 省かれたデータはinterpolateやsample処理の際に、誤差を発生することなく
	 * 復元されます。</p>
	 * </div><div lang="en">
	 * Represents a thinning compression without error
	 *
	 * <p>In this type of compression, rows that have the same
	 * value registered immediately before and after are thinned.
	 * The thinned values are recovered in the interpolation or sampling
	 * processing without information loss.</p>
	 * </div>
	 */
	SS,

	/**
	 * <div lang="ja">
	 * 誤差あり間引き圧縮方式であることを示します。
	 *
	 * <p>誤差あり間引き圧縮では、前回まで及び直後に登録したデータと同じ傾斜を
	 * 表すロウは省かれます。
	 * 同じ傾斜かを判定する条件はユーザが指定できます。
	 * 指定されたカラムが条件を満たし、それ以外のカラムの値が前回のデータと
	 * 同じ場合のみ省かれます。
	 * 省かれたデータはinterpolateやsample処理の際に、指定された誤差の範囲内で
	 * 復元されます。</p>
	 * </div><div lang="en">
	 * Represents a thinning compression with error
	 *
	 * <p>In this type of compression, values that represent
	 * gradient same as before and immediately after may be thinned.
	 * The algorithm to determine the equality of the gradients
	 * can be specified by users.
	 * Only if specified column meets the above conditions and
	 * other columns are the same as the last registered data, the
	 * row is thinned.
	 * The thinned value is recovered in an interpolation or
	 * sampling processing within the specified error range.</p>
	 * </div>
	 */
	HI

}
