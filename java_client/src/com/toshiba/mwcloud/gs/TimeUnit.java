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
 * 時系列処理で用いる時間の単位を示します。
 * </div><div lang="en">
 * Represents the time unit(s) used in TimeSeries data operation.
 * </div>
 */
public enum TimeUnit {

	/**
	 * <div lang="ja">
	 * 年単位であることを示します。
	 * </div><div lang="en">
	 * </div>
	 */
	YEAR,

	/**
	 * <div lang="ja">
	 * 月単位であることを示します。
	 * </div><div lang="en">
	 * </div>
	 */
	MONTH,

	/**
	 * <div lang="ja">
	 * 日単位であることを示します。
	 * </div><div lang="en">
	 * </div>
	 */
	DAY,

	/**
	 * <div lang="ja">
	 * 時間単位であることを示します。
	 * </div><div lang="en">
	 * </div>
	 */
	HOUR,

	/**
	 * <div lang="ja">
	 * 分単位であることを示します。
	 * </div><div lang="en">
	 * </div>
	 */
	MINUTE,

	/**
	 * <div lang="ja">
	 * 秒単位であることを示します。
	 * </div><div lang="en">
	 * </div>
	 */
	SECOND,

	/**
	 * <div lang="ja">
	 * ミリ秒単位であることを示します。
	 * </div><div lang="en">
	 * </div>
	 */
	MILLISECOND,

	/**
	 * <div lang="ja">
	 * マイクロ秒単位であることを示します。
	 * @since 5.3
	 * </div><div lang="en">
	 * @since 5.3
	 * </div>
	 */
	MICROSECOND,

	/**
	 * <div lang="ja">
	 * ナノ秒単位であることを示します。
	 * @since 5.3
	 * </div><div lang="en">
	 * @since 5.3
	 * </div>
	 */
	NANOSECOND

}
