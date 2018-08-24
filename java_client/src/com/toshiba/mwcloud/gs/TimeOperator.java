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
 * {@link TimeSeries}のキー時刻に基づく、ロウの特定方法を表します。
 *
 * <p>別途指定する時刻と組み合わせることで、最も近い時刻のキーを持つロウなどを
 * 特定できます。該当するロウが存在しない場合の扱いは、この列挙型を使用する
 * それぞれの機能により異なります。</p>
 * </div><div lang="en">
 * Represents how to specify a Row based on a key timestamp of {@link TimeSeries}</a>.
 *
 * <p>It can be used in combination with the timestamp (specified separately) to specify
 * a Row with the nearest timestamp etc. When no relevant Row exists,
 * it behaves differently depending on the function using this enumeration type.</p>
 * </div>
 */
public enum TimeOperator {

	/**
	 * <div lang="ja">
	 * 指定時刻と同一またはより前の時刻のロウのうち、最も新しいものを求めます。
	 * </div><div lang="en">
	 * Returns the newest among the Rows whose timestamp are identical with or earlier than the specified time.
	 * </div>
	 */
	PREVIOUS,

	/**
	 * <div lang="ja">
	 * 指定より前の時刻のロウのうち、最も新しいものを求めます。
	 * </div><div lang="en">
	 * Returns the newest among the Rows whose timestamp are earlier than the specified time.
	 * </div>
	 */
	PREVIOUS_ONLY,

	/**
	 * <div lang="ja">
	 * 指定時刻同一またはまたはより後の時刻のロウのうち、最も古いものを求めます。
	 * </div><div lang="en">
	 * Returns the oldest among the Rows whose timestamp are identical with or later than the specified time.
	 * </div>
	 */
	NEXT,

	/**
	 * <div lang="ja">
	 * 指定時刻より後の時刻のロウのうち、最も古いものを求めます。
	 * </div><div lang="en">
	 * Returns the oldest among the Rows whose timestamp are later than the specified time.
	 * </div>
	 */
	NEXT_ONLY,

}
