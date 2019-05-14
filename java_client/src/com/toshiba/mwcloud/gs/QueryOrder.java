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
 * クエリにおける要求ロウ順序を表します。
 *
 * <p>各種クエリ機能別に定義される判定対象を基準として、
 * 順序指定を行うために使用します。具体的な判定対象は、
 * 個別の機能によって異なります。</p>
 * </div><div lang="en">
 * Represents the order of Rows requested by a query.
 *
 * <p>It is used to specify the order of Rows targeted by each query function. Specific targets differ with individual
 * functions.</p>
 * </div>
 */
public enum QueryOrder {

	/**
	 * <div lang="ja">
	 * 要求ロウ順序が昇順であることを表します。
	 * </div><div lang="en">
	 * Indicates the requested order of Rows is an ascending order.
	 * </div>
	 */
	ASCENDING,

	/**
	 * <div lang="ja">
	 * 要求ロウ順序が降順であることを表します。
	 * </div><div lang="en">
	 * Indicates the requested order of Rows is a descending order.
	 * </div>
	 */
	DESCENDING

}
