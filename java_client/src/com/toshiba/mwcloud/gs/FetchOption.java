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
 * クエリ実行結果を取得する際のオプション項目です。
 * </div><div lang="en">
 * The options for fetching the result of a query.
 * </div>
 */
public enum FetchOption {

	/**
	 * <div lang="ja">
	 * 取得するロウの数の最大値を設定するために使用します。
	 *
	 * <p>実行結果のロウ数が最大値を超えた場合、{@link RowSet}で得られる順番で
	 * 先頭から最大値の分だけが取得できます。それ以降のロウは取得できません。</p>
	 *
	 * <p>サポートされる設定値の型は、{@link Integer}または{@link Long}です。
	 * 負の値は指定できません。設定が省略された場合、上限は設定されません。</p>
	 * </div><div lang="en">
	 * Used to set the maximum number of Rows to be fetched.
	 *
	 * <p>If the number of Rows in a result exceeds the maximum, the maximum number of
	 * Rows counting from the 0-th in {@link RowSet} are fetched. The rest of the Rows
	 * in the result cannot be fetched. </p>
	 *
	 * <p>The supported types of values are {@link Integer} and {@link Long}. Negative values are not available.
	 * If the setting is omitted, the limit is not defined. </p>
	 * </div>
	 */
	LIMIT,

	/**
	 * <div lang="ja">
	 * 非公開のオプションです。
	 *
	 * @deprecated
	 *
	 * <p>十分検証されていないため、意図しない挙動を示す可能性があります。</p>
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	SIZE,

	/**
	 * <div lang="ja">
	 * 部分実行モードを設定するために使用します。
	 *
	 * <p>部分実行モードでは、クエリの中間処理や結果送受信に用いる
	 * バッファのサイズなどがなるべく一定の範囲に収まるよう、必要に応じて
	 * 実行対象のデータ範囲を分割し、この部分範囲ごとに実行とフェッチを
	 * リクエストすることがあります。そのため、{@link RowSet}を取得した時点で
	 * 一部の範囲の結果が求まっていないことや、結果ロウを順に参照していく
	 * 段階で、残りの範囲を部分的に実行していくことがあります。</p>
	 *
	 * <p>部分実行モードは、現バージョンでは次の条件すべてを満たすクエリに
	 * 使用できます。また、{@link #LIMIT}オプションと併用することができます。
	 * 条件を満たさない場合でも、各種フェッチオプションの設定時点ではエラーを
	 * 検知しない場合があります。</p>
	 * <ul>
	 * <li>TQL文からなるクエリであること</li>
	 * <li>TQL文において、選択式が「*」のみからなり、ORDER BY節を含まない
	 * こと</li>
	 * <li>対応する{@link Container}が個々の部分的なクエリ実行時点において
	 * 常に自動コミットモードに設定されていること</li>
	 * </ul>
	 *
	 * <p>部分実行モードでは、対応する {@link Container}のトランザクション
	 * 分離レベルや状態に基づき、個々の部分的なクエリ実行時点において
	 * 参照可能なロウが使用されます。ただし、クエリ全体の実行開始時点で
	 * 存在しないロウは、実行対象から外れる場合があります。</p>
	 *
	 * <p>部分実行モードを有効にした場合に{@link RowSet}に対して使用
	 * できない操作や特有の挙動については、個別の定義を参照してください。</p>
	 *
	 * <p>サポートされる設定値の型は、{@link Boolean}のみです。
	 * 部分実行モードを有効にするには、{@link Boolean#TRUE}と一致する値を
	 * 指定します。現バージョンでは、未設定の場合には部分実行モードを有効に
	 * しません。</p>
	 *
	 * @since 4.0
	 * </div><div lang="en">
	 * Used to set the partial execution mode.
	 *
	 * <p>In the partial execution mode, it is trying for the
	 * buffer size of the intermediate query processing and the
	 * data transfer, etc. to fit inside a fixed size by dividing
	 * the target data and getting the query results in each
	 * divided range. Therefore the results for some data ranges may
	 * not be determined when the {@link RowSet} is obtained,
	 * and in the middle of getting the results,
	 * there are the cases that the query is executed partially
	 * for the rest of the ranges.</p>
	 *
	 * <p>In this version, the partial execution mode can be used
	 * for queries satisfying all the following conditions.
	 * And it can be used in combination with {@link #LIMIT}
	 * option. Even if not satisfying the conditions, the error may
	 * not be detected when setting the fetch option.</p>
	 * <ul>
	 * <li>The query must be specified by TQL</li>
	 * <li>The SELECT clause must be consisted of only '*' and an
	 * ORDER BY clause must not be specified.</li>
	 * <li>The target {@link Container} must have been set to the
	 * auto commit mode at the each partial execution of the
	 * query.</li>
	 * </ul>
	 *
	 * <p>In the partial execution mode, rows that can be fetched
	 * at the each partial execution of the query based on the
	 * separation level and the status of the corresponding
	 * {@link Container} transaction are used. However rows that
	 * don't exist at the first time of the whole query execution
	 * may not be reflected to the results.</p>
	 *
	 * <p>For inhibited operations and behaviors on {@link RowSet}
	 * in this mode, see the individual definitions.</p>
	 *
	 * <p>The only supported type for this setting is {@link Boolean}.
	 * The value matching to {@link Boolean#TRUE} must
	 * be specified to activate this mode. In this version, the
	 * partial execution mode is not effective unless setting the
	 * mode explicitly.</p>
	 *
	 * @since 4.0
	 * </div>
	 */
	PARTIAL_EXECUTION

}
