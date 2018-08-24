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

import java.io.Closeable;


/**
 * <div lang="ja">
 * 特定の{@link Container}に対応付けられたクエリを保持し、結果取得方法の設定
 * ならびに実行・結果取得を行う機能を持ちます。
 *
 * @param <R> 期待される取得結果の{@link RowSet}要素の型
 * </div><div lang="en">
 * Provides the functions of holding the information about a query related
 * to a specific {@link Container},
 * specifying the options for fetching and retrieving the result.
 *
 * @param <R> the type of {@link RowSet} elements to be obtained
 * </div>
 */
public interface Query<R> extends Closeable {

	/**
	 * <div lang="ja">
	 * 結果取得に関するオプションを設定します。
	 *
	 * <p>設定可能なオプション項目と値の定義については、{@link FetchOption}を
	 * 参照してください。</p>
	 *
	 * @param option オプション項目
	 * @param value オプションの値
	 *
	 * @throws GSException オプションの値がオプション項目と対応しない場合
	 * @throws GSException このオブジェクトまたは対応する{@link Container}の
	 * クローズ後に呼び出された場合
	 * @throws NullPointerException 1つ以上の引数に{@code null}が指定された場合
	 *
	 * @see FetchOption
	 * </div><div lang="en">
	 * Sets an option for a result set.
	 *
	 * <p>See {@link FetchOption} for the definitions of supported option items and values. </p>
	 *
	 * @param option an option item
	 * @param value an option value
	 *
	 * @throws GSException if the specified option value is invalid for the specified option item.
	 * @throws GSException if called after this object or the relevant {@link Container} is closed.
	 * @throws NullPointerException if a {@code null} parameter(s) is specified.
	 *
	 * @see FetchOption
	 * </div>
	 */
	public void setFetchOption(
			FetchOption option, Object value) throws GSException;

	/**
	 * <div lang="ja">
	 * このクエリを実行し、実行結果に対応するロウ集合を取得します。
	 *
	 * <p>更新用ロックを要求せずに{@link #fetch(boolean)}を呼び出した場合と
	 * 同様です。</p>
	 *
	 * @throws GSException 正しくないパラメータ・構文・命令を含むクエリを
	 * 実行しようとした場合。たとえば、TQLでは、関数の引数に対応しない型のカラムを
	 * 指定した場合。具体的な制約は、このクエリを作成する機能の各種定義を
	 * 参照のこと
	 * @throws GSException TQLを実行した際、実行結果が期待する結果
	 * {@link RowSet}の各要素の型と合致しない場合
	 * @throws GSException この処理または関連するトランザクションのタイムアウト、
	 * 対応するコンテナの削除もしくはスキーマ変更、接続障害が発生した場合
	 * @throws GSException このオブジェクトまたは対応する{@link Container}の
	 * クローズ後に呼び出された場合
	 * @throws NullPointerException このクエリを作成する際に与えられたパラメータの
	 * 中に、許容されない{@code null}が含まれていた場合。GridDB上で実行される
	 * TQL文の評価処理の結果として送出されることはない
	 *
	 * @see #fetch(boolean)
	 * </div><div lang="en">
	 * Executes a query and returns a set of Rows as an execution result.
	 *
	 * <p>It behaves in the same way as {@link #fetch(boolean)} is called
	 * without requesting a lock for update. </p>
	 *
	 * @throws GSException if the target query contains any wrong parameter, syntax,
	 * or directive. For example, in the case of TQL, if the type of a specified
	 * Column does not match the parameter of a function. For detailed restrictions,
	 * see the descriptions of the functions to create a query.
	 * @throws GSException if the requested type of {@link RowSet} elements does not match
	 * the type of the result set in executing a TQL query.
	 * @throws GSException if a timeout occurs during this operation or
	 * its related transaction, the relevant Container is deleted, its schema is
	 * changed or a connection failure occurs.
	 * @throws GSException if called after this object or the relevant {@link Container} is closed.
	 * @throws NullPointerException if an unsupported {@code null} parameter is
	 * specified when creating this query. It will not be thrown as a result
	 * of evaluating a TQL statement in GridDB.
	 *
	 * @see #fetch(boolean)
	 * </div>
	 */
	public RowSet<R> fetch() throws GSException;

	/**
	 * <div lang="ja">
	 * オプションを指定してこのクエリを実行し、実行結果に対応するロウ集合を取得します。
	 *
	 * <p>{@code forUpdate}に{@code true}が指定された場合、取得対象のロウ
	 * すべてをロックします。ロックすると、対応するトランザクションが有効である間、
	 * 他のトランザクションからの対象ロウに対する変更操作が阻止されます。
	 * 対応するコンテナの自動コミットモードが無効の場合のみ、指定できます。</p>
	 *
	 * <p>新たなロウ集合を取得すると、このクエリについて前回実行した結果の
	 * {@link RowSet}はクローズされます。</p>
	 *
	 * <p>一度に大量のロウを取得しようとした場合、GridDBノードが管理する
	 * 通信バッファのサイズの上限に到達し、失敗することがあります。
	 * 上限サイズについては、GridDBテクニカルリファレンスを参照してください。</p>
	 *
	 * @throws GSException 対応するコレクションの自動コミットモード有効であるにも
	 * かかわらず、{@code forUpdate}に{@code true}が指定された場合
	 * @throws GSException ロックできないクエリであるにもかかわらず、
	 * {@code forUpdate}に{@code true}が指定された場合。
	 * 具体的なロックの可否は、このクエリを作成する機能の各種定義を参照のこと
	 * @throws GSException 正しくないパラメータ・構文・命令・オプション設定を
	 * 含むクエリを実行しようとした場合。たとえば、TQLでは、関数の引数に対応
	 * しない型のカラムを指定した場合。具体的な制約は、このクエリを作成する
	 * 機能の各種定義を参照のこと
	 * @throws GSException TQLの実行時、求める{@link RowSet}の要素の型と
	 * 取得結果の型が一致しない場合
	 * @throws GSException この処理または関連するトランザクションのタイムアウト、
	 * 対応するコンテナの削除もしくはスキーマ変更、接続障害が発生した場合
	 * @throws GSException このオブジェクトまたは対応する{@link Container}の
	 * クローズ後に呼び出された場合
	 * @throws NullPointerException このクエリを作成する際に与えられたパラメータの
	 * 中に、許容されない{@code null}が含まれていた場合
	 * </div><div lang="en">
	 * Executes a query with the specified option and returns a set of Rows as an execution result.
	 *
	 * <p>It locks all target Rows if {@code true} is specified
	 * as {@code forUpdate}. If the target Rows are locked, update operations
	 * on the Rows by any other transactions are blocked while a relevant
	 * transaction is active. TRUE can be specified only if the autocommit mode
	 * is disabled on a relevant Container. </p>
	 *
	 * <p>If it is called to obtain new Rows, {@link RowSet} of the last result
	 * of this query will be closed.</p>
	 *
	 * <p>It will be failed if too much Rows are obtained, because of the data buffer
	 * size upper limit of GridDB node. See "System limiting values" in the GridDB Technical Reference about
	 * the buffer size upper limit.</p>
	 *
	 * @throws GSException if {@code true} is specified as {@code forUpdate} although the autocommit
	 * mode is enabled on a relevant Container.
	 * @throws GSException if {@code true} is specified as {@code forUpdate} for a query which
	 * cannot acquire a lock. For the availability of a lock, see the descriptions
	 * of the functions to create a query.
	 * @throws GSException if the target query contains any wrong parameter, syntax,
	 * directive or option setting. For example, in the case of TQL, if the type of a specified
	 * Column does not match the parameter of a function. For detailed restrictions,
	 * see the descriptions of the functions to create a query.
	 * @throws GSException if the requested type of {@link RowSet} elements
	 * does not match the type of the result set in executing a TQL query.
	 * @throws GSException if a timeout occurs during this operation or its related
	 * transaction, the relevant Container is deleted, its schema is changed or
	 * a connection failure occurs.
	 * @throws GSException if called after this object or the relevant {@link Container}
	 * is closed.
	 * @throws NullPointerException if an unsupported {@code null} parameter
	 * is specified when creating this query.
	 * </div>
	 */
	public RowSet<R> fetch(boolean forUpdate) throws GSException;

	/**
	 * <div lang="ja">
	 * 関連するリソースを適宜解放します。
	 *
	 * @throws GSException 現バージョンでは送出されない
	 *
	 * @see Closeable#close()
	 * </div><div lang="en">
	 * Releases related resources properly.
	 *
	 * @throws GSException not be thrown in the current version.
	 *
	 * @see Closeable#close()
	 * </div>
	 */
	public void close() throws GSException;

	/**
	 * <div lang="ja">
	 * 直近に実行した結果の{@link RowSet}を取得します。
	 *
	 * <p>一度取得すると、以降新たにこのクエリを実行するまで{@code null}が
	 * 返却されるようになります。</p>
	 *
	 * <p>{@link FetchOption#PARTIAL_EXECUTION}が有効に設定されていた場合、
	 * クエリ実行処理の続きを行う場合があります。</p>
	 *
	 * @return 直近に実行した結果の{@link RowSet}。取得済みの場合、
	 * もしくは、一度もクエリを実行したことのない場合は{@code null}
	 *
	 * @throws GSException この処理または関連するトランザクションのタイムアウト、
	 * 対応するコンテナの削除もしくはスキーマ変更、接続障害が発生した場合
	 * @throws GSException このオブジェクトまたは対応する{@link Container}の
	 * クローズ後に呼び出された場合
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Returns {@link RowSet} of the latest result.
	 *
	 * <p>When {@link FetchOption#PARTIAL_EXECUTION} has been set
	 * to be effective, the continuation of the query processing
	 * may be executed.</p>
	 *
	 * <p>After {@link RowSet} returned once, it returns {@code null} until the
	 * new query is executed.</p>
	 *
	 * @return {@link RowSet} of the latest result at the first time,
	 * or {@code null} from the second time or no query executed before.
	 *
	 * @throws GSException when there is a timeout of this processing or of related transaction, the target container has been deleted or its schema has been changed.
	 * @throws GSException if called after this object or the relevant {@link Container}
	 * is closed.
	 * </div>
	 */
	public RowSet<R> getRowSet() throws GSException;

}
