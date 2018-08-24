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
 * クエリ実行より求めたロウの集合を管理します。
 *
 * <p>ロウ単位・ロウフィールド単位での操作機能を持ち、対象とするロウを
 * 指し示すための、{@link java.sql.ResultSet}と同様のカーソル状態を保持します。
 * 初期状態のカーソルは、ロウ集合の先頭より手前に位置しています。</p>
 * </div><div lang="en">
 * Manages a set of Rows obtained by executing a query.
 *
 * <p>It has a function of per-Row and per-Row-field manipulation and holds a cursor state similar to
 * {@link java.sql.ResultSet} to specify a target Row. The cursor is initially located just before the head of
 * a Row set.</p>
 * </div>
 */
public interface RowSet<R> extends Closeable {

	/**
	 * <div lang="ja">
	 * 現在のカーソル位置を基準として、ロウ集合内に後続のロウが存在するかどうかを
	 * 取得します。
	 *
	 * @throws GSException 現バージョンでは送出されない
	 * </div><div lang="en">
	 * Returns TRUE if a Row set has at least one Row ahead of the current cursor position.
	 *
	 * @throws GSException It will not be thrown in the current version.
	 * </div>
	 */
	public boolean hasNext() throws GSException;

	/**
	 * <div lang="ja">
	 * ロウ集合内の後続のロウにカーソル移動し、移動後の位置にあるロウオブジェクトを
	 * 取得します。
	 *
	 * <p>{@link FetchOption#PARTIAL_EXECUTION}が有効に設定されていた場合、
	 * クエリ実行処理の続きを行う場合があります。</p>
	 *
	 * @throws GSException 対象位置のロウが存在しない場合
	 * @throws GSException 接続障害によりロウオブジェクトの生成に失敗した場合
	 * @throws GSException この処理または関連するトランザクションのタイムアウト、
	 * 対応するコンテナの削除もしくはスキーマ変更、接続障害が発生した場合
	 * @throws GSException このオブジェクトまたは対応する{@link Container}の
	 * クローズ後に呼び出された場合
	 * </div><div lang="en">
	 * Moves the cursor to the next Row in a Row set and returns the Row object at the moved position.
	 *
	 * <p>When {@link FetchOption#PARTIAL_EXECUTION} has been set
	 * to be effective, the continuation of the query execution may
	 * be executed in this method.</p>
	 *
	 * @throws GSException if there is no Row at the cursor position.
	 * @throws GSException if a connection failure caused an error in creation of a Row object
	 * @throws GSException when there is a timeout of transaction for this processing or of related processing, the target container has been deleted, its schema has been changed or there is a connection failure.
	 * @throws GSException if called after this object or the relevant {@link Container} is closed.
	 * </div>
	 */
	public R next() throws GSException;

	/**
	 * <div lang="ja">
	 * 現在のカーソル位置のロウを削除します。
	 *
	 * <p>ロックを有効にして取得した{@link RowSet}に対してのみ使用できます。
	 * また、{@link Container#remove(Object)}と同様、
	 * コンテナの種別ならびに設定によっては、さらに制限が設けられています。</p>
	 *
	 * @throws GSException 対象位置のロウが存在しない場合
	 * @throws GSException ロックを有効にせずに取得した{@link RowSet}に対して
	 * 呼び出された場合
	 * @throws GSException 特定コンテナ固有の制限に反する操作を行った場合
	 * @throws GSException この処理または関連するトランザクションのタイムアウト、
	 * 対応するコンテナの削除もしくはスキーマ変更、接続障害が発生した場合
	 * @throws GSException このオブジェクトまたは対応する{@link Container}の
	 * クローズ後に呼び出された場合
	 * </div><div lang="en">
	 * Deletes the Row at the current cursor position.
	 *
	 * <p>It can be used only for {@link RowSet} obtained with locking enabled. Like {@link Container#remove(Object)},
	 * further limitations are placed depending on the type and settings of a Container.
	 *
	 * @throws GSException if there is no Row at the cursor position.
	 * @throws GSException If called on {@link RowSet} obtained without locking enabled.
	 * @throws GSException if its operation is contrary to the restrictions specific to a particular Container.
	 * @throws GSException if a timeout occurs during this operation or its related transaction, the relevant Container
	 * is deleted, its schema is changed or a connection failure occurs.
	 * @throws GSException if called after this object or the relevant {@link Container} is closed.
	 * </div>
	 */
	void remove() throws GSException;

	/**
	 * <div lang="ja">
	 * 現在のカーソル位置のロウについて、指定のロウオブジェクトを使用してロウキー
	 * 以外の値を更新します。
	 *
	 * <p>{@code null}は指定できません。指定のロウオブジェクトに含まれるロウキーは
	 * 無視されます。</p>
	 *
	 * <p>ロックを有効にして取得した{@link RowSet}に対してのみ使用できます。
	 * また、{@link Container#put(Object, Object)}と同様、
	 * コンテナの種別ならびに設定によっては、さらに制限が設けられています。</p>
	 *
	 * @throws GSException 対象位置のロウが存在しない場合
	 * @throws GSException ロックを有効にせずに取得した{@link RowSet}に対して
	 * 呼び出された場合
	 * @throws GSException 特定コンテナ固有の制限に反する操作を行った場合
	 * @throws GSException この処理または関連するトランザクションのタイムアウト、
	 * 対応するンテナの削除もしくはスキーマ変更、接続障害が発生した場合
	 * @throws GSException このオブジェクトまたは対応する{@link Container}の
	 * クローズ後に呼び出された場合
	 * @throws ClassCastException 指定のロウオブジェクトがマッピング処理で使用される
	 * ロウオブジェクトの型と対応しない場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合。
	 * また、ロウフィールドに対応するロウオブジェクト内のオブジェクトが1つ以上存在しない場合
	 * </div><div lang="en">
	 * Updates the values except a Row key of the Row at the cursor position, using the specified Row object.
	 *
	 * <p>{@code null} cannot be specified. The Row key contained in the specified Row object is ignored.</p>
	 *
	 * <p>It can be used only for{@link RowSet} obtained with locking enabled.
	 * Like {@link Container#put(Object, Object)}, further limitations are placed depending on the type and settings of
	 * a Container.
	 *
	 * @throws GSException if there is no Row at the cursor position.
	 * @throws GSException If called on {@link RowSet} obtained without locking enabled.
	 * @throws GSException if its operation is contrary to the restrictions specific to a particular Container.
	 * @throws GSException if a timeout occurs during this operation or its related transaction, the relevant Container
	 * is deleted, its schema is changed or a connection failure occurs.
	 * @throws GSException if called after this object or the relevant {@link Container} is closed.
	 * @throws ClassCastException if the specified Row object does not match the value types of the Row object used in
	 * mapping operation.
	 * @throws NullPointerException If a {@code null} parameter is specified;
	 * or if no object exists in the Row object which corresponds to the Row field.
	 * </div>
	 */
	public void update(R rowObj) throws GSException;

	/**
	 * <div lang="ja">
	 * サイズ、すなわちロウ集合作成時点におけるロウの数を取得します。
	 *
	 * <p>{@link FetchOption#PARTIAL_EXECUTION}が有効に設定されていた場合、
	 * クエリ実行処理の進行状況によらず、結果を求めることはできません。</p>
	 *
	 * @throws IllegalStateException オプション設定の影響によりロウの数を取得
	 * できない場合
	 *
	 * </div><div lang="en">
	 * Returns the size, namely the number of Rows at the time of creating a Row set.
	 *
	 * <p>If {@link FetchOption#PARTIAL_EXECUTION} has been set to
	 * be effective, the result cannot be obtained despite
	 * the status of the query processing progress.</p>
	 *
	 * @throws IllegalStateException if the number of rows cannot be obtained by the option setting.
	 * </div>
	 */
	public int size();

	/**
	 * <div lang="ja">
	 * 関連するリソースを適宜解放します。
	 *
	 * @throws GSException 現バージョンでは送出されない
	 *
	 * @see Closeable#close()
	 * </div><div lang="en">
	 * Releases related resources as necessary.
	 *
	 * @throws GSException It will not be thrown in the current version.
	 *
	 * @see Closeable#close()
	 * </div>
	 */
	public void close() throws GSException;

}
