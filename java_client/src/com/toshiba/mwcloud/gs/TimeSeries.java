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

import java.util.Date;
import java.util.Set;

/**
 * <div lang="ja">
 * 時刻をロウキーとする、時系列処理に特化したコンテナです。
 *
 * <p>一般的に、範囲取得や集計演算といった処理は、{@link Collection}よりも
 * 効率的な実装が選択されます。</p>
 *
 * <p>ロウ操作については、{@link Collection}と異なり一部制限が
 * 設けられています。
 * {@link TimeSeriesProperties}に基づき圧縮オプションが
 * 設定されている場合、次のロウ操作を行えません。</p>
 * <ul>
 * <li>指定ロウの更新</li>
 * <li>指定ロウの削除</li>
 * <li>指定時刻より新しい時刻のロウが存在する場合の、ロウの新規作成</li>
 * </ul>
 *
 * <p>{@link #query(String)}や{@link GridStore#multiGet(java.util.Map)}などより
 * 複数のロウの内容を一度に取得する場合、特に指定がなければ、
 * 返却されるロウの順序はロウキーの時刻を基準として{@link QueryOrder#ASCENDING}
 * 相当の順序に整列されます。</p>
 *
 * <p>ロック粒度は、1つ以上のロウ集合をひとまとまりとする内部格納
 * 単位となります。したがって、特定ロウについてロックする際、そのロウが属する
 * 内部格納単位上の他のロウも同時にロックしようとします。</p>
 * </div><div lang="en">
 * A specialized Container with a time-type Row key for TimeSeries data operation.
 *
 * <p>Generally, in extraction of a specific range and aggregation operations on TimeSeries,
 * more efficient implementation is selected than on {@link Collection}.</p>
 *
 * <p>There are some limitations on row operations unlike {@link Collection}.
 * If a compression option based on {@link TimeSeriesProperties}
 * has been set, the following operations cannot be performed.</p>
 * <ul>
 * <li>Update of the specified row</li>
 * <li>Deletion of the specified row</li>
 * <li>Creation of a new row that has an older timestamp key than the latest one.</li>
 * </ul>
 *
 * <p>If the order of Rows requested by {@link #query(String)} or {@link GridStore#multiGet(java.util.Map)}
 * is not specified, the Rows in a result set are sorted in ascending order of Row key.</p>
 *
 * <p>The granularity of locking is an internal storage unit, i.e., a set of one or more Rows.
 * Accordingly, when locking a specific Row, GridDB will attempt to lock other Rows in the
 * same internal storage unit as the Row.
</p>
 * </div>
 */
public interface TimeSeries<R> extends Container<Date, R> {

	/**
	 * <div lang="ja">
	 * GridDB上の現在時刻をロウキーとして、ロウを新規作成または更新します。
	 *
	 * <p>GridDB上の現在時刻に相当するTIMESTAMP値をロウキーとする点を除き、
	 * {@link #put(Date, Object)}と同様に振る舞います。
	 * 指定のロウオブジェクト内のロウキーは無視されます。</p>
	 *
	 * <p>圧縮オプションが設定された状態の時系列に対しては、
	 * GridDB上の現在時刻より新しい時刻のロウが存在しない場合のみ使用できます。
	 * 最も新しい時刻を持つ既存ロウの時刻が現在時刻と一致する場合、
	 * 何も変更は行わず既存ロウの内容を保持します。</p>
	 *
	 * <p>手動コミットモードの場合、対象のロウがロックされます。
	 * また、内部格納単位が同一の他のロウもロックされます。</p>
	 *
	 * @param row 新規作成または更新するロウの内容と対応するロウオブジェクト
	 *
	 * @return GridDB上の現在時刻と一致するロウが存在したかどうか
	 *
	 * @throws GSException この時系列について圧縮オプションが設定されており、
	 * 現在時刻より新しい時刻のロウが存在した場合
	 * @throws GSException この処理またはトランザクションのタイムアウト、
	 * このコンテナの削除もしくはスキーマ変更、接続障害が発生した場合、
	 * クローズ後に呼び出された場合、またはサポート範囲外の値がロウオブジェクトに
	 * 含まれていた場合
	 * @throws ClassCastException 指定のキーもしくはロウオブジェクトと、マッピング処理で
	 * 使用される型との間で対応しないものがある場合
	 * @throws NullPointerException {@code row}に{@code null}が
	 * 指定された場合。また、ロウフィールドに対応するロウオブジェクト内の
	 * オブジェクトの中に、設定されていないものが存在した場合
	 *
	 * @see #put(Date, Object)
	 * @see TimeSeriesProperties#getCompressionMethod()
	 * </div><div lang="en">
	 * Newly creates or updates a Row with a Row key of the current time on GridDB.
	 *
	 * <p>It behaves in the same way as {@link #put(Date, Object)}, except that it sets as a Row key the TIMESTAMP
	 * value equivalent to the current time on GridDB. The Row key in the specified Row object is ignored.</p>
	 *
	 * <p>In the manual commit mode, the target Row is locked.
	 * Other Rows in the same internal storage unit are also locked.</p>
	 *
	 * @param row A Row object representing the content of a Row to be newly created or updated.
	 *
	 * @return TRUE if a Row is found whose timestamp is identical with the current time on GridDB.
	 *
	 * @throws GSException if a timeout occurs during this operation or the transaction,
	 * this Container is deleted, its schema is changed or a connection failure occurs;
	 * or if called after the connection is closed; or if an unsupported value is set
	 * in the Row object.
	 * @throws ClassCastException if the specified key or Row object does not completely match the types used
	 * in mapping operation.
	 * @throws NullPointerException if {@code null} is specified as {@code row}; or if the Row object
	 * lacks some object corresponding to a Row field.
	 *
	 * @see #put(Date, Object)
	 * @see TimeSeriesProperties#getCompressionMethod()
	 * </div>
	 */
	public boolean append(R row) throws GSException;

	/**
	 * <div lang="ja">
	 * 必要に応じ別途ロウキーを指定して、ロウを新規作成または更新します。
	 *
	 * <p>{@code key}により別途ロウキーを指定した場合はその値を、
	 * 指定しない場合は指定のロウオブジェクト内のロウキーを基にロウを新規作成します。</p>
	 *
	 * <p>圧縮オプションが設定された状態の時系列に対しては、
	 * 最も新しい時刻を持つ既存ロウより新しい時刻のロウのみを新規作成できます。
	 * 最も新しい時刻を持つ既存ロウの時刻が指定の時刻と一致する場合、
	 * 何も変更は行わず既存ロウの内容を保持します。</p>
	 *
	 * <p>手動コミットモードの場合、対象のロウがロックされます。
	 * また、内部格納単位が同一の他のロウもロックされます。</p>
	 *
	 * @return 指定のロウキーと一致するロウが存在したかどうか
	 *
	 * @throws GSException この時系列について圧縮オプションが設定されており、
	 * 指定時刻より新しい時刻のロウが存在した場合
	 * @throws GSException この処理またはトランザクションのタイムアウト、
	 * このコンテナの削除もしくはスキーマ変更、接続障害が発生した場合、
	 * クローズ後に呼び出された場合、またはサポート範囲外の値がキーまたはロウオブジェクトに
	 * 含まれていた場合
	 * @throws ClassCastException 指定のキーもしくはロウオブジェクトと、マッピング処理で
	 * 使用される型との間で対応しないものがある場合
	 * @throws NullPointerException {@code row}に{@code null}が
	 * 指定された場合。また、ロウフィールドに対応するロウオブジェクト内の
	 * オブジェクトの中に、設定されていないものが存在した場合
	 *
	 * @see Container#put(Object, Object)
	 * @see TimeSeriesProperties#getCompressionMethod()
	 * </div><div lang="en">
	 * Newly creates or updates a Row, based on the specified Row object and also the Row key specified as needed.
	 *
	 * <p>It newly creates a Row, based on the value of the Row key specified as {@code key} or the Row key in
	 * the specified Row object if {@code key} is not specified.</p>
	 *
	 * <p>Only rows that have a newer timestamp key than any
	 * existing row can be created. If the timestamp is equal to
	 * the newest one, no update occurs and the existing row
	 * is held.</p>
	 *
	 * <p>In the manual commit mode, the target Row is locked. Other Rows in the same internal storage unit
	 * are also locked.</p>
	 *
	 * @return TRUE if a Row is found whose timestamp is identical with the specified time.
	 *
	 * @throws GSException if a timeout occurs during this operation or the transaction,
	 * this Container is deleted, its schema is changed or a connection failure occurs;
	 * or if called after the connection is closed; or if an unsupported value is set
	 * in the Row object.
	 * @throws ClassCastException if the specified key or Row object does not completely match the types used
	 * in mapping operation.
	 * @throws NullPointerException if {@code null} is specified as {@code row}; or if the Row object
	 * lacks some object corresponding to a Row field.
	 *
	 * @see Container#put(Object, Object)
	 * @see TimeSeriesProperties#getCompressionMethod()
	 * </div>
	 */
	public boolean put(Date key, R row) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のオプションに従い、ロウキーに対応するロウの内容を取得します。
	 *
	 * <p>{@link Container#get(Object)}と同様です。ただし、ロウキーの型が
	 * 固定であるため、{@link ClassCastException}が送出されることはありません。</p>
	 *
	 * @return 対応するロウが存在したかどうか
	 *
	 * @throws GSException この処理またはトランザクションのタイムアウト、
	 * このコンテナの削除もしくはスキーマ変更、接続障害が発生した場合、
	 * クローズ後に呼び出された場合、またはサポート範囲外の値がキーとして
	 * 設定されていた場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 *
	 * @see Container#get(Object)
	 * </div><div lang="en">
	 * Returns the content of a Row corresponding to the Row key, based on the specified option.
	 *
	 * <p>It behaves in the same way as {@link Container#get(Object)}. However, since the type of a Row key is
	 * predetermined, {@link ClassCastException} will not be thrown.</p>
	 *
	 * @return TRUE if a corresponding Row exists.
	 *
	 * @throws GSException if a timeout occurs during this operation or the transaction, this Container is deleted,
	 * its schema is changed or a connection failure occurs; or if called after the connection is closed;
	 * or if an unsupported value is specified as {@code key}.
	 * @throws NullPointerException if a {@code null} parameter is specified.
	 *
	 * @see Container#get(Object)
	 * </div>
	 */
	public R get(Date key) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のロウキーに対応するロウを削除します。
	 *
	 * <p>ロウキーに対応するカラムが存在する場合のみ使用できます。
	 * 対応するロウが存在しない場合は何も変更しません。</p>
	 *
	 * <p>圧縮オプションが設定された状態の時系列に対しては使用できません。</p>
	 *
	 * <p>手動コミットモードの場合、対象のロウはロックされます。</p>
	 *
	 * @return 対応するロウが存在したかどうか
	 *
	 * @throws GSException ロウキーに対応するカラムが存在しない場合
	 * @throws GSException この時系列について圧縮オプションが設定されていた場合
	 * @throws GSException この処理またはトランザクションのタイムアウト、
	 * このコンテナの削除もしくはスキーマ変更、接続障害が発生した場合、
	 * クローズ後に呼び出された場合、またはサポート範囲外の値がキーとして指定された場合
	 * @throws ClassCastException 指定のロウキーがマッピング処理で使用される
	 * ロウキーの型と対応しない場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 *
	 * @see Container#remove(Object)
	 * @see TimeSeriesProperties#getCompressionMethod()
	 * </div><div lang="en">
	 * Deletes a Row corresponding to the specified Row key.
	 *
	 * <p>It can be used only if a Column exists which corresponds to a specified Row key.
	 * If no corresponding Row exists, nothing is changed.</p>
	 *
	 * <p>It can not be used for time series that has a setting of
	 * compression option.</p>
	 *
	 * <p>In the manual commit mode, the target Row is locked.</p>
	 *
	 * @return TRUE if a corresponding Row exists.
	 *
	 * @throws GSException if no Column exists which corresponds to the specified Row key.
	 * @throws GSException if a timeout occurs during this operation or the transaction, this Container is deleted,
	 * its schema is changed or a connection failure occurs; or if called after the connection is closed;
	 * or if an unsupported value is specified as {@code key}.
	 * @throws ClassCastException if the specified Row key does not match the type of a Row key used in mapping operation.
	 * @throws NullPointerException if a {@code null} parameter is specified.
	 *
	 * @see Container#remove(Object)
	 * @see TimeSeriesProperties#getCompressionMethod()
	 * </div>
	 */
	public boolean remove(Date key) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定の時刻を基準として、関係する1つのロウを取得します。
	 *
	 * @param base 基準となる時刻
	 * @param timeOp 取得方法
	 *
	 * @return 条件に一致するロウ。存在しない場合は{@code null}
	 *
	 * @throws GSException この処理またはトランザクションのタイムアウト、
	 * このコンテナの削除もしくはスキーマ変更、接続障害が発生した場合、
	 * クローズ後に呼び出された場合、またはサポート範囲外の値が基準時刻として
	 * 指定された場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Returns one Row related with the specified time.
	 *
	 * @param base a base time point
	 * @param timeOp what to obtain
	 *
	 * @return The Row(s) satisfying the conditions; or {@code null} if no such Row is found.
	 *
	 * @throws GSException if a timeout occurs during this operation or the transaction, this Container is deleted,
	 * its schema is changed or a connection failure occurs; or if called after the connection is closed;
	 * or if an unsupported value is specified as the base time point.
	 * @throws NullPointerException if a {@code null} parameter is specified.
	 * </div>
	 */
	public R get(Date base, TimeOperator timeOp) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定時刻に相当するロウオブジェクトについて、線形補間などを行い生成します。
	 *
	 * <p>一致する時系列ロウの指定のカラムの値、もしくは、前後時刻の
	 * ロウの指定カラムの値を線形補間して得られた値を基にロウオブジェクトを生成します。
	 * 前後時刻のロウの少なくともいずれか、もしくは、一致するロウが存在しない場合は
	 * 生成されません。</p>
	 *
	 * <p>補間対象として指定できるカラムの型は、数値型のみです。
	 * 指定のカラムならびにロウキー以外のフィールドには、指定時刻と同一または
	 * より前の時刻のロウのうち、最も新しい時刻を持つロウのフィールドの値が
	 * 設定されます。</p>
	 *
	 * @param base 基準となる時刻
	 * @param column 線形補間対象のカラム
	 *
	 * @throws GSException 対応する名前のカラムが存在しない場合。
	 * また、サポートされていない型のカラムが指定された場合
	 * @throws GSException この処理またはトランザクションのタイムアウト、
	 * このコンテナの削除もしくはスキーマ変更、接続障害が発生した場合、
	 * クローズ後に呼び出された場合、またはサポート範囲外の値が基準時刻として
	 * 指定された場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Performs linear interpolation etc. of a Row object corresponding to the specified time.
	 *
	 * <p>It creates a Row object, based on the value of the specified Column of the TimeSeries Row identical with
	 * the base time or the value obtained by linearly interpolating the values of the specified Columns of adjacent
	 * Rows around the base time. If there is no Row with the same timestamp as the base time nor no Row with an earlier
	 * or later timestamp, no Row object is created.</p>
	 *
	 * <p>Only Columns of numeric type can be interpolated. The field values of the Row whose timestamp is identical
	 * with the specified timestamp or the latest among those with earlier timestamps are set on the specified Column
	 * and the fields other than a Row key.</p>
	 *
	 * @param base a base time point
	 * @param column A Column to be interpolated
	 *
	 * @throws GSException if no Column has the specified name; or if an unsupported type of Column is specified.
	 * @throws GSException if a timeout occurs during this operation or the transaction, this Container is deleted,
	 * its schema is changed or a connection failure occurs; or if called after the connection is closed; or if
	 * an unsupported value is specified as the base time point.
	 * @throws NullPointerException if a {@code null} parameter is specified.
	 * </div>
	 */
	public R interpolate(Date base, String column) throws GSException;

	/**
	 * <div lang="ja">
	 * 開始時刻・終了時刻を指定して、特定範囲のロウ集合を求めるクエリを作成します。
	 *
	 * <p>取得対象には、開始・終了時刻と合致する境界上のロウも含まれます。
	 * 終了時刻より新しい開始時刻を指定した場合、すべてのロウが対象外となります。
	 * 要求するロウ集合は昇順、すなわち古い時刻から新しい時刻の順となります。</p>
	 *
	 * <p>{@link Query#fetch(boolean)}を通じてロウ集合を求める際、
	 * 更新用ロックのオプションを有効にすることもできます。</p>
	 *
	 * @param start 開始時刻または{@code null}。{@code null}の場合、
	 * この時系列上の最も古いロウの時刻が開始時刻として指定されたものとみなす
	 * @param end 終了時刻または{@code null}。{@code null}の場合、
	 * この時系列上の最も新しいロウの時刻が終了時刻として指定されたものとみなす
	 *
	 * @throws GSException 現バージョンでは送出されない
	 * </div><div lang="en">
	 * Creates a query to obtain a set of Rows within a specific range between the specified start and end times.
	 *
	 * <p>The boundary Rows whose timestamps are identical with the start or end time are included in the range of
	 * fetch. If the specified start time is later than the end time, all Rows are excluded from the range. The Rows
	 * in a result set are sorted in ascending order, i.e., from an earlier timestamp to a later timestamp.</p>
	 *
	 * <p>The option of locking for update can be enabled when obtaining a set of Rows using
	 * {@link Query#fetch(boolean)}.</p>
	 *
	 * @param start start time or {@code null}. A {@code null} value indicates the timestamp of the oldest Row in
	 * this TimeSeries.
	 * @param end end time or {@code null}. A {@code null} value indicates the timestamp of the newest Row in this TimeSeries.
	 *
	 * @throws GSException It will not be thrown in the current version.
	 * </div>
	 */
	public Query<R> query(Date start, Date end) throws GSException;

	/**
	 * <div lang="ja">
	 * 開始時刻・終了時刻・順序を指定して、特定範囲のロウ集合を求めるクエリを作成します。
	 *
	 * <p>取得対象には、開始・終了時刻と合致する境界上のロウも含まれます。
	 * 終了時刻より新しい開始時刻を指定した場合、すべてのロウが対象外となります。</p>
	 *
	 * <p>{@link Query#fetch(boolean)}を通じてロウ集合を求める際、
	 * 更新用ロックのオプションを有効にすることもできます。</p>
	 *
	 * <p>{@code null}を指定できない引数で{@code null}を指定したことによる
	 * {@link NullPointerException}は送出されません。引数に誤りがあった場合、
	 * 得られたクエリをフェッチする際に例外が送出されます。</p>
	 *
	 * @param start 開始時刻または{@code null}。{@code null}の場合、
	 * この時系列上の最も古いロウの時刻が開始時刻として指定されたものとみなす
	 * @param end 終了時刻または{@code null}。{@code null}の場合、
	 * この時系列上の最も新しいロウの時刻が終了時刻として指定されたものとみなす
	 * @param order 取得するロウ集合の時刻順序。{@code null}は指定できない。
	 * {@link QueryOrder#ASCENDING}の場合は古い時刻から新しい時刻の順、
	 * {@link QueryOrder#DESCENDING}の場合は新しい時刻から古い時刻の順
	 * となる
	 *
	 * @throws GSException 現バージョンでは送出されない
	 * </div><div lang="en">
	 * Creates a query to obtain a set of Rows sorted in the specified order within a specific range between
	 * the specified start and end times.
	 *
	 * <p>The boundary Rows whose timestamps are identical with the start or end time are included in the range
	 * of fetch. If the specified start time is later than the end time, all Rows are excluded from the range.</p>
	 *
	 * <p>The option of locking for update can be enabled when obtaining a set of Rows using
	 * {@link Query#fetch(boolean)}.</p>
	 *
	 * <p>For arguments that cannot specify {@code NULL}, depending on the {@code NULL} status,
	 * {@link NullPointerException} may not be dispatched. If there is an error in the argument,
	 * an exception will be thrown when the query result is fetched. </p>
	 *
	 * @param start start time or {@code null}. A {@code null} value indicates the timestamp of the oldest Row in
	 * this TimeSeries.
	 * @param end end time or {@code null}. A value indicates the timestamp of the newest Row in this TimeSeries.
	 * @param order the time order of Rows in a result set. {@code null} cannot be specified
	 * {@link QueryOrder#ASCENDING} indicates the order from older to newer, and {@link QueryOrder#DESCENDING} indicates
	 * the order from newer to older.
	 *
	 * @throws GSException It will not be thrown in the current version.
	 * </div>
	 */
	public Query<R> query(
			Date start, Date end, QueryOrder order) throws GSException;

	/**
	 * <div lang="ja">
	 * 特定範囲のロウ集合をサンプリングするクエリを作成します。
	 *
	 * <p>サンプリング対象の時刻は、開始時刻に対し非負整数倍のサンプリング
	 * 間隔を加えた時刻のうち、終了時刻と同じかそれ以前のもののみです。
	 * 終了時刻より新しい開始時刻を指定した場合、すべてのロウが対象外となります。</p>
	 *
	 * <p>作成したクエリを実行すると、各サンプリング位置の時刻と一致するロウが
	 * 存在する場合は該当ロウの値を、存在しない場合は{@code columnSet}と
	 * {@code mode}引数の指定に従い補間された値を使用しロウ集合を生成します。
	 * 個別の補間方法については、{@link InterpolationMode}の定義を
	 * 参照してください。</p>
	 *
	 * <p>補間のために参照する必要のあるロウが存在しない場合、
	 * 該当するサンプリング時刻のロウは生成されず、該当箇所の数だけ
	 * 結果件数が減少します。
	 * サンプリング間隔をより短く設定すると、補間方法次第では
	 * 異なるサンプリング時刻であっても同一のロウの内容が使用される
	 * 可能性が高まります。</p>
	 *
	 * <p>{@link Query#fetch(boolean)}を通じてロウ集合を求める際、
	 * 更新用ロックのオプションを有効にすることはできません。</p>
	 *
	 * <p>現バージョンでは、{@link GSException}や、{@code null}を指定できない
	 * 引数で{@code null}を指定したことによる{@link NullPointerException}は
	 * 送出されません。引数に誤りがあった場合、得られたクエリをフェッチする
	 * 際に例外が送出されます。</p>
	 *
	 * @param start 開始時刻。{@code null}は指定できない
	 * @param end 終了時刻。{@code null}は指定できない
	 * @param columnSet {@code mode}に基づき特定の補間処理を適用する
	 * カラムの名前の集合。空集合の場合は適用対象のカラムを何も指定しないことを示す。
	 * {@code null}の場合は空集合を指定した場合と同様
	 * @param mode 補間方法。{@code null}は指定できない
	 * @param interval サンプリング間隔。{@code 0}および負の値は指定できない
	 * @param intervalUnit サンプリング間隔の時間単位。
	 * {@link TimeUnit#YEAR}、{@link TimeUnit#MONTH}は指定できない。
	 * また、{@code null}は指定できない
	 *
	 * @throws GSException 現バージョンでは送出されない
	 * </div><div lang="en">
	 * Creates a query to take a sampling of Rows within a specific range.
	 *
	 * <p>Each sampling time point is defined by adding a sampling interval multiplied by a non-negative integer to
	 * the start time, excluding the time points later than the end time. If the specified start time is later than
	 * the end time, all Rows are excluded from the range.</p>
	 *
	 * <p>If executed, the query creates a set of Rows, using the values of the Rows whose timestamps are identical
	 * with each sampling time point, or the interpolated values according to the parameters {@code columnSet} and
	 * {@code mode} if such a Row is not found. For specific interpolation methods, see the description of
	 * {@link InterpolationMode}.
	 *
	 * <p>If there is no Rows to be referenced for interpolation at a specific sampling time point, a corresponding
	 * Row is not generated, and thus the number of results returned is reduced by the number of such time points.
	 * A shorter sampling interval increases the likelihood that identical Row field values will be used even at
	 * different sampling time points, depending on the interpolation method.</p>
	 *
	 * <p>The option of locking for update cannot be enabled when obtaining a set of Rows using {@link Query#fetch(boolean)}.
	 *
	 * <p>In the current version, for arguments that cannot specify {@link GSException} or {@code NULL},
	 * depending on the {@code NULL} status, {@link NullPointerException} may not be dispatched.
	 * If there is an error in the argument, an exception will be thrown when the query result is fetched. </p>
	 *
	 * @param start start time. {@code null} cannot be specified
	 * @param end end time. {@code null} cannot be specified
	 * @param columnSet a set of names of the Columns to be interpolated according to {@code mode}. An empty set
	 * indicates no specification of target Columns. A {@code null} value indicates the same as an empty set.
	 * @param mode an interpolation method. {@code null} cannot be specified
	 * @param interval a sampling interval.{@code 0} or a negative value cannot be specified.
	 * @param intervalUnit the time unit of the sampling interval.
	 * {@link TimeUnit#YEAR} and {@link TimeUnit#MONTH} cannot be specified. {@code null} cannot be specified
	 *
	 * @throws GSException It will not be thrown in the current version.
	 * </div>
	 */
	public Query<R> query(
			Date start, Date end,
			Set<String> columnSet, InterpolationMode mode,
			int interval, TimeUnit intervalUnit)
			throws GSException;

	/**
	 * <div lang="ja">
	 * {@link InterpolationMode#LINEAR_OR_PREVIOUS}の補間方法を用い、
	 * 特定範囲のロウ集合をサンプリングするクエリを作成するための、
	 * 廃止されたメソッドです。
	 *
	 * @deprecated
	 * {@link #query(Date, Date, Set, InterpolationMode, int, TimeUnit)}に
	 * 置き換えられました。
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	public Query<R> query(
			Date start, Date end, Set<String> columnSet,
			int interval, TimeUnit intervalUnit)
			throws GSException;

	/**
	 * <div lang="ja">
	 * 開始・終了時刻指定を指定して、 ロウ集合またはその特定のカラムに対し
	 * 集計演算を行います。
	 *
	 * <p>{@code column}は、{@code aggregation}次第で無視されることが
	 * あります。演算対象には、開始・終了時刻と合致する境界上のロウも含まれます。
	 * 終了時刻より新しい開始時刻を指定した場合、すべてのロウが対象外となります。</p>
	 *
	 * @param start 開始時刻
	 * @param end 終了時刻
	 * @param column 集計対象のカラム名。合計演算のように、特定のカラムを
	 * 対象としない場合は{@code null}
	 * @param aggregation 集計方法
	 *
	 * @return 集計結果が設定された場合、対応する{@link AggregationResult}。
	 * 設定されなかった場合は{@code null}。詳細は{@link Aggregation}の定義を
	 * 参照のこと
	 *
	 * @throws GSException 指定の演算方法で許可されていない型のカラムを
	 * 指定した場合
	 * @throws GSException この処理またはトランザクションのタイムアウト、
	 * このコンテナの削除もしくはスキーマ変更、接続障害が発生した場合、
	 * クローズ後に呼び出された場合
	 * @throws NullPointerException {@code start}、{@code end}、
	 * {@code aggregation}に{@code null}が指定された場合
	 *
	 * @see Aggregation
	 * </div><div lang="en">
	 * Performs an aggregation operation on a Row set or its specific Columns, based on the specified start and end times.
	 *
	 * <p>The parameter {@code column} might be ignored depending on the parameter {@code aggregation}.
	 *
	 * The boundary Rows whose timestamps are identical with the start or end time are included in the range of operation.
	 * If the specified start time is later than the end time, all Rows are excluded from the range.</p>
	 *
	 * @param start Start time
	 * @param end End time
	 * @param column The name of a target Column.
	 * Specify {@code null} if the specified aggregation method does not target a specific Column.
	 * @param aggregation An aggregation method
	 *
	 * @return {@link AggregationResult} if the aggregation result is stored in it, or {@code null} if not.
	 * See the description of {@link AggregationResult} for more information.
	 *
	 * @throws GSException if the type of the specified Column is not supported by the specified aggregation method.
	 * @throws GSException if a timeout occurs during this operation or the transaction, this Container is deleted,
	 * its schema is changed, or a connection failure occurs; or if called after the connection is closed.
	 * @throws NullPointerException if {@code null} is specified as {@code start}, {@code end}, or {@code aggregation}.
	 *
	 * @see Aggregation
	 * </div>
	 */
	public AggregationResult aggregate(
			Date start, Date end, String column, Aggregation aggregation)
			throws GSException;

	/**
	 * <div lang="ja">
	 * {@link TimeSeries}ならびにその型パラメータと結びつく
	 * {@link Container.BindType}を構築するための、補助クラスです。
	 *
	 * @see Container.BindType
	 *
	 * @since 4.3
	 * </div><div lang="en">
	 * Auxiliary class for configuring {@link Container.BindType} which is associated
	 * with {@link TimeSeries} and its type parameters.
	 *
	 * @see Container.BindType
	 *
	 * @since 4.3
	 * </div>
	 */
	public static class BindType {

		private BindType() {
		}

		/**
		 * <div lang="ja">
		 * 指定のロウオブジェクト型、ならびに、{@link TimeSeries}と結びつく
		 * {@link Container.BindType}を取得します。
		 *
		 * @param <R> ロウオブジェクトの型
		 * @param rowClass ロウオブジェクトの型に対応するクラスオブジェクト
		 *
		 * @throws GSException ロウキーの型と、ロウオブジェクトの型との間で
		 * 不整合を検出した場合
		 *
		 * @since 4.3
		 * </div><div lang="en">
		 * Returns the specified Row object type and {@link Container.BindType} associated
		 * with {@link TimeSeries}.
		 *
		 * @param <R> the type of a Row object
		 * @param rowClass Class object corresponding to the type of the Row object
		 *
		 * @throws GSException if an inconsistency is detected between the type of Row
		 * key and the type of Row object
		 *
		 * @since 4.3
		 * </div>
		 */
		public static <R> Container.BindType<
		Date, R, ? extends TimeSeries<R>> of(Class<R> rowClass)
				throws GSException {
			return new Container.BindType<Date, R, TimeSeries<R>>(
					Date.class, rowClass, TimeSeries.class);
		}

	}

}
