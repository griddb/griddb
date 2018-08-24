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
import java.util.List;
import java.util.Map;


/**
 * <div lang="ja">
 * 接続したGridDBシステム内のデータベースに属するデータを操作するための機能を提供します。
 *
 * <p>コレクションや時系列といったコンテナの追加・削除・構成変更、ならびに、
 * コンテナを構成するロウの操作機能を提供します。</p>
 *
 * <p>コンテナ種別などの違いによらず、1つのデータベースのコンテナ間で、
 * ASCIIの大文字・小文字表記だけが異なる名前のものを複数定義することは
 * できません。コンテナ名は、ベースコンテナ名単独、もしくは、ベースコンテナ名の
 * 後ろにノードアフィニティ名をアットマーク「@」で連結した形式で表記します。
 * その他、コンテナの定義において使用できるコンテナ名の文字種や長さには制限が
 * あります。具体的には、GridDBテクニカルリファレンスを参照してください。
 * 特に記載のない限り、コンテナ名を指定する操作では、ASCIIの大文字・小文字
 * 表記の違いは区別されません。</p>
 *
 * <p>このインタフェースまたはこのインタフェースを通じて得られたインスタンスの
 * インタフェースが送出する{@link GSException}は、エラーに関する次のパラメータを
 * 含むことがあります。</p>
 * <table>
 * <thead><tr><th>パラメータ名</th><th>説明</th></tr></thead>
 * <tbody>
 * <tr><td>address</td><td>接続先クラスタノードのアドレス・ポート。ホスト名
 * またはIPアドレスとポート番号とをコロン「:」で連結した文字列により
 * 構成されます。このインタフェースまたはこのインタフェースを通じて得られた
 * インスタンスのインタフェースにおいて、クラスタへのアクセスを伴う操作を
 * 呼び出した際にエラーを検知すると、このパラメータを含むことがあります。
 * このパラメータを含む場合、パラメータが示すクラスタノードにおいてエラーの
 * 詳細が記録されていることがあります。</td></tr>
 * <tr><td>container</td><td>例外に関係しうるコンテナの名前。任意個数の
 * コンテナを一括して扱う操作において、そのうち少なくとも一つのコンテナに
 * ついての操作を行えないことが判明した場合に、このパラメータを含むことが
 * あります。任意個数のコンテナを扱う具体的な操作については、個々の
 * インタフェースの定義を参照してください。クラスタノードへのリクエスト
 * 準備段階でのリソース不足など、どのコンテナの問題か特定し切れないことが
 * あるため、どのようなエラーでもこのパラメータを含むとは限りません。
 * また、複数のコンテナについて操作できない可能性があったとしても、パラメータに
 * 含まれるのは高々一つのコンテナの名前のみです。</td></tr>
 * </tbody>
 * </table>
 *
 * <p>各メソッドのスレッド安全性は保証されません。</p>
 *
 * @see Collection
 * @see TimeSeries
 * @see Container
 * @see GSException#getParameters()
 * </div><div lang="en">
 * A function is provided for processing the data in the connected GridDB system.
 *
 * <p>A function to add, delete, or change the composition of Collection
 * and TimeSeries Containers as well as to process the Rows constituting
 * a Container is provided.</p>
 *
 * <p>Regardless of container types, etc., multiple container names
 * different only in uppercase and lowercase ASCII characters cannot
 * be defined in a database. A container name is represented by only
 * a base container name or by connecting the base name and a node
 * affinity name with '@'. See the GridDB Technical Reference for the
 * details. In the operations specifying a container name, uppercase
 * and lowercase ASCII characters are identified as the same unless
 * otherwise noted.</p>
 *
 * <p>The {@link GSException} thrown by this interface or the
 * interface of the instance which is acquired through this interface may contain the
 * following parameters related the error.</p>
 * <table>
 * <thead><tr><th>Parameter name</th><th>Description</th></tr></thead>
 * <tbody>
 * <tr><td>address</td><td>Address and port of connecting cluster
 * node. It is a string connecting the host name or the IP address
 * and the port number with a colon ":". In this interface or the
 * interface of the instance which is acquired through this interface,
 * when an error is detected in invoking an operation including a
 * cluster access, this parameter may be contained. In that case, the
 * details of the error may be logged in the cluster node shown by this
 * parameter.</td></tr>
 * <tr><td>container</td><td>The name of container which may relate
 * the exception. When operating an arbitrary number of containers and
 * detected that the operation cannot be performed for one of the
 * containers, this parameter may be contained. For instance of such
 * operations, see the definition of each interface. For such as
 * resource shortage in preparing requests to cluster nodes, it may
 * not be possible to determine which container is the cause, so this parameter
 * may not be contained in some error cases. And even if it is
 * not possible to operate multiple containers, this parameter contains
 * only one container name at most.</td></tr>
 * </tbody>
 * </table>
 *
 * <p>Thread safety of each method is not guaranteed. </p>
 *
 * @see Collection
 * @see TimeSeries
 * @see Container
 * @see GSException#getParameters()
 * </div>
 */
public interface GridStore extends Closeable {

	/**
	 * <div lang="ja">
	 * コンテナ名とロウキーからなる文字列が示すロウについて、新規作成または更新します。
	 *
	 * <p>コンテナ名とロウキー文字列を{@code "/"}により結合した
	 * 文字列により、対象ロウの場所を表現します。
	 * ロウキーの型が文字列型以外の場合、クエリ文の定数表記(引用符を除く)と同一の
	 * 文字列表現を指定する必要があります。
	 * たとえばTIMESTAMP型({@link java.util.Date})の場合、
	 * {@link TimestampUtils#format(java.util.Date)}を用いると適切な文字列表現に
	 * 変換できます。ロウオブジェクトとロウのマッピング方法については、{@link Container}の
	 * 定義を参照してください。</p>
	 *
	 * <p>ロウキーに対応するカラムが存在する場合のみ使用できます。</p>
	 *
	 * <p>指定のロウキーに対応するロウがコンテナ内に存在しない場合は新規作成、
	 * 存在する場合は更新します。
	 * ただし、コンテナの種別ならびに設定によっては、制限が設けられています。
	 * 具体的な制限事項は、{@link Container}のサブインタフェースの定義を
	 * 参照してください。
	 * 指定のロウオブジェクト内のロウキーは無視されます。</p>
	 *
	 * <p>このメソッドを使用するためには、{@link #getCollection(String, Class)}などを
	 * 使用し、クローズされていない{@link Container}を作成しておく必要があります。</p>
	 *
	 * @deprecated {@link Container#put(Object, Object)}に置き換えられました。
	 *
	 * @param pathKey 対象ロウの場所を表現する、コンテナ名とロウキーを
	 * {@code "/"}で結合した文字列
	 * @param rowObject ロウオブジェクト
	 * @return 既存のロウが存在したかどうか
	 *
	 * @throws GSException {@code pathKey}の形式が正しくない場合
	 * @throws GSException ロウキーに対応するカラムが存在しない場合
	 * @throws GSException 指定のコンテナ名、ならびにロウオブジェクトの型に
	 * 対応する{@link Container}が存在しない場合
	 * @throws GSException 特定コンテナ固有の制限に反する操作を行った場合
	 * @throws GSException この処理のタイムアウト、対応するコンテナの削除
	 * もしくはスキーマ変更、接続障害が発生した場合、クローズ後に呼び出された場合、
	 * またはサポート範囲外の値がロウオブジェクトに含まれていた場合
	 * @throws NullPointerException 1つ以上の引数に{@code null}が指定された場合。
	 * また、ロウフィールドに対応するロウオブジェクト内のオブジェクトが1つ以上存在しない場合
	 *
	 * @see Container#put(Object, Object)
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	public boolean put(String pathKey, Object rowObject) throws GSException;

	/**
	 * <div lang="ja">
	 * コンテナ名とロウキーからなる文字列を指定し、対応するロウの内容を取得します。
	 *
	 * <p>取得先を指定する文字列の構成、ならびに、このメソッドを使用するための
	 * 条件は、{@link #put(String, Object)}と同様です。</p>
	 *
	 * <p>取得結果のロウオブジェクトは、カラムの型によらず、ロウオブジェクトを
	 * 参照できる限り維持されます。</p>
	 *
	 * @deprecated {@link Container#get(Object)}に置き換えられました。
	 *
	 * @param pathKey 取得先を示す、コンテナ名とロウキーを{@code "/"}で
	 * 結合した文字列
	 * @return 対応するロウオブジェクト。存在しない場合は{@code null}
	 *
	 * @throws GSException {@code pathKey}の形式が正しくない場合
	 * @throws GSException ロウキーに対応するカラムが存在しない場合
	 * @throws GSException 指定のコンテナ名、ならびにロウオブジェクトの型に
	 * 対応する{@link Container}が存在しない場合
	 * @throws GSException この処理のタイムアウト、対応するコンテナの削除
	 * もしくはスキーマ変更、接続障害が発生した場合、またはクローズ後に呼び出された場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 *
	 * @see #put(String, Object)
	 * @see Container#get(Object)
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	public Object get(String pathKey) throws GSException;

	/**
	 * <div lang="ja">
	 * コンテナ名とロウキーからなる文字列により指定したロウを、
	 * 指定の型にマッピングして取得します。
	 *
	 * <p>型を指定する点を除き、{@link #get(String)}と同様です。
	 * このメソッドを使用するためには、{@link #getCollection(String, Class)}などを
	 * 使用し、指定の型に対応するクローズされていない{@link Container}を
	 * 作成しておく必要があります。</p>
	 *
	 * @deprecated {@link Container#get(Object)}に置き換えられました。
	 *
	 * @param pathKey 対象ロウの場所を示す、コンテナ名とロウキーを{@code "/"}で
	 * 結合した文字列
	 * @return 対応するロウオブジェクト。存在しない場合は{@code null}
	 *
	 * @throws GSException {@code pathKey}の形式が正しくない場合
	 * @throws GSException ロウキーに対応するカラムが存在しない場合
	 * @throws GSException 指定のコンテナ名、ならびにロウオブジェクトの型に
	 * 対応する{@link Container}が存在しない場合
	 * @throws GSException この処理のタイムアウト、対応するコンテナの削除
	 * もしくはスキーマ変更、接続障害が発生した場合、またはクローズ後に呼び出された場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 *
	 * @see #get(String)
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	public <R> R get(String pathKey, Class<R> rowType) throws GSException;

	/**
	 * <div lang="ja">
	 * コンテナ名とロウキーからなる文字列を指定し、対応するロウを削除します。
	 *
	 * <p>削除対象を指定する文字列の構成、ならびに、このメソッドを使用するための
	 * 条件は、{@link #put(String, Object)}と同様です。</p>
	 *
	 * <p>コンテナの種別ならびに設定によっては、制限が設けられています。
	 * 具体的な制限事項は、{@link Container}のサブインタフェースの定義を
	 * 参照してください。</p>
	 *
	 * <p>対応するロウが存在しない場合は何も変更しません。</p>
	 *
	 * @deprecated {@link Container#remove(Object)}に置き換えられました。
	 *
	 * @return 対応するロウが存在したかどうか
	 *
	 * @throws GSException {@code pathKey}の形式が正しくない場合
	 * @throws GSException ロウキーに対応するカラムが存在しない場合
	 * @throws GSException 指定のコンテナ名に対応するコンテナが
	 * 存在しない場合
	 * @throws GSException 特定コンテナ固有の制限に反する操作を行った場合
	 * @throws GSException この処理のタイムアウト、対応するコンテナの削除
	 * もしくはスキーマ変更、接続障害が発生した場合、またはクローズ後に呼び出された場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 *
	 * @see #put(String, Object)
	 * @see Container#remove(Object)
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	public boolean remove(String pathKey) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定の名前のコンテナに関する情報を取得します。
	 *
	 * <p>返却される{@link ContainerInfo}に含まれるコンテナ名は、GridDB上に
	 * 格納されているものが設定されます。したがって、指定したコンテナ名と比較すると、
	 * ASCIIの大文字・小文字表記が異なる場合があります。</p>
	 *
	 * <p>カラム順序を無視するかどうかについては、無視しない状態に設定されます。
	 * この設定は、{@link ContainerInfo#isColumnOrderIgnorable()}を通じて
	 * 確認できます。</p>
	 *
	 * @param name 処理対象のコンテナの名前
	 *
	 * @return 指定の名前のコンテナに関する情報
	 *
	 * @return 指定のコンテナが存在する場合は対応する{@link ContainerInfo}、
	 * 存在しない場合は{@code null}
	 * @throws GSException GSException この処理のタイムアウト、接続障害が発生した場合、
	 * またはクローズ後に呼び出された場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Get information related to a Container with the specified name.
	 *
	 * <p>A name stored in GridDB is set for the Container name to be included
	 * in a returned {@link ContainerInfo}. Therefore, compared to the specified
	 * Container name, the notation of the ASCII uppercase characters and
	 * lowercase characters may differ.</p>
	 *
	 * <p>The column sequence is set to Do Not Ignore.
	 * This setting can be verified through
	 * {@link ContainerInfo#isColumnOrderIgnorable()}.</p>
	 *
	 * @param name the target Container name
	 *
	 * @return Container Info of the specific Container.
	 *
	 * @return The relevant {@link ContainerInfo} if the Container exists, otherwise
	 * {@code null}.
	 * @throws GSException if a timeout occurs during this operation or a connection
	 * failure occurs; or if called after the connection is closed.
	 * @throws NullPointerException if {@code null} is specified as the parameter.
	 * </div>
	 */
	public ContainerInfo getContainerInfo(String name) throws GSException;

	/**
	 * <div lang="ja">
	 * コレクションを新規作成または変更します。
	 *
	 * <p>同名のコンテナが存在しない場合、指定のクラスにより定義された
	 * カラムレイアウトに従い、新規にコレクションを作成します。
	 * すでに同名のコンテナが存在し、既存のカラムレイアウトの内容がすべて
	 * 一致する場合、 実行中のトランザクションを待機する点を除き
	 * {@link #getCollection(String, Class)}と同様に振る舞います。</p>
	 *
	 * <p>指定の型とカラムレイアウトとの対応関係については、{@link Container}の
	 * 定義を参照してください。</p>
	 *
	 * <p>すでに同名のコレクションが存在し、かつ、該当するコレクションにおいて実行中の
	 * トランザクションが存在する場合、それらの終了を待機してから処理を行います。</p>
	 *
	 * @param name 処理対象のコレクションの名前
	 * @param rowType 処理対象のコレクションのカラムレイアウトと対応する
	 * ロウオブジェクトの型
	 *
	 * @return 対応する{@link Collection}
	 *
	 * @throws GSException 同名のコレクションが存在する場合。
	 * または、既存の同名の時系列に関してカラムレイアウトならびに追加設定の内容が
	 * 一致しない場合
	 * @throws GSException この処理のタイムアウト、接続障害が発生した場合、
	 * またはクローズ後に呼び出された場合
	 * @throws NullPointerException 1つ以上の引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Newly creates or update a Collection.
	 *
	 * <p>If a Container with the specified name does not exist, it newly creates
	 * a Collection based on the Column layout defined by the specified class.
	 * If a Container with the specified name already exists and its whole Column
	 * layout matches the specified type, it behaves in the same way as
	 * {@link #getCollection(String, Class)}, except that it waits for active
	 * transactions to complete. </p>
	 *
	 * <p>For the correspondence between a specified type and a Column layout,
	 * see the description of {@link Container}</p>
	 *
	 * <p>If a Collection with the specified name exists and if a transaction(s)
	 * is active in the Collection, it does the operation after waiting for the
	 * transaction(s) to complete. </p>
	 *
	 * @param name Name of a Collection subject to processing
	 * @param rowType Type of Row object corresponding to the column layout
	 * of the Collection subject to processing
	 *
	 * @return A {@link Collection} object created or updated
	 *
	 * @throws GSException if a Collection with the specified name exists; or if
	 * the Column layout and additional settings conflict with the specified type
	 * with regard to the existing TimeSeries with the specified name.
	 * @throws GSException if a timeout occurs during this operation or a connection
	 * failure occurs; or if called after the connection is closed.
	 * @throws NullPointerException a {@code null} parameter(s) is specified.
	 * </div>
	 */
	public <K, R> Collection<K, R> putCollection(
			String name, Class<R> rowType) throws GSException;

	/**
	 * <div lang="ja">
	 * 変更オプションを指定して、コレクションを新規作成または変更します。
	 *
	 * <p>同名のコンテナが存在しない場合、指定のクラスにより定義された
	 * カラムレイアウトに従い、新規にコレクションを作成します。
	 * すでに同名のコレクションが存在し、既存のカラムレイアウトの内容がすべて
	 * 一致する場合、実行中のトランザクションを待機する点を除き
	 * {@link #getCollection(String, Class)}と同様に振る舞います。</p>
	 *
	 * <p>{@code modifiable}が{@code true}であり、すでに同名のコレクションが
	 * 存在する場合、必要に応じカラムレイアウトを変更します。
	 * 変更する際、要求したカラムと同一の名前・型の既存のカラムは保持されます。
	 * 一致しないカラムのうち、既存のコレクションにない名前のカラムは追加し、要求側にない
	 * カラムはデータも含め削除します。型が異なる同名のカラムが存在する場合は
	 * 失敗します。また、ロウキーに対応するカラムの追加と削除はできません。</p>
	 *
	 * <p>コンテナにトリガが設定されており、カラムレイアウト変更によってトリガが
	 * 通知対象としているカラムが削除された場合、そのカラムはトリガの通知対象
	 * から削除されます。</p>
	 *
	 * <p>新たに追加されるカラムの値は、{@link Container}にて定義されている
	 * 空の値を初期値として初期化されます。</p>
	 *
	 * <p>指定の型とカラムレイアウトとの対応関係については、{@link Container}の
	 * 定義を参照してください。</p>
	 *
	 * <p>すでに同名のコレクションが存在し、かつ、該当するコレクションにおいて実行中の
	 * トランザクションが存在する場合、それらの終了を待機してから処理を行います。</p>
	 *
	 * <p>ロウキーを持つコレクションを新規に作成する場合、ロウキーに対し、
	 * {@link Container#createIndex(String)}にて定義されているデフォルト種別の
	 * 索引が作成されます。この索引は、削除することができます。</p>
	 *
	 * <p>現バージョンでは、コンテナの規模など諸条件を満たした場合、
	 * カラムレイアウトの変更開始から終了までの間に、処理対象のコンテナに対して
	 * コンテナ情報の参照、更新ロックなしでのロウの参照操作を行える場合が
	 * あります。それ以外の操作は、{@link Container}での定義通り待機させる
	 * 場合があります。カラムレイアウトの変更途中に別の操作が行われる場合は、
	 * 変更前のカラムレイアウトが使用されます。</p>
	 *
	 * @param name 処理対象のコレクションの名前
	 * @param rowType 処理対象のコレクションのカラムレイアウトと対応する
	 * ロウオブジェクトの型
	 * @param modifiable 既存コレクションのカラムレイアウト変更を許可するかどうか
	 *
	 * @return 対応する{@link Collection}
	 *
	 * @throws GSException 同名の時系列が存在する場合。
	 * または、{@code modifiable}が{@code false}であり、既存の同名の
	 * コレクションに関してカラムレイアウトの内容が一致しない場合や、
	 * {@code modifiable}が{@code true}であり、既存の同名のコレクションに関して
	 * 変更できない項目を変更しようとした場合
	 * @throws GSException 指定の型がロウオブジェクトの型として適切でない場合。
	 * 詳しくは{@link Container}の定義を参照
	 * @throws GSException この処理のタイムアウト、接続障害が発生した場合、
	 * またはクローズ後に呼び出された場合
	 * @throws NullPointerException 1つ以上の引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Newly creates or update a Collection with the specified option.
	 *
	 * <p>If a Container with the specified name does not exist, it newly creates
	 * a Collection based on the Column layout defined by the specified class.
	 * If a Collection with the specified name already exists and its whole
	 * Column layout matches the specified type, it behaves in the same way as
	 * {@link #getCollection(String, Class)}, except that it waits for active
	 * transactions to complete. </p>
	 *
	 * <p>If {@code modifiable} is {@code true} and a Collection with the
	 * specified name exists, it changes its layout as necessary. When changing
	 * the layout, it leaves unchanged the existing Columns with the same name
	 * and type as defined by the specified class. If a Column name defined by
	 * the class is not found in the existing Collection, it creates a Column
	 * with the name; and it deletes other Columns in the existing Collection
	 * as well as their data. It fails if a Column with the same name but of a
	 * different type exists. It is not possible to create or delete a Column
	 * corresponding to a Row key. </p>
	 *
	 * <p>If a trigger is set in a Container, and if a column whose trigger
	 * is subject to notification is deleted due to a change in the column layout,
	 * the column will be deleted from the list of triggers subject to notification.</p>
	 *
	 * <p>The values of Columns to be newly created are initialized with an empty
	 * value defined in {@link Container} as an initial value.</p>
	 *
	 * <p>For the correspondence between a specified type and a Column layout,
	 * see the description of {@link Container}</p>
	 *
	 * <p>If a Collection with the specified name exists and if a transaction(s)
	 * is active in the Collection, it does the operation after waiting for the
	 * transaction(s) to complete. </p>
	 *
	 * <p>When creating a Collection with Row key, an index of default type of
	 * the Row key which is defined by {@link Container#createIndex(String)}
	 * is created. The index is removable.</p>
	 *
	 * <p> In the current version, when conditions such as the size of a container
	 * are met, from the beginning to the end of changing the column layout, it is
	 * sometimes possible to refer to the container information for the container
	 * to be processed and reference the row without updating lock. There are cases
	 * where other operations are made to wait as defined in {@link Container}.
	 * If another operation is performed during column layout change, the prior layout
	 * will be used. </p>
	 *
	* @param name Name of a Collection subject to processing
	 * @param rowType Type of Row object corresponding to the column layout
	 * of the Collection subject to processing
	 * @param modifiable Indicates whether the column layout of the existing
	 * Collection can be changed or not
	 *
	 * @return A {@link Collection} object created or updated
	 *
	 * @throws GSException if a TimeSeries with the same name exists; or if
	 * {@code modifiable} is {@code false} and the Column layout of the
	 * existing Collection with the specified name conflicts with the requested
	 * layout; or if {@code modifiable} is {@code true} and it attempted to
	 * change the unchangeable items in the existing Collection with the
	 * specified name.
	 * @throws GSException if the specified type is not proper as a type of a
	 * Row object. For more information, see the description of {@link Container}.
	 * @throws GSException if a timeout occurs during this operation or a
	 * connection failure occurs; or if called after the connection is closed.
	 * @throws NullPointerException if a {@code null} parameter(s) is specified.
	 * </div>
	 */
	public <K, R> Collection<K, R> putCollection(
			String name, Class<R> rowType,
			boolean modifiable) throws GSException;

	/**
	 * <div lang="ja">
	 * 時系列を新規作成または変更します。
	 *
	 * <p>同名のコンテナが存在しない場合、指定のクラスにより定義された
	 * カラムレイアウトに従い、新規に時系列を作成します。
	 * すでに同名の時系列が存在し、既存のカラムレイアウトの内容がすべて
	 * 一致する場合、実行中のトランザクションを待機する点を除き
	 * {@link #getTimeSeries(String, Class)}と同様に振る舞います。</p>
	 *
	 * <p>指定の型とカラムレイアウトとの対応関係については、{@link Container}の
	 * 定義を参照してください。</p>
	 *
	 * <p>すでに同名の時系列が存在し、かつ、該当する時系列において実行中の
	 * トランザクションが存在する場合、それらの終了を待機してから処理を行います。</p>
	 *
	 * @param name 処理対象の時系列の名前
	 * @param rowType 処理対象の時系列のカラムレイアウトと対応する
	 * ロウオブジェクトの型
	 *
	 * @return 対応する{@link TimeSeries}
	 *
	 * @throws GSException 同名のコレクションが存在する場合。
	 * または、既存の同名の時系列に関してカラムレイアウトならびに追加設定の内容が
	 * 一致しない場合
	 * @throws GSException 指定の型がロウオブジェクトの型として適切でない場合。
	 * 詳しくは{@link Container}の定義を参照
	 * @throws GSException この処理のタイムアウト、接続障害が発生した場合、
	 * またはクローズ後に呼び出された場合
	 * @throws NullPointerException 1つ以上の引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Newly creates or updates a TimeSeries.
	 *
	 * <p>If a Container with the specified name does not exist, it newly
	 * creates a TimeSeries based on the Column layout defined by the
	 * specified class. If a TimeSeries with the specified name already
	 * exists and its whole Column layout matches the specified type, it
	 * behaves in the same way as {@link #getTimeSeries(String, Class)},
	 * except that it waits for active transactions to complete. </p>
	 *
	 * <p>For the correspondence between a specified type and a Column
	 * layout, see the description of {@link Container}.</p>
	 *
	 * <p>If a TimeSeries with the specified name exists and if a
	 * transaction(s) is active in the TimeSeries, it does the operation
	 * after waiting for the transaction(s) to complete. </p>
	 *
	 * @param name Name of TimeSeries subject to processing
	 * @param rowType Type of Row object corresponding to the column layout
	 * of the TimeSeries subject to processing
	 *
	 * @return A {@link TimeSeries} object created or updated
	 *
	 * @throws GSException if a Collection with the same name exists; or if
	 * the Column layout and additional settings conflict with the specified
	 * type with regard to the existing TimeSeries with the specified name.
	 * @throws GSException if the specified type is not proper as a type of
	 * a Row object. For more information, see the description of {@link Container}.
	 * @throws GSException if a timeout occurs during this operation or a
	 * connection failure occurs; or if called after the connection is closed.
	 * @throws NullPointerException if a {@code null} parameter(s) is specified.
	 * </div>
	 */
	public <R> TimeSeries<R> putTimeSeries(
			String name, Class<R> rowType) throws GSException;

	/**
	 * <div lang="ja">
	 * 追加設定や変更オプションを指定して、時系列を新規作成または変更します。
	 *
	 * <p>同名のコンテナが存在しない場合、指定のクラスにより定義された
	 * カラムレイアウトや追加設定に従い、新規に時系列を作成します。
	 * すでに同名の時系列が存在し、既存のカラムレイアウトならびに追加設定の内容が
	 * すべて一致する場合、実行中のトランザクションを待機する点を除き
	 * {@link #getTimeSeries(String, Class)}と同様に振る舞います。</p>
	 *
	 * <p>{@code modifiable}が{@code true}であり、すでに同名の時系列が
	 * 存在する場合、必要に応じカラムレイアウトを変更します。
	 * 変更する際、要求したカラムと同一の名前・型の既存のカラムは保持されます。
	 * 一致しないカラムのうち、既存の時系列にない名前のカラムは追加し、要求側にない
	 * カラムはデータも含め削除します。型が異なる同名のカラムが存在する場合は失敗します。
	 * また、ロウキーに対応するカラムの追加と削除、時系列構成オプションの変更は
	 * できません。時系列構成オプションを指定する場合は、既存の設定内容とすべて
	 * 同値にする必要があります。</p>
	 *
	 * <p>コンテナにトリガが設定されており、カラムレイアウト変更によってトリガが
	 * 通知対象としているカラムが削除された場合、そのカラムはトリガの通知対象
	 * から削除されます。</p>
	 *
	 * <p>新たに追加されるカラムの初期値については、
	 * {@link #putCollection(String, Class, boolean)}の定義を参照してください。</p>
	 *
	 * <p>指定の型とカラムレイアウトとの対応関係については、{@link Container}の
	 * 定義を参照してください。</p>
	 *
	 * <p>すでに同名の時系列が存在し、かつ、該当する時系列において実行中の
	 * トランザクションが存在する場合、それらの終了を待機してから処理を行います。</p>
	 *
	 * @param name 処理対象の時系列の名前
	 * @param rowType 処理対象の時系列のカラムレイアウトと対応する
	 * ロウオブジェクトの型
	 * @param props 時系列の構成オプション。{@code null}を指定すると、
	 * 同名の時系列が存在する場合は既存の設定が継承され、存在しない場合は
	 * 初期状態の{@link TimeSeriesProperties}を指定したものとみなされる。
	 * @param modifiable 既存時系列のカラムレイアウト変更を許可するかどうか
	 *
	 * @return 対応する{@link TimeSeries}
	 *
	 * @throws GSException 同名のコレクションが存在する場合。
	 * または、{@code modifiable}が{@code false}であり、既存の同名の
	 * 時系列に関してカラムレイアウトならびに追加設定の内容が一致しない場合や、
	 * {@code modifiable}が{@code true}であり、既存の同名の時系列に関して
	 * 変更できない項目を変更しようとした場合
	 * @throws GSException 指定の型がロウオブジェクトの型として適切でない場合。
	 * 詳しくは{@link Container}の定義を参照
	 * @throws GSException この処理のタイムアウト、接続障害が発生した場合、
	 * またはクローズ後に呼び出された場合
	 * @throws NullPointerException 1つ以上の引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Newly creates or updates a TimeSeries with the specified additional settings
	 * and update option.
	 *
	 * <p>If a Container with the specified name does not exist, it newly creates a
	 * TimeSeries based on the Column layout and additional settings defined by the
	 * specified class. If a TimeSeries with the specified name already exists and
	 * its whole Column layout and additional settings match the specified type,
	 * it behaves in the same way as {@link #getTimeSeries(String, Class)}, except
	 * that it waits for active transactions to complete. </p>
	 *
	 * <p>If {@code modifiable} is {@code true} and a TimeSeries with the specified
	 * name exists, it changes its layout as necessary. When changing the layout,
	 * it leaves unchanged the existing Columns with the same name and type as
	 * defined by the specified class. If a Column name defined by the class is
	 * not found in the existing TimeSeries, it creates a Column with the name;
	 * and it deletes other Columns in the existing Collection as well as their
	 * data. It fails if a Column with the same name but of a different type exists.
	 * It is not possible to create or delete a Column corresponding to a Row key
	 * or change the options for configuring a TimeSeries. When specifying some
	 * options for configuring a TimeSeries, specified values must be the same as
	 * the current settings. </p>
	 *
	 * <p>If a trigger is set in a Container, and if a column whose trigger
	 * is subject to notification is deleted due to a change in the column layout,
	 * the column will be deleted from the list of triggers subject to notification.</p>
	 *
	 * <p>For the initial values for newly created Columns, see the description of
	 * {@link #putCollection(String, Class, boolean)}.</p>
	 *
	 * <p>For the correspondence between a specified type and a Column layout,
	 * see the description of {@link Container}</p>
	 *
	 * <p>If a TimeSeries with the specified name exists and if a transaction(s)
	 * is active in the TimeSeries, it does the operation after waiting for the
	 * transaction(s) to complete. </p>
	 *
	 * @param name Name of TimeSeries subject to processing
	 * @param rowType Type of Row object corresponding to the column layout
	 * of the TimeSeries subject to processing
	 * @param props Composition option of TimeSeries. If {@code null} is specified,
	 * the existing settings will be inherited if a TimeSeries with the same name
	 * exists. If not, they will be deemed as specified in the initial status
	 * of {@link TimeSeriesProperties}.
	 * @param modifiable To permit a change in the column layout of an existing
	 * TimeSeries or not
	 *
	 * @return A {@link TimeSeries} object to created or deleted
	 *
	 * @throws GSException if a Collection with the same name exists; or if
	 * {@code modifiable} is {@code false} and the Column layout and additional
	 * settings conflict with the specified type with regard to the existing
	 * TimeSeries with the specified name; or if {@code modifiable} is {@code true}
	 * and it attempted to change the unchangeable items in the existing TimeSeries
	 * with the specified name.
	 * @throws GSException if the specified type is not proper as a type of a
	 * Row object. For more information, see the description of {@link Container}.
	 * @throws GSException if a timeout occurs during this operation or a
	 * connection failure occurs; or if called after the connection is closed.
	 * @throws NullPointerException if a {@code null} parameter(s) is specified.
	 * </div>
	 */
	public <R> TimeSeries<R> putTimeSeries(
			String name, Class<R> rowType,
			TimeSeriesProperties props, boolean modifiable)
			throws GSException;

	/**
	 * <div lang="ja">
	 * 指定の名前のコレクションを操作するための{@link Collection}オブジェクトを
	 * 取得します。
	 *
	 * <p>指定の型とカラムレイアウトとの対応関係については、{@link Container}の
	 * 定義を参照してください。</p>
	 *
	 * @param name 処理対象のコレクションの名前
	 * @param rowType 処理対象のコレクションのカラムレイアウトと対応する
	 * ロウオブジェクトの型
	 *
	 * @return コレクションが存在する場合は対応する{@link Collection}、
	 * 存在しない場合は{@code null}
	 *
	 * @throws GSException 同名の時系列が存在する場合
	 * @throws GSException 指定の型と既存のカラムレイアウトが一致しない場合
	 * @throws GSException 指定の型がロウオブジェクトの型として適切でない場合。
	 * 詳しくは{@link Container}の定義を参照
	 * @throws GSException この処理のタイムアウト、接続障害が発生した場合、
	 * またはクローズ後に呼び出された場合
	 * @throws NullPointerException 1つ以上の引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Return a {@link Collection} object to manipulate a Collection
	 * with the specified name.
	 *
	 * <p>For the correspondence between a specified type and a Column layout,
	 * see the description of {@link Container}.</p>
	 *
	 * @param name Name of a Collection subject to processing
	 * @param rowType Type of Row object corresponding to the column layout
	 * of the Collection subject to processing
	 *
	 * @return A Collection object if a {@link Collection} with the specified
	 * name exist; or {@code null} if not.
	 *
	 * @throws GSException if a TimeSeries with the same name exists.
	 * @throws GSException if the specified type and the existing Column layout
	 * conflict each other.
	 * @throws GSException if the specified type is not proper as a type of a
	 * Row object. For more information, see the description of {@link Container}.
	 * @throws GSException if a timeout occurs during this operation or a
	 * connection failure occurs; or if called after the connection is closed.
	 * @throws NullPointerException if a {@code null} parameter(s) is specified.
	 * </div>
	 */
	public <K, R> Collection<K, R> getCollection(
			String name, Class<R> rowType) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定の名前の時系列を操作するための{@link TimeSeries}オブジェクトを
	 * 取得します。
	 *
	 * <p>指定の型とカラムレイアウトとの対応関係については、{@link Container}の
	 * 定義を参照してください。</p>
	 *
	 * @param name 処理対象の時系列の名前
	 * @param rowType 処理対象の時系列のカラムレイアウトと対応する
	 * ロウオブジェクトの型
	 *
	 * @return 時系列が存在する場合は対応する{@link TimeSeries}、
	 * 存在しない場合は{@code null}
	 *
	 * @throws GSException 同名のコレクションが存在する場合
	 * @throws GSException 指定の型と既存のカラムレイアウトが一致しない場合
	 * @throws GSException 指定の型がロウオブジェクトの型として適切でない場合。
	 * 詳しくは{@link Container}の定義を参照
	 * @throws GSException この処理のタイムアウト、接続障害が発生した場合、
	 * またはクローズ後に呼び出された場合
	 * @throws NullPointerException 1つ以上の引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Returns a {@link TimeSeries} object to manipulate a TimeSeries with
	 * the specified name.
	 *
	 * <p>For the correspondence between a specified type and a Column layout,
	 * see the description of {@link Container}.</p>
	 *
	 * @param name Name of TimeSeries subject to processing
	 * @param rowType Type of Row object corresponding to the column layout
	 * of the TimeSeries subject to processing
	 *
	 * @return A {@link TimeSeries} object if a TimeSeries with the specified
	 * name exists; or {@code null} if not.
	 *
	 * @throws GSException if a Collection with the same name exists.
	 * @throws GSException if the specified type and the existing Column layout
	 * conflict each other.
	 * @throws GSException if the specified type is not proper as a type of a
	 * Row object. For more information, see the description of {@link Container}.
	 * @throws GSException if a timeout occurs during this operation or a
	 * connection failure occurs; or if called after the connection is closed.
	 * @throws NullPointerException if a {@code null} parameter(s) is specified.
	 * </div>
	 */
	public <R> TimeSeries<R> getTimeSeries(
			String name, Class<R> rowType) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定の名前を持つコレクションを削除します。
	 *
	 * <p>削除済みの場合の扱い、トランザクションの扱い、削除要求完了直後の
	 * 状態に関しては、{@link #dropContainer(String)}と同様です。</p>
	 *
	 * @param name 処理対象のコレクションの名前
	 *
	 * @throws GSException 種別の異なるコンテナを削除しようとした場合
	 * @throws GSException この処理のタイムアウト、接続障害が発生した場合、
	 * またはクローズ後に呼び出された場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Deletes a Collection with the specified name.
	 *
	 * <p>Deletion handling, transaction handling, and the immediate state
	 * after deletion request is completed are the same as those found in
	 * {@link #dropContainer(String)} </p>
	 *
	 * @param name Name of Collection subject to processing
	 *
	 * @throws GSException if the Container type is unmatched.
	 * @throws GSException if a timeout occurs during this operation or
	 * a connection failure occurs; or if called after the connection is
	 * closed.
	 * @throws NullPointerException if a {@code null} parameter is specified.
	 * </div>
	 */
	public void dropCollection(String name) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定の名前を持つ時系列を削除します。
	 *
	 * <p>削除済みの場合の扱い、トランザクションの扱い、削除要求完了直後の
	 * 状態に関しては、{@link #dropContainer(String)}と同様です。</p>
	 *
	 * @param name 処理対象の時系列の名前
	 *
	 * @throws GSException 種別の異なるコンテナを削除しようとした場合
	 * @throws GSException この処理のタイムアウト、接続障害が発生した場合、
	 * またはクローズ後に呼び出された場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Deletes a TimeSeries with the specified name.
	 *
	 * <p>Deletion handling, transaction handling, and the immediate state
	 * after deletion request is completed are the same as those found in
	 * {@link #dropContainer(String)} </p>
	 *
	 * @param name Name of TimeSeries subject to processing
	 *
	 * @throws GSException if the Container type is unmatched.
	 * @throws GSException if a timeout occurs during this operation or
	 * a connection failure occurs; or if called after the connection is
	 * closed.
	 * @throws NullPointerException if a {@code null} parameter is specified.
	 * </div>
	 */
	public void dropTimeSeries(String name) throws GSException;

	/**
	 * <div lang="ja">
	 * GridDBとの接続状態を解除し、必要に応じて関連するリソースを解放します。
	 *
	 * <p>{@link GSException}が送出された場合であっても、接続状態は解除や
	 * ローカルのリソース解放は適宜実施されます。ただし、GridDB上のトランザクション
	 * 状態などは残る可能性があります。
	 * すでにクローズ済みの場合、このメソッドを呼び出しても何の効果もありません。 </p>
	 *
	 * @throws GSException 接続障害が発生した場合
	 * </div><div lang="en">
	 * Disconnects with GridDB and releases related resources as necessary.
	 *
	 * <p>Even if {@link GSException} is thrown, the connection and local resources
	 * are released properly. However, the transaction state etc. might remain in
	 * GridDB. If the connection is already closed, this method will not work
	 * effectively. </p>
	 *
	 * @throws GSException if a connection failure occurs
	 * </div>
	 */
	public void close() throws GSException;

	/**
	 * <div lang="ja">
	 * {@link ContainerInfo}を指定して、コンテナを新規作成または変更します。
	 *
	 * <p>次の点を除き、{@link #putCollection(String, Class, boolean)}
	 * もしくは{@link #putTimeSeries(String, Class, TimeSeriesProperties, boolean)}
	 * と同様に振る舞います。</p>
	 * <ul>
	 * <li>{@link ContainerInfo}を用いてコンテナ種別、カラムレイアウト、
	 * ならびに、必要に応じ時系列構成オプションを指定する</li>
	 * <li>返却される{@link Container}のロウオブジェクトの型が常に{@link Row}となる</li>
	 * </ul>
	 * <p>それぞれの同名の引数{@code modifiable}の用法についても同様です。</p>
	 *
	 * <p>コンテナに関する情報の指定方法の一覧は次の通りです。</p>
	 * <table>
	 * <thead><tr><th>項目</th><th>引数</th><th>説明</th></tr></thead>
	 * <tbody>
	 * <tr><td>コンテナ名</td><td>{@code name}または{@code info}</td>
	 * <td>少なくともいずれかの引数に{@code null}ではない値を指定する。
	 * 両方に指定する場合、異なる値を指定してはならない。</td></tr>
	 * <tr><td>コンテナ種別</td><td>{@code info}</td>
	 * <td>{@code null}ではない値を指定する。
	 * {@link ContainerType#COLLECTION}を指定した場合、
	 * {@link #putCollection(String, Class, boolean)}と同様の振る舞いとなる。
	 * {@link ContainerType#TIME_SERIES}を指定した場合、
	 * {@link #putTimeSeries(String, Class, TimeSeriesProperties, boolean)}と
	 * 同様の振る舞いとなる。</td></tr>
	 * <tr><td>カラムレイアウト</td><td>{@code info}</td>
	 * <td>{@link Container}にて規定された制約に合致するよう
	 * {@link ColumnInfo}のリストならびにロウキーの有無を設定する。</td></tr>
	 * <tr><td>カラム順序の無視</td><td>{@code info}</td>
	 * <td>無視する場合、同名の既存のコンテナのカラム順序と一致するかどうかを
	 * 検証しない。</td></tr>
	 * <tr><td>時系列構成オプション</td><td>{@code info}</td>
	 * <td>コンテナ種別が{@link ContainerType#TIME_SERIES}の場合のみ、
	 * {@code null}ではない値を指定できる。</td></tr>
	 * <tr><td>索引設定</td><td>{@code info}</td>
	 * <td>現バージョンでは無視される。
	 * 今後のバージョンでは、
	 * {@link Container#createIndex(String, IndexType)}の規則に合致しない
	 * 設定が含まれていた場合、例外が送出される可能性がある。</td></tr>
	 * <tr><td>トリガ設定</td><td>{@code info}</td>
	 * <td>現バージョンでは無視される。
	 * 今後のバージョンでは、
	 * {@link Container#createTrigger(TriggerInfo)}の規則に合致しない
	 * 設定が含まれていた場合、例外が送出される可能性がある。</td></tr>
	 * <tr><td>コンテナ類似性</td><td>{@code info}</td>
	 * <td>{@code null}以外を指定し新規作成する場合、指定の内容が反映される。
	 * 既存コンテナの設定を変更することはできない。{@code null}を指定した
	 * 場合は無視される。</td></tr>
	 * </tbody>
	 * </table>
	 *
	 * @param name 処理対象のコンテナの名前
	 * @param info 処理対象のコンテナの情報
	 * @param modifiable 既存コンテナのカラムレイアウト変更を許可するかどうか
	 *
	 * @return 対応する{@link Container}。コンテナ種別として
	 * {@link ContainerType#COLLECTION}を指定した場合は{@link Collection}、
	 * {@link ContainerType#TIME_SERIES}を指定した場合は{@link TimeSeries}の
	 * インスタンスとなる。
	 *
	 * @throws GSException {@code name}ならびに{@code info}引数の
	 * 内容が規則に合致しない場合。また、指定のコンテナ種別に対応する
	 * コンテナ新規作成・変更メソッドの規則に合致しない場合
	 * @throws GSException この処理のタイムアウト、接続障害が発生した場合、
	 * またはクローズ後に呼び出された場合
	 * @throws NullPointerException {@code info}引数に{@code null}が
	 * 指定された場合
	 *
	 * @see #putCollection(String, Class, boolean)
	 * @see #putTimeSeries(String, Class, TimeSeriesProperties, boolean)
	 * @see Container
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Specify {@link ContainerInfo} and create a new Container or update a Container.
	 *
	 * <p>Excluding the next point, the behavior will be the same
	 * as {@link #putCollection(String, Class, boolean)}
	 * or {@link #putTimeSeries(String, Class, TimeSeriesProperties, boolean)}.</p>
	 * <ul>
	 * <li>Use {@link ContainerInfo} to specify the Container type, column layout,
	 * as well as the TimeSeries composition option where necessary</li>
	 * <li>The Row object type of the {@link Container} returned will always
	 * be {@link Row}</li>
	 * </ul>
	 * <p>Arguments {@code modifiable} with the same respective name are used
	 * in the same way as well.</p>
	 *
	 * <p>A list of the methods to specify Container-related information is
	 * given below.</p>
	 * <table>
	 * <thead><tr><th>item</th><th>argument</th><th>description</th></tr></thead>
	 * <tbody>
	 *
	 * <tr><td>Container name</td><td>{@code name} or {@code info}</td>
	 * <td>Specify a value that is not {@code null} in at least one of the arguments.
	 * A different value must be specified when specifying both sides. </td></tr>
	 * <tr><td>Container type</td><td>{@code info}</td>
	 * <td>Specify a value that is not {@code null}.
	 * If {@link ContainerType#COLLECTION} is specified, the behavior
	 * will be the same as {@link #putCollection(String, Class, boolean)}.
	 * If {@link ContainerType#TIME_SERIES} is specified, the behavior will be the same as
	 * {@link #putTimeSeries(String, Class, TimeSeriesProperties, boolean)}. </td></tr>
	 * <tr><ts>column layout</td><td>{@code info}</td>
	 * <td>Set the {@link ColumnInfo} list and whether there is any Row key
	 * so as to conform to the restrictions stipulated in {@link Container}. </td></tr>
	 * <tr><td>ignore column sequence</td><td>{@code info}</td>
	 * <td>If ignored, no verification of the conformance with the column sequence
	 * of existing Containers with the same name will be carried out. </td></tr>
	 * <tr><td>TimeSeries composition option</td><td>{@code info}</td>
	 * <td>A value that is not {@code null} can be specified only
	 * if the Container type is {@link ContainerType#TIME_SERIES}. </td></tr>
	 * <tr><td>index setting</td><td>{@code info}</td>
	 * <td>Ignored in the current version.
	 * In future versions, if settings that do not conform to the rules of
	 * {@link Container#createIndex(String, IndexType)} are included,
	 * an exception may be sent out. </td></tr>
	 * <tr><td>trigger setting</td><td>{@code info}</td>
	 * <td>Ignored in the current version. In future versions, if settings
	 * that do not conform to the rules of
	 * {@link Container#createTrigger(TriggerInfo)} are included,
	 * an exception may be sent out. </td></tr>
	 * <tr><td>Container similarity</td><td>{@code info}</td>
	 * <td>The specified contents will be reflected if a setting other than
	 * {@code null} is specified and newly created. The settings
	 * of an existing Container cannot be changed. The settings are ignored
	 * if {@code null} is specified. </td></tr>
	 * </tbody>
	 * </table>
	 *
	 * @param name Name of a Container subject to processing
	 * @param info Information of a Container subject to processing
	 * @param modifiable To permit a change in the column layout of existing
	 * Container data or not
	 *
	 * @return Corresponding {@link Container}. If {@link ContainerType#COLLECTION}
	 * is specified as the Container type,
	 * the instance created will be a {@link Collection} instance.
	 * If {@link ContainerType#TIME_SERIES} is specified,
	 * the instance created will be a {@link TimeSeries} instance.
	 *
	 * @throws GSException If the contents of the arguments {@code name} and
	 * {@code info} do not conform to the rules.
	 * If the contents also do not conform to the rules of the new Container
	 * creation and update method for the specified Container type
	 * @throws GSException If this process times out, a connection failure were
	 * to occur, or if it is invoked after being closed
	 * @throws NullPointerException If {@code null} is specified in the argument
	 * {@code info}
	 *
	 * @see #putCollection(String, Class, boolean)
	 * @see #putTimeSeries(String, Class, TimeSeriesProperties, boolean)
	 * @see Container
	 * </div>
	 */
	public <K> Container<K, Row> putContainer(String name,
			ContainerInfo info, boolean modifiable) throws GSException;

	/**
	 * <div lang="ja">
	 * {@link Row}によりロウ操作できる{@link Container}オブジェクトを
	 * 取得します。
	 *
	 * <p>次の点を除き、{@link #getCollection(String, Class)}
	 * もしくは{@link #getTimeSeries(String, Class)}と同様に振る舞います。</p>
	 * <ul>
	 * <li>既存のコンテナの種別ならびにカラムレイアウトに基づき{@link Container}
	 * オブジェクトを返却する</li>
	 * <li>コンテナの種別ならびにカラムレイアウトを指定しないため、これらの
	 * 不一致に伴うエラーが発生しない</li>
	 * <li>返却される{@link Container}のロウオブジェクトの型が常に{@link Row}となる</li>
	 * </ul>
	 * <p>それぞれの同名の引数{@code name}の用法についても同様です。</p>
	 *
	 * @param name 処理対象のコンテナの名前
	 *
	 * @return コンテナが存在する場合は対応する{@link Container}、
	 * 存在しない場合は{@code null}。コンテナが存在し、種別が
	 * {@link ContainerType#COLLECTION}であった場合は{@link Collection}、
	 * {@link ContainerType#TIME_SERIES}であった場合は{@link TimeSeries}の
	 * インスタンスとなる。
	 *
	 * @throws GSException この処理のタイムアウト、接続障害が発生した場合、
	 * またはクローズ後に呼び出された場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 *
	 * @see #getCollection(String, Class)
	 * @see #getTimeSeries(String, Class)
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Get a {@link Container} object whose Rows can be processed using
	 * a {@link Row}.
	 *
	 * <p>Excluding the next point, the behavior will be the same as
	 * {@link #getCollection(String, Class)} or {@link #getTimeSeries(String, Class)}.</p>
	 * <ul>
	 * <li>Return a {@link Container} object based on the existing Container type
	 * and column layout</li>
	 * <li>No error accompanying these non-conformances will occur as the Container
	 * type and column layout are not specified</li>
	 * <li>The Row object type of the {@link Container} returned will always be
	 * {@link Row}</li>
	 * </ul>
	 * <p>Arguments {@code name} with the same respective name are used in the same way
	 * as well.</p>
	 *
	 * @param name Name of a Container subject to processing
	 *
	 * @return Corresponding {@link Container} if a Container exists and {@code null} if not.
	 * If a Container exists and the type is {@link ContainerType#COLLECTION},
	 * the instance created will be a {@link Collection} instance. If the type
	 * is {@link ContainerType#TIME_SERIES}, the instance created will be a
	 * {@link TimeSeries} instance.
	 *
	 * @throws GSException If this process times out, a connection failure were
	 * to occur, or if it is invoked after being closed
	 * @throws NullPointerException If {@code null} is specified in the argument
	 *
	 * @see #getCollection(String, Class)
	 * @see #getTimeSeries(String, Class)
	 * </div>
	 */
	public <K> Container<K, Row> getContainer(
			String name) throws GSException;

	/**
	 * <div lang="ja">
	 * {@link ContainerInfo}を指定して、コレクションを新規作成または変更します。
	 *
	 * <p>コンテナ種別が{@link ContainerType#COLLECTION}に限定され、
	 * 返却される型が{@link Collection}となる点を除き、
	 * {@link #putContainer(String, ContainerInfo, boolean)}
	 * と同様に振る舞います。</p>
	 *
	 * @param name 処理対象のコレクションの名前
	 * @param info 処理対象のコレクションの情報。コンテナ種別には{@code null}
	 * もしくは{@link ContainerType#COLLECTION}のいずれかを指定
	 * @param modifiable 既存コレクションのカラムレイアウト変更を許可するかどうか
	 *
	 * @return 対応する{@link Collection}
	 *
	 * @throws GSException コンテナ種別以外の指定内容に関して、
	 * {@link #putContainer(String, ContainerInfo, boolean)}
	 * の規則に合致しない場合。また、コンテナ種別に関する制限に合致しない場合
	 * @throws GSException この処理のタイムアウト、接続障害が発生した場合、
	 * またはクローズ後に呼び出された場合
	 * @throws NullPointerException 引数{@code info}に{@code null}が
	 * 指定された場合
	 *
	 * @see #putContainer(String, ContainerInfo, boolean)
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Specify {@link ContainerInfo} and create a new Collection or update
	 * a Collection.
	 *
	 * <p>Except for points where the Container type is limited to
	 * {@link ContainerType#COLLECTION} and the returned type is {@link Collection},
	 * the behavior will be the same as {@link #putContainer(String, ContainerInfo, boolean)}.</p>
	 *
	 * @param name Name of Collection subject to processing
	 * @param info Collection information subject to processing Specify either
	 * {@code null} or {@link ContainerType#COLLECTION} in the Container type
	 * @param modifiable To permit a change in the column layout of an existing
	 * Collection or not
	 *
	 * @return Corresponding {@link Collection}.
	 *
	 * @throws GSException If specifications other than the Container type do not
	 * conform to the rules of {@link #putContainer(String, ContainerInfo, boolean)}.
	 * If the specifications do not conform to the restrictions related
	 * to the Container type as well
	 * @throws GSException If this process times out, a connection failure were
	 * to occur, or if it is invoked after being closed
	 * @throws NullPointerException If {@code null} is specified in the argument
	 * {@code info}
	 *
	 * @see #putContainer(String, ContainerInfo, boolean)
	 * </div>
	 */
	public <K> Collection<K, Row> putCollection(String name,
			ContainerInfo info, boolean modifiable) throws GSException;

	/**
	 * <div lang="ja">
	 * {@link Row}によりロウ操作できる{@link Collection}オブジェクトを
	 * 取得します。
	 *
	 * <p>期待するコンテナ種別が{@link ContainerType#COLLECTION}に限定され、
	 * 返却される型が{@link Collection}となる点を除き、
	 * {@link #getContainer(String)}と同様に振る舞います。</p>
	 *
	 * @return コレクションが存在する場合は対応する{@link Collection}、
	 * 存在しない場合は{@code null}
	 *
	 * @throws GSException 同名の時系列が存在する場合
	 * @throws GSException この処理のタイムアウト、接続障害が発生した場合、
	 * またはクローズ後に呼び出された場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 *
	 * @see #getContainer(String)
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Get a {@link Collection} object whose Rows can be processed using a {@link Row}.
	 *
	 * <p>Except for points where the expected Container type is limited to
	 * {@link ContainerType#COLLECTION} and the returned type is {@link Collection},
	 * the behavior will be the same as {@link #getContainer(String)}.</p>
	 *
	 * @return Corresponding {@link Collection} if a Collection exists
	 * and {@code null} if not.
	 *
	 * @throws GSException If a TimeSeries with the same name exists
	 * @throws GSException If this process times out, a connection failure were
	 * to occur, or if it is invoked after being closed
	 * @throws NullPointerException If {@code null} is specified in the argument
	 *
	 * @see #getContainer(String)
	 * </div>
	 */
	public <K> Collection<K, Row> getCollection(
			String name) throws GSException;

	/**
	 * <div lang="ja">
	 * {@link ContainerInfo}を指定して、時系列を新規作成または変更します。
	 *
	 * <p>コンテナ種別が{@link ContainerType#TIME_SERIES}に限定され、
	 * 返却される型が{@link TimeSeries}となる点を除き、
	 * {@link #putContainer(String, ContainerInfo, boolean)}
	 * と同様に振る舞います。</p>
	 *
	 * @param name 処理対象の時系列の名前
	 * @param info 処理対象の時系列の情報。コンテナ種別には{@code null}
	 * もしくは{@link ContainerType#TIME_SERIES}のいずれかを指定
	 * @param modifiable 既存時系列のカラムレイアウト変更を許可するかどうか
	 *
	 * @return 対応する{@link TimeSeries}
	 *
	 * @throws GSException コンテナ種別以外の指定内容に関して、
	 * {@link #putContainer(String, ContainerInfo, boolean)}
	 * の規則に合致しない場合。また、コンテナ種別に関する制限に合致しない場合
	 * @throws GSException この処理のタイムアウト、接続障害が発生した場合、
	 * またはクローズ後に呼び出された場合
	 * @throws NullPointerException 引数{@code info}に{@code null}が
	 * 指定された場合
	 *
	 * @see #putContainer(String, ContainerInfo, boolean)
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Specify {@link ContainerInfo} and create a new or update TimeSeries.
	 *
	 * <p>Except for points where the Container type is limited
	 * to {@link ContainerType#TIME_SERIES} and the returned type
	 * is {@link TimeSeries}, the behavior will be the same as
	 * {@link #putContainer(String, ContainerInfo, boolean)}.</p>
	 *
	 * @param name Name of TimeSeries subject to processing
	 * @param info Information of TimeSeries subject to processing. Specify
	 * either {@code null} or {@link ContainerType#TIME_SERIES} in the Container type
	 * @param modifiable To permit a change in the column layout of an existing
	 * TimeSeries or not
	 *
	 * @return Corresponding {@link TimeSeries}
	 *
	 * @throws GSException If specifications other than the Container type do not
	 * conform to the rules of {@link #putContainer(String, ContainerInfo, boolean)}.
	 * If the specifications do not conform to the restrictions related
	 * to the Container type as well
	 * @throws GSException If this process times out, a connection failure were
	 * to occur, or if it is invoked after being closed
	 * @throws NullPointerException If {@code null} is specified in the argument
	 * {@code info}
	 *
	 * @see #putContainer(String, ContainerInfo, boolean)
	 * </div>
	 */
	public TimeSeries<Row> putTimeSeries(String name,
			ContainerInfo info, boolean modifiable) throws GSException;

	/**
	 * <div lang="ja">
	 * {@link Row}によりロウ操作できる{@link TimeSeries}オブジェクトを
	 * 取得します。
	 *
	 * <p>期待するコンテナ種別が{@link ContainerType#TIME_SERIES}に限定され、
	 * 返却される型が{@link TimeSeries}となる点を除き、
	 * {@link #getTimeSeries(String)}と同様に振る舞います。</p>
	 *
	 * @return 時系列が存在する場合は対応する{@link TimeSeries}、
	 * 存在しない場合は{@code null}
	 *
	 * @throws GSException 同名のコレクションが存在する場合
	 * @throws GSException この処理のタイムアウト、接続障害が発生した場合、
	 * またはクローズ後に呼び出された場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 *
	 * @see #getContainer(String)
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Get a {@link TimeSeries} object whose Rows can be processed using
	 * a {@link Row}.
	 *
	 * <p>Except for points where the expected Container type is limited
	 * to {@link ContainerType#TIME_SERIES} and the returned type is
	 * {@link TimeSeries}, the behavior will be the same as
	 * {@link #getTimeSeries(String)}.</p>
	 *
	 * @return Corresponding {@link TimeSeries} if a TimeSeries exists
	 * and {@code null} if not.
	 *
	 * @throws GSException If a Collection with the same name exists
	 * @throws GSException If this process times out, a connection
	 * failure were to occur, or if it is invoked after being closed
	 * @throws NullPointerException If {@code null} is specified in the
	 * argument
	 *
	 * @see #getContainer(String)
	 * </div>
	 */
	public TimeSeries<Row> getTimeSeries(String name) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定の名前を持つコンテナを削除します。
	 *
	 * <p>削除済みの場合は何も変更しません。</p>
	 *
	 * <p>処理対象のコンテナにおいて実行中のトランザクションが存在する場合、
	 * それらの終了を待機してから削除を行います。</p>
	 *
	 * <p>コンテナの削除要求が完了した直後は、削除したコンテナの索引や
	 * ロウなどのために使用されていたメモリやストレージ領域を他の用途に
	 * ただちに再利用できない場合があります。また、削除処理に関連した処理が
	 * クラスタ上で動作することにより、削除前と比べて負荷が高まる期間が
	 * 一定程度継続する場合があります。</p>
	 *
	 * @param name 処理対象のコンテナの名前
	 *
	 * @throws GSException この処理のタイムアウト、接続障害が発生した場合、
	 * またはクローズ後に呼び出された場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 *
	 * @see #dropCollection(String)
	 * @see #dropTimeSeries(String)
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Delete a Container having the specified name
	 *
	 * <p>Nothing is changed if the Container has already been deleted.</p>
	 *
	 * <p>If a transaction under execution exists in a Container subject
	 * to processing, the system will wait for these to be completed
	 * before deleting the data.</p>
	 *
	 * <p>Immediately after the container deletion request is completed,
	 * the memory and storage area used for the index or row of the container
	 * may not be reused immediately. In addition, when delete processing
	 * is run on the cluster, increase in load may occur for sometime.</p>
	 *
	 * @param name Name of a Container subject to processing
	 *
	 * @throws GSException Name of a Container subject to processing
	 * @throws NullPointerException If {@code null} is specified in the argument
	 *
	 * @see #dropCollection(String)
	 * @see #dropTimeSeries(String)
	 * </div>
	 */
	public void dropContainer(String name) throws GSException;

	/**
	 * <div lang="ja">
	 * {@link ContainerInfo}を指定して、{@link Row}を新規作成します。
	 *
	 * <p>{@link Container}にて規定された制約に合致するよう、
	 * {@link ColumnInfo}のリストならびにロウキーの有無を含む
	 * カラムレイアウトを{@link ContainerInfo}に指定します。</p>
	 *
	 * <p>また、コンテナ種別を{@link ContainerInfo}に含めることで、
	 * 特定のコンテナ種別固有の制約に合致するかどうかを検証できます。
	 * ただし、作成された{@link Row}に対して{@link Row#getSchema()}
	 * を呼び出したとしても、コンテナ種別は含まれません。</p>
	 *
	 * <p>作成された{@link Row}の各フィールドには、{@link Container}にて定義
	 * されている空の値が初期値として設定されます。</p>
	 *
	 * <p>作成された{@link Row}に対する操作は、この{@link GridStore}オブジェクトの
	 * クローズ有無に影響しません。</p>
	 *
	 * @param info カラムレイアウトを含むコンテナ情報。その他の内容は無視される
	 *
	 * @return 作成された{@link Row}
	 *
	 * @throws GSException コンテナ種別もしくはカラムレイアウトの制約に合致しない場合
	 * @throws GSException クローズ後に呼び出された場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 *
	 * @see Container
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Specify {@link ContainerInfo} and create a new {@link Row}.
	 *
	 * <p>Include the {@link ColumnInfo} list and whether there is any Row key
	 * so as to conform to the restrictions stipulated in {@link Container}.
	 * Specify the column layout in {@link ContainerInfo}.</p>
	 *
	 * <p>In addition, by including the Container type in {@link ContainerInfo},
	 * it can be verified whether the restrictions unique to a specific Container
	 * type are conformed to or not.
	 * However, the Container type will not be included even if a
	 * {@link Row#getSchema()} is invoked against the created {@link Row}.</p>
	 *
	 * <p>An empty value defined in {@link Container} is set as the initial value
	 * for each field of the created {@link Row}.</p>
	 *
	 * <p>The operation on the created {@link Row} also does not affect whether this
	 * {@link GridStore} object is closed or not.</p>
	 *
	 * @param info Container information including the column layout. Other contents
	 * are ignored
	 *
	 * @return Created {@link Row}
	 *
	 * @throws GSException If the Container type or restrictions of the column layout
	 * are not conformed to
	 * @throws GSException If invoked after being closed
	 * @throws NullPointerException If {@code null} is specified in the argument
	 *
	 * @see Container
	 * </div>
	 */
	public Row createRow(ContainerInfo info) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定された任意個数の{@link Query}について、可能な限りリクエスト単位を大きくして
	 * クエリ実行とフェッチを行います。
	 *
	 * <p>指定のリストに含まれる各{@link Query}に対して、個別に{@link Query#fetch()}
	 * を行った場合と同様にクエリ実行とフェッチを行い、結果の{@link RowSet}を設定します。
	 * 各{@link Query}の実行結果を取り出すには、{@link Query#getRowSet()}
	 * を使用します。ただし、個別に行う場合と違い、同一の格納先などの可能な限り
	 * 大きな単位で対象ノードに対しリクエストしようとします。これにより、リストの要素数が
	 * 多くなるほど、対象ノードとやりとりする回数が削減される可能性が高くなります。
	 * リスト内の{@link Query}の実行順序は不定です。</p>
	 *
	 * <p>指定のリストには、この{@link GridStore}オブジェクトを介して得られた、
	 * 対応する{@link Container}を含めクローズされていない{@link Query}
	 * のみを含めることができます。
	 * {@link Query#fetch()}と同様、各{@link Query}が持つ最後に生成された
	 * {@link RowSet}がクローズされます。
	 * 同一のインスタンスがリストに複数含まれていた場合、それぞれ異なる
	 * インスタンスであった場合と同様に振る舞います。</p>
	 *
	 * <p>他のコンテナ・ロウ操作と同様、異なるコンテナ間での整合性は保証されません。
	 * したがって、あるコンテナに対する処理の結果は、その処理の開始前に完了した
	 * 他の操作命令の影響を受けることがあります。</p>
	 *
	 * <p>指定の{@link Query}に対応する各{@link Container}のコミットモードが
	 * 自動コミットモード、手動コミットモードのいずれであったとしても、使用できます。
	 * トランザクション状態はクエリの実行結果に反映されます。
	 * 正常に操作が完了した場合、トランザクションタイムアウト時間に到達しない限り、
	 * 対応する各{@link Container}のトランザクションをアボートすることはありません。</p>
	 *
	 * <p>各{@link Query}に対する処理の途中で例外が発生した場合、
	 * 一部の{@link Query}についてのみ新たな{@link RowSet}
	 * が設定されることがあります。また、指定の{@link Query}に対応する
	 * 各{@link Container}の未コミットのトランザクションについては、アボートされる
	 * ことがあります。</p>
	 *
	 * <p>一度に大量のロウを取得しようとした場合、GridDBノードが管理する
	 * 通信バッファのサイズの上限に到達し、失敗することがあります。
	 * 上限サイズについては、GridDBテクニカルリファレンスを参照してください。</p>
	 *
	 * <p>送出する{@link GSException}は、{@code container}パラメータを含む
	 * ことがあります。エラーに関するパラメータの詳細は、{@link GridStore}の定義を
	 * 参照してください。</p>
	 *
	 * @param queryList 対象とする{@link Query}のリスト
	 *
	 * @throws GSException この{@link GridStore}オブジェクトを介して得られた
	 * {@link Query}以外の{@link Query}が含まれていた場合
	 * @throws GSException 正しくないパラメータ・構文・命令を含むクエリを
	 * 実行しようとした場合。たとえば、TQLでは、関数の引数に対応しない型のカラムを
	 * 指定した場合。具体的な制約は、このクエリを作成する機能の各種定義を
	 * 参照のこと
	 * @throws GSException TQLを実行した際、実行結果が期待する結果
	 * {@link RowSet}の各要素の型と合致しない場合
	 * @throws GSException この処理または関連するトランザクションのタイムアウト、
	 * 対応するコンテナの削除もしくはスキーマ変更、接続障害が発生した場合、
	 * または対応するコンテナのクローズ後に呼び出された場合
	 * @throws NullPointerException 引数{@code queryList}に
	 * {@code null}が指定された場合、または、引数{@code queryList}の
	 * 要素として{@code null}が含まれていた場合
	 * @throws NullPointerException このクエリを作成する際に与えられたパラメータの
	 * 中に、許容されない{@code null}が含まれていた場合。GridDB上で実行される
	 * TQL文の評価処理の結果として送出されることはない
	 *
	 * @see Query#fetch()
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Query execution and fetch is carried out on a specified arbitrary number
	 * of {@link Query}, with the request unit enlarged as much as possible.
	 *
	 * <p>For each {@link Query} included in a specified list, perform
	 * a similar query execution and fetch as when {@link Query#fetch()}
	 * is performed individually and set the {@link RowSet} in the results.
	 * Use {@link Query#getRowSet()} to extract the execution results of each
	 * {@link Query}. However, unlike the case when carried out individually,
	 * the target node is requested for the same storage destination, etc.
	 * with a unit that is as large as possible. Based on this, the larger
	 * the number of elements in the list, the higher is the possibility
	 * that the number of correspondences with the target node will be reduced.
	 * {@link Query} in a list are not executed in any particular order.</p>
	 *
	 * <p>Only a {@link Query} that has not been closed, including corresponding
	 * {@link Container} acquired via this {@link GridStore} object, can be included
	 * in a specified list.
	 * Like a {@link Query#fetch()}, the {@link RowSet} formed last and held
	 * by each {@link Query} will be closed.
	 * If the same instance is included multiple times in a list, the behavior
	 * will be the same as the case in which the respective instances differ.</p>
	 *
	 * <p>Like other Container or Row operations, consistency between Containers
	 * is not guaranteed.
	 * Therefore, the processing results for a certain Container may be affected
	 * by other operation commands that have been completed prior to the start
	 * of the process.</p>
	 *
	 * <p>The commit mode of each {@link Container} corresponding to the specified
	 * {@link Query} can be used in either the auto commit mode or manual
	 * commit mode.
	 * The transaction status is reflected in the execution results of the query.
	 * If the operation is completed normally, the corresponding transaction of
	 * each {@link Container} will not be aborted so long as the transaction
	 * timeout time has not been reached.</p>
	 *
	 * <p>If an exception occurs in the midst of processing each {@link Query},
	 * a new {@link RowSet} may be set for only some of the {@link Query}. In addition,
	 * uncommitted transactions of each {@link Query} corresponding to the designated
	 * {@link Container} may be aborted.</p>
	 *
	 * <p>If the system tries to acquire a large number of Rows all at once,
	 * the upper limit of the communication buffer size managed by the GridDB
	 * node may be reached, possibly resulting in a failure.
	 * Refer to "System limiting values" in the GridDB Technical Reference for the upper limit size.</p>
	 *
	 * <p>The thrown {@link GSException} may contain {@code container}
	 * parameter. For the details of the parameters
	 * related the error, see the definition of {@link GridStore}.</p>
	 *
	 * @param queryList List of {@link Query} targeted
	 *
	 * @throws If a {@link Query} other than a {@link Query} obtained via this
	 * {@link GridStore} object is included
	 * @throws GSException If the system tries to execute a query containing
	 * a wrong parameter, syntax or command. For example, when a column type
	 * that is not compatible with the argument of the function is specified
	 * in the TQL. Refer to the various definitions of the function to create
	 * this query for the specific restrictions
	 * @throws GSException If the execution results do not conform to the expected
	 * type of each component of the {@link RowSet}, when a TQL is executed.
	 * @throws GSException If this process or related transaction times out,
	 * if the corresponding Container is deleted or the schema is changed,
	 * if a connection failure occurs, or if the corresponding Container is
	 * invoked after being closed
	 * @throws NullPointerException If {@code null} is specified in the argument
	 * {@code queryList}, or if {@code null} is included as a component
	 * the argument {@code queryList}
	 * @throws NullPointerException If a non-permitted {@code null} is included
	 * in the parameter given when creating this query. To be executed in GridDB
	 * The evaluation results of the TQL text will not be sent out
	 *
	 * @see Query#fetch()
	 * </div>
	 */
	public void fetchAll(
			List<? extends Query<?>> queryList) throws GSException;

	/**
	 * <div lang="ja">
	 * 任意のコンテナの任意個数のロウについて、可能な限りリクエスト単位を大きくして
	 * 新規作成または更新操作を行います。
	 *
	 * <p>指定のマップに含まれる各ロウオブジェクトについて、個別に
	 * {@link Container#put(Object)}を呼び出した場合と同様に
	 * 新規作成または更新操作を行います。
	 * ただし、個別に行う場合と違い、同一の格納先などの可能な限り大きな単位で
	 * 対象ノードに対しリクエストしようとします。これにより、対象コンテナの総数や
	 * 指定のロウオブジェクトの総数が多くなるほど、対象ノードとやりとりする回数が
	 * 削減される可能性が高くなります。</p>
	 *
	 * <p>指定のマップは、コンテナ名をキー、ロウオブジェクトのリストを値とする任意個数の
	 * エントリから構成されます。対象とするコンテナとして、コンテナ種別やカラムレイアウトが
	 * 異なるものを混在させることができます。ただし、すでに存在するコンテナで
	 * なければなりません。マップのキーまたは値として{@code null}を含めることは
	 * できません。</p>
	 *
	 * <p>各ロウオブジェクトのリストには、対象のコンテナと同一のカラムレイアウトの
	 * {@link Row}のみを任意個数含めることができます。
	 * 現バージョンでは、カラム順序についてもすべて同一でなければなりません。
	 * リストの要素として{@code null}を含めることはできません。</p>
	 *
	 * <p>コンテナの種別ならびに設定によっては、操作できるロウの内容について
	 * {@link Container#put(Object)}と同様の制限が設けられています。
	 * 具体的な制限事項は、{@link Container}のサブインタフェースの定義を
	 * 参照してください。</p>
	 *
	 * <p>指定のマップ内に同一コンテナを対象とした同一ロウキーを持つ複数の
	 * ロウオブジェクトが存在する場合、異なるリスト間であればマップエントリ集合の
	 * イテレータからの取り出し順、同一リスト内であればリストの要素順を基準として、
	 * 同値のロウキーを持つ最も後方にあるロウオブジェクトの内容が反映されます。</p>
	 *
	 * <p>トランザクションを維持し、ロックを保持し続けることはできません。
	 * ただし、既存のトランザクションが対象ロウに影響するロックを確保している場合、
	 * すべてのロックが解放されるまで待機し続けます。</p>
	 *
	 * <p>他のコンテナ・ロウ操作と同様、異なるコンテナ間での整合性は保証されません。
	 * したがって、あるコンテナに対する処理の結果は、その処理の開始前に完了した
	 * 他の操作命令の影響を受けることがあります。</p>
	 *
	 * <p>各コンテナならびにロウに対する処理の途中で例外が発生した場合、
	 * 一部のコンテナの一部のロウに対する操作結果のみが反映されたままとなることが
	 * あります。</p>
	 *
	 * <p>送出する{@link GSException}は、{@code container}パラメータを含む
	 * ことがあります。エラーに関するパラメータの詳細は、{@link GridStore}の定義を
	 * 参照してください。</p>
	 *
	 * @param containerRowsMap 対象とするコンテナの名前とロウオブジェクトの
	 * リストからなるマップ
	 *
	 * @throws GSException 対象とするコンテナが存在しない場合、また、対象とする
	 * コンテナとロウオブジェクトとのカラムレイアウトが一致しない場合
	 * @throws GSException 特定コンテナ種別固有の制限に反する操作を行った場合
	 * @throws GSException この処理またはトランザクションのタイムアウト、
	 * 接続障害が発生した場合、クローズ後に呼び出された場合、または
	 * サポート範囲外の値が ロウオブジェクトに含まれていた場合
	 * @throws NullPointerException 引数{@code containerRowsMap}として
	 * {@code null}が指定された場合、このマップのキーまたは値として{@code null}が
	 * 含まれていた場合、もしくは、マップを構成するリストの要素として{@code null}が
	 * 含まれていた場合
	 *
	 * @see Container#put(Object)
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * New creation or update operation is carried out on an arbitrary number
	 * of rows of a Container, with the request unit enlarged as much as possible.
	 *
	 * <p>For each Row object included in a specified map, a new creation
	 * or update operation is carried out just like the case when
	 * {@link Container#put(Object)} is invoked individually.
	 * However, unlike the case when carried out individually, the target node
	 * is requested for the same storage destination, etc. with a unit
	 * that is as large as possible. Based on this, the larger the total number
	 * of Row objects specified and the larger the total number of target Containers,
	 * the higher is the possibility that the number of correspondences
	 * with the target node will be reduced.</p>
	 *
	 * <p>A specified map is composed of an arbitrary number of entries that adopt
	 * the Container name as its key and list of Row objects as its value.
	 * A subject Container may be a mixture of different Container types and
	 * column layouts. However, the Containers must already exist. The Container
	 * cannot include {@code null} as a key or value of the map.</p>
	 *
	 * <p>An arbitrary number of {@link Row} with the same column layout as the
	 * subject Container can be included in each list of Row objects.
	 * In the current version, all the column sequences must also be the same.
	 * The Container cannot include {@code null} as a component of the list.</p>
	 *
	 * <p>Depending on the Container type and setting, the same restrictions
	 * as {@link Container#put(Object)} are established for the contents of Rows
	 * that can be operated.
	 * Refer to the sub-interface definition of {@link Container} for the specific
	 * restrictions.</p>
	 *
	 * <p>If there are multiple Row objects having the same Row key targeting
	 * the same Container in the designated map, the contents of the rear-most
	 * Row object having a Row key with the same value will be reflected using
	 * the take-out sequence from the iterator of the map entry group as
	 * a reference if it is between different lists, or the component sequence
	 * of the list as a reference if it is within the same list.</p>
	 *
	 * <p>The transaction cannot be maintained and the lock cannot continue
	 * to be retained.
	 * However, if the lock that affects the target Row is secured by an existing
	 * transaction, the system will continue to wait for all the locks
	 * to be released.</p>
	 *
	 * <p>Like other Container or Row operations, consistency between Containers
	 * is not guaranteed.
	 * Therefore, the processing results for a certain Container may be affected
	 * by other operation commands that have been completed prior to the start
	 * of the process.</p>
	 *
	 * <p>If an exclusion occurs in the midst of processing a Container
	 * and its Rows, only the results for some of the Rows of some of the
	 * Containers may remain reflected.</p>
	 *
	 * <p>The thrown {@link GSException} may contain {@code container}
	 * parameter. For the details of the parameters
	 * related the error, see the definition of {@link GridStore}.</p>
	 *
	 * @param containerRowsMap A map made up of a list of Row objects
	 * and target Container names
	 *
	 * @throws GSException If the target Container does not exist, or if the column
	 * layouts of the target Container and Row object do not match
	 * @throws GSException When an operation violating the restrictions unique
	 * to a specific Container type is carried out
	 * @throws GSException If this process or transaction times out, a connection
	 * failure were to occur, or if it is invoked after being closed, or if a value
	 * outside the supported range is included in the Row object
	 * @throws NullPointerException If {@code null} is specified as an argument
	 * {@code containerRowsMap}, if {@code null} is included as a key or value
	 * of this map, or if {@code null} is included as a component of the list
	 * constituting the map
	 *
	 * @see Container#put(Object)
	 * </div>
	 */
	public void multiPut(
			Map<String, List<Row>> containerRowsMap) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定の条件に基づき、任意のコンテナの任意個数・範囲のロウについて、
	 * 可能な限りリクエスト単位を大きくして取得します。
	 *
	 * <p>指定のマップに含まれる条件に従い、個別に{@link Container#get(Object)}
	 * もしくは{@link Query#fetch()}を呼び出した場合と同様に、ロウの内容を取得します。
	 * ただし、個別に行う場合と違い、同一の格納先などの可能な限り大きな単位で
	 * 対象ノードに対しリクエストしようとします。これにより、対象コンテナの総数や
	 * 条件に合致するロウの総数が多くなるほど、対象ノードとやりとりする回数が
	 * 削減される可能性が高くなります。</p>
	 *
	 * <p>指定のマップは、コンテナ名をキー、{@link RowKeyPredicate}で表現される
	 * 取得条件を値とする任意個数のエントリから構成されます。
	 * 同一の{@link RowKeyPredicate}インスタンスを複数含めることもできます。
	 * また、対象とするコンテナとして、コンテナ種別やカラムレイアウトが異なるものを
	 * 混在させることができます。
	 * ただし、コンテナの構成によっては評価できない取得条件が存在します。
	 * 具体的な制限については、{@link RowKeyPredicate}に対する各種設定機能の
	 * 定義を参照してください。
	 * マップのキーまたは値として{@code null}を含めることはできません。</p>
	 *
	 * <p>返却されるマップは、コンテナ名をキー、ロウオブジェクトのリストを
	 * 値とするエントリにより構成されます。また、返却されるマップには、取得条件
	 * として指定したマップに含まれるコンテナ名のうち、リクエスト時点で実在する
	 * コンテナに 関するエントリのみが含まれます。
	 * ASCIIの大文字・小文字表記だけが異なり同一のコンテナを指すコンテナ名の
	 * 設定された、複数のエントリが指定のマップに含まれていた場合、返却される
	 * マップにはこれらを1つにまとめたエントリが格納されます。
	 * 同一のリストに複数のロウオブジェクトが含まれる場合、格納される順序は
	 * コンテナ種別と対応する{@link Container}のサブインタフェースの定義に従います。
	 * 指定のコンテナに対応するロウが1つも存在しない場合、対応するロウオブジェクトの
	 * リストは空になります。</p>
	 *
	 * <p>返却されたマップもしくはマップに含まれるリストに対して変更操作を行った場合に、
	 * {@link UnsupportedOperationException}などの実行時例外が発生するか
	 * どうかは未定義です。</p>
	 *
	 * <p>他のコンテナ・ロウ操作と同様、異なるコンテナ間での整合性は保証されません。
	 * したがって、あるコンテナに対する処理の結果は、その処理の開始前に完了した
	 * 他の操作命令の影響を受けることがあります。</p>
	 *
	 * <p>{@link Container#get(Object, boolean)}もしくは
	 * {@link Query#fetch(boolean)}のように、トランザクションを維持し、
	 * 更新用ロックを要求することはできません。</p>
	 *
	 * <p>一度に大量のロウを取得しようとした場合、GridDBノードが管理する
	 * 通信バッファのサイズの上限に到達し、失敗することがあります。
	 * 上限サイズについては、GridDBテクニカルリファレンスを参照してください。</p>
	 *
	 * <p>送出する{@link GSException}は、{@code container}パラメータを含む
	 * ことがあります。エラーに関するパラメータの詳細は、{@link GridStore}の定義を
	 * 参照してください。</p>
	 *
	 * @param containerPredicateMap 対象とするコンテナの名前と条件からなるマップ
	 *
	 * @return 条件に合致するロウ集合をコンテナ別に保持するマップ
	 *
	 * @throws GSException 指定のコンテナに関して評価できない取得条件が
	 * 指定された場合
	 * @throws GSException この処理またはトランザクションのタイムアウト、
	 * 接続障害が発生した場合、クローズ後に呼び出された場合
	 * @throws NullPointerException 引数{@code containerPredicateMap}として
	 * {@code null}が指定された場合、このマップのキーまたは値として{@code null}が
	 * 含まれていた場合
	 *
	 * @see Container#get(Object)
	 * @see Query#fetch()
	 * @see RowKeyPredicate
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Get an arbitrary number and range of Rows of a Container based on the
	 * specified conditions, with the request unit enlarged as much as possible.
	 *
	 * <p>Get the Row contents in accordance with the conditions included in the
	 * specified map, similar to invoking {@link Container#get(Object)}
	 * or {@link Query#fetch()} individually. However, unlike the case
	 * when carried out individually, the target node is requested for the same
	 * storage destination, etc. with a unit that is as large as possible.
	 * Based on this, the larger the total number of Rows conforming
	 * to the conditions and the larger the total number of target Containers,
	 * the higher is the possibility that the number of correspondences
	 * with the target node will be reduced.</p>
	 *
	 * <p>A specified map is composed of an arbitrary number of entries
	 * that adopt the Container name as the key and the acquisition condition
	 * represented by {@link RowKeyPredicate} as the value.
	 * Multiple instances with the same {@link RowKeyPredicate} can also be included.
	 * In addition, a subject Container may be a mixture of different Container
	 * types and column layouts.
	 * However, there are some acquisition conditions that cannot be evaluated
	 * due to the composition of the Container. Refer to the definitions of
	 * the various setting functions for {@link RowKeyPredicate} for the specific
	 * restrictions.
	 * In addition, the specified Container name must be a real Container.
	 * The Container cannot include {@code null} as a key or value of the map.</p>
	 *
	 * <p>A returned map is composed of entries that adopt the Container name
	 * as its key and list of Row objects as its value. And only the
	 * real Container names at the request included in a specified
	 * map as acquisition conditions are included in a returned map.
	 * If multiple entries in which Container names with different
	 * notations in uppercase and lowercase letters that specify
	 * the same Container are set are included in a specified map,
	 * a single entry consolidating these is stored in the returned map.
	 * If multiple Row objects are included in the same list, the stored sequence
	 * follows the Container type and the definition of the sub-interface
	 * of the corresponding {@link Container}.
	 * If not a single Row corresponding to the specified Container exists,
	 * the list of corresponding Row objects is blank.</p>
	 *
	 * <p>When a returned map or list included in a map is updated, whether
	 * an exception will occur or not when {@link UnsupportedOperationException},
	 * etc. is executed is still not defined.</p>
	 *
	 * <p>Like other Container or Row operations, consistency between Containers
	 * is not guaranteed.
	 * Therefore, the processing results for a certain Container may be affected
	 * by other operation commands that have been completed prior to the start
	 * of the process.</p>
	 *
	 * <p>Like {@link Container#get(Object, boolean)} or {@link Query#fetch(boolean)},
	 * a transaction cannot be maintained and requests for updating locks cannot
	 * be made.</p>
	 *
	 * <p>If the system tries to acquire a large number of Rows all at once,
	 * the upper limit of the communication buffer size managed by the GridDB
	 * node may be reached, possibly resulting in a failure.
	 * Refer to "System limiting values" in the GridDB Technical Reference for the upper limit size.</p>
	 *
	 * <p>The thrown {@link GSException} may contain {@code container}
	 * parameter. For the details of the parameters
	 * related the error, see the definition of {@link GridStore}.</p>
	 *
	 * @param containerPredicateMap Map made up of targeted Container names
	 * and conditions
	 *
	 * @return Map that maintains Row groups conforming to the conditions
	 * by Container
	 *
	 * @throws GSException If acquisition conditions concerning a specified
	 * Container that cannot be evaluated are specified
	 * @throws GSException If this process or transaction times out, a connection
	 * failure were to occur, or if it is invoked after being closed
	 * @throws NullPointerException If {@code null} is specified as an argument
	 * {@code containerRowsPredicate}, if {@code null} is included as a key or
	 * value of this map
	 *
	 * @see Container#get(Object)
	 * @see Query#fetch()
	 * @see RowKeyPredicate
	 * </div>
	 */
	public Map<String, List<Row>> multiGet(
			Map<String, ? extends RowKeyPredicate<?>> containerPredicateMap)
					throws GSException;

	/**
	 * <div lang="ja">
	 * 対応するGridDBクラスタについての{@link PartitionController}を取得します。
	 *
	 * <p>この{@link GridStore}をクローズした時点で使用できなくなります。</p>
	 *
	 * @return 対応するGridDBクラスタについての{@link PartitionController}
	 *
	 * @throws GSException クローズ後に呼び出された場合
	 *
	 * @see PartitionController
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Get {@link PartitionController} on the corresponding GridDB cluster.
	 *
	 * <p>Use is no longer possible once this {@link GridStore} is closed.</p>
	 *
	 * @return {@link PartitionController} on the corresponding GridDB cluster
	 *
	 * @throws GSException If invoked after being closed
	 *
	 * @see PartitionController
	 * </div>
	 */
	public PartitionController getPartitionController() throws GSException;

	/**
	 * <div lang="ja">
	 * ロウオブジェクトの型と{@link ContainerInfo}を指定して、コンテナを
	 * 新規作成または変更します。
	 *
	 * <p>主に、ロウオブジェクトの型を指定して、追加設定を持つコンテナを新規作成
	 * する場合に使用します。</p>
	 *
	 * <p>カラムレイアウトならびにカラム順序の無視設定を{@link ContainerInfo}に
	 * 指定できない点を除けば、
	 * {@link #putContainer(String, ContainerInfo, boolean)}と同様です。</p>
	 *
	 * @param name 処理対象のコンテナの名前
	 * @param rowType 処理対象のコレクションのカラムレイアウトと対応する
	 * ロウオブジェクトの型
	 * @param info 処理対象のコンテナの情報。{@code null}の場合は無視される
	 * @param modifiable 既存コンテナのカラムレイアウト変更を許可するかどうか
	 *
	 * @return 対応する{@link Container}。コンテナ種別として
	 * {@link ContainerType#COLLECTION}を指定した場合は{@link Collection}、
	 * {@link ContainerType#TIME_SERIES}を指定した場合は{@link TimeSeries}の
	 * インスタンスとなる。
	 *
	 * @throws GSException {@code name}ならびに{@code info}引数の
	 * 内容が規則に合致しない場合。また、指定のコンテナ種別に対応する
	 * コンテナ新規作成・変更メソッドの規則に合致しない場合
	 * @throws GSException 指定の型がロウオブジェクトの型として適切でない場合。
	 * 詳しくは{@link Container}の定義を参照
	 * @throws GSException この処理のタイムアウト、接続障害が発生した場合、
	 * またはクローズ後に呼び出された場合
	 * @throws NullPointerException {@code rowType}引数に{@code null}が
	 * 指定された場合
	 *
	 * @see #putContainer(String, ContainerInfo, boolean)
	 * @see #putCollection(String, Class, boolean)
	 * @see #putTimeSeries(String, Class, TimeSeriesProperties, boolean)
	 *
	 * @since 2.1
	 * </div><div lang="en">
	 * Specify the Row object type and {@link ContainerInfo} and create a new
	 * Container or update a Container.
	 *
	 * <p>Mainly used when specifying the type of Row object and creating a new
	 * Container with additional settings.</p>
	 *
	 * <p>Same as {@link #putContainer(String, ContainerInfo, boolean)}
	 * if points for which the ignore setting of the column layout and column
	 * sequence cannot be specified are excluded.</p>
	 *
	 * @param name Name of a Container subject to processing
	 * @param rowType Type of Row object corresponding to the column layout
	 * of the Collection subject to processing
	 * @param info Information of a Container subject to processing. Ignored
	 * if {@code null} is specified
	 * @param modifiable To permit a change in the column layout of existing
	 * Container data or not
	 *
	 * @return Corresponding {@link Container}. If {@link ContainerType#COLLECTION}
	 * is specified as the Container type, the instance created will be a
	 * {@link Collection} instance. If {@link ContainerType#TIME_SERIES}
	 * is specified, the instance created will be a {@link TimeSeries} instance.
	 *
	 * @throws GSException If the contents of the arguments {@code name}
	 * and {@code info} do not conform to the rules. If the contents also do not
	 * conform to the rules of the new Container creation and update method
	 * for the specified Container type
	 * @throws GSException If the specified type is not suitable as the Row
	 * object type.
	 * Refer to the definition of {@link Container} for details.
	 *
	 * @throws GSException If this process times out, a connection failure
	 * were to occur, or if it is invoked after being closed
	 * @throws NullPointerException If this process times out, a connection
	 * failure were to occur, or if it is invoked after being closed
	 *
	 * @see #putContainer(String, ContainerInfo, boolean)
	 * @see #putCollection(String, Class, boolean)
	 * @see #putTimeSeries(String, Class, TimeSeriesProperties, boolean)
	 * </div>
	 */
	public <K, R> Container<K, R> putContainer(
			String name, Class<R> rowType,
			ContainerInfo info, boolean modifiable)
			throws GSException;

}
