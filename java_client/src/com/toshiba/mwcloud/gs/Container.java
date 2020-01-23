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
import java.net.URL;
import java.sql.Blob;

import javax.sql.rowset.serial.SerialBlob;

import com.toshiba.mwcloud.gs.common.GSErrorCode;
import com.toshiba.mwcloud.gs.common.RowMapper;

/**
 * <div lang="ja">
 * 同一タイプのロウ集合からなるGridDBの構成要素に対しての、管理機能を提供します。
 *
 * <p>ロウオブジェクトを入出力の基本単位として、各種管理機能を提供します。
 * ロウオブジェクトとGridDB上のロウは、指定のロウオブジェクト型とGridDB上の
 * スキーマとの対応関係に基づいて、相互にマッピングされます。</p>
 *
 * <p>GridDB上のスキーマを構成する各カラムは、ロウオブジェクト内の
 * フィールドやメソッド定義と対応関係を持ちます。
 * 1つのコンテナは1つ以上のカラムにより構成されます。
 * 各カラムとの対応関係は、指定の型のpublic・protected・デフォルトアクセスのフィールド
 * またはgetter・setterメソッドに基づき決定されます。
 * ただし、{@link TransientRowField}が指定されたフィールドやメソッド、
 * transientフィールドは対象外です。
 * また、ロウオブジェクトを動的に生成するために、public・protected・デフォルトアクセスの
 * デフォルトコンストラクタを持つ必要があります。内部クラスの場合、静的内部クラスである
 * 必要があります。</p>
 *
 * <p>getterは、{@code boolean}値を返す場合「is」または「get」、それ以外の型の
 * 値を返す場合「get」から始まる名前を持つ、引数なしのメソッドです。
 * setterは、「set」から始まる名前を持ち、設定対象の値1つのみを引数とする
 * メソッドです。
 * GridDB上のカラム名は、特に指定のない限り、フィールド名、もしくは、getter・
 * setterのメソッド名から「get」などの固定の接頭辞を除いたものと対応します。
 * 別のカラム名と対応付けるには、{@link RowField#name()}を使用します。
 * getter・setterの一方しか存在しないものは無視されます。同名のフィールドと
 * getter・setterの両方が存在する場合は、getter・setterを使用します。
 * getter・setter名の大文字・小文字表記が異なる場合、getterのものが優先されます。
 * カラムがロウキーを持つ場合は、対応するフィールドまたはメソッドに{@link RowKey}を
 * 設定します。</p>
 *
 * <p>1つのコンテナのカラム間で、ASCIIの大文字・小文字表記だけが異なる
 * 名前のものを複数定義することはできません。その他、コンテナ定義における
 * カラム名の文字種や長さ、カラム数には制限があります。具体的には、
 * GridDBテクニカルリファレンスを参照してください。特に記載のない限り、
 * カラム名を指定する操作では、ASCIIの大文字・小文字表記の違いは区別
 * されません。</p>
 *
 * <p>カラムの型と、ロウオブジェクト内の各値の型との対応は、それぞれ次の通りです。</p>
 * <table>
 * <thead><tr><th>カラム型</th><th>ロウオブジェクト内の各値の型</th></tr></thead>
 * <tbody>
 * <tr><td>STRING</td><td>{@link String}</td></tr>
 * <tr><td>BOOL</td><td>{@link Boolean}または{@code boolean}</td></tr>
 * <tr><td>BYTE</td><td>{@link Byte}または{@code byte}</td></tr>
 * <tr><td>SHORT</td><td>{@link Short}または{@code short}</td></tr>
 * <tr><td>INTEGER</td><td>{@link Integer}または{@code int}</td></tr>
 * <tr><td>LONG</td><td>{@link Long}または{@code long}</td></tr>
 * <tr><td>FLOAT</td><td>{@link Float}または{@code float}</td></tr>
 * <tr><td>DOUBLE</td><td>{@link Double}または{@code double}</td></tr>
 * <tr><td>TIMESTAMP</td><td>{@link java.util.Date}</td></tr>
 * <tr><td>GEOMETRY</td><td>{@link Geometry}</td></tr>
 * <tr><td>BLOB</td><td>{@link Blob}を実装したクラス</td></tr>
 * <tr><td>STRING配列</td><td>{@code String[]}</td></tr>
 * <tr><td>BOOL配列</td><td>{@code boolean[]}</td></tr>
 * <tr><td>BYTE配列</td><td>{@code byte[]}</td></tr>
 * <tr><td>SHORT配列</td><td>{@code short[]}</td></tr>
 * <tr><td>INTEGER配列</td><td>{@code int[]}</td></tr>
 * <tr><td>LONG配列</td><td>{@code long[]}</td></tr>
 * <tr><td>FLOAT配列</td><td>{@code float[]}</td></tr>
 * <tr><td>DOUBLE配列</td><td>{@code double[]}</td></tr>
 * <tr><td>TIMESTAMP配列</td><td>{@code java.util.Date[]}</td></tr>
 * </tbody>
 * </table>
 *
 * <p>フィールドの値の表現範囲やサイズには制限があります。
 * 具体的には、付録の章の値の範囲の説明、ならびに、GridDBテクニカル
 * リファレンスを参照してください。制限に反する値をコンテナに格納することは
 * できません。</p>
 *
 * <p>ロウキーとして許可されている型や、ロウキーに対応するカラムの有無、
 * ロウ操作の可否といった制約は、このコンテナのサブインタフェースの定義によって
 * 異なります。</p>
 *
 * <p>GridDB上のロウにおけるNULLは、NOT NULL制約が設定されていない
 * 限り保持することができます。ロウオブジェクトのフィールドまたはgetter・setter
 * メソッドが参照型の値を入出力できる場合、GridDB上のロウにおけるNULLは
 * {@code null}として入出力できます。それ以外の場合、NULLはロウオブジェクトに
 * おいて後述の空の値にマッピングされます。</p>
 *
 * <p>ロウオブジェクト型におけるNOT NULL制約は、{@link NotNull}ならびに
 * {@link Nullable}により明示的に指定できます。NOT NULL制約がいずれの
 * 指定対象にも指定されていない場合、ロウキー以外のカラムはNOT NULL
 * 制約なしであるとみなされます。ロウキーは暗黙的にNOT NULL制約が設定された
 * 状態となっており、この制約を外すような指定はできません。また、同一の
 * 指定対象や、getter、setterメソッド間での矛盾したNOT NULL制約は
 * 指定できません。たとえば、ロウオブジェクト型に{@link NotNull}と
 * {@link Nullable}が同時に指定された場合、矛盾したNOT NULL制約が
 * 指定されたとみなされます。NOT NULL制約の有無に関する指定対象別の
 * 優先順位は、次の通りです。</p>
 * <ol>
 * <li>ロウオブジェクトのフィールドまたはgetter・setterメソッド</li>
 * <li>ロウオブジェクトの型</li>
 * <li>ロウオブジェクトのエンクロージング型(例: ロウオブジェクトのクラスを
 * 内部クラスとして取り囲むインタフェース)、または、その型から再帰的に
 * 求まるエンクロージング型。再帰的に求まるエンクロージング型のうち、
 * 制約の指定があり、かつ、最初に見つかった型を優先。</li>
 * <li>ロウオブジェクトの型が属するパッケージ</li>
 * </ol>
 *
 * <p>空の値は、{@link Row}の作成など各種操作の初期値などとして用いられる
 * ことのある、フィールド値の一種です。以下のように、カラム型ごとに値が定義
 * されています。</p>
 * <table>
 * <thead><tr><th>カラム型</th><th>空の値</th></tr></thead>
 * <tbody>
 * <tr><td>STRING</td><td>{@code ""}(長さ0の文字列)</td></tr>
 * <tr><td>BOOL</td><td>偽({@code false})</td></tr>
 * <tr><td>数値型</td><td>{@code 0}</td></tr>
 * <tr><td>TIMESTAMP</td><td>{@code 1970-01-01T00:00:00Z}</td></tr>
 * <tr><td>GEOMETRY</td><td>{@code POINT(EMPTY)}</td></tr>
 * <tr><td>BLOB</td><td>長さ0のBLOBデータ</td></tr>
 * <tr><td>配列型</td><td>要素数0の配列</td></tr>
 * </tbody>
 * </table>
 *
 * <p>トランザクション処理では、デフォルトで自動コミットモードが有効になっています。
 * 自動コミットモードでは、変更操作は逐次確定し、明示的に取り消すことができません。
 * 手動コミットモードにおいて、このオブジェクトを介した操作によりクラスタノード上でエラーが
 * 検出され{@link GSException}が送出された場合、コミット前の更新操作は
 * すべて取り消されます。トランザクション分離レベルはREAD COMMITTEDのみを
 * サポートします。
 * ロック粒度は、コンテナの種別によって異なります。</p>
 *
 * <p>この{@link Container}の生成後またはトランザクション終了後、最初にロウの更新・追加・
 * 削除、ならびに更新用ロック獲得が行われた時点で、新たなトランザクションが開始されます。
 * 自動コミットモードでは、トランザクションを開始したロウ操作が完了するとトランザクションは
 * 自動的にコミットされ終了します。
 * 手動コミットモードでは、明示的にトランザクションを制御するか有効期限に到達するまで
 * トランザクションは終了しません。トランザクションをコミットするには{@link #commit()}、
 * アボートするには{@link #abort()}を使用します。
 * この{@link Container}またはその生成元の{@link GridStore}がクローズされた場合も
 * トランザクションはアボートされ終了します。
 * また、トランザクションを開始させた操作を行った時刻を起点として、GridDB上で
 * 定められている期間だけ経過すると有効期限に到達します。有効期限到達後にアボートすること
 * なく、続けてロウ操作やトランザクションコミットを行おうとすると、{@link GSException}が
 * 送出されるようになります。</p>
 *
 * <p>あるコンテナへの操作要求に対するクラスタノード上での処理が開始され、
 * 終了するまでの間、同一のコンテナに対する操作が待機させられる場合が
 * あります。ここでの操作には、コンテナのスキーマや索引などの定義変更、コンテナ
 * 情報の参照、ロウ操作などが含まれます。一貫性レベルが{@code IMMEDIATE}の
 * {@link GridStore}インスタンスを通じてコンテナを操作する場合、同一の
 * コンテナに対する{@code IMMEDIATE}設定での他の操作処理の途中、原則としては
 * 待機させられます。また、コンテナに対する他の操作処理の途中の状態に基づいて
 * 処理が行われることは、原則としてはありません。例外事項については、個別の操作
 * ごとの説明を参照してください。</p>
 *
 * @param <K> ロウキーの型。ロウキーが存在しない場合は{@link Void}または
 * {@link Row.Key}を指定
 * @param <R> マッピングに用いるロウオブジェクトの型
 * </div><div lang="en">
 * Provides the functions of managing the components of GridDB,
 * each consisting of a set of Rows of a single type.
 *
 * <p>It provides various management functions treating a Row object
 * as a unit of input/output. A Row object and a Row in GridDB are
 * mapped to each other, based on the correspondence between the
 * specified type of a Row object and the schema defined in GridDB.
 * </p>
 *
 * <p>Each Column composing a schema in GridDB has a correspondence
 * relation with a field and methods defined in a Row object. The
 * number of Columns is from 1 to 1024 per Container. The
 * correspondence relation with each column is determined based on
 * the public, protected and default access fields of the specified
 * type or the getter and setter methods, excluding fields and methods
 * specified as {@link TransientRowField} and transient fields.	A
 * default constructor with a public, protected or default access
 * modifier must be prepared to generate a Row object dynamically.
 * Internal classes must be static. </p>
 *
 * <p>The getter is a method with no parameters which has a name
 * beginning with "is" or "get" if it return a Boolean value, or a
 * name with beginning with "get" if it returns any other type value.
 * The setter is a method with only one parameter specifying a setting
 * value which has a name beginning with "set." Unless specified by
 * {@link RowField}, the column names used in GridDB correspond with
 * the character strings obtained by removing prefixes, such as "get,"
 * from the names of field, getter or setter methods. If either (not both) of a
 * getter or a setter is only defined, it is ignored. If a field with
 * the same name and both of a getter and a setter are defined, the
 * getter and the setter are used. If there is a difference in case
 * between a getter and a setter, the getter is given priority. If a
 * Column has a Row key, {@link RowKey} is set on the corresponding
 * field or methods. </p>
 *
 * <p>Multiple column names that are different only in upper-
 * and lowercase letters cannot be defined in a table.
 * Further the allowed characters, the length of column names and
 * the number of columns are limited. See the GridDB Technical
 * Reference for the details. In the operations specifying column
 * names, ASCII uppercase and lowercase characters are identified as
 * same unless otherwise noted. Use {@link RowField} to specify
 * column names that cannot be defined as fields or methods (such as
 * the names in which the first character is a number and Java
 * reserved words).</p>
 *
 * <p>The correspondence between the type of a Column and the type of
 * each value in a Row object is as follows:</p>
 * <table>
 * <thead><tr><th>Column type</th><th>Type of each value in a Row object</th></tr></thead>
 * <tbody>
 * <tr><td>STRING</td><td>{@link String}</td></tr>
 * <tr><td>BOOL</td><td>{@link Boolean} or {@code boolean}</td></tr>
 * <tr><td>BYTE</td><td>{@link Byte} or {@code byte}</td></tr>
 * <tr><td>SHORT</td><td>{@link Short} or {@code short}</td></tr>
 * <tr><td>INTEGER</td><td>{@link Integer} or {@code int}</td></tr>
 * <tr><td>LONG</td><td>{@link Long} or {@code long}</td></tr>
 * <tr><td>FLOAT</td><td>{@link Float} or {@code float}</td></tr>
 * <tr><td>DOUBLE</td><td>{@link Double} or {@code double}</td></tr>
 * <tr><td>TIMESTAMP</td><td>{@link java.util.Date}</td></tr>
 * <tr><td>BLOB</td><td>Class implementing {@link Blob}</td></tr>
 * <tr><td>STRING array</td><td>{@code String[]}</td></tr>
 * <tr><td>BOOL array</td><td>{@code boolean[]}</td></tr>
 * <tr><td>BYTE array</td><td>{@code byte[]}</td></tr>
 * <tr><td>SHORT array</td><td>{@code short[]}</td></tr>
 * <tr><td>INTEGER array</td><td>{@code int[]}</td></tr>
 * <tr><td>LONG array</td><td>{@code long[]}</td></tr>
 * <tr><td>FLOAT array</td><td>{@code float[]}</td></tr>
 * <tr><td>DOUBLE array</td><td>{@code double[]}</td></tr>
 * <tr><td>TIMESTAMP array</td><td>{@code java.util.Date[]}</td></tr>
 * </tbody>
 * </table>
 *
 * <p>There are the restrictions on the display range and size of the
 * field value. See the GridDB Technical Reference and the appendix
 * "Range of values" for the details.
 * Values contrary to the restriction cannot be stored in a Container.</p>
 *
 * <p>Restrictions such as the datatypes permitted as a Row key, existence of
 * columns corresponding to the Row key, and permissibility of Row operations
 * differ depending on the definition of the sub-interfaces of this Container.</p>
 *
 * <p>NULL in GridDB rows can be retained unless the NOT NULL constraint is set.
 * When the field of the row object or the getter / setter method can input / output
 * the value of the reference type, NULL in GridDB rows can be input and output as
 * {@code null}. Otherwise, NULL is mapped to an empty value in the row object. </p>
 *
 * <p>The NOT NULL constraint on the row object type can be explicitly specified with
 * {@link NotNull} and {@link Nullable}. If a NOT NULL constraint is not specified for
 * any of the specifications, the column other than the row key is assumed to be without
 * the NOT NULL constraint. The row key is implicitly set to the NOT NULL constraint
 * and cannot be specified to exclude this constraint. Also, it is not possible to specify
 * conflicting NOT NULL constraints between the same object and between getter and setter
 * methods. For example, if {@link NotNull} and {@link Nullable} are specified
 * simultaneously for the row object type, it is assumed that a conflicting NOT NULL
 * constraint was specified. The priority order by designation target on the presence
 * or absence of the NOT NULL constraint is as follows. </p>
 * <ol>
 * <li>Field of row object or getter/setter method</li>
 * <li>Raw object </li>
 * <li>Enclosing of row object (Example: An interface that surrounds the class of the
 * row object as an inner class) or an enclosing that is recursively obtained. Among
 * enclosing types that are determined in recurrence, there are constraint specifications,
 * and the type found first is given priority。</li>
 * <li>Package to which the type of row object belongs to</li>
 * </ol>
 *
 * <p> An empty value is a type of field value that may be used as initial value of various
 * operations such as creation of {@link Row}. Values are defined for each column as follows.</p>
 * <table>
 * <thead><tr><th>Column</th><th>empty value</th></tr></thead>
 * <tbody>
 * <tr><td>STRING</td><td>{@code ""}(String with length 0)</td></tr>
 * <tr><td>BOOL</td><td>False ({@code false})</td></tr>
 * <tr><td>NUMERAL</td><td>{@code 0}</td></tr>
 * <tr><td>TIMESTAMP</td><td>{@code 1970-01-01T00:00:00Z}</td></tr>
 * <tr><td>GEOMETRY</td><td>{@code POINT(EMPTY)}</td></tr>
 * <tr><td>BLOB</td><td>BLOB data of length 0</td></tr>
 * <tr><td>ARRAY</td><td>Array with 0 element</td></tr>
 * </tbody>
 * </table>
 *
 * <p>During transaction processing, the auto commit mode is enabled by default.
 * In the auto commit mode, change operations are confirmed sequentially
 * and cannot be deleted explicitly.
 * In the manual commit mode, if an error in the cluster node is detected
 * by an operation via this object and {@link GSException} is sent out,
 * all update operations before committing are deleted.
 * The transaction separation level supports only READ COMMITTED.
 * The lock particle size differs depending on the type of Container.</p>
 *
 * <p>After generation or transaction of this {@link Container} ends, a new
 * transaction is started at the point the Row is first updated, added or deleted,
 * and the lock for updating purposes is acquired.
 * In the auto commit mode, when the Row operation which started the transaction ends,
 * the transaction is automatically committed and ended.
 * In the manual commit mode, a transaction will not be ended until
 * the validity period for controlling the transaction explicitly is reached.
 * {@link #commit()} is used to commit a transaction while {@link #abort()} is
 * used to abort a transaction.
 * Even if this {@link Container} or the {@link GridStore} of the generation source
 * is closed, the transaction will be aborted and ended.
 * In addition, the time an operation is carried out to start a transaction
 * is adopted as the start point, and the validity period is reached only
 * when the period defined in GridDB has passed. When you try to continue
 * with the Row operations and transaction commitment without aborting
 * after the validity period is reached, {@link GSException} will be sent out.</p>
 *
 * <p>In some cases, operations on the same container may have to wait until
 * the processing on the cluster node is started in response to an operation
 * request to a certain container. Operations here include changes of definitions
 * such as container schema and index, container information reference, row operation,
 * etc. When manipulating containers through {@link GridStore} instances with
 * a consistency level of {@code IMMEDIATE}, in principle, they are made to wait
 * in the middle of other manipulations with the {@code IMMEDIATE} setting for
 * the same container. In principle, processing is not performed based on the state
 * in the middle of other operation processing on the container. For exceptional
 * items, see the explanation for each individual operation. </p>
 *
 * @param <K> the type of a Row key. If no Row key is used, specify {@link Void}
 * or {@link Row.Key}.
 * @param <R> the type of a Row object used for mapping
 * </div>
 */
public interface Container<K, R> extends Closeable {

	/**
	 * <div lang="ja">
	 * 必要に応じ別途ロウキーを指定して、ロウを新規作成または更新します。
	 *
	 * <p>ロウキーに対応するカラムが存在する場合、ロウキーとコンテナの状態を
	 * 基に、ロウを新規作成するか、更新するかを決定します。この際、対応する
	 * ロウがコンテナ内に存在しない場合は新規作成、存在する場合は更新します。
	 * ロウオブジェクトとは別にロウキーを指定した場合、ロウオブジェクト内の
	 * ロウキーより優先して使用されます。</p>
	 *
	 * <p>ロウキーに対応するカラムを持たない場合、常に新規のロウを作成します。
	 * 別途指定するロウキーには、常に{@code null}を指定します。</p>
	 *
	 * <p>コンテナの種別ならびに設定によっては、制限が設けられています。
	 * 具体的な制限事項は、サブインタフェースの定義を参照してください。</p>
	 *
	 * <p>手動コミットモードの場合、対象のロウはロックされます。</p>
	 *
	 * @param key 処理対象のロウキー
	 * @param row 新規作成または更新するロウの内容と対応するロウオブジェクト
	 *
	 * @return 指定のロウキーと一致するロウが存在したかどうか
	 *
	 * @throws GSException ロウキーに対応するカラムが存在しないにもかかわらず、
	 * キーが指定された場合
	 * @throws GSException 特定コンテナ固有の制限に反する操作を行った場合
	 * @throws GSException この処理またはトランザクションのタイムアウト、
	 * このコンテナの削除 もしくはスキーマ変更、接続障害が発生した場合、
	 * クローズ後に呼び出された場合、またはサポート範囲外の値がキーまたはロウオブジェクトに
	 * 含まれていた場合
	 * @throws ClassCastException 指定のキーもしくはロウオブジェクトと、マッピング処理で
	 * 使用される型との間で対応しないものがある場合
	 * @throws NullPointerException {@code row}に{@code null}が
	 * 指定された場合。 また、ロウフィールドに対応するロウオブジェクト内のオブジェクト
	 * について、NOT NULL制約があるにも関わらず{@code null}が設定されている
	 * 場合や、配列型の場合に{@code null}の要素が含まれている場合
	 * </div><div lang="en">
	 * Newly creates or updates a Row, based on the specified Row object
	 * and also the Row key specified as needed.
	 *
	 * <p>If a Column exists which corresponds to the specified Row key,
	 * it determines whether to newly create or update a Row, based on the
	 * Row key and the state of the Container. If there is no corresponding
	 * Row in the Container, it determines to newly create a Row; otherwise,
	 * it updates a relevant Row. If a Row key is specified besides a Row
	 * object, the specified Row key is used in preference to the Row key
	 * in the Row object. </p>
	 *
	 * <p>If no Column exists which corresponds to the specified Row key,
	 * it always creates a new Row. In such a case, specify {@code null}
	 * as key. </p>
	 *
	 * <p>Restrictions are applied depending on the type of Container and
	 * its settings. See the descriptions of subinterfaces for detailed
	 * restrictions. </p>
	 *
	 * <p>In the manual commit mode, the target Row is locked. </p>
	 *
	 * @param key A target Row key
	 * @param row A Row object representing the content of a Row to be newly created
	 * or updated.
	 *
	 * @return {@code TRUE} if a Row exists which corresponds to the
	 * specified Row key.
	 *
	 * @throws GSException if a Row key is specified although no Column
	 * exists which corresponds to the key.
	 * @throws GSException if its operation is contrary to the restrictions
	 * specific to a particular Container.
	 * @throws GSException if a timeout occurs during this operation or
	 * the transaction, this Container is deleted, its schema is changed or
	 * a connection failure occurs; or if called after the connection is
	 * closed; or if an unsupported value is set in the key or the Row object.
	 * @throws ClassCastException if the specified key or Row object does
	 * not completely match the type(s) used in mapping operation.
	 * @throws NullPointerException if {@code null} is specified as {@code row};
	 * For objects in row objects corresponding to row fields, if there is a NOT NULL
	 * constrain but {@code null} is set, it will in include elements of {@code null}
	 * when array type is selected.
	 * </div>
	 */
	public boolean put(K key, R row) throws GSException;

	/**
	 * <div lang="ja">
	 * 常にロウオブジェクトのみを指定して、ロウを新規作成または更新します。
	 *
	 * <p>指定のロウオブジェクト内のロウキーを使用する点を除き、
	 * {@link #put(Object, Object)}と同等です。</p>
	 *
	 * @see #put(Object, Object)
	 * </div><div lang="en">
	 * Newly creates or updates a Row, based on the specified Row object only.
	 *
	 * <p>It behaves in the same way as {@link #put(Object, Object)}, except
	 * that it uses a Row key in the specified Row object. </p>
	 *
	 * @see #put(Object, Object)
	 * </div>
	 */
	public boolean put(R row) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のロウオブジェクト集合に基づき、任意個数のロウをまとめて
	 * 新規作成または更新します。
	 *
	 * <p>指定のロウオブジェクト集合の各ロウについて、イテレータからの
	 * 取り出し順序に従って{@link #put(Object)}を呼び出した場合と同様に
	 * 新規作成または更新操作を行います。</p>
	 *
	 * <p>指定のロウオブジェクト集合内に同一のロウキーを持つ複数のロウが存在する場合、
	 * ロウオブジェクト集合のイテレータからの取り出し順序を基準として、
	 * 同一のロウキーを持つ最も後方にあるロウオブジェクトの内容が反映されます。</p>
	 *
	 * <p>コンテナの種別ならびに設定によっては、操作できるロウの内容について
	 * {@link Container#put(Object)}と同様の制限が設けられています。
	 * 具体的な制限事項は、サブインタフェースの定義を参照してください。</p>
	 *
	 * <p>手動コミットモードの場合、対象のロウがロックされます。</p>
	 *
	 * <p>自動コミットモードのときに、コンテナならびにロウに対する処理の途中で例外が発生した場合、
	 * コンテナの一部のロウに対する操作結果のみが反映されたままとなることがあります。</p>
	 *
	 * @return 現バージョンでは、常に{@code false}
	 *
	 * @throws GSException 特定コンテナ種別固有の制限に反する操作を行った場合
	 * @throws GSException この処理またはトランザクションのタイムアウト、
	 * このコンテナの削除もしくはスキーマ変更、接続障害が発生した場合、
	 * クローズ後に呼び出された場合、またはサポート範囲外の値がロウオブジェクトに
	 * 含まれていた場合
	 * @throws ClassCastException 指定の各ロウオブジェクトがマッピング処理で使用される
	 * ロウオブジェクトの型と対応しない場合
	 * @throws NullPointerException {@code rowCollection}またはその要素として
	 * {@code null}が指定された場合。また、{@link #put(Object, Object)}と
	 * 同様ロウオブジェクトの特定の箇所に{@code null}が含まれていた場合
	 *
	 * @see #put(Object)
	 * </div><div lang="en">
	 * Based on the specified Row object group, an arbitrary number of Rows
	 * will be consolidated to create a new group or updated.
	 *
	 * <p>For each Row in the specified Row object group, a new creation or
	 * update operation is carried out just like the case when {@link #put(Object)}
	 * is invoked in accordance with the take-out sequence from the iterator.</p>
	 *
	 * <p>If multiple Rows having the same Row key exist in the specified Row object
	 * group,
	 * the contents of the rear-most Row having the same Row key will be reflected
	 * using the take-out sequence from the iterator of the Row object group
	 * as a reference.</p>
	 *
	 * <p>Depending on the Container type and setting, the same restrictions
	 * as {@link Container#put(Object)} are established for the contents of Rows
	 * that can be operated.
	 * Refer to definition of the sub-interface for the specific restrictions.</p>
	 *
	 * <p>In the manual commit mode, the target Rows are locked.</p>
	 *
	 * <p>In the auto commit mode, if an exclusion occurs in the midst of processing
	 * a Container and its Rows, only the results for some of the Rows
	 * in the Container
	 * may remain reflected.</p>
	 *
	 * @return Always {@code false} in the current version
	 *
	 * @throws GSException When an operation violating the restrictions unique
	 * to a specific Container type is carried out
	 * @throws GSException if a timeout occurs during this operation or
	 * the transaction, this Container is deleted, its schema is changed or
	 * a connection failure occurs; or if called after the connection is
	 * closed; or if an unsupported value is set in the key or the Row object.
	 * @throws ClassCastException if the specified Row objects does not match
	 * the value types of Row objects used
	 * in mapping operation, respectively.
	 * @throws NullPointerException if NULL is specified as {@code rowCollection} or
	 * its element; As with {@link #put(Object, Object)}, if {@code null} is
	 * included in a specific part of the row object.
	 *
	 * @see #put(Object)
	 * </div>
	 */
	public boolean put(java.util.Collection<R> rowCollection) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のロウキーに対応するロウの内容を取得します。
	 *
	 * <p>更新用ロックを要求せずに{@link #get(Object, boolean)}を呼び出した場合と
	 * 同等です。</p>
	 *
	 * @see #get(Object, boolean)
	 * </div><div lang="en">
	 * Returns the content of a Row corresponding to the specified Row key.
	 *
	 * <p>It behaves in the same way as {@link #get(Object, boolean)}
	 * called without requesting a lock for update. </p>
	 *
	 * @see #get(Object, boolean)
	 * </div>
	 */
	public R get(K key) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のオプションに従い、ロウキーに対応するロウの内容を取得します。
	 *
	 * <p>ロウキーに対応するカラムが存在する場合のみ使用できます。</p>
	 *
	 * <p>手動コミットモードにおいて更新用ロックを要求した場合、
	 * トランザクションが終了するかタイムアウトするまで対象ロウのロックを維持します。
	 * ロックされたロウに対する他のトランザクションからの更新・削除操作は、
	 * このトランザクションが終了するかタイムアウトするまで待機するようになります。
	 * 対象ロウが削除されたとしても、ロックは維持されます。</p>
	 *
	 * <p>自動コミットモードの場合、更新用ロックを要求できません。</p>
	 *
	 * @param forUpdate 更新用ロックを要求するかどうか
	 *
	 * @return 対応するロウオブジェクト。存在しない場合は{@code null}
	 *
	 * @throws GSException ロウキーに対応するカラムが存在しない場合
	 * @throws GSException 自動コミットモードにもかかわらず、更新用ロックを
	 * 要求しようとした場合
	 * @throws GSException この処理またはトランザクションのタイムアウト、
	 * このコンテナの削除もしくはスキーマ変更、接続障害が発生した場合、
	 * クローズ後に呼び出された場合、またはサポート範囲外の値がキーとして設定されていた場合
	 * @throws ClassCastException 指定のロウキーがマッピング処理で使用される
	 * ロウキーの型と対応しない場合
	 * @throws NullPointerException {@code key}に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Returns the content of a Row corresponding to the specified
	 * Row key according to the specified option.
	 *
	 * <p>It can be used only if a Column exists which corresponds to
	 * the specified Row key.</p>
	 *
	 * <p>If it requests a lock for update in the manual commit mode,
	 * it will hold the lock until a relevant transaction completes or
	 * a timeout occurs. The update or deletion operation by any other
	 * transaction on the locked Row will wait until the transaction
	 * completes or a timeout occurs. The lock will be held even if the
	 * target Row is deleted.</p>
	 *
	 * <p>In the autocommit mode, it cannot request a lock for update.</p>
	 *
	 * @param forUpdate indicates whether it requests a lock for update.
	 *
	 * @return A target Row object. {@code null} if no target Row exists.
	 *
	 * @throws GSException if no Column exists which corresponds to the
	 * specified Row key.
	 * @throws GSException if it requests a lock for update in the
	 * autocommit mode.
	 * @throws GSException if a timeout occurs during this operation or
	 * the transaction, this Container is deleted, its schema is changed
	 * or a connection failure occurs; or if called after the connection
	 * is closed; or if an unsupported value is specified as key.
	 * @throws ClassCastException if the specified Row key does not match
	 * the type of a Row key used in mapping operation.
	 * @throws NullPointerException if {@code null} is specified as {@code key}.
	 * </div>
	 */
	public R get(K key, boolean forUpdate) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のロウキーに対応するロウを削除します。
	 *
	 * <p>ロウキーに対応するカラムが存在する場合のみ使用できます。
	 * 対応するロウが存在しない場合は何も変更しません。</p>
	 *
	 * <p>コンテナの種別ならびに設定によっては、制限が設けられています。
	 * 具体的な制限事項は、サブインタフェースの定義を参照してください。</p>
	 *
	 * <p>手動コミットモードの場合、対象のロウはロックされます。</p>
	 *
	 * @return 対応するロウが存在したかどうか
	 *
	 * @throws GSException ロウキーに対応するカラムが存在しない場合
	 * @throws GSException 特定コンテナ固有の制限に反する操作を行った場合
	 * @throws GSException この処理またはトランザクションのタイムアウト、
	 * このコンテナの削除もしくはスキーマ変更、接続障害が発生した場合、
	 * クローズ後に呼び出された場合、またはサポート範囲外の値がキーとして指定された場合
	 * @throws ClassCastException 指定のロウキーがマッピング処理で使用される
	 * ロウキーの型と対応しない場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Deletes a Row corresponding to the specified Row key.
	 *
	 * <p>It can be used only if a Column exists which corresponds to
	 * the specified Row key. If no corresponding Row exists, nothing
	 * is changed. </p>
	 *
	 * <p>Restrictions are applied depending on the type of Container
	 * and its settings. See the descriptions of subinterfaces for
	 * detailed restrictions. </p>
	 *
	 * <p>In the manual commit mode, the target Row is locked.</p>
	 *
	 * @return TRUE if a corresponding Row exists.
	 *
	 * @throws GSException if no Column exists which corresponds to the
	 * specified Row key.
	 * @throws GSException if its operation is contrary to the restrictions
	 * specific to a particular Container.
	 * @throws GSException if a timeout occurs during this operation or the
	 * transaction, this Container is deleted, its schema is changed or a
	 * connection failure occurs; or if called after the connection is closed;
	 * or if an unsupported value is specified as key.
	 * @throws ClassCastException if the specified Row key does not match the
	 * type of a Row key used in mapping operation.
	 * @throws NullPointerException if {@code null} is specified.
	 * </div>
	 */
	public boolean remove(K key) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のTQL文を実行するためのクエリを作成します。
	 *
	 * <p>選択式に集計演算を含むクエリなど、実行結果の出力形式がこのコンテナの
	 * ロウの形式と対応しないクエリに対しては、使用できません。
	 * 代わりに、{@link #query(String, Class)}が利用できます。</p>
	 *
	 * <p>{@link Query#fetch(boolean)}を通じてロウ集合を求める際に
	 * 更新用ロックのオプションを有効できるのは、このコンテナ上に
	 * 実在しないロウが選択されることのないクエリのみです。
	 * たとえば、補間演算を含むクエリに対しては有効にできません。</p>
	 *
	 * <p>現バージョンでは、TQL文の誤りによる{@link GSException}や、
	 * {@code null}を指定できない引数で{@code null}を指定したことによる
	 * {@link NullPointerException}は送出されません。引数に誤りがあった場合、
	 * 得られたクエリをフェッチする際に例外が送出されます。</p>
	 *
	 * @param tql TQL文。{@code null}は指定できない
	 *
	 * @throws GSException 現バージョンでは送出されない
	 *
	 * @see #query(String, Class)
	 * </div><div lang="en">
	 * Creates a query object to execute the specified TQL statement.
	 *
	 * <p>It cannot be used for a query whose output format does not match the
	 * types of Rows in this Container, such as a query containing an aggregation
	 * operation in its selection expression. For such a query,
	 * {@link #query(String, Class)} can be used instead. </p>
	 *
	 * <p>When obtaining a set of Rows using {@link Query#fetch(boolean)},
	 * the option of locking for update can be enabled only for the queries that
	 * will not select Rows which do not exist in this Container. For example, it
	 * cannot be enabled for a query containing an interpolation operation. </p>
	 *
	 * <p> In the current version, due to an error in the TQL statement,
	 * {@link GSException} and by specifying {@code null} with an argument
	 * that cannot specify {@code null}, {@link NullPointerException} will
	 * not be dispatched. If there is an error in the argument, an exception
	 * will be thrown when the resulting query is fetched. </p>
	 *
	 * @param tql TQL statement. {@code null} cannot be specified
	 *
	 * @throws GSException It will not be thrown in the current version.
	 *
	 * @see #query(String, Class)
	 * </div>
	 */
	public Query<R> query(String tql) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のTQL文・対応付け用クラスを使用する、クエリオブジェクトを作成します。
	 *
	 * <p>集計演算のように、このコンテナのロウと異なる型の結果を期待する場合に
	 * 使用します。</p>
	 * <p>{@code rowType}には次の型または{@code null}のみを指定できます。</p>
	 * <dl>
	 * <dt>コンテナのロウの型</dt>
	 * <dd>{@link #query(String)}と同様、このコンテナと対応する型のロウデータを
	 * 受け取ります。</dd>
	 * <dt>{@link AggregationResult}</dt>
	 * <dd>集計演算の実行結果を受け取ります。</dd>
	 * <dt>{@link QueryAnalysisEntry}</dt>
	 * <dd>EXPLAIN文ならびEXPLAIN ANALYZE文の実行結果を受け取ります。</dd>
	 * <dt>{@code null}</dt>
	 * <dd>実行結果に応じた適切な型により結果を受け取ります。</dd>
	 * </dl>
	 * <p>上記以外の値は指定できません。</p>
	 *
	 * <p>{@link Query#fetch(boolean)}を通じてロウ集合を求める際に
	 * 更新用ロックのオプションを有効できるのは、このコンテナ上に
	 * 実在しないロウが選択されることのないクエリのみです。
	 * たとえば、補間演算を含むクエリに対しては有効にできません。</p>
	 *
	 * <p>現バージョンでは、TQL文の誤りによる{@link GSException}や、
	 * {@code null}を指定できない引数で{@code null}を指定したことによる
	 * {@link NullPointerException}は送出されません。引数に誤りがあった場合、
	 * 得られたクエリをフェッチする際に例外が送出されます。</p>
	 *
	 * @param tql TQL文。{@code null}は指定できない
	 * @param rowType 期待するロウオブジェクトの型または{@code null}
	 *
	 * @throws GSException サポートされない型を{@code rowType}に指定した場合
	 * </div><div lang="en">
	 * Creates a query object to execute the specified TQL statement and return
	 * the specified type of result.
	 *
	 * <p>It is used for a query whose output format does not match the types of
	 * Rows in this Container, such as an aggregation operation. The following
	 * types and {@code null} are only available as {@code rowType}.</p>
	 * <dl>
	 * <dt>Row type of Container </dt>
	 * <dd>Indicates receiving Row data of the type matching the Rows in this
	 * Container, as in {@link #query(String)}.</dd>
	 * <dt>{@link AggregationResult}</dt>
	 * <dd>Indicates receiving the result of executing an aggregation operation. </dd>
	 * <dt>{@link QueryAnalysisEntry}</dt>
	 * <dd>Indicates receiving the result of executing an EXPLAIN or EXPLAIN
	 * ANALYZE statement.</dd>
	 * <dt>{@code null}</dt>
	 * <dd>Indicates receiving a proper type of result, depending on the operation. </dd>
	 * </dl>
	 * <p>No other value can be specified. </p>
	 *
	 * <p>When obtaining a set of Rows using {@link Query#fetch(boolean)}, the
	 * option of locking for update can be enabled only for the queries that
	 * will not select Rows which do not exist in this Container. For example,
	 * it cannot be enabled for a query containing an interpolation operation.</p>
	 *
	 * <p>In the current version, due to an error in the TQL statement,
	 * {@link GSException} and by specifying {@code null} with an argument
	 * that cannot specify {@code null}, {@link NullPointerException} will
	 * not be dispatched. If there is an error in the argument, an exception
	 * will be thrown when the resulting query is fetched. </p>
	 *
	 * @param tql TQL statement. {@code null} cannot be specified
	 * @param rowType the expected row object type or {@code null}
	 *
	 * @throws GSException if an unsupported type is specified as {@code rowType}.
	 * </div>
	 */
	public <S> Query<S> query(
			String tql, Class<S> rowType) throws GSException;

	/**
	 * <div lang="ja">
	 * この{@link Container}に対して巨大なバイナリデータを格納するための{@link Blob}を
	 * 作成します。
	 *
	 * <p>作成された{@link Blob}は、ロウのフィールドとして利用できます。
	 * まず、{@link Blob#setBinaryStream(long)}などを用いてバイナリデータを
	 * セットし、続いて{@link #put(Object)}などを用いて{@link Container}に
	 * 格納します。</p>
	 *
	 * <p>このメソッドにより得られた{@link Blob}に対し、少なくとも次のメソッドを
	 * 呼び出すことができます。</p>
	 * <ul>
	 * <li>{@link Blob#length()}</li>
	 * <li>{@link Blob#setBinaryStream(long)}</li>
	 * <li>{@link Blob#setBytes(long, byte[])}</li>
	 * <li>{@link Blob#setBytes(long, byte[], int, int)}</li>
	 * <li>{@link Blob#free()}</li>
	 * </ul>
	 *
	 * <p>ロウオブジェクトに設定するBLOBは、必ずしもこのメソッドで作成した
	 * {@link Blob}を使う必要はありません。{@link SerialBlob}など、{@link Blob}を
	 * 実装した他のクラスのインスタンスを設定することもできます。
	 * また、作成された{@link Blob}に有効期間はありません。</p>
	 *
	 * <p>現バージョンでは、ロウ全体をクライアントのメモリ上にキャッシュするため、
	 * このVMのメモリ使用量の制限を超えるような巨大なデータは登録できない
	 * 可能性があります。</p>
	 *
	 * <p>現バージョンでは、{@link GSException}が送出されることはありません。</p>
	 * </div><div lang="en">
	 * Creates a Blob to store a large size of binary data for a {@link Container}.
	 *
	 * <p>The created Blob can be used as a Row field. First, set binary data
	 * in the Blob using Blob.setBinaryStream(long) etc. and then store it
	 * in {@link Container} using {@link #put(Object)} etc. </p>
	 *
	 * <p>At least the following methods can be called on the {@link Blob}
	 * obtained by this method.</p>
	 * <ul>
	 * <li>{@link Blob#length()}</li>
	 * <li>{@link Blob#setBinaryStream(long)}</li>
	 * <li>{@link Blob#setBytes(long, byte[])}</li>
	 * <li>{@link Blob#setBytes(long, byte[], int, int)}</li>
	 * <li>{@link Blob#free()}</li>
	 * </ul>
	 *
	 * <p>You do not have to use the {@link Blob} created by this method as
	 * BLOB to be set on a Row object. You can set an instance of
	 * other class implementing {@link Blob}, such as {@link SerialBlob}.
	 * The created {@link Blob} does not have any validity period. </p>
	 *
	 * <p>In the current version, since the entire Row is cached in memory,
	 * it might be impossible to store larger data than the maximum VM
	 * memory size. </p>
	 *
	 * <p>{@link GSException} will not be thrown in the current version. </p>
	 * </div>
	 */
	public Blob createBlob() throws GSException;

	/**
	 * <div lang="ja">
	 * 手動コミットモードにおいて、現在のトランザクションにおける操作結果を確定させ、
	 * トランザクションを終了します。
	 *
	 * @throws GSException 自動コミットモードのときに呼び出された場合
	 * @throws GSException この処理またはトランザクションのタイムアウト、
	 * このコンテナの削除、接続障害が発生した場合、またはクローズ後に呼び出された場合
	 * </div><div lang="en">
	 * Commits the result of the current transaction and start a new
	 * transaction in the manual commit mode.
	 *
	 * @throws GSException if called not in the autocommit mode
	 * @throws GSException if a timeout occurs during this operation
	 * or the transaction, this Container is deleted or a connection
	 * failure occurs; or if called after the connection is closed.
	 * </div>
	 */
	public void commit() throws GSException;

	/**
	 * <div lang="ja">
	 * 手動コミットモードにおいて、現在のトランザクションの操作結果を元に戻し、
	 * トランザクションを終了します。
	 *
	 * @throws GSException 自動コミットモードのときに呼び出された場合
	 * @throws GSException この処理またはトランザクションのタイムアウト、
	 * このコンテナの削除、接続障害が発生した場合、またはクローズ後に呼び出された場合
	 * </div><div lang="en">
	 * Rolls back the result of the current transaction and starts a new
	 * transaction in the manual commit mode.
	 *
	 * @throws GSException if called not in the autocommit mode.
	 * @throws GSException if a timeout occurs during this operation or
	 * the transaction, this Container is deleted or a connection failure
	 * occurs; or if called after the connection is closed.
	 * </div>
	 */
	public void abort() throws GSException;

	/**
	 * <div lang="ja">
	 * コミットモードの設定を変更します。
	 *
	 * <p>自動コミットモードでは、直接トランザクション状態を制御できず、
	 * 変更操作が逐次コミットされます。
	 * 自動コミットモードが有効でない場合、すなわち手動コミットモードの場合は、
	 * 直接{@link #commit()}を呼び出すかトランザクションがタイムアウトしない限り、
	 * このコンテナ内で同一のトランザクションが使用され続け、
	 * 変更操作はコミットされません。</p>
	 *
	 * <p>自動コミットモードが無効から有効に切り替わる際、未コミットの変更内容は
	 * 暗黙的にコミットされます。コミットモードに変更がない場合、
	 * トランザクション状態は変更されません。この挙動は、
	 * {@link java.sql.Connection#setAutoCommit(boolean)}と同様です。</p>
	 *
	 * @throws GSException モード変更に伴いコミット処理を要求した際に、
	 * この処理またはトランザクションのタイムアウト、このコンテナの削除、
	 * 接続障害が発生した場合、またはクローズ後に呼び出された場合
	 * </div><div lang="en">
	 * Change the setting of the commit mode.
	 *
	 * <p>In the auto commit mode, the transaction state cannot be controlled
	 * directly and change operations are committed sequentially.
	 * If the auto commit mode is disabled, i.e. in the manual commit mode,
	 * as long as the transaction has not timed out or {@link #commit()} has been
	 * invoked directly, the same transaction will continue to be used
	 * in this Container and change operations will not be committed. </p>
	 *
	 * <p>When the autocommit mode is switched from On to Off, uncommitted
	 * updates are committed implicitly. Unless the commit mode is changed,
	 * the state of the transaction will not be changed. These behaviors are
	 * the same as those of {@link java.sql.Connection#setAutoCommit(boolean)}.</p>
	 *
	 * @throws GSException if a timeout occurs during this operation or the
	 * transaction, this Container is deleted or a connection failure occurs,
	 * when a commit is requested after a mode change; or if called after the
	 * connection is closed.
	 * </div>
	 */
	public void setAutoCommit(boolean enabled) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定された名前のカラムに対し、デフォルトの種別で名前のない索引を
	 * 作成します。
	 *
	 * <p>カラム名のみが設定された{@link IndexInfo}を指定して
	 * {@link #createIndex(IndexInfo)}を呼び出した場合と同様に
	 * 振る舞います。</p>
	 *
	 * @throws GSException 指定のカラム名が{@link #createIndex(IndexInfo)}の
	 * 規則に合致しない場合
	 * @throws GSException この処理のタイムアウト、このコンテナの削除もしくは
	 * スキーマ変更、接続障害が発生した場合、またはクローズ後に呼び出された場合
	 * @throws GSException 索引設定がサポートされていないカラムが指定された場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Creates an unnamed index with default type for the column with the specified name.
	 *
	 * <p>Behaves the same as calling {@link #createIndex(IndexInfo)} with {@link IndexInfo}
	 * with only the column name set. </p>
	 *
	 * @throws GSException If the specified column name does not conform to the rule of
	 * {@link #createIndex(IndexInfo)}
	 * @throws GSException if a timeout occurs during this operation, this Container
	 * is deleted, its schema is changed or a connection failure occurs; or if called
	 * after the connection is closed.
	 * @throws GSException if indexing is not supported on the specified Column.
	 * @throws NullPointerException if a {@code null} parameter is specified.
	 * </div>
	 */
	public void createIndex(String columnName) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定された名前のカラムに対し、指定された種別で名前のない索引を
	 * 作成します。
	 *
	 * <p>カラム名と種別のみが設定された{@link IndexInfo}を指定して
	 * {@link #createIndex(IndexInfo)}を呼び出した場合と同様に
	 * 振る舞います。</p>
	 *
	 * @throws GSException 指定のカラム名と種別が
	 * {@link #createIndex(IndexInfo)}の規則に合致しない場合
	 * @throws GSException この処理のタイムアウト、このコンテナの削除もしくは
	 * スキーマ変更、接続障害が発生した場合、またはクローズ後に呼び出された場合
	 * @throws GSException 指定のカラムにおいてサポートされていない索引種別が
	 * 指定された場合
	 * @throws NullPointerException 1つ以上の引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Creates an unnamed index with the specified type for the column with the specified name.
	 *
	 * <p>Behaves the same as calling {@link #createIndex(IndexInfo)} with
	 * {@link IndexInfo} with only the column name and type set. </p>
	 *
	 * @throws GSException If the specified column name and type does not conform to the rule of
	 * {@link #createIndex(IndexInfo)}
	 * @throws GSException if a timeout occurs during this operation, this Container
	 * is deleted, its schema is changed or a connection failure occurs; or if called
	 * after the connection is closed.
	 * @throws GSException if the specified type of index is not supported on the
	 * specified Column type.
	 * @throws NullPointerException if a {@code null} parameter(s) is specified.
	 * </div>
	 */
	public void createIndex(
			String columnName, IndexType type) throws GSException;

	/**
	 * <div lang="ja">
	 * {@link IndexInfo}で設定されている内容に従い、索引を作成します。
	 *
	 * <p>作成対象の索引のカラムについては、カラム名列またはカラム番号列の
	 * 少なくとも一方が設定されており、かつ、対応するコンテナにおいて実在する
	 * ものが設定されている必要があります。カラム名列とカラム番号列が共に設定
	 * されている場合、対応するカラム列が順序を含め一致している必要が
	 * あります。</p>
	 *
	 * <p>索引種別が設定されていないか{@link IndexType#DEFAULT}が
	 * 設定されていた場合、後述の基準に従い、デフォルト種別の索引が
	 * 選択されます。</p>
	 *
	 * <p>1つのコンテナの索引間で、ASCIIの大文字・小文字表記だけが異なる
	 * 名前のものを複数定義することはできません。その他、索引の定義において
	 * 使用できる索引名の文字種や長さには制限があります。具体的には、
	 * GridDBテクニカルリファレンスを参照してください。特に記載のない限り、
	 * 索引名を指定する操作では、ASCIIの大文字・小文字表記の違いは
	 * 区別されません。</p>
	 *
	 * <p>既存の同名の索引が存在した場合、後述の条件を満たす同一設定の
	 * {@link IndexInfo}を指定しなければならず、その場合新たな索引は作成
	 * されません。一方、既存の異なる名前の索引または名前のない索引と同一設定の
	 * {@link IndexInfo}を指定することはできません。</p>
	 *
	 * <p>索引名が設定されていない場合は、名前のない索引の作成が要求された
	 * ものとみなされます。名前を除いて同一設定の索引がすでに存在していた場合、
	 * 名前のない索引でなければならず、その場合新たな索引は作成されません。</p>
	 *
	 * <p>現バージョンでは、少なくとも{@link Container}を通じて作成された
	 * 索引において、次の条件を満たす場合に索引名を除いて同一設定の
	 * 索引であるとみなされます。</p>
	 * <ul>
	 * <li>索引対象のカラム列が順序を含め一致すること。カラム名列、カラム
	 * 番号列、単一カラム指定、といった、カラム列の指定方法の違いは
	 * 無視される</li>
	 * <li>索引種別が一致すること。デフォルト指定の有無といった索引種別の
	 * 指定方法の違いは無視される</li>
	 * </ul>
	 *
	 * <p>現バージョンにおける、{@link GridStoreFactory#getInstance()}を基に
	 * 生成された{@link Container}インスタンスでは、コンテナの種別、対応する
	 * カラムの型などに基づき、次の索引種別がデフォルトとして選択されます。</p>
	 * <table>
	 * <thead>
	 * <tr><th>カラムの型</th><th>コレクション</th><th>時系列</th></tr>
	 * </thead>
	 * <tbody>
	 * <tr><td>STRING</td>
	 * <td>{@link IndexType#TREE}</td>
	 * <td>{@link IndexType#TREE}</td></tr>
	 * <tr><td>BOOL</td>
	 * <td>{@link IndexType#TREE}</td>
	 * <td>{@link IndexType#TREE}</td></tr>
	 * <tr><td>数値型</td>
	 * <td>{@link IndexType#TREE}</td>
	 * <td>{@link IndexType#TREE}</td></tr>
	 * <tr><td>TIMESTAMP</td>
	 * <td>{@link IndexType#TREE}</td>
	 * <td>{@link IndexType#TREE}※制限あり</td></tr>
	 * <tr><td>GEOMETRY</td>
	 * <td>{@link IndexType#SPATIAL}</td>
	 * <td>(なし)</td></tr>
	 * <tr><td>BLOB</td>
	 * <td>(なし)</td>
	 * <td>(なし)</td></tr>
	 * <tr><td>配列型</td>
	 * <td>(なし)</td>
	 * <td>(なし)</td></tr>
	 * </tbody>
	 * </table>
	 * <p>時系列のロウキー(TIMESTAMP型)には索引を設定できません。
	 * また、カラム列を構成するカラム型によってデフォルト種別が異なる場合
	 * には、選択できません。</p>
	 *
	 * <p>この{@link Container}インスタンスが未コミットのトランザクションを
	 * 保持していた場合、コミットしてから作成を行います。処理対象のコンテナ
	 * において実行中の他のトランザクションが存在する場合、それらの終了を
	 * 待機してから作成を行います。すでに索引が存在しており新たな索引が
	 * 作成されなかった場合、他のトランザクションによって待機するかどうかは
	 * 未定義です。またこの場合、この{@link Container}インスタンスが保持
	 * している未コミットのトランザクションが常にコミットされるかどうかは
	 * 未定義です。</p>
	 *
	 * <p>現バージョンでは、コンテナの規模など諸条件を満たした場合、索引の
	 * 作成開始から終了までの間に、処理対象のコンテナに対してコンテナ情報の
	 * 参照、一部の索引操作、トリガ操作、ロウ操作(更新含む)を行える場合が
	 * あります。それ以外の操作は、{@link Container}での定義通り待機させる
	 * 場合があります。索引の作成途中に別の操作が行われる場合は、作成途中の
	 * 索引に関する情報はコンテナ情報には含まれません。</p>
	 *
	 * @throws GSException 作成対象のカラム、索引名が上記の規則に
	 * 合致しない場合
	 * @throws GSException この処理のタイムアウト、このコンテナの削除もしくは
	 * スキーマ変更、接続障害が発生した場合、またはクローズ後に呼び出された場合
	 * @throws GSException 指定のカラムにおいてサポートされていない索引種別が
	 * 指定された場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 *
	 * @since 3.5
	 * </div><div lang="en">
	 * Create an index according to the contents set in {@link IndexInfo}.
	 *
	 * <p>For the column of the index to be created, at least one of the column name
	 * sequence and column number sequence must be set, and the actual column must be
	 * set in the corresponding container. If both are set, corresponding columns and
	 * its order must match each other. </p>
	 *
	 * <p>If the index type is not set or {@link IndexType#DEFAULT} is set,
	 * the default index type is selected according to the criteria described below. </p>
	 *
	 * <p>If an index name is set, a new index is created only if
	 * there is no index with the same name or the different name
	 * only in upper- or lowercase letters in the target
	 * container. See the GridDB Technical Reference for the
	 * details. In defining an index name, there are limits on
	 * the allowed characters and the length. In the operations of
	 * index, the names are not case-sensitive unless otherwise
	 * noted.</p>
	 *
	 * <p>If a name index gets duplicated, you must specify the
	 * same setting {@link IndexInfo} that satisfies the
	 * conditions described below, in which case no new index will
	 * be created. On the other hand, you can not specify an
	 * existing {@link IndexInfo} that has the same name as an
	 * index with a different name or an unnamed index. </p>
	 *
	 * <p>If an index name is not set, it is assumed that creation
	 * of an unnamed index was requested. If an identical index
	 * already exists (excluding name), it must be an unnamed
	 * index, which in this case no new index will be
	 * created. </p>
	 *
	 * <p>In the current version, an index created through {@link Container}
	 * is considered to be the same set of indexes
	 * except for index names if the following conditions are
	 * satisfied.</p>
	 * <ul>
	 * <li>The columns to be indexed must match, including its order. Differences in
	 * column specification methods, such as column name sequence, column number
	 * sequence and single column are ignored.</li>
	 * <li>The index types must match. Differences in the specification method of
	 * index type such as existence of default designation are ignored.</li>
	 * </ul>
	 *
	 * <p> In the current version, for the {@link Container} instance generated based on
	 * {@link GridStoreFactory#getInstance ()}, the following index type is selected as default
	 * based on the type of container, the type of corresponding column, etc.</p>
	 * <table>
	 * <thead>
	 * <tr><th>Column type</th><th>collection</th><th>time series</th></tr>
	 * </thead>
	 * <tbody>
	 * <tr><td>STRING</td>
	 * <td>{@link IndexType#TREE}</td>
	 * <td>{@link IndexType#TREE}</td></tr>
	 * <tr><td>BOOL</td>
	 * <td>{@link IndexType#TREE}</td>
	 * <td>{@link IndexType#TREE}</td></tr>
	 * <tr><td>NUMERAL</td>
	 * <td>{@link IndexType#TREE}</td>
	 * <td>{@link IndexType#TREE}</td></tr>
	 * <tr><td>TIMESTAMP</td>
	 * <td>{@link IndexType#TREE}</td>
	 * <td>{@link IndexType#TREE} Note:restriction applies</td></tr>
	 * <tr><td>GEOMETRY</td>
	 * <td>{@link IndexType#SPATIAL}</td>
	 * <td>(-)</td></tr>
	 * <tr><td>BLOB</td>
	 * <td>(-)</td>
	 * <td>(-)</td></tr>
	 * <tr><td>ARRAY</td>
	 * <td>(-)</td>
	 * <td>(-)</td></tr>
	 * </tbody>
	 * </table>
	 * <p> An Index cannot be set for Time Series Row Keys (TIMESTAMP type).
	 * These index types cannot be selected if the default type differs
	 * depending on the column type that configures the column.</p>
	 * <p>If this {@link Container} instance holds an uncommitted transaction,
	 * commit before create. Container to be processed. If there are other transactions
	 * being executed at the same time, wait for them to finish before creating.
	 * If an index already exists and no new index is created, it is undefined whether
	 * to wait by another transaction. In this case, it is undefined whether uncommitted
	 * transactions held by this {@link Container} instance are always committed or not. </p>
	 *
	 * <p>In the current version, in the case of satisfying the
	 * conditions such as the size of the container, during
	 * creating an index, the reference of the container
	 * information, a part of the index operations, the trigger
	 * operations, and the row operations (including the update of
	 * rows) may be performed. Under the definition of {@link Container},
	 * other operations may be waited. For the
	 * operations during the index creation, the container
	 * information doesn't include the index information to be
	 * created.</p>
	 *
	 * @throws GSException When the column or index name to be created does not conform to the above rule
	 * @throws GSException If this process's timeout, deletion of this container or schema change,
	 * connection failure occurs, or when called after closing
	 * @throws GSException When an unsupported index type is specified in the specified column
	 * @throws NullPointerException when {@code null} is specified as argument
	 *
	 * @since 3.5
	 * </div>
	 */
	public void createIndex(IndexInfo info) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定された名前のカラムのうち、デフォルトの種別の索引のみを削除します。
	 *
	 * <p>カラム名とデフォルトの種別が設定された{@link IndexInfo}を指定して
	 * {@link #dropIndex(IndexInfo)}を呼び出した場合と同様に
	 * 振る舞います。</p>
	 *
	 * @throws GSException 指定のカラム名が{@link #dropIndex(IndexInfo)}の
	 * 規則に合致しない場合
	 * @throws GSException この処理のタイムアウト、このコンテナの削除もしくは
	 * スキーマ変更、接続障害が発生した場合、またはクローズ後に呼び出された場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Delete only the default type index from the column with the specified name.
	 *
	 * <p>Behaves the same as calling {@link #dropIndex(IndexInfo)} with
	 * {@link IndexInfo} with column name and default type set. </p>
	 *
	 * @throws GSException When the specified column name does not conform
	 * to the rule of {@link #dropIndex(IndexInfo)}
	 * @throws GSException if a timeout occurs during this operation, this
	 * Container is deleted, its schema is changed or a connection failure
	 * occurs; or if called after the connection is closed.
	 * @throws NullPointerException if a {@code null} parameter is specified.
	 * </div>
	 */
	public void dropIndex(String columnName) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定された名前のカラムのうち、指定された種別の索引のみを削除します。
	 *
	 * <p>カラム名と種別が設定された{@link IndexInfo}を指定して
	 * {@link #dropIndex(IndexInfo)}を呼び出した場合と同様に
	 * 振る舞います。</p>
	 *
	 * @throws GSException 指定のカラム名が{@link #dropIndex(IndexInfo)}の
	 * 規則に合致しない場合
	 * @throws GSException この処理のタイムアウト、このコンテナの削除もしくは
	 * スキーマ変更、接続障害が発生した場合、またはクローズ後に呼び出された場合
	 * @throws NullPointerException 1つ以上の引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Deletes only the index of the specified type from the column with the specified name.
	 *
	 * <p>Behaves the same as calling {@link #dropIndex(IndexInfo)} with {@link IndexInfo}
	 * with column name and type set. </p>
	 *
	 * @throws GSException when the specified column name does not conform to the rule of
	 * {@link #dropIndex(IndexInfo)}
	 * @throws GSException if a timeout occurs during this operation, this Container
	 * is deleted, its schema is changed or a connection failure occurs; or if called
	 * after the connection is closed.
	 * @throws NullPointerException if {@code null} is specified as more than
	 * one parameter.
	 * </div>
	 */
	public void dropIndex(
			String columnName, IndexType type) throws GSException;

	/**
	 * <div lang="ja">
	 * {@link IndexInfo}で設定されている内容に一致する、すべての索引を
	 * 削除します。
	 *
	 * <p>{@link IndexInfo}の設定内容は、削除対象の索引を絞り込む条件として
	 * 使用されます。絞り込み条件は、カラム列、索引種別、索引名の3つに分類
	 * されます。それぞれ設定するかどうかは任意です。いずれも設定されていない
	 * 場合は、作成済みのすべての索引が削除されます。</p>
	 *
	 * <p>カラム名列またはカラム番号列が設定されている場合、対応するコンテナ
	 * において実在するものである必要があります。カラム名列とカラム番号列が共に
	 * 設定されている場合、対応するカラムが互いに一致している必要があります。
	 * カラム名列ならびにカラム番号列が共に設定されていない場合、他の絞り込み
	 * 条件(索引種別、索引名)を満たす任意のカラム列に対する索引が削除対象
	 * となります。</p>
	 *
	 * <p>索引種別が設定されている場合、指定の種別の索引のみが削除対象と
	 * なります。{@link IndexType#DEFAULT}が設定されている場合、
	 * {@link #createIndex(IndexInfo)}の基準に従い、デフォルト種別の索引が
	 * 選択されます。索引をサポートしていないカラムや指定の種別の索引をサポート
	 * していないカラムについては、削除対象にはなりません。索引種別が設定されて
	 * いない場合、他の絞り込み条件(カラム列、索引名)を満たす任意の種別の索引が
	 * 削除対象となります。</p>
	 *
	 * <p>索引名が設定されている場合、指定の名前の索引のみが削除対象と
	 * なります。索引名の同一性は、{@link #createIndex(IndexInfo)}の基準に
	 * 従います。索引名が設定されていない場合、他の絞り込み条件(カラム列、
	 * 索引種別)を満たす、任意の名前の索引ならびに名前のない索引が削除対象
	 * となります。</p>
	 *
	 * <p>削除対象となる索引が一つも存在しない場合、索引の削除は
	 * 行われません。</p>
	 *
	 * <p>トランザクションの扱いは、{@link #createIndex(IndexInfo)}と
	 * 同様です。また、複数の索引が削除対象となった場合に、一部の索引のみが
	 * 削除された状態で他のトランザクションが実行されることがありうるかどうかは
	 * 未定義です。</p>
	 *
	 * <p>索引の削除要求の完了直後の状態に関しては、
	 * {@link GridStore#dropContainer(String)}と同様です。</p>
	 *
	 * @throws GSException 削除対象のカラム、索引名が上記の規則に
	 * 合致しない場合
	 * @throws GSException この処理のタイムアウト、このコンテナの削除もしくは
	 * スキーマ変更、接続障害が発生した場合、またはクローズ後に呼び出された場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 *
	 * @since 3.5
	 * </div><div lang="en">
	 * Delete all indexes that match the content set in {@link IndexInfo}.
	 *
	 * <p>The setting information of {@link IndexInfo} are used as a condition
	 * to narrow down the index to be deleted. Filtering conditions are classified
	 * into three categories: column sequence, index type, and index name. Setting each
	 * of them is optional. If none of them are set, all created indexes are deleted. </p>
	 *
	 * <p>If a column name sequence or column number sequence is set, it must exist in
	 * the corresponding container. If both column name sequence and column number
	 * sequence are set, corresponding columns must match each other. If neither the
	 * column name sequence nor the column number sequence is set, the index for any
	 * column sequence that satisfies other refinement conditions (index type, index
	 * name) will be deleted.</p>
	 *
	 * <p>When the index type is set, only the index of the specified type will be
	 * deleted. If {@link IndexType#DEFAULT} is set, the default type index is
	 * selected according to the standard of {@link #createIndex(IndexInfo)}.
	 * Columns that do not support indexes and columns that do not support indexes
	 * of the specified type are not eligible for deletion. If the index type is not set,
	 * index that fulfil the conditions (column sequence, index name) will be deleted. </p>
	 *
	 * <p>If an index name is set, only the index with the specified name will be deleted.
	 * The identity of the index name follows the criteria of {@link #createIndex(IndexInfo)}.
	 * If an index name is not set, an index with an arbitrary name and an unnamed index
	 * that fulfils the conditions (column sequence, index type) will be deleted.</p>
	 *
	 * <p>If there is no index to be deleted, the index will not get deleted.</p>
	 *
	 * <p>Transaction handling is similar to {@link #createIndex(IndexInfo)}. Also,
	 * it is undefined whether or not other transactions may be executed while only
	 * a few indexes are deleted when multiple indexes are subject to deletion. </p>
	 *
	 * <p>The immediate state after completion of the index deletion request is similar
	 * to {@link GridStore#dropContainer(String)}. </p>
	 *
	 * @throws GSException if the column or index name to be deleted does not conform to the above rule
	 * @throws GSException if the process is timeout, deletion of this container or schema change,
	 * connection failure occurs, or called after closing
	 * @throws NullPointerException when {@code null} is specified as argument
	 *
	 * @since 3.5
	 * </div>
	 */
	public void dropIndex(IndexInfo info) throws GSException;

	/**
	 * <div lang="ja">
	 * 変更結果の通知先の一つを設定します。
	 *
	 * <p>このコンテナに変更があると、指定のURLにリクエストが送信されるようになります。
	 * 作成済みの場合は何も変更しません。</p>
	 * <p>リクエストの通知条件・内容の詳細は次の通りです。</p>
	 * <table>
	 * <tbody>
	 * <tr><td>通知条件</td>
	 * <td>このコンテナに対するロウ新規作成・更新・削除操作命令の実行後、
	 * 結果が確定(コミット)した場合。自動コミットの場合は、該当するロウ操作命令が
	 * 実行されるたびに通知。複数ロウ一括操作の場合、1回の命令ごとに1度だけ通知。
	 * 結果として内容に変更がない場合でも、通知されることがある。また、設定されている
	 * URLに通知を行っても一定時間以内に応答がない場合、タイムアウトする。</td></tr>
	 * <tr><td>プロトコル</td><td>HTTP 1.1 (SSLは未サポート)</td></tr>
	 * <tr><td>リクエストメソッド</td><td>POST</td></tr>
	 * <tr><td>URL書式</td><td>
	 * <pre>
	 * http://(ホスト名):(ポート番号)/(パス)</pre>
	 * </td></tr>
	 * <tr><td>MIMEタイプ</td><td>application/json</td></tr>
	 * <tr><td>リクエスト内容</td><td>
	 * <pre>
	 * {
	 *   "update" : {
	 *     "container" : "(コンテナ名)"
	 *   }
	 * }</pre>
	 * </td></tr>
	 * </tbody>
	 * </table>
	 * <p>GridDBからの通知の際に、設定されている通知先URLにリクエストに対応する
	 * HTTPサーバが応答しない場合、タイムアウト時刻までの待機処理が発生します。
	 * この待機処理は、このコンテナならびに他の一部のコンテナに対するロウの
	 * 新規作成・更新・削除操作のレスポンスが低下する要因となります。
	 * したがって、無効となった通知先URLは
	 * {@link #dropEventNotification(URL)}により削除することが推奨されます。</p>
	 *
	 * <p>処理対象のコンテナにおいて実行中のトランザクションが存在する場合、
	 * それらの終了を待機してから設定を行います。</p>
	 *
	 * @deprecated バージョン1.5より非推奨となりました。
	 *
	 * @throws GSException 指定のURLが規定の構文に合致しない場合
	 * @throws GSException この処理のタイムアウト、このコンテナの削除、
	 * 接続障害が発生した場合、またはクローズ後に呼び出された場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	public void createEventNotification(URL url) throws GSException;

	/**
	 * <div lang="ja">
	 * 変更結果の通知先の一つを削除します。
	 *
	 * <p>削除済みの場合は何も変更しません。</p>
	 *
	 * <p>処理対象のコンテナにおいて実行中のトランザクションが存在する場合、
	 * それらの終了を待機してから削除を行います。</p>
	 *
	 * @deprecated バージョン1.5より非推奨となりました。
	 *
	 * @throws GSException この処理のタイムアウト、このコンテナの削除、
	 * 接続障害が発生した場合、またはクローズ後に呼び出された場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	public void dropEventNotification(URL url) throws GSException;

	/**
	 * <div lang="ja">
	 * トリガを設定します。
	 *
	 * <p>このコンテナに対して特定の種別の更新操作が行われた場合に、
	 * 指定のURIに通知が送信されるようになります。
	 * 指定されたトリガと同名のトリガが存在した場合、設定内容が上書きされます。</p>
	 *
	 * <p>トリガ設定内容の詳細は、{@link TriggerInfo}の定義を参照してください。
	 * トリガ名、トリガ種別、通知条件、通知先URI、通知内容の詳細は以下の通り
	 * です。</p>
	 *
	 * <b>トリガ名</b>
	 * <p>トリガ種別や通知条件などの違いによらず、1つのコンテナのトリガ間で、
	 * ASCIIの大文字・小文字表記を含め同一の名前のものを複数定義することは
	 * できません。その他、トリガの定義において使用できるトリガ名の文字種や
	 * 長さには制限があります。具体的には、GridDBテクニカルリファレンスを
	 * 参照してください。特に記載のない限り、トリガ名を指定する操作では、
	 * ASCIIの大文字・小文字表記の違いが区別されます。</p>
	 *
	 * <b>トリガ種別</b>
	 * <p>次のトリガ種別をサポートします。
	 * <table>
	 * <thead><tr><th>名称</th><th>説明</th></tr></thead>
	 * <tbody>
	 * <tr>
	 * <td>REST</td>
	 * <td>コンテナに指定された種別の更新操作が行われた際に、
	 * 指定されたURIにREST(HTTP POSTメソッド)で通知するトリガです。</td>
	 * </tr>
	 * <tr>
	 * <td>Java Message Service(JMS)</td>
	 * <td>コンテナに指定された種別の更新操作が行われた際に、
	 * 指定されたURIのJMSサーバへJMSメッセージを通知するトリガです。
	 * JMSプロバイダとしてApache ActiveMQを使用します。</td>
	 * </tr>
	 * </tbody>
	 * </table>
	 *
	 * <b>通知条件</b>
	 * <p>このコンテナに対するロウ新規作成/更新
	 * ({@link Container#put(Object)}、
	 * {@link Container#put(Object, Object)}、
	 * {@link Container#put(java.util.Collection)}、
	 * {@link GridStore#multiPut(java.util.Map)}、
	 * {@link RowSet#update(Object)})・
	 * 削除({@link Container#remove(Object)}、{@link RowSet#remove()})
	 * 操作命令の実行直後に通知を行います。
	 * 監視対象として複数の操作が指定された場合は、
	 * そのうちのいずれかが実行された際に通知を行います。</p>
	 *
	 * <p>通知を行った時点でのレプリケーションの完了は保証されません。
	 * 自動コミットモード無効で実行されたロウ新規作成/更新・
	 * 削除命令に対応する通知については、
	 * 通知を行った時点でトランザクションが未コミットであったり、
	 * 通知後にトランザクションがアボートされたりした場合、
	 * 通知を受けた時点で通知に含まれるデータが取得できないことがあります。</p>
	 *
	 * <p>複数ロウ一括操作の場合、1件のロウ操作ごとに通知を行います。
	 * 指定されたURIに通知を行っても一定時間以内に応答がない場合、
	 * タイムアウトし再送は行いません。
	 * GridDBクラスタに障害が発生した場合、
	 * ある更新操作に対応する通知が行われないことのほか、
	 * 複数回通知されることがあります。</p>
	 *
	 * <b>通知先URI</b>
	 * <p>
	 * 通知先URIは次の書式で記述します。</p>
	 * <pre>
	 * (メソッド名)://(ホスト名):(ポート番号)/(パス)</pre>
	 * <p>ただし、トリガ種別がRESTの場合、メソッド名にはhttpのみ指定できます。</p>
	 *
	 * <b>通知内容</b>
	 * <p>更新が行われたコンテナ名、更新操作名、
	 * 更新されたロウデータの指定したカラムの値を通知します。
	 * 更新操作名は、ロウ新規作成/更新では{@code "put"}、
	 * 削除では{@code "delete"}となります。</p>
	 *
	 * <p>通知する値は、ロウ新規作成では新規作成直後、更新では更新後、
	 * 削除では削除前のロウデータについての、指定カラムの値となります。
	 * カラムの型がTIMESTAMPの場合、{@code 1970-01-01T00:00:00Z}
	 * からの経過ミリ秒を示す整数が値として設定されます。
	 * カラムの型がBLOB型、GEOMETRY型、配列型の場合、
	 * 空文字列が値として設定されます。</p>
	 *
	 * <b>通知方法―RESTの場合</b>
	 * <p>以下のようなJSON文字列を、MIMEタイプapplication/jsonで送信します。</p>
	 * <pre>
	 * {
	 *   "container" : "(コンテナ名)",
	 *   "event" : "(更新操作名)",
	 *   "row" : {
	 *     "(カラム名)" : (カラムデータ),
	 *     "(カラム名)" : (カラムデータ),
	 *     ...
	 *   }
	 * }</pre>
	 *
	 * <b>通知方法―JMSの場合</b>
	 * <p>javax.jms.TextMessageを、指定されたデスティネーション種別・
	 * デスティネーション名で送信します。</p>
	 *
	 * <p>コンテナ名は、
	 * {@code javax.jms.Message#setStringProperty("@container", "(コンテナ名)")}
	 * で設定されます。
	 * 更新操作名は、
	 * {@code javax.jms.Message#setStringProperty("@event", "(更新操作名)")}
	 * で設定されます。</p>
	 *
	 * <p>カラムの値は、カラムの型に応じた
	 * {@code javax.jms.Message#setXXXProperty("(カラム名)", (カラムデータ))}
	 * で設定されます。</p>
	 *
	 * <p>トリガが設定されているコンテナに対して
	 * {@link GridStore#putCollection(String, Class, boolean)}、
	 * {@link GridStore#putTimeSeries(String, Class, TimeSeriesProperties, boolean)}
	 * などによりカラムレイアウトが変更された際に、
	 * トリガの通知対象となっているカラムの削除または名称変更があった場合、
	 * 該当するカラムはトリガの通知対象から削除されます。</p>
	 *
	 * <p>GridDBからの通知の際に、設定されている通知先URIへのリクエストに
	 * 対応するサーバが応答しなかった場合、タイムアウト時刻までの待機処理が
	 * 発生します。この待機処理は、このコンテナならびに他の一部のコンテナの
	 * 更新に対する通知が遅れる要因となります。
	 * したがって、無効となった通知先URIを持つトリガは
	 * {@link #dropTrigger(String)}により削除することが推奨されます。</p>
	 *
	 * <p>一つのコンテナに対して設定できるトリガの最大数、ならびに、トリガの
	 * 各種設定値の上限については、GridDBテクニカルリファレンスを参照してください。</p>
	 *
	 * @param info 設定対象のトリガ情報
	 *
	 * @throws GSException トリガ名が{@code null}、空、またはその他の規則に
	 * 合致しない場合
	 * @throws GSException 監視対象更新操作の指定がない場合
	 * @throws GSException 通知先URIが規定の構文に合致しない場合
	 * @throws GSException トリガ種別でJMSが指定され、
	 * かつJMSデスティネーション種別が{@code null}、または空、
	 * または指定の書式に合致しない場合
	 * @throws GSException トリガ種別でJMSが指定され、
	 * かつJMSデスティネーション名が{@code null}、または空の場合
	 * @throws GSException この処理のタイムアウト、このコンテナの削除、
	 * 接続障害が発生した場合、またはクローズ後に呼び出された場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 *
	 * Set the trigger.
	 *
	 * <p>If a update operation of a specific type is carried out on this Container,
	 * a notification will be sent to the specified URI.
	 * If a trigger with the same name as the specified trigger exists,
	 * the setting will be overwritten.</p>
	 *
	 * <p>Refer to the definition of {@link TriggerInfo} for the detailed trigger
	 * settings. The details of the trigger name, the trigger type, notification conditions,
	 * notification destination URI and notification contents are as shown below.</p>
	 *
	 * <b>Trigger name</b>
	 * <p>Multiple trigger names which are identified as the same,
	 * including the names only different in ASCII uppercase and
	 * lowercase characters, even if they have different types or
	 * notification conditions, in a container cannot be
	 * defined. And there are the limitations, the allowed characters
	 * and the length, on the trigger names. See the GridDB
	 * Technical Reference for the details. Trigger names are
	 * case-sensitive unless otherwise noted.</p>
	 *
	 * <b>Trigger type</b>
	 * <p>The following trigger types are supported.
	 * <table>
	 * <thead><tr><th>name</th><th>description</th></tr></thead>
	 * <tbody>
	 * <tr>
	 * <td>REST</td>
	 * <td>Trigger to notify the specified URI with a REST notification (HTTP POST method)
	 * when an update operation of the specified type is carried out on a Container. </td>
	 * </tr>
	 * <tr>
	 * <td>Java Message Service(JMS)</td>
	 * <td>Trigger to send a JMS message to the JMS server of the specified URI
	 * when an update operation of the specified type is carried out on a Container.
	 * Apache ActiveMQ is used as the JMS provider.</td>
	 * </tr>
	 * </tbody>
	 * </table>
	 *
	 * <b>Notification condition</b>
	 * <p>Create new Row/ update Row for this Container
	 * ({@link Container#put(Object)},
	 * {@link Container#put(Object, Object)},
	 * {@link Container#put(java.util.Collection)},
	 * {@link GridStore#multiPut(java.util.Map)},
	 * {@link RowSet#update(Object)}),
	 * delete ({@link Container#remove(Object)}, {@link RowSet#remove()})
	 * Perform notification immediately after executing operation command.
	 * If multiple operations are specified as monitoring targets,
	 * perform notification after executing any one of these operations.</p>
	 *
	 * <p>Completion of replication at the point notification is carried out
	 * is not guaranteed.
	 * For notifications corresponding to a create new Row/ update Row or
	 * delete command that has been executed by disabling the auto commit mode,
	 * if the transaction is not committed at the point of the notification,
	 * or if the transaction is aborted after the notification, it may not be
	 * possible to get the data included in the notification at the point
	 * the notification is received.</p>
	 *
	 * <p>For batch operations involving multiple Rows, notification is carried out
	 * for each Row operation.
	 * If there is no response within a specified time even if notification has been
	 * sent to the specified URl, time out is performed and it will not be sent again.
	 * If a failure occurs in a GridDB cluster, in addition to not sending
	 * any notification to support a certain update operation, multiple notifications
	 * may be sent.</p>
	 *
	 * <b>Notification destination URI</b>
	 * <p>
	 * A notification destination URI is described in the following format.</p>
	 * <pre>
	 * (method name)://(host name):(port number)/(path)</pre>
	 * <p>However, if the trigger type is REST, only http can be specified in the method name.</p>
	 *
	 * <b>Notification contents</b>
	 * <p>Provide notification of the updated Container name, update operation name,
	 * and specified column value of the updated Row data.
	 * For the update operation name, use {@code "put"} to create a new Row/ update
	 * Row and {@code "delete"} to delete.</p>
	 *
	 * <p>The notification value shall be the specified column value of the Row data
	 * that is newly created immediately after a new Row is created, or updated
	 * in an update operation, or before deletion in a delete operation.
	 * If the column type is TIMESTAMP, an integer to indicate the time passed
	 * in milliseconds starting from {@code 1970-01-01T00:00:00Z} is set as the value.
	 * If the column type if BLOB, GEOMETRY, or array, a blank character string
	 * will be set as the value.</p>
	 *
	 * <b>Notification method - For REST</b>
	 * <p>JSON character strings such as those shown below are sent with the MIME type application/json.</p>
	 * <pre>
	 * {
	 *   "container" : "(container name)",
	 *   "event" : "(update operation name)",
	 *   "row" : {
	 *     "(column name)" : (column data),
	 *     "(column name)" : (column data),
	 *     ...
	 *   }
	 * }</pre>
	 *
	 * <b>Notification method - For JMS</b>
	 * <p>A javax.jms.TextMessage is sent with the specified destination type
	 * and destination name.</p>
	 *
	 * <p>The container name is set by
	 * {@code javax.jms.Message#setStringProperty("@container", "(container name)")}.
	 * The update operation name is set by
	 * {@code javax.jms.Message#setStringProperty("@event", "(update operation name)")}.</p>
	 *
	 * <p>The column value is set with a {@code javax.jms.Message#setXXXProperty("(column name)", (column data))}
	 * in accordance with the column type.</p>
	 *
	 * <p>When the column layout is changed by a
	 * {@link GridStore#putCollection(String, Class, boolean)},
	 * {@link GridStore#putTimeSeries(String, Class, TimeSeriesProperties, boolean)}, etc.
	 * in relation to a Container with a set trigger, if a column subject
	 * to trigger notification is deleted or if its name is changed,
	 * the corresponding column will be deleted from the trigger notification targets.</p>
	 *
	 * <p>If the server does not respond to a request sent to the notification
	 * destination URI that has been set up when sending a notification
	 * from GridDB, standby processing will occur until the process times out.
	 * This standby process becomes a cause for the delay in serving notification
	 * of an update in the Container as well as some other containers.
	 * Therefore, a trigger having an invalid notification destination URI
	 * is recommended to be deleted by using {@link #dropTrigger(String)}.</p>
	 *
	 * <p>See the GridDB Technical Reference for the maximum number of
	 * triggers that can be set for a single Container and the upper limit of the
	 * values for various trigger settings.</p>
	 *
	 * @param info Trigger information of the setting target
	 *
	 * @throws GSException If the trigger name is {@code null}, blank, or does not follow to other rules
	 * @throws GSException If the update operation subject to monitoring is not specified
	 * @throws GSException If the notification destination URI does not conform to the stipulated syntax
	 * @throws GSException If the JMS is specified by the trigger type, and the JMS destination type is {@code null},
	 * or is blank, or does not conform to the specified format
	 * @throws GSException If the JMS is specified by the trigger type, and the JMS destination name is {@code null},
	 * or is blank
	 * @throws GSException If the JMS is specified by the trigger type, and the JMS destination name is {@code null},
	 * or is blank
	 * @throws NullPointerException If {@code null} is specified in the argument
	 *
	 * @since 1.5
	 *</div>
	 */
	public void createTrigger(TriggerInfo info) throws GSException;

	/**
	 * <div lang="ja">
	 * トリガを削除します。
	 *
	 * <p>指定された名前のトリガが存在しない場合は何も変更しません。</p>
	 *
	 * @throws GSException この処理のタイムアウト、このコンテナの削除、
	 * 接続障害が発生した場合、またはクローズ後に呼び出された場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Delete a trigger.
	 *
	 * <p>Nothing is changed if the trigger of the specified name does not exist.</p>
	 *
	 * @throws GSException If this process times out, this Container is deleted, a connection failure were to occur,
	 * or if it is invoked after being closed
	 * @throws NullPointerException If {@code null} is specified in the argument
	 *</div>
	 */
	public void dropTrigger(String name) throws GSException;

	/**
	 * <div lang="ja">
	 * これまでの更新結果をSSDなどの不揮発性記憶媒体に書き出し、
	 * すべてのクラスタノードが突然停止したとしても内容が
	 * 失われないようにします。
	 *
	 * <p>通常より信頼性が要求される処理のために使用します。
	 * ただし、頻繁に実行すると性能低下を引き起こす可能性が高まります。</p>
	 *
	 * <p>書き出し対象のクラスタノードの範囲など、挙動の詳細はGridDB上の
	 * 設定によって変化します。</p>
	 *
	 * @throws GSException この処理のタイムアウト、このコンテナの削除、
	 * 接続障害が発生した場合、またはクローズ後に呼び出された場合
	 * </div><div lang="en">
	 * Writes the results of earlier updates to a non-volatile storage medium,
	 * such as SSD, so as to prevent the data from being lost even if all
	 * cluster nodes stop suddenly.
	 *
	 * <p>It can be used for operations requiring higher reliability than
	 * normal. However, frequent execution of this would cause degradation
	 * in response time. </p>
	 *
	 * <p>Its behaviors vary, such as the scope of cluster nodes concerned,
	 * depending on the configuration of GridDB. </p>
	 *
	 * @throws GSException if a timeout occurs during this operation, this
	 * Container is deleted or a connection failure occurs; or if called
	 * after the connection is closed.
	 * </div>
	 */
	public void flush() throws GSException;

	/**
	 * <div lang="ja">
	 * GridDBとの接続状態を解除し、必要に応じて関連するリソースを解放します。
	 *
	 * <p>トランザクションを保持している場合、未コミットの更新内容は
	 * すべて元に戻されます。</p>
	 *
	 * <p>{@link GSException}が送出された場合であっても、接続状態の解除や
	 * ローカルのリソース解放処理は適宜実施されます。ただし、GridDB上のトランザクション
	 * 状態などは残る可能性があります。
	 * すでにクローズ済みの場合、このメソッドを呼び出しても解放処理は行われません。</p>
	 *
	 * @throws GSException 接続障害が発生した場合
	 * </div><div lang="en">
	 * Disconnects with GridDB and releases related resources
	 * as necessary.
	 *
	 * <p>When a transaction is held, uncommitted updates will be
	 * rolled back. </p>
	 *
	 * <p>Even if {@link GSException} is thrown, the connection and local
	 * resources will be released properly. However, the transaction state
	 * might remain in GridDB. If the transaction is already closed, no
	 * release operation is invoked by this method. </p>
	 *
	 * @throws GSException if a connection failure occurs
	 * </div>
	 */
	public void close() throws GSException;

	/**
	 * <div lang="ja">
	 * このコンテナの種別を取得します。
	 *
	 * <p>現バージョンでは、インスタンス生成時点で常に種別が確定するため、
	 * この操作によりGridDBクラスタに問い合わせを行うことはありません。</p>
	 *
	 * @throws GSException クローズ後に呼び出された場合
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Get the type of this Container.
	 *
	 * <p>In the current version, no inquiry is sent to the GridDB cluster
	 * by this operation as the type is always confirmed at the point
	 * the instance is generated.</p>
	 *
	 * @throws GSException If invoked after being closed
	 *</div>
	 */
	public ContainerType getType() throws GSException;

	/**
	 * <div lang="ja">
	 * このコンテナのカラムレイアウトに基づき、ロウオブジェクトを新規作成します。
	 *
	 * <p>コンテナのロウオブジェクトの型が{@link Row}の場合、
	 * 作成される{@link Row}の各フィールドには
	 * {@link GridStore#createRow(ContainerInfo)}により作成した場合と
	 * 同様に既定の初期値が設定されます。
	 * またこの場合、作成された{@link Row}に対する操作は、
	 * この{@link Container}オブジェクトのクローズ有無に影響しません。</p>
	 *
	 * @throws GSException ユーザ定義型のロウオブジェクトを作成する場合に
	 * 例外が送出された場合
	 * @throws GSException クローズ後に呼び出された場合
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Create a new Row object based on the column layout of this Container.
	 *
	 * <p>If the Row object type of the Container is {@link Row}, a fixed
	 * default value is set in each field of the {@link Row} to be created,
	 * similar to the case when it is created using
	 * a {@link GridStore#createRow(ContainerInfo)}.
	 * In this case, the operation on the created {@link Row} also does not affect
	 * whether this {@link Container} object is closed or not.</p>
	 *
	 * @throws GSException If an exclusion is sent out when creating a user-defined
	 * Row object
	 * @throws GSException If invoked after being closed
	 * </div>
	 */
	public R createRow() throws GSException;

	/**
	 * <div lang="ja">
	 * このオブジェクトと結びつく型情報を取得します。
	 *
	 * <p>取得する型情報には、このオブジェクトを構築する際に与えられた
	 * ロウキーの型、ロウオブジェクトの型と同一のものが設定されます。</p>
	 *
	 * <p>{@link Container}またはそのサブインタフェースの型については、構築時に
	 * 与えられた型と同一になるとは限らず、そのサブインタフェースの型が求まる
	 * 可能性があります。また、{@link ContainerType}により区別されるコンテナ
	 * 種別と一対一で対応することは保証しません。</p>
	 *
	 * @return このオブジェクトと結びつく型情報
	 *
	 * @throws GSException クローズ後に呼び出された場合
	 *
	 * @since 4.3
	 * </div><div lang="en">
	 * Returns the type information associated with this object.
	 *
	 * <p>The type information obtained is set to be the same as the Row key type
	 * and Row object type specified when the object was configured.</p>
	 *
	 * <p>The type of {@link Container} or its subinterface is not necessarily
	 * the same as the type specified at the time of configuration; the type obtained
	 * may be that of the subinterface. Furthermore, a one-to-one correspondence
	 * with the container type represented by {@link ContainerType} is not
	 * guaranteed.</p>
	 *
	 * @return Type information associated with this object
	 *
	 * @throws GSException If invoked after being closed
	 *
	 * @since 4.3
	 * </div>
	 */
	public BindType<K, R, ? extends Container<K, R>> getBindType()
			throws GSException;

	/**
	 * <div lang="ja">
	 * {@link Container}ならびにその型パラメータと結びつく型情報を表します。
	 *
	 * <p>{@link Container}インスタンスを構築する際に与える複数の
	 * 型情報について、一つのオブジェクトとしてまとめて保持することが
	 * できます。</p>
	 *
	 * <p>ロウキーの型と{@link Container}インスタンスの型との対応関係の妥当性
	 * など、内容の妥当性については、このクラスのインスタンスの生成段階で
	 * 必ずしも検査するとは限りません。</p>
	 *
	 * @param <K> ロウキーの型。ロウキーが存在しない場合は{@link Void}または
	 * {@link Row.Key}を指定
	 * @param <R> マッピングに用いるロウオブジェクトの型
	 * @param <C> 対応する{@link Container}インスタンスにおいて実装
	 * されていることを期待する、{@link Container}またはそのサブインタフェースの型
	 *
	 * @see GridStore#getContainer(String, Container.BindType)
	 *
	 * @since 4.3
	 * </div><div lang="en">
	 * Represents the type information associated with the {@link Container} and
	 * its type parameters.
	 *
	 * <p>Multiple pieces of type information given when constructing a
	 * {@link Container} instance can be collectively stored as one object.</p>
	 *
	 * <p>The validity of the contents, such as the validity of the correspondence
	 * between the type of the Row key and the type of the {@link Container}
	 * instance, is not always checked when the instance of this class is
	 * generated.</p>
	 *
	 * @param <K> Type of Row key. If no Row key is used, specify {@link Void}
	 * or {@link Row.Key}
	 * @param <R> The type of a Row object used for mapping
	 * @param <C> The type of {@link Container} or its subinterfaces that are
	 * expected to be implemented in the corresponding {@link Container} instance
	 *
	 * @see GridStore#getContainer(String, Container.BindType)
	 *
	 * @since 4.3
	 * </div>
	 */
	public class BindType<K, R, C extends Container<K, R>> {

		static {
			RowMapper.BindingTool.setFactory(createFactory());
		}

		private final Class<K> keyClass;

		private final Class<R> rowClass;

		private final Class<? extends Container<?, ?>> containerClass;

		<D extends Container<?, ?>> BindType(
				Class<K> keyClass, Class<R> rowClass, Class<D> containerClass)
				throws GSException {
			this.keyClass = keyClass;
			this.rowClass = rowClass;
			this.containerClass = containerClass;
		}

		private static RowMapper.BindingTypeFactory createFactory() {
			return new RowMapper.BindingTypeFactory() {
				@Override
				public
				<K, R, C extends Container<K, R>, D extends Container<?, ?>>
				BindType<K, R, C> create(
						Class<K> keyType, Class<R> rowType,
						Class<D> containerType) throws GSException {
					return new BindType<K, R, C>(keyType, rowType, containerType);
				}
			};
		}

		/**
		 * <div lang="ja">
		 * 指定のロウオブジェクト型から得られるロウキーの型、指定の
		 * ロウオブジェクト型、ならびに、任意の型の{@link Container}と結びつく
		 * {@link BindType}を取得します。
		 *
		 * @param <K> ロウキーの型
		 * @param <R> ロウオブジェクトの型
		 * @param rowClass ロウオブジェクトの型に対応するクラスオブジェクト
		 *
		 * @return 対応する型情報
		 *
		 * @throws GSException 指定のロウオブジェクト型からロウキーの型が
		 * 得られなかった場合
		 *
		 * @since 4.3
		 * </div><div lang="en">
		 * Returns the {@link BindType} associated with the Row key type obtained from
		 * the specified Row object type, the specified Row object type, and
		 * {@link Container} of any type.
		 *
		 * @param <K> Type of Row key
		 * @param <R> Type of Row object
		 * @param rowClass Class object corresponding to the type of the Row object
		 *
		 * @return corresponding type information
		 *
		 * @throws GSException if the Row key type cannot be obtained from the
		 * specified Row object type
		 *
		 * @since 4.3
		 * </div>
		 */
		public static <K, R extends Row.WithKey<K>> BindType<
		K, R, ? extends Container<K, R>> of(Class<R> rowClass)
				throws GSException {
			return of(
					RowMapper.BindingTool.resolveKeyClass(rowClass),
					rowClass);
		}

		/**
		 * <div lang="ja">
		 * 指定のロウキー型、指定のロウオブジェクト型、ならびに、任意の型の
		 * {@link Container}と結びつく{@link BindType}を取得します。
		 *
		 * @param <K> ロウキーの型
		 * @param <R> ロウオブジェクトの型
		 * @param keyClass ロウキーの型に対応するクラスオブジェクト
		 * @param rowClass ロウオブジェクトの型に対応するクラスオブジェクト
		 *
		 * @return 対応する型情報
		 *
		 * @throws GSException ロウキーの型と、ロウオブジェクトの型との間で
		 * 不整合を検出した場合
		 *
		 * @since 4.3
		 * </div><div lang="en">
		 * Returns the {@link BindType} associated with the specified Row key type,
		 * the specified Row object type, and {@link Container} of any type.
		 *
		 * @param <K> Type of Row key
		 * @param <R> Type of Row object
		 * @param keyClass Class object corresponding to the type of Row key
		 * @param rowClass Class object corresponding to the type of the Row object
		 *
		 * @return corresponding type information
		 *
		 * @throws GSException if an inconsistency is detected between the type
		 * of Row key and the type of Row object
		 *
		 * @since 4.3
		 * </div>
		 */
		public static <K, R> BindType<K, R, ? extends Container<K, R>> of(
				Class<K> keyClass, Class<R> rowClass) throws GSException {
			return new BindType<K, R, Container<K, R>>(
					RowMapper.BindingTool.checkKeyClass(keyClass, rowClass),
					rowClass, Container.class);
		}

		/**
		 * <div lang="ja">
		 * ロウキーを持たず、指定のロウオブジェクト型、ならびに、任意の型の
		 * {@link Container}と結びつく{@link BindType}を取得します。
		 *
		 * @param <R> ロウオブジェクトの型
		 * @param rowClass ロウオブジェクトの型に対応するクラスオブジェクト
		 *
		 * @return 対応する型情報
		 *
		 * @throws GSException ロウキーの型と、ロウオブジェクトの型との間で
		 * 不整合を検出した場合
		 *
		 * @since 4.3
		 * </div><div lang="en">
		 * Returns the {@link BindType}, which has no Row key, associated with the
		 * specified Row object type and {@link Container} of any type.
		 *
		 * @param <R> Type of Row object
		 * @param rowClass Class object corresponding to the type of the Row object
		 *
		 * @return corresponding type information
		 *
		 * @throws GSException if an inconsistency is detected between the type
		 * of Row key and the type of Row object
		 *
		 * @since 4.3
		 * </div>
		 */
		public static <R> BindType<
		Void, R, ? extends Container<Void, R>> noKeyOf(Class<R> rowClass)
				throws GSException {
			return of(Void.class, rowClass);
		}

		/**
		 * <div lang="ja">
		 * ロウキーの型を取得します。
		 *
		 * @return ロウキーの型
		 *
		 * @since 4.3
		 * </div><div lang="en">
		 * Returns the type of the Row key.
		 *
		 * @return Type of Row key
		 *
		 * @since 4.3
		 * </div>
		 */
		public Class<K> getKeyClass() {
			return keyClass;
		}

		/**
		 * <div lang="ja">
		 * ロウオブジェクトの型を取得します。
		 *
		 * @return ロウオブジェクトの型
		 *
		 * @since 4.3
		 * </div><div lang="en">
		 * Returns the type of the Row object.
		 *
		 * @return Type of Row object
		 *
		 * @since 4.3
		 * </div>
		 */
		public Class<R> getRowClass() {
			return rowClass;
		}

		/**
		 * <div lang="ja">
		 * {@link Container}またはそのサブインタフェースの型を取得します。
		 *
		 * @return 対応する{@link Container}インスタンスにおいて実装されている
		 * ことを期待する、{@link Container}またはそのサブインタフェースの型
		 *
		 * @since 4.3
		 * </div><div lang="en">
		 * Returns the type of {@link Container} or its subinterface.
		 *
		 * @return The type of {@link Container} or its subinterfaces that are expected
		 * to be implemented in the corresponding {@link Container} instance
		 *
		 * @since 4.3
		 * </div>
		 */
		public Class<? extends Container<?, ?>> getContainerClass() {
			return containerClass;
		}

		/**
		 * <div lang="ja">
		 * このオブジェクトが保持するロウキーの型にキャストします。
		 *
		 * @param obj キャスト対象のオブジェクト
		 *
		 * @return キャストされたオブジェクト
		 *
		 * @since 4.3
		 * </div><div lang="en">
		 * Casts to the type of the Row key held by this object.
		 *
		 * @param obj The object to cast
		 *
		 * @return Casted object
		 *
		 * @since 4.3
		 * </div>
		 */
		public K castKey(Object obj) {
			return keyClass.cast(obj);
		}

		/**
		 * <div lang="ja">
		 * このオブジェクトが保持するロウオブジェクトの型にキャストします。
		 *
		 * @param obj キャスト対象のオブジェクト
		 *
		 * @return キャストされたオブジェクト
		 *
		 * @since 4.3
		 * </div><div lang="en">
		 * Casts to the type of the Row object held by this object.
		 *
		 * @param obj The object to cast
		 *
		 * @return Casted object
		 *
		 * @since 4.3
		 * </div>
		 */
		public R castRow(Object obj) {
			return rowClass.cast(obj);
		}

		/**
		 * <div lang="ja">
		 * このオブジェクトが保持するロウキーの型ならびにロウオブジェクトの型を
		 * 型パラメータとして持つ、{@link Container}またはそのサブインタフェースの
		 * 型にキャストします。
		 *
		 * @param container キャスト対象の{@link Container}インスタンス
		 *
		 * @return キャストされたオブジェクト
		 *
		 * @throws GSException キャスト対象の{@link Container}インスタンスと、
		 * このオブジェクトが保持する型情報との間で、ロウキーの型ならびに
		 * ロウオブジェクトの型が一致しない場合
		 *
		 * @since 4.3
		 * </div><div lang="en">
		 * Casts to the type of {@link Container} or its subinterface that has the type
		 * of Row key and the type of Row object held by this object as type parameters.
		 *
		 * @param container the {@link Container} instance to be casted
		 *
		 * @return Casted object
		 *
		 * @throws GSException if the type of Row key and the type of Row object do not
		 * match between the {@link Container} instance to be casted and the type
		 * information held by this object
		 *
		 * @since 4.3
		 * </div>
		 */
		public C castContainer(Container<?, ?> container) throws GSException {
			if (container == null) {
				return null;
			}

			final BindType<?, ?, ?> anotherType = container.getBindType();

			if (keyClass != null && keyClass != anotherType.keyClass) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"Can not cast container because of different " +
						"key class (expected=" + keyClass.getName() +
						", specified=" + anotherType.keyClass.getName() + ")");
			}

			if (rowClass != null && rowClass != anotherType.rowClass) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"Can not cast container because of different " +
						"row class (expected=" + rowClass.getName() +
						", specified=" + anotherType.rowClass.getName() + ")");
			}

			@SuppressWarnings("unchecked")
			final C typedContainer = (C) containerClass.cast(container);

			return typedContainer;
		}

	}

}
