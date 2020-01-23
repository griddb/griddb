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
import java.util.Collections;
import java.util.Properties;

import com.toshiba.mwcloud.gs.common.GridStoreFactoryProvider;
import com.toshiba.mwcloud.gs.common.LoggingUtils;
import com.toshiba.mwcloud.gs.common.LoggingUtils.BaseGridStoreLogger;

/**
 * <div lang="ja">
 * {@link GridStore}インスタンスを管理します。
 *
 * <p>{@link GridStore}インスタンス共通のクライアント設定や
 * 使用済みのコネクションを管理します。</p>
 *
 * <p>GridDBにアクセスするためには、このファクトリを介して
 * {@link GridStore}インスタンスを取得する必要があります。</p>
 *
 * <p>このクラスの公開メソッドは、すべてスレッド安全です。</p>
 *
 * <p>また、クライアントロギングとクライアント設定ファイルの機能を使用できます。</p>
 *
 * <p><b>クライアントロギング</b></p>
 * <p>クラスパスにロギングライブラリを含めることで、ログ出力を有効にした場合にログを出力できます。</p>
 * <p>ロギングフレームワークはSLF4Jを使用します。
 * ロガーの名称は「com.toshiba.mwcloud.gs.GridStoreLogger」で始まります。
 * また、SLF4Jのバージョンは1.6.0以上を推奨しています。</p>
 *
 * <p><b>クライアント設定ファイル</b></p>
 * <p>設定ファイル「gs_client.properties」を含むディレクトリと、設定ライブラリ「gridstore-conf.jar」をクラスパスに
 * 共に含めることで、{@link GridStoreFactory}に設定ファイルの内容を適用することができます。
 * 設定ファイルを用いることで、アプリケーションコードを修正せずに、接続設定を変更できます。
 * 設定ファイルには以下のPropertiesオブジェクトと同じプロパティ項目を指定できます。
 * 設定ファイルの内容はPropertiesオブジェクトの設定より優先的に適用されます。</p>
 * <dl><dt>factory カテゴリプロパティ</dt>
 * <dd>有効なプロパティの項目は{@link GridStoreFactory#setProperties(Properties)}の仕様に準拠します。</dd>
 * <dd>以下のように「factory.」をプロパティ名の前に付けて記述します。
 * <pre>factory.maxConnectionPoolSize = 10</pre>
 * </dd>
 * <dt>store カテゴリプロパティ</dt>
 * <dd>有効なプロパティの項目は{@link GridStoreFactory#getGridStore(Properties)}の仕様に準拠します。</dd>
 * <dd>以下のように「store.」をプロパティ名の前に付けて記述します。
 * <pre>store.clusterName = Project1</pre>
 * </dd>
 * <dd>
 * <p>以下の場合は例外が発生します。</p>
 * <ul><li>設定ファイルを含む複数のディレクトリがクラスパスに含まれる場合</li>
 * <li>設定ライブラリのみがクラスパスに含まれる場合</li>
 * <li>存在しないカテゴリ名が指定された場合</li><li>プロパティ名がカテゴリ名のみからなる場合</li></ul>
 * <p>また、設定ファイルを含むディレクトリのみがクラスパスに含まれ、設定ライブラリがクラスパスに含まれない場合には、
 * 設定ファイルの内容は適用されません。</p>
 * </dd>
 * </dl>
 *
 * </div><div lang="en">
 * Manages a {@link GridStore} instance.
 *
 * <p>It manages the client settings shared by {@link GridStore} instances and used connections.</p>
 *
 * <p>To access GridDB, you need to get a {@link GridStore} instance using this Factory.</p>
 *
 * <p>All the public methods of this class are thread safe.</p>
 *
 * <p>Client Logging and Client Properties File are available.</p>
 *
 * <p><b>Client Logging</b></p>
 * <p>Logging is enabled by adding the logging library SLF4J to the classpath.</p>
 * <p>The name of the logger begins with "com.toshiba.mwcloud.gs.GridStoreLogger". The recommended version of SLF4J
 * is 1.6.0 or above.</p>
 *
 * <p><b>Client Properties File</b></p>
 * <p>By including both of the directory including the properties file "gs_client.properties"
 * and the configuration library "gridstore-conf.jar" in the classpath,
 * properties of the file are applied to GridStoreFactory.
 * Connection properties can be changed without editing application codes
 * by using the properties file.
 * The priority of applying the properties of the file is higher than
 * the properties of Properties object.
 * The following properties are available for the Client Properties File.
 * <dl><dt>Factory Category Property</dt>
 * <dd>Properties conform to {@link GridStoreFactory#setProperties(Properties)} specifications are available.</dd>
 * <dd>Add "factory." before each property name as following.
 * <pre>factory.maxConnectionPoolSize = 10</pre>
 * </dd>
 * <dt>Store Category Property</dt>
 * <dd>Properties conform to {@link GridStoreFactory#getGridStore(Properties)} specifications are available.</dd>
 * <dd>Add "store." before each property name as following.
 * <pre>store.clusterName = Project1</pre>
 * </dd>
 * <dd>
 * <p>Exceptions will be thrown in the cases as following.</p>
 * <ul><li>If two or more directories of the properties files are included in the classpath</li>
 * <li>If only the configuration library is included in the classpath</li>
 * <li>If an unavailable property name is specified</li>
 * <li>If a specified property name is made up of only the category name</li></ul>
 * <p>By including only the directory including the properties file, properties
 * of the file are not applied to GridStoreFactory.</p>
 * </dd>
 * </dl>
 *
 * </div>
 */
public abstract class GridStoreFactory implements Closeable {

	static {
		registerLoggers();
	}

	private static final GridStoreFactory INSTANCE = newInstance();

	protected GridStoreFactory() {
	}

	private static GridStoreFactory newInstance() {
		return GridStoreFactoryProvider.getProvider(
				GridStoreFactoryProvider.class,
				GridStoreFactory.class,
				Collections.<Class<?>>emptySet()).getFactory();
	}

	/**
	 * <div lang="ja">
	 * デフォルトのインスタンスを取得します。
	 *
	 * <p>このクラスのロード時に、{@link GridStoreFactory}クラスのデフォルトのサブクラスが
	 * ロードされ、インスタンスが生成されます。</p>
	 * </div><div lang="en">
	 * Returns a default instance.
	 *
	 * <p>When loading this class, a default subclass of the {@link GridStoreFactory}
	 * class is loaded and an instance is created.</p>
	 * </div>
	 */
	public static GridStoreFactory getInstance() {
		return INSTANCE;
	}

	/**
	 * <div lang="ja">
	 * 指定のプロパティを持つ{@link GridStore}を取得します。
	 *
	 * <p>{@link GridStore}を取得した時点では、各{@link Container}を
	 * 管理するマスタノード(以下、マスタ)のアドレス探索を必要に応じて行うだけであり、
	 * 認証処理は行われません。
	 * 実際に各{@link Container}に対応するノードに接続する必要が生じた
	 * タイミングで、認証処理が行われます。</p>
	 *
	 * <p>以下のプロパティを指定できます。サポート外の名称のプロパティは
	 * 無視されます。</p>
	 * <table>
	 * <thead><tr><th>名称</th><th>説明</th></tr></thead>
	 * <tbody>
	 * <tr><td>host</td><td>接続先ホスト名。IPアドレス(IPV4のみ)も可。
	 * マスタを手動指定する場合は必須。マスタを自動検出する場合は設定しない</td></tr>
	 * <tr><td>port</td><td>接続先ポート番号。{@code 0}から
	 * {@code 65535}までの数値の文字列表現。マスタを手動指定する場合は必須。
	 * マスタを自動検出する場合は設定しない</td></tr>
	 * <tr><td>notificationAddress</td><td>マスタ自動検出に用いられる通知情報を
	 * 受信するためのIPアドレス(IPV4のみ)。省略時はデフォルトのアドレスを使用。
	 * notificationMemberおよびnotificationProviderと同時に指定することはできない</td></tr>
	 * <tr><td>notificationPort</td><td>マスタ自動検出に用いられる通知情報を
	 * 受信するためのポート番号。{@code 0}から{@code 65535}までの数値の
	 * 文字列表現。省略時はデフォルトのポートを使用</td></tr>
	 * <tr><td>clusterName</td><td>クラスタ名。接続先のクラスタに設定されている
	 * クラスタ名と一致するかどうかを確認するために使用される。省略時もしくは空文字列を
	 * 指定した場合、クラスタ名の確認は行われない</td></tr>
	 * <tr><td>database</td><td>接続先のデータベース名。省略時は全てのユーザが
	 * アクセス可能な「public」データベースに自動接続される。接続ユーザは接続
	 * データベースに属するコンテナを操作できる。</td></tr>
	 * <tr><td>user</td><td>ユーザ名</td></tr>
	 * <tr><td>password</td><td>ユーザ認証用のパスワード</td></tr>
	 * <tr><td>consistency</td><td>次のいずれかの一貫性レベル。
	 * <dl>
	 * <dt>{@code "IMMEDIATE"}</dt><dd>他のクライアントからの更新結果は、
	 * 該当トランザクションの完了後即座に反映される</dd>
	 * <dt>{@code "EVENTUAL"}</dt><dd>他のクライアントからの更新結果は、
	 * 該当トランザクションが完了した後でも反映されない場合がある。
	 * {@link Container}に対する更新操作は実行できない</dd>
	 * </dl>
	 * デフォルトでは{@code "IMMEDIATE"}が適用されます
	 * </td></tr>
	 * <tr><td>transactionTimeout</td><td>トランザクションタイムアウト時間の最低値。
	 * 関係する{@link Container}における各トランザクションの開始時点から適用。
	 * {@code 0}以上{@link Integer#MAX_VALUE}までの値の文字列表現であり、
	 * 単位は秒。ただし、タイムアウト時間として有効に機能する範囲に上限があり、
	 * 上限を超える指定は上限値が指定されたものとみなされる。
	 * {@code 0}の場合、後続のトランザクション処理がタイムアウトエラーに
	 * なるかどうかは常に不定となる。省略時は接続先GridDB上のデフォルト値を使用
	 * </td></tr>
	 * <tr><td>failoverTimeout</td><td>フェイルオーバ処理にて新たな接続先が
	 * 見つかるまで待機する時間の最低値。{@code 0}以上
	 * {@link Integer#MAX_VALUE}までの数値の文字列表現であり、単位は秒。
	 * {@code 0}の場合、フェイルオーバ処理を行わない。省略時はこのファクトリの
	 * 設定値を使用</td></tr>
	 * <tr><td>containerCacheSize</td><td>
	 * コンテナキャッシュに格納するコンテナ情報の最大個数。
	 * {@code 0}以上{@link Integer#MAX_VALUE}までの数値の文字列表現。
	 * 値が{@code 0}の場合、コンテナキャッシュを使用しないことを意味する。
	 * {@link Container}を取得する際にキャッシュにヒットした場合は、
	 * GridDBへのコンテナ情報の問い合わせを行わない。
	 * 省略時は既存の設定値を使用。バージョン1.5よりサポート</td></tr>
	 * <tr><td>dataAffinityPattern</td><td>
	 * データアフィニティ機能のアフィニティ文字列を次のようにコンテナパターン
	 * とペアで任意個数指定する。
	 * <pre>(コンテナ名パターン1)=(アフィニティ文字列1),(コンテナ名パターン2)=(アフィニティ文字列2),...</pre>
	 * {@link ContainerInfo#setDataAffinity(String)}が未指定の
	 * {@link Container}を追加する際に、コンテナ名が指定したいずれかの
	 * コンテナ名パターンに合致する場合に、ペアで指定したアフィニティ文字列が
	 * 適用される。複数のコンテナ名パターンが合致する場合は、記述された
	 * 順番で最初に合致したパターンが用いられる。コンテナ名パターンは
	 * ワイルドカード「%」を使用できる他は、コンテナ名の規則に準拠する。
	 * アフィニティ文字列は{@link ContainerInfo#setDataAffinity(String)}の
	 * 規則に準拠する。パターンやその区切りに使用される記号をコンテナ名などに
	 * 用いるには、「\」を用いてエスケープする。ただしコンテナ名やアフィニティ
	 * 文字列の規則に反する記号は使用できない。
	 * バージョン2.7よりサポート
	 * </td></tr>
	 * <tr><td>notificationMember</td><td>
	 * 固定リスト方式を使用して構成されたクラスタに接続する場合に、クラスタノードのアドレス・ポートのリストを
	 * 次のように指定する。
	 * <pre>(アドレス1):(ポート1),(アドレス2):(ポート2),...</pre>
	 * notificationAddressおよびnotificationProviderと同時に指定することはできない。
	 * バージョン2.9よりサポート
	 * </td></tr>
	 * <tr><td>notificationProvider</td><td>
	 * プロバイダ方式を使用して構成されたクラスタに接続する場合に、アドレスプロバイダのURLを指定する。
	 * notificationAddressおよびnotificationMemberと同時に指定することはできない。
	 * バージョン2.9よりサポート
	 * </td></tr>
	 * <tr><td>applicationName</td><td>アプリケーションの名前。
	 * アプリケーションの識別を補助するための情報として、接続先のクラスタ上での
	 * 各種管理情報の出力の際に含められる場合がある。ただし、アプリケーションの
	 * 同一性を どのように定義するかについては関与しない。省略時は
	 * アプリケーション名の指定がなかったものとみなされる。空文字列は指定
	 * できない。
	 * バージョン4.2よりサポート</td></tr>
	 * <tr><td>timeZone</td><td>タイムゾーン情報。
	 * TQLでのTIMESTAMP値演算などに使用される。
	 * 「{@code ±hh:mm}」または「{@code ±hhmm}」形式によるオフセット値
	 * ({@code ±}は{@code +}または{@code -}、{@code hh}は時、
	 * {@code mm}は分)、 「{@code Z}」({@code +00:00}に相当)、
	 * 「{@code auto}」(実行環境に応じ自動設定)のいずれかを指定する。
	 * {@code auto}が使用できるのは夏時間を持たないタイムゾーンに
	 * 限定される。
	 * バージョン4.3よりサポート</td></tr>
	 * </tbody>
	 * </table>
	 *
	 * <p>クラスタ名、データベース名、ユーザ名、パスワードについては、
	 * ASCIIの大文字・小文字表記の違いがいずれも区別されます。その他、
	 * これらの定義に使用できる文字種や長さの上限などの制限については、
	 * GridDBテクニカルリファレンスを参照してください。ただし、制限に反する
	 * 文字列をプロパティ値として指定した場合、各ノードへの接続のタイミングまで
	 * エラーが検知されないことや、認証情報の不一致など別のエラーになる
	 * ことがあります。</p>
	 *
	 * <p>取得のたびに、新たな{@link GridStore}インスタンスが生成されます。
	 * 異なる{@link GridStore}インスタンスならびに関連するオブジェクトに対する操作は、
	 * スレッド安全です。すなわち、ある2つのオブジェクトがそれぞれ{@link GridStore}
	 * インスタンスを基にして生成されたものまたは{@link GridStore}インスタンスそのものであり、
	 * かつ、該当する関連{@link GridStore}インスタンスが異なる場合、一方のオブジェクトに
	 * 対してどのスレッドからどのタイミングでメソッドが呼び出されていたとしても、
	 * 他方のオブジェクトのメソッドを呼び出すことができます。
	 * ただし、{@link GridStore}自体のスレッド安全性は保証されていないため、
	 * 同一{@link GridStore}インスタンスに対して複数スレッドから任意のタイミングで
	 * メソッド呼び出しすることはできません。</p>
	 *
	 * @param properties 取得設定を指示するためのプロパティ
	 *
	 * @throws GSException 指定のホストについて名前解決できなかった場合
	 * @throws GSException 指定のプロパティが上で説明した形式・制限に
	 * 合致しないことを検知できた場合
	 * @throws GSException すでにクローズ済みの場合
	 * @throws NullPointerException {@code properties}に{@code null}が
	 * 指定された場合
	 * </div><div lang="en">
	 * Returns a {@link GridStore} with the specified properties.
	 *
	 * <p>When obtaining {@link GridStore}, it just searches for the name of a master node
	 * (hereafter, a master) administering each {@link Container} as necessary, but
	 * authentication is not performed. When a client really needs to connect to
	 * a node corresponding to each Container, authentication is performed.</p>
	 *
	 * <p>The following properties can be specified. Unsupported property
	 * names are ignored.</p>
	 * <table>
	 * <thead><tr><th>Property</th><th>Description</th></tr></thead>
	 * <tbody>
	 * <tr><td>host</td><td>A destination host name. An IP address (IPV4 only) is
	 * also available. Mandatory for manually setting a master. For autodetection
	 * of a master, omit the setting.</td></tr>
	 * <tr><td>port</td><td>A destination port number. A string representing of
	 * a number from {@code 0} to {@code 65535}. Mandatory for manually setting a master.
	 * For autodetection of a master, omit the setting.</td></tr>
	 * <tr><td>notificationAddress</td><td>An IP address (IPV4 only) for receiving
	 * a notification used for autodetection of a master. A default address is
	 * used if omitted.
	 * This property cannot be specified with neither notificationMember nor
	 * notificationProvider properties at the same time.
	 * </td></tr>
	 * <tr><td>notificationPort</td><td>A port number for receiving a notification
	 * used for autodetection of a master. A string representing of a number
	 * from {@code 0} to {@code 65535}. A default port number is used if omitted.</td></tr>
	 * <tr><td>clusterName</td><td>A cluster name. It is used to verify whether it
	 * matches the cluster name assigned to the destination cluster. If it is omitted
	 * or an empty string is specified, cluster name verification is not performed.</td></tr>
	 * <tr><td>database</td><td>Name of the database to be connected.
	 * If it is omitted, "public" database that all users can
	 * access is automatically connected. Users can handle the
	 * containers belonging to the connected database.</td></tr>
	 * <tr><td>user</td><td>A user name</td></tr>
	 * <tr><td>password</td><td>A password for user authentication</td></tr>
	 * <tr><td>consistency</td><td>Either one of the following consistency levels:
	 * <dl>
	 * <dt>{@code "IMMEDIATE"}</dt><dd>The updates by other clients are committed
	 * immediately after a relevant transaction completes.</dd>
	 * <dt>{@code "EVENTUAL"}</dt><dd>The updates by other clients may not be
	 * committed even after a relevant transaction completes. No update operation
	 * cannot be applied to {@link Container}.</dd>
	 * </dl>
	 * By default, "IMMEDIATE" is selected.
	 * </td></tr>
	 * <tr><td>transactionTimeout</td><td>The minimum value of transaction timeout
	 * time. The transaction timeout is counted from the beginning of each
	 * transaction in a relevant {@link Container}. A string representing of a number from {@code 0}
	 * to {@link Integer#MAX_VALUE} in seconds. The value {@code 0} indicates
	 * that it is always uncertain whether a timeout error will occur during
	 * a subsequent transaction. If a value specified over the internal upper limit
	 * of timeout, timeout will occur at the internal upper limit value. If omitted,
	 * the default value used by a destination GridDB is applied.
	 * </td></tr>
	 * <tr><td>failoverTimeout</td><td>The minimum value of waiting time until
	 * a new destination is found in a failover. A numeric string representing
	 * from {@code 0} to {@link Integer#MAX_VALUE} in seconds. The value {@code 0}
	 * indicates that no failover is performed. If omitted, the default value
	 * used by this Factory is applied. </td></tr>
	 * <tr><td>containerCacheSize</td><td>The maximum number of ContainerInfos
	 * on the Container cache. A string representing of a number from
	 * {@code 0} to {@link Integer#MAX_VALUE}. The Container cache is not used if
	 * the value is {@code 0}. To obtain a {@link Container}, its ContainerInfo
	 * might be obtained from the Container cache instead of request to GridDB.
	 * A default number is used if omitted. </td></tr>
	 * <tr><td>dataAffinityPattern</td><td>Specifies the arbitrary
	 * number of patterns as show below, using pairs of an
	 * affinity string for the function of data affinity and a
	 * container pattern.
	 * <pre>(ContainerNamePattern1)=(DataAffinityString1),(ContainerNamePattern2)=(DataAffinityString2),...</pre>
	 * When {@link Container} is added by {@link ContainerInfo#setDataAffinity(String)}, the affinity string
	 * pairing with a container name pattern that matches the
	 * container name is applied. If there are multiple patterns
	 * that match the name, the first pattern in the specified
	 * order is selected. Each container name pattern follows the
	 * naming rules of container, except a wild card character '%'
	 * can also be specified in the pattern. The affinity string
	 * follows the rules of {@link ContainerInfo#setDataAffinity(String)}.
	 * To specify special characters used in the patterns or as
	 * delimiters for the patterns in a container name, etc., they
	 * must be escaped by '\'. But the characters against the
	 * naming rules of container or affinity cannot be specified.
	 * Supported since the version 2.7.</td></tr>
	 * <tr><td>notificationMember</td><td>
	 * A list of address and port pairs in cluster. It is used to connect to
	 * cluster which is configured with FIXED_LIST mode, and specified as
	 * follows.
	 * <pre>(Address1):(Port1),(Address2):(Port2),...</pre>
	 * This property cannot be specified with neither notificationAddress nor
	 * notificationProvider properties at the same time.
	 * This property is supported on version 2.9 or later.
	 * </td></tr>
	 * <tr><td>notificationProvider</td><td>
	 * A URL of address provider. It is used to connect to cluster which is
	 * configured with PROVIDER mode.
	 * This property cannot be specified with neither notificationAddress nor
	 * notificationMember properties at the same time.
	 * This property is supported on version 2.9 or later.
	 *
	 * </td></tr>
	 * <tr><td>applicationName</td><td>Name of an application. It may be
	 * contained in various information for management on the connected
	 * cluster. However, the cluster shall not be involved with the identity of
	 * applications. If the property is omitted, it is regarded that the name
	 * is not specified. Empty string cannot be specified.
	 * This property is supported on version 4.2 or later.</td></tr>
	 * <tr><td>timeZone</td><td>Time zone information. It is used for
	 * TIMESTAMP value operations in TQL.
	 * Specifies an offset value in the "{@code ±hh:mm}" or the "{@code ±hhmm}" format
	 * (where {@code ±} is {@code +} or {@code -}, {@code hh} is hours, and {@code mm}
	 * is minutes), "{@code Z}" (equivalent to {@code +00:00}), or "{@code auto}"
	 * (automatically set according to the execution environment).
	 * "{@code auto}" can only be used for the time zone that does not observe the
	 * daylight saving time.
	 * This property is supported on version 4.3 or later.</td></tr>
	 * </tbody>
	 * </table>
	 *
	 * <p>Cluster names, database names, user names and passwords
	 * are case-sensitive. See the GridDB Technical Reference for
	 * the details of the limitations, such as allowed characters
	 * and maximum length. When a name violating the limitations has
	 * been specified as a property value, the error detection may
	 * be delayed until the authentication processing. And there
	 * are the cases that the error is identified as an authentication
	 * error, etc., not a violation error for the limitations.</p>
	 *
	 * <p>A new {@link GridStore} instance is created by each call of this method.
	 * Operations on different {@link GridStore} instances and related objects are thread
	 * safe. That is, if some two objects are each created based on {@link GridStore}
	 * instances or they are just {@link GridStore} instances, and if they are related to
	 * different {@link GridStore} instances respectively, any method of one object can be
	 * called, no matter when a method of the other object may be called from any
	 * thread. However, since thread safety is not guaranteed for {@link GridStore} itself,
	 * it is not allowed to call a method of a single {@link GridStore} instance from two or
	 * more threads at an arbitrary time. </p>
	 *
	 * @param properties Properties specifying the settings for the object to be
	 * obtained.
	 *
	 * @throws GSException if host name resolution fails.
	 * @throws GSException if any specified property does not match the format
	 * explained above.
	 * even if connection or authentication will not succeed with their values.
	 * @throws GSException if the connection is closed.
	 * @throws NullPointerException {@code null} is specified as {@code properties}.
	 * </div>
	 */
	public abstract GridStore getGridStore(
			Properties properties) throws GSException;

	/**
	 * <div lang="ja">
	 * このファクトリの設定を変更します。
	 *
	 * <p>設定の変更は、このファクトリより生成された{@link GridStore}、
	 * ならびに、今後このファクトリで生成される{@link GridStore}に反映されます。</p>
	 *
	 * <p>以下のプロパティを指定できます。サポート外の名称のプロパティは無視されます。</p>
	 * <table>
	 * <thead><tr><th>名称</th><th>説明</th></tr></thead>
	 * <tbody>
	 * <tr><td>maxConnectionPoolSize</td><td>内部で使用される
	 * コネクションプールの最大コネクション数。{@code 0}以上
	 * {@link Integer#MAX_VALUE}までの数値の文字列表現。
	 * 値が{@code 0}の場合、コネクションプールを使用しないことを意味する。
	 * 省略時は既存の設定値を使用</td></tr>
	 * <tr><td>failoverTimeout</td><td>
	 * フェイルオーバ処理にて新たな接続先が
	 * 見つかるまで待機する時間の最低値。{@code 0}以上
	 * {@link Integer#MAX_VALUE}までの数値の文字列表現であり、単位は秒。
	 * {@code 0}の場合、フェイルオーバ処理を行わない。
	 * 省略時は既存の設定値を使用</td></tr>
	 * </tbody>
	 * </table>
	 *
	 * @throws GSException 指定のプロパティが上で説明した形式に合致しない場合
	 * @throws GSException すでにクローズ済みの場合
	 * @throws NullPointerException {@code properties}に{@code null}が
	 * 指定された場合
	 * </div><div lang="en">
	 * Changes the settings for this Factory.
	 *
	 * <p>The changed settings are reflected in {@link GridStore} already created
	 * by this Factory and {@link GridStore} to be created by this Factory later. </p>
	 *
	 * <p>The following properties can be specified. Unsupported property names are ignored.</p>
	 * <table>
	 * <thead><tr><th>Property</th><th>Description</th></tr></thead>
	 * <tbody>
	 * <tr><td>maxConnectionPoolSize</td><td>The maximum number of connections in the
	 * connection pool used inside. A numeric string representing {@code 0} to
	 * {@link Integer#MAX_VALUE}. The value {@code 0} indicates no use of the
	 * connection pool. If omitted, the default value is used.</td></tr>
	 * <tr><td>failoverTimeout</td><td>The minimum value of waiting time until a new
	 * destination is found in a failover. A numeric string representing {@code 0}
	 * to {@link Integer#MAX_VALUE} in seconds. The value {@code 0} indicates
	 * that no failover is performed. If omitted, the default value is used.
	 * </td></tr>
	 * </tbody>
	 * </table>
	 *
	 * @throws GSException if any specified property does not match the format shown above.
	 * @throws GSException if the connection is closed.
	 * @throws NullPointerException {@code null} is specified as {@code properties}.
	 * </div>
	 */
	public abstract void setProperties(Properties properties) throws GSException;

	/**
	 * <div lang="ja">
	 * このファクトリより作成された{@link GridStore}をすべてクローズし、
	 * 必要に応じて関連するリソースを解放します。
	 *
	 * <p>{@link GSException}が送出された場合でも、関連するコネクションリソースは
	 * すべて解放されます。すでにクローズ済みの場合、このメソッドを呼び出しても
	 * 何の効果もありません。なお、現在のVMの終了時にも呼び出されます。</p>
	 *
	 * @throws GSException クローズ処理中に接続障害などが発生した場合
	 *
	 * @see Closeable#close()
	 * </div><div lang="en">
	 * Closes all {@link GridStore} instances created by this Factory and release
	 * related resources as necessary.
	 *
	 * <p>Even if {@link GSException} is thrown, all related connection resources
	 * are released. If the connection is already closed, this method will not work
	 * effectively. It is also called when stopping the current VM.</p>
	 *
	 * @throws GSException if an connection failure etc. occurs while closing.
	 *
	 * @see Closeable#close()
	 * </div>
	 */
	public abstract void close() throws GSException;

	private static void registerLoggers() {
		boolean loggerAvailable = false;
		try {
			Class.forName("org.slf4j.Logger");
			Class.forName("org.slf4j.LoggerFactory");
			loggerAvailable = true;
		}
		catch (ClassNotFoundException e) {
		}

		for (String subName : LoggingUtils.SUB_LOGGER_NAMES) {
			final BaseGridStoreLogger logger;
			if (loggerAvailable) {
				try {
					logger = (BaseGridStoreLogger) Class.forName(
							LoggingUtils.DEFAULT_LOGGER_NAME + "$" +
							subName).newInstance();
				}
				catch (Exception e) {
					throw new Error(e);
				}
			}
			else {
				logger = null;
			}
			LoggingUtils.registerLogger(subName, logger);
		}
	}

}
