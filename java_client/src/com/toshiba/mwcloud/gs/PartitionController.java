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
import java.net.InetAddress;
import java.util.List;

/**
 * <div lang="ja">
 * パーティション状態の取得や操作のためのコントローラです。
 *
 * <p>パーティションとは、データを格納する論理的な領域です。
 * GridDBクラスタ内のデータ配置に基づいた操作を行う
 * ために使用します。</p>
 *
 * @since 1.5
 * </div><div lang="en">
 * Controller for acquiring and processing the partition status.
 *
 * <p>A partition is a theoretical region where data is stored.
 * It is used to perform operations based on the data arrangement in a GridDB cluster.</p>
 * </div>
 */
public interface PartitionController extends Closeable {

	/**
	 * <div lang="ja">
	 * 対象とするGridDBクラスタのパーティション数を取得します。
	 *
	 * <p>対象とするGridDBクラスタにおけるパーティション数設定の値を
	 * 取得します。一度取得した結果はキャッシュされ、次にクラスタ障害・
	 * クラスタノード障害を検知するまで再びGridDBクラスタに問い合わせる
	 * ことはありません。</p>
	 *
	 * @return パーティション数
	 *
	 * @throws GSException この処理のタイムアウト、接続障害が発生した場合、
	 * このオブジェクトまたは対応する{@link GridStore}のクローズ後に呼び出された場合
	 * </div><div lang="en">
	 * Get the number of partitions in the target GridDB cluster.
	 *
	 * <p>Get the value of the number of partitions set in the target GridDB cluster.
	 * Results are cached once acquired and until the next cluster failure and
	 * cluster node failure is detected, no inquiry will be sent to the GridDB cluster again.</p>
	 *
	 * @return Number of partitions
	 *
	 * @throws GSException If this process times out or when a connection error occurs,
	 * or if the process is invoked after this object or corresponding {@link GridStore} is closed
	 * </div>
	 */
	public int getPartitionCount() throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のパーティションに属するコンテナの総数を取得します。
	 *
	 * <p>コンテナ数を求める際の計算量は、コンテナ数にはおおむね依存しません。</p>
	 *
	 * @param partitionIndex パーティションインデックス。{@code 0}以上
	 * パーティション数未満の値
	 *
	 * @return コンテナ数
	 *
	 * @throws GSException 範囲外のパーティションインデックスが指定された場合
	 * @throws GSException この処理のタイムアウト、接続障害が発生した場合、
	 * このオブジェクトまたは対応する{@link GridStore}のクローズ後に呼び出された場合
	 *
	 * @see #getPartitionIndexOfContainer(String)
	 * </div><div lang="en">
	 * Get the total number of containers belonging to a specified partition.
	 *
	 * <p>The calculated quantity when determining the number of containers is generally not dependent
	 * on the number of containers.</p>
	 *
	 * @param partitionIndex Partition index. A value of {@code 0} or above and less than the number of partitions.
	 *
	 * @return Number of containers
	 *
	 * @throws GSException If a partition index outside the range is specified
	 * @throws GSException If this process times out, when a connection error occurs,
	 * or if the process is invoked after this object or corresponding {@link GridStore} is closed
	 *
	 * @see #getPartitionIndexOfContainer(String)
	 * </div>
	 */
	public long getContainerCount(int partitionIndex) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のパーティションに所属するコンテナの名前の一覧を取得します。
	 *
	 * <p>指定のパーティションについてコンテナの新規作成・構成変更・削除が
	 * 行われたとしても、該当コンテナを除くとその前後で一覧の取得結果の順序が
	 * 変わることはありません。それ以外の一覧の順序に関しては不定です。
	 * 重複する名前が含まれることはありません。</p>
	 *
	 * <p>取得件数の上限が指定された場合、上限を超える場合、後方のものから
	 * 切り捨てられます。指定条件に該当するものが存在しない場合、空のリストが
	 * 返却されます。</p>
	 *
	 * <p>返却されたリストに対して変更操作を行った場合に、
	 * {@link UnsupportedOperationException}などの実行時例外が発生するか
	 * どうかは未定義です。</p>
	 *
	 * @param partitionIndex パーティションインデックス。{@code 0}以上
	 * パーティション数未満の値
	 * @param start 取得範囲の開始位置。{@code 0}以上の値
	 * @param limit 取得件数の上限。{@code null}の場合、上限なしとみなされる
	 *
	 * @return コンテナの名前を要素とする{@link List}
	 *
	 * @throws GSException 範囲外のパーティションインデックスが指定された場合
	 * @throws GSException この処理のタイムアウト、接続障害が発生した場合、
	 * このオブジェクトまたは対応する{@link GridStore}のクローズ後に呼び出された場合
	 * </div><div lang="en">
	 * Get a list of the Container names belonging to a specified partition.
	 *
	 * <p>For the specified partition, the sequence of the list of acquisition results
	 * before and after will not be changed when the relevant Container is excluded
	 * even if a Container is newly created, its composition changed or the Container is deleted.
	 * All other lists are compiled in no particular order.
	 * No duplicate names will be included.</p>
	 *
	 * <p>If the upper limit of the number of acquisition cases is specified,
	 * the cases will be cut off starting from the ones at the back if the upper limit is exceeded.
	 * If no relevant specified condition exists, a blank list is returned.</p>
	 *
	 * <p>When a returned list is updated, whether an exception will occur or not
	 * when {@link UnsupportedOperationException}, etc. is executed is still not defined.</p>
	 *
	 * @param partitionIndex Partition index. A value of {@code 0} or above and less than the number of partitions.
	 * @param start Start position of the acquisition range. A value of {@code 0} and above
	 * @param limit Upper limit of the number of cases acquired. If {@code null}, no upper limit is assumed
	 *
	 * @return Assuming the Container name as a component {@link List}
	 *
	 * @throws GSException If a partition index outside the range is specified
	 * @throws GSException If this process times out, when a connection error occurs,
	 * or if the process is invoked after this object or corresponding {@link GridStore} is closed
	 * </div>
	 */
	public List<String> getContainerNames(
			int partitionIndex, long start, Long limit) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のパーティションに対応するノードのアドレス一覧を取得します。
	 *
	 * <p>一覧の順序に関しては不定です。重複するアドレスが含まれることはありません。</p>
	 *
	 * <p>返却されたリストに対して変更操作を行った場合に、
	 * {@link UnsupportedOperationException}などの実行時例外が発生するか
	 * どうかは未定義です。</p>
	 *
	 * @param partitionIndex パーティションインデックス。{@code 0}以上
	 * パーティション数未満の値
	 *
	 * @return ノードのアドレスを表す{@link InetAddress}を要素とする、{@link List}
	 *
	 * @throws GSException 範囲外のパーティションインデックスが指定された場合
	 * @throws GSException この処理のタイムアウト、接続障害が発生した場合、
	 * このオブジェクトまたは対応する{@link GridStore}のクローズ後に呼び出された場合
	 * </div><div lang="en">
	 * Get a list of the addresses of the nodes corresponding to a specified partition.
	 *
	 * <p>The list will be compiled in no particular order. No duplicate address will be included.</p>
	 *
	 * <p>When a returned list is updated, whether an exception will occur or not
	 * when {@link UnsupportedOperationException}, etc. is executed is still not defined.</p>
	 *
	 * @param partitionIndex Partition index. A value of {@code 0} or above and less than the number of partitions.
	 *
	 * @return Assuming {@link InetAddress}, which represents the address of the node, as a component, {@link List}
	 *
	 * @throws GSException If a partition index outside the range is specified
	 * @throws GSException If this process times out, when a connection error occurs,
	 * or if the process is invoked after this object or corresponding {@link GridStore} is closed
	 * </div>
	 */
	public List<InetAddress> getHosts(int partitionIndex) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のパーティションに対応するオーナノードのアドレスを取得します。
	 *
	 * <p>オーナノードとは、
	 * {@link GridStoreFactory#getGridStore(java.util.Properties)}における
	 * 一貫性レベルとして{@code "IMMEDIATE"}を指定した場合に、
	 * 常に選択されるノードのことです。</p>
	 *
	 * @param partitionIndex パーティションインデックス。{@code 0}以上
	 * パーティション数未満の値
	 *
	 * @return オーナノードのアドレスを表す{@link InetAddress}
	 *
	 * @throws GSException 範囲外のパーティションインデックスが指定された場合
	 * @throws GSException この処理のタイムアウト、接続障害が発生した場合、
	 * このオブジェクトまたは対応する{@link GridStore}のクローズ後に呼び出された場合
	 * </div><div lang="en">
	 * Get the address of the owner node corresponding to a specified partition.
	 *
	 * <p>An owner node is a node that is always selected when {@code "IMMEDIATE"} is specified as a consistency level
	 * in {@link GridStoreFactory#getGridStore(java.util.Properties)}.</p>
	 *
	 * @param partitionIndex Partition index. A value of {@code 0} or above and less than the number of partitions.
	 *
	 * @return Represents the address of the owner node {@link InetAddress}
	 *
	 * @throws GSException If a partition index outside the range is specified
	 * @throws GSException If this process times out, when a connection error occurs,
	 * or if the process is invoked after this object or corresponding {@link GridStore} is closed
	 * </div>
	 */
	public InetAddress getOwnerHost(int partitionIndex) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のパーティションに対応するバックアップノードのアドレス一覧を取得します。
	 *
	 * <p>バックアップノードとは、
	 * {@link GridStoreFactory#getGridStore(java.util.Properties)}における
	 * 一貫性レベルとして{@code "EVENTUAL"}を指定した場合に、
	 * 優先的に選択されるノードのことです。</p>
	 *
	 * <p>一覧の順序に関しては不定です。重複するアドレスが含まれることはありません。</p>
	 *
	 * <p>返却されたリストに対して変更操作を行った場合に、
	 * {@link UnsupportedOperationException}などの実行時例外が発生するか
	 * どうかは未定義です。</p>
	 *
	 * @param partitionIndex パーティションインデックス。{@code 0}以上
	 * パーティション数未満の値
	 *
	 * @return バックアップノードのアドレスを表す{@link InetAddress}を要素とする、{@link List}
	 *
	 * @throws GSException 範囲外のパーティションインデックスが指定された場合
	 * @throws GSException この処理のタイムアウト、接続障害が発生した場合、
	 * このオブジェクトまたは対応する{@link GridStore}のクローズ後に呼び出された場合
	 * </div><div lang="en">
	 * Get a list of the addresses of the backup nodes corresponding to a specified partition.
	 *
	 * <p>A backup node is a node that is selected with a higher priority when {@code "EVENTUAL"} is
	 * specified as a consistency level in {@link GridStoreFactory#getGridStore(java.util.Properties)}.</p>
	 *
	 * <p>The list will be compiled in no particular order. No duplicate address will be included.</p>
	 *
	 * <p>When a returned list is updated, whether an exception will occur or not
	 * when {@link UnsupportedOperationException}, etc. is executed is still not defined.</p>
	 *
	 * @param partitionIndex Partition index. A value of {@code 0} or above and less than the number of partitions.
	 *
	 * @return Assuming {@link InetAddress}, which represents the address of the backup node,
	 * as a component, {@link List}
	 *
	 * @throws GSException If a partition index outside the range is specified
	 * @throws GSException If this process times out, when a connection error occurs,
	 * or if the process is invoked after this object or corresponding {@link GridStore} is closed
	 * </div>
	 */
	public List<InetAddress> getBackupHosts(
			int partitionIndex) throws GSException;

	/**
	 * <div lang="ja">
	 * 優先的に選択されるホストのアドレスを設定します。
	 *
	 * <p>バックアップノードへの接続など、可能な接続先が複数存在する場合に、
	 * 設定されたアドレスが候補に含まれていれば常に選択されるようになります。
	 * それ以外の場合は設定が無視されます。</p>
	 *
	 * @param partitionIndex パーティションインデックス。{@code 0}以上
	 * パーティション数未満の値
	 *
	 * @param host 優先的に選択されるホストのアドレス。{@code null}の場合、
	 * 設定が解除される
	 *
	 * @throws GSException 範囲外のパーティションインデックスが指定された場合
	 * @throws GSException このオブジェクトまたは対応する{@link GridStore}の
	 * クローズ後に呼び出された場合
	 *
	 * @see #getBackupHosts(int)
	 * </div><div lang="en">
	 * Set the address of the host to be prioritized in the selection.
	 *
	 * <p>If multiple possible destinations exist e.g. connections to backup nodes, etc.,
	 * the address set will always be selected if it is included in the candidate destination.
	 * The setting is ignored otherwise.</p>
	 *
	 * @param partitionIndex Partition index. A value of {@code 0} or above and less than the number of partitions.
	 *
	 * @param host Address of the host to be prioritized in the selection.
	 * For {@code null}, the setting is cancelled
	 *
	 * @throws GSException If a partition index outside the range is specified
	 * @throws GSException If invoked after this object or corresponding {@link GridStore} is closed.
	 *
	 * @see #getBackupHosts(int)
	 * </div>
	 */
	public void assignPreferableHost(
			int partitionIndex, InetAddress host) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のコンテナ名に対応するパーティションインデックスを取得します。
	 *
	 * <p>一度GridDBクラスタが構築されると、コンテナの所属先のパーティションが
	 * 変化することはなく、パーティションインデックスも一定となります。
	 * 指定の名前に対応するコンテナが存在するかどうかは、結果に依存しません。</p>
	 *
	 * <p>パーティションインデックスの算出に必要とする情報はキャッシュされ、
	 * 次にクラスタ障害・クラスタノード障害を検知するまで再びGridDBクラスタに
	 * 問い合わせることはありません。</p>
	 *
	 * @param containerName コンテナ名。{@code null}は指定できない
	 *
	 * @return 指定のコンテナ名に対応するパーティションインデックス
	 *
	 * @throws GSException コンテナ名として許可されない文字列が指定された場合
	 * @throws GSException この処理のタイムアウト、接続障害が発生した場合、
	 * このオブジェクトまたは対応する{@link GridStore}のクローズ後に呼び出された場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Get the partition index corresponding to the specified Container name.
	 *
	 * <p>Once a GridDB cluster is constructed, there will not be any changes in the partitions of the destination
	 * that the Container belongs to and the partition index will also be fixed.
	 * Whether there is a Container corresponding to the specified name or not does not depend on the results</p>
	 *
	 * <p>Information required in the computation of the partition index is cached and
	 * until the next cluster failure and cluster node failure is detected,
	 * no inquiry will be sent to the GridDB cluster again.</p>
	 *
	 * @param containerName Container name. {@code null} cannot be specified
	 *
	 * @return Partition index corresponding to the specified Container name
	 *
	 * @throws GSException If a character string allowed as a Container name is specified
	 * @throws GSException If this process times out, when a connection error occurs,
	 * or if the process is invoked after this object or corresponding {@link GridStore} is closed
	 * @throws NullPointerException If {@code null} is specified in the argument
	 * </div>
	 */
	public int getPartitionIndexOfContainer(
			String containerName) throws GSException;

	/**
	 * <div lang="ja">
	 * GridDBとの接続状態を解除し、必要に応じて関連するリソースを解放します。
	 *
	 * @throws GSException 現バージョンでは送出されない
	 * </div><div lang="en">
	 * The connection status with GridDB is released and related resources are released where necessary.
	 *
	 * @throws GSException Not sent out in the current version
	 * </div>
	 */
	@Override
	public void close() throws GSException;

}
