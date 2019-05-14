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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.toshiba.mwcloud.gs.common.GSErrorCode;
import com.toshiba.mwcloud.gs.common.RowMapper;

/**
 * <div lang="ja">
 * 特定のコンテナに関する情報を表します。
 *
 * <p>コンテナ名の表記、もしくは、コンテナ種別と時系列オプションの有無の対応などの
 * 内容の妥当性について、必ずしも検査するとは限りません。</p>
 * </div><div lang="en">
 * Represents the information about a specific Container.
 *
 * <p>It does not guarantee the validity of values e.g. notation of the Container name,
 * and the existence of TimeSeries option related to its Container type.</p>
 * </div>
 */
public class ContainerInfo {

	private String name;

	private ContainerType type;

	private List<ColumnInfo> columnInfoList;

	private List<IndexInfo> indexInfoList;

	private boolean rowKeyAssigned;

	private TimeSeriesProperties timeSeriesProperties;

	private boolean columnOrderIgnorable;

	private List<TriggerInfo> triggerInfoList;

	private String dataAffinity;

	/**
	 * <div lang="ja">
	 * カラムレイアウトに関する情報などを指定してコンテナ情報を作成します。
	 *
	 * @param name コンテナ名。{@code null}を指定すると未設定状態となる
	 * @param type コンテナ種別。{@code null}を指定すると未設定状態となる
	 * @param columnInfoList カラム情報のリスト。{@code null}は指定できない
	 * @param rowKeyAssigned ロウキーに対応するカラムの有無。ロウキーを持つ
	 * 場合は{@code true}、持たない場合は{@code false}
	 *
	 * @throws NullPointerException {@code columnInfoList}に{@code null}が
	 * 指定された場合
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Container information is created by specifying information on the column layout.
	 *
	 * @param name Container name. Not set when {@code null} is specified.
	 * @param type Container type. Not set when {@code null} is specified.
	 * @param columnInfoList List of column information. {@code null} cannot be specified.
	 * @param rowKeyAssigned Presence or absence of a column corresponding to a row key.
	 * {@code true} if you have a row key, and {@code false} if you don’t have one.
	 *
	 * @throws NullPointerException when {@code null} is specified as argument
	 *
	 * @since 1.5

	 * </div>
	 */
	public ContainerInfo(String name, ContainerType type,
			List<ColumnInfo> columnInfoList, boolean rowKeyAssigned) {
		try {
			this.name = name;
			this.type = type;
			this.columnInfoList = new ArrayList<ColumnInfo>(columnInfoList);
			this.indexInfoList = Collections.emptyList();
			this.rowKeyAssigned = rowKeyAssigned;
			this.triggerInfoList = Collections.emptyList();
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(
					columnInfoList, "columnInfoList", e);
		}
	}

	/**
	 * <div lang="ja">
	 * 空のコンテナ情報を作成します。
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Creates an empty ContainerInfo.
	 * </div>
	 */
	public ContainerInfo() {
		this.columnInfoList = Collections.emptyList();
		this.indexInfoList = Collections.emptyList();
		this.triggerInfoList = Collections.emptyList();
	}

	/**
	 * <div lang="ja">
	 * 指定のコンテナ情報を複製します。
	 *
	 * @param containerInfo 複製元のコンテナ情報。{@code null}は指定できない
	 *
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Duplicates the information about the specified Container.
	 *
	 * @param containerInfo Copied Container information. {@code null} cannot be specified.
	 *
	 * @throws NullPointerException when {@code null} is specified as argument
	 *
	 * @since 1.5
	 * </div>
	 */
	public ContainerInfo(ContainerInfo containerInfo) {
		this();

		setName(containerInfo.getName());
		setType(containerInfo.getType());

		final int columnCount = containerInfo.getColumnCount();
		if (columnCount > 0) {
			final List<ColumnInfo> columnInfoList =
					new ArrayList<ColumnInfo>();
			for (int i = 0; i < columnCount; i++) {
				columnInfoList.add(containerInfo.getColumnInfo(i));
			}
			setColumnInfoList(columnInfoList);
		}

		setRowKeyAssigned(containerInfo.isRowKeyAssigned());
		setIndexInfoList(containerInfo.getIndexInfoList());
		setTimeSeriesProperties(containerInfo.getTimeSeriesProperties());
		setTriggerInfoList(containerInfo.getTriggerInfoList());

		setDataAffinity(containerInfo.getDataAffinity());
	}

	/**
	 * <div lang="ja">
	 * コンテナ名を取得します。
	 *
	 * @return コンテナ名。未設定の場合は{@code null}
	 * </div><div lang="en">
	 * Returns the name of the specified Container.
	 *
	 * @return A Container name, or {@code null} if unspecified.
	 * </div>
	 */
	public String getName() {
		return name;
	}

	/**
	 * <div lang="ja">
	 * コンテナの種別を取得します。
	 *
	 * @return コンテナの種別。未設定の場合は{@code null}
	 *
	 * @see ContainerType
	 * </div><div lang="en">
	 * Returns the Container type of the specified Container.
	 *
	 * @return A Container type, or {@code null} if unspecified.
	 *
	 * @see ContainerType
	 * </div>
	 */
	public ContainerType getType() {
		return type;
	}

	/**
	 * <div lang="ja">
	 * カラム数を取得します。
	 *
	 * @return カラム数。カラムレイアウト未設定の場合は{@code 0}
	 * </div><div lang="en">
	 * Returns the number of Columns of the specified Container.
	 *
	 * @return A number of Columns, or {@code 0} if Column layout is unspecified.
	 *
	 * </div>
	 */
	public int getColumnCount() {
		return columnInfoList.size();
	}

	/**
	 * <div lang="ja">
	 * 指定カラムに関する情報を取得します。
	 *
	 * @param column カラムを特定するための番号。{@code 0}以上かつカラム数未満の値
	 *
	 * @return 指定カラム番号に対応するカラム情報
	 *
	 * @throws IllegalArgumentException 範囲外のカラム番号を指定した場合
	 *
	 * @see RowField#columnNumber()
	 * </div><div lang="en">
	 * Returns the information about the specified Column.
	 *
	 * @param column An index of a Column, from {@code 0} to number of Columns minus one.
	 *
	 * @return The information of the Column corresponding to the specified index.
	 *
	 * @throws IllegalArgumentException If the specified Column number is out of range.
	 *
	 * @see RowField#columnNumber()
	 * </div>
	 */
	public ColumnInfo getColumnInfo(int column) {
		try {
			return columnInfoList.get(column);
		}
		catch (IndexOutOfBoundsException e) {
			throw new IllegalArgumentException(
					"Column number out of range (column=" + column + ", " +
					"reason=" + e.getMessage() + ")", e);
		}
	}

	/**
	 * <div lang="ja">
	 * ロウキーに対応するカラムの有無を取得します。
	 *
	 * <p>コンテナがロウキーを持つ場合、対応するカラム番号は{@code 0}です。</p>
	 *
	 * @return ロウキーの有無
	 * </div><div lang="en">
	 * Checks if a Column is assigned as a Row key.
	 *
	 * <p>If the Container has a Row key, the number of its corresponding Column is {@code 0}. </p>
	 *
	 * @return {@code true} If a Row key is assigned, otherwise {@code false}.
	 * </div>
	 */
	public boolean isRowKeyAssigned() {
		return rowKeyAssigned;
	}

	/**
	 * <div lang="ja">
	 * カラム順序が無視できるかどうかを返します。
	 *
	 * @return カラム順序が無視できるか
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Checks if the order of Columns can be ignored.
	 *
	 * @return {@code true} if yes, otherwise {@code false}.
	 * </div>
	 */
	public boolean isColumnOrderIgnorable() {
		return columnOrderIgnorable;
	}

	/**
	 * <div lang="ja">
	 * カラム順序が無視できるかどうかを設定します。
	 *
	 * <p>デフォルトでは無視しない({@code false})状態に設定されています。</p>
	 *
	 * @param ignorable カラム順序が無視できるか
	 *
	 * @see GridStore#putContainer(String, ContainerInfo, boolean)
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Sets whether the order of Columns can be ignored.
	 *
	 * <p>Default value cannot be ignored ({@code false}).</p>
	 *
	 * @param ignorable If the order of Columns can be ignored
	 *
	 * @see GridStore#putContainer(String, ContainerInfo, boolean)
	 * </div>
	 */
	public void setColumnOrderIgnorable(boolean ignorable) {
		this.columnOrderIgnorable = ignorable;
	}

	/**
	 * <div lang="ja">
	 * コンテナ名を設定します。
	 *
	 * @param name コンテナ名。{@code null}の場合、設定が解除される
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Sets a name of the Container.
	 *
	 * @param name A name of the Container. For {@code null}, the setting is cancelled.
	 * </div>
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * <div lang="ja">
	 * コンテナ種別を設定します。
	 *
	 * @param type コンテナ種別。{@code null}の場合、設定が解除される
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Sets a type of the Container.
	 *
	 * @param type A type of the Container. For {@code null}, the setting is cancelled.
	 * </div>
	 */
	public void setType(ContainerType type) {
		this.type = type;
	}

	/**
	 * <div lang="ja">
	 * すべてのカラムの情報をまとめて設定します。
	 *
	 * <p>カラム順序を無視しない場合、指定のカラム情報の並びが実際のコンテナの
	 * カラムの並びと対応します。</p>
	 *
	 * <p>ロウキーに対応するカラムの有無の設定状態によらず、設定を解除することが
	 * できます。</p>
	 *
	 * <p>指定したオブジェクトの内容を呼び出し後に変更したとしても、
	 * このオブジェクトの内容は変化しません。</p>
	 *
	 * @param columnInfoList カラム情報のリスト。{@code null}または空のリスト場合、
	 * 設定が解除される
	 *
	 * @see #setColumnOrderIgnorable(boolean)
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Set information of all Columns all at once.
	 *
	 * <p>The order of the Columns of the Container corresponds to the order of
	 * information of the Columns, if its can be ignored.</p>
	 *
	 * <p>It can be undefined, regardless of the existence of a Row key Column.</p>
	 *
	 * <p>Updates of the specified object after this function is specified will not change
	 * the object.</p>
	 *
	 * @param columnInfoList List of the information of Columns.
	 * For {@code null} or an empty list, the setting is cancelled.
	 *
	 * @see #setColumnOrderIgnorable(boolean)
	 * </div>
	 */
	public void setColumnInfoList(List<ColumnInfo> columnInfoList) {
		if (columnInfoList == null) {
			this.columnInfoList = Collections.emptyList();
			return;
		}

		this.columnInfoList = new ArrayList<ColumnInfo>(columnInfoList);
	}

	/**
	 * <div lang="ja">
	 * ロウキーに対応するカラムの有無を設定します。
	 *
	 * <p>デフォルトではロウキーなしに設定されています。</p>
	 *
	 * <p>カラムレイアウトの設定状態によらず使用できます。</p>
	 *
	 * @param assigned ロウキーに対応するカラムの有無。ロウキーを持つ場合は
	 * {@code true}、持たない場合は{@code false}
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Sets a Row key Column is assigned or not.
	 *
	 * <p>A Row key is assigned default.</p>
	 *
	 * <p>This function is available regardless of the layout of Columns.</p>
	 *
	 * @param assigned A Row key Column is assigned or not. {@code true} for assigned, otherwise {@code false}.
	 * </div>
	 */
	public void setRowKeyAssigned(boolean assigned) {
		this.rowKeyAssigned = assigned;
	}

	/**
	 * <div lang="ja">
	 * 索引情報の一覧を取得します。
	 *
	 * <p>返却された値に対して変更操作を行った場合、
	 * {@link UnsupportedOperationException}が発生することがあります。
	 * また、このオブジェクトに対する操作により、返却されたオブジェクトの内容が
	 * 変化することはありません。</p>
	 *
	 * @return 索引情報の一覧
	 *
	 * @since 3.5
	 * </div><div lang="en">
	 * Get a list of index information.
	 *
	 * <p>{@link UnsupportedOperationException} may occur if the returned value is changed.
	 * Also, the operation on this object does not change the contents of the returned object. </p>
	 *
	 * @return List of index information.
	 *
	 * @since 3.5
	 * </div>
	 */
	public List<IndexInfo> getIndexInfoList() {
		return indexInfoList;
	}

	/**
	 * <div lang="ja">
	 * 索引情報の一覧を設定します。
	 *
	 * <p>指定したオブジェクトの内容を呼び出し後に変更したとしても、
	 * このオブジェクトの内容は変化しません。</p>
	 *
	 * @param indexInfoList 索引情報の一覧。{@code null}または空のリスト場合、
	 * 設定が解除される
	 *
	 * @since 3.5
	 * </div><div lang="en">
	 * Set a list of index information.
	 *
	 * <p> Even if you change the contents of the specified object after calling,
	 * the contents of this object will not change. </p>
	 *
	 * @param indexInfoList List of index information. Not set in the case of {@code null} or empty list.
	 *
	 * @since 3.5
	 * </div>
	 */
	public void setIndexInfoList(List<IndexInfo> indexInfoList) {
		if (indexInfoList == null || indexInfoList.isEmpty()) {
			this.indexInfoList = Collections.emptyList();
			return;
		}

		final List<IndexInfo> dest =
				new ArrayList<IndexInfo>(indexInfoList.size());
		for (IndexInfo info : indexInfoList) {
			dest.add(info.toUnmodifiable());
		}

		this.indexInfoList = Collections.unmodifiableList(dest);
	}

	/**
	 * <div lang="ja">
	 * 時系列構成オプションを取得します。
	 *
	 * <p>返却されたオブジェクトの内容を呼び出し後に変更した場合に、
	 * このオブジェクトの内容が変化するかどうかは未定義です。
	 * また、このオブジェクトに対する操作により、返却されたオブジェクトの内容が
	 * 変化するかどうかは未定義です。</p>
	 *
	 * @return 時系列構成オプション。未設定の場合は{@code null}
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Returns the optional properties of TimeSeries.
	 *
	 * <p>If the contents of the returned object is changed after it has been invoked,
	 * it is not defined whether the contents of this content will be changed or not.
	 * Moreover, it is not defined whether the contents of the returned object
	 * will be changed or not by operating this object.</p>
	 *
	 * @return The optional properties of TimeSeries, or {@code null} if undefined.
	 * </div>
	 */
	public TimeSeriesProperties getTimeSeriesProperties() {
		return timeSeriesProperties;
	}

	/**
	 * <div lang="ja">
	 * 時系列構成オプションを設定します。
	 *
	 * <p>コンテナ種別の設定状態によらず使用できます。</p>
	 *
	 * <p>指定したオブジェクトの内容を呼び出し後に変更したとしても、
	 * このオブジェクトの内容は変化しません。</p>
	 *
	 * @param props 時系列構成オプション。{@code null}の場合、設定が解除される
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Sets the optional properties of TimeSeries.
	 *
	 * <p>This function is available for any Container type.</p>
	 *
	 * <p>Updates of the specified object after this function is invoked will not change
	 * the object.</p>
	 *
	 * @param props The optional properties of TimeSeries.
	 * For {@code null}, the setting is cancelled.
	 * </div>
	 */
	public void setTimeSeriesProperties(TimeSeriesProperties props) {
		if (props == null) {
			timeSeriesProperties = null;
			return;
		}

		timeSeriesProperties = props.clone();
	}

	/**
	 * <div lang="ja">
	 * @deprecated
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	public Set<IndexType> getIndexTypes(int column) {
		throw new Error("Not supported on any version except for V1.5 beta");
	}

	/**
	 * <div lang="ja">
	 * @deprecated
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	public void setIndexTypes(int column, Set<IndexType> indexTypes) {
		throw new Error("Not supported on any version except for V1.5 beta");
	}

	/**
	 * <div lang="ja">
	 * トリガ情報の一覧を取得します。
	 *
	 * <p>返却された値に対して変更操作を行った場合、
	 * {@link UnsupportedOperationException}が発生することがあります。
	 * また、このオブジェクトに対する操作により、返却されたオブジェクトの内容が
	 * 変化することはありません。</p>
	 *
	 * @return トリガ情報の一覧
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Returns a list of the trigger information.
	 *
	 * <p>{@link UnsupportedOperationException} can occur when the returned value is updated.
	 * After objects are returned, updates of this object do not change the returned object.</p>
	 *
	 * @return A list of the trigger information.
	 * </div>
	 */
	public List<TriggerInfo> getTriggerInfoList() {
		return triggerInfoList;
	}

	/**
	 * <div lang="ja">
	 * トリガ情報の一覧を設定します。
	 *
	 * <p>返却された値に対して変更操作を行った場合、
	 * {@link UnsupportedOperationException}が発生することがあります。
	 * また、このオブジェクトに対する操作により、返却されたオブジェクトの内容が
	 * 変化することはありません。</p>
	 *
	 * @param triggerInfoList トリガ情報のリスト。{@code null}または空のリスト場合、
	 * 設定が解除される
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Sets all information of triggers.
	 *
	 * @param triggerInfoList A list of trigger information.
	 * For {@code null}, the setting is cancelled.
	 * </div>
	 */
	public void setTriggerInfoList(List<TriggerInfo> triggerInfoList) {
		if (triggerInfoList == null) {
			this.triggerInfoList = Collections.emptyList();
			return;
		}

		this.triggerInfoList = Collections.unmodifiableList(
				new ArrayList<TriggerInfo>(triggerInfoList));
	}

	/**
	 * <div lang="ja">
	 * データ配置最適化のために用いられる、コンテナ間の類似性を示す
	 * 文字列を取得します。
	 *
	 * @return 時系列間の類似性を示す文字列。標準設定の場合は{@code null}
	 *
	 * @see #setDataAffinity(String)
	 *
	 * @since 2.1
	 * </div><div lang="en">
	 * Returns a data affinity string of the Container.
	 *
	 * @return A data affinity string, or {@code null} as default.
	 *
	 * @see #setDataAffinity(String)
	 * </div>
	 */
	public String getDataAffinity() {
		return dataAffinity;
	}

	/**
	 * <div lang="ja">
	 * データ配置最適化のために用いられる、コンテナ間の類似性(データ
	 * アフィニティ)を示す文字列を設定します。
	 *
	 * <p>同一クラスタノード上の同一管理領域内に格納されるコンテナについて、
	 * 配置先を最適化するために使用されます。</p>
	 *
	 * <p>データアフィニティが同一のコンテナの内容は、近接する配置先に格納される
	 * 可能性が高くなります。また、解放期限が設定され、近接する配置先に
	 * 格納された時系列について、登録頻度などの変更パターンが類似している場合、
	 * 解放期限に到達したロウの解放処理が効率的に行われる可能性が
	 * 高くなります。</p>
	 *
	 * <p>コンテナの定義において使用できるデータアフィニティ文字列の文字種や
	 * 長さには制限があります。具体的には、GridDBテクニカルリファレンスを参照
	 * してください。ただし、文字列を設定した時点で必ずしもすべての制限を
	 * 検査するとは限りません。特に記載のない限り、データアフィニティ文字列が
	 * 使用される操作では、ASCIIの大文字・小文字表記の違いが区別されます。</p>
	 *
	 * @param dataAffinity コンテナ間の類似性を示す文字列。{@code null}が
	 * 指定された場合は標準設定を優先することを示す。規則に合致しない文字列は
	 * 指定できない場合がある
	 *
	 * @throws IllegalArgumentException 制限に反する文字列が指定された
	 * ことを検知できた場合
	 *
	 * @since 2.1
	 * </div><div lang="en">
	 *
	 * Sets a string to represent similarity between containers
	 * (data affinity). The string is used for optimizing the data
	 * allocation.
	 *
	 * <p>A data affinity string is for optimizing the arrangement of Containers
	 * among the nodes of the cluster.</p>
	 *
	 * <p>Containers which have the same data affinity may be stored
	 * near each other. Therefore the efficiency for the expiration of Rows
	 * may be improved by using the same data affinity string for TimeSeries Containers
	 * which includes Rows with similar elapsed time periods.</p>
	 *
	 * <p>There are the limitations, allowed characters and maximum
	 * length, for the data affinity string. See GridDB Technical
	 * Reference for the details. All the limitations may not be
	 * checked when setting the string. The data affinity string
	 * is case-sensitive unless otherwise noted.</p>
	 *
	 * @param dataAffinity A string to represent similarity
	 * between containers. If {@code null} is specified, the
	 * Container will be stored as usual. There are the cases that
	 * string against the limitations cannot be specified.
	 *
	 * @throws IllegalArgumentException If the specified string is not proper.
	 *
	 * @since 2.1
	 * </div>
	 */
	public void setDataAffinity(String dataAffinity) {
		if (dataAffinity != null) {
			try {
				RowMapper.checkSymbol(dataAffinity, "data affinity");
			}
			catch (GSException e) {
				throw new IllegalArgumentException(e);
			}
		}

		this.dataAffinity = dataAffinity;
	}
}
