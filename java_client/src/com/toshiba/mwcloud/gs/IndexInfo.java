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

import com.toshiba.mwcloud.gs.common.GSErrorCode;

/**
 * <div lang="ja">
 * 索引の設定内容を表します。
 *
 * <p>カラム名の表記やカラム番号の妥当性について、必ずしも検査するとは
 * 限りません。</p>
 *
 * @since 3.5
 * </div><div lang="en">
 * shows the index setting
 *
 * <p>The notation of column names and the validity of column numbers
 * are not inspected. </p>
 *
 * @since 3.5
 * </div>
 */
public class IndexInfo {

	private IndexType type;

	private String name;

	private List<Integer> columnList = Collections.emptyList();

	private List<String> columnNameList = Collections.emptyList();

	/**
	 * <div lang="ja">
	 * 空の索引情報を作成します。
	 *
	 * <p>索引種別、索引名、カラム番号、カラム名がいずれも未設定の状態の
	 * 索引情報を作成します。</p>
	 * </div><div lang="en">
	 * Create empty index information.
	 *
	 * <p> Creates index information with no index type, index name,
	 * column number, column name. </p>
	 * </div>
	 */
	public IndexInfo() {
	}

	/**
	 * <div lang="ja">
	 * 指定の内容を引き継ぎ、新たな索引情報を作成します。
	 *
	 * @param info 引き継ぎ対象の索引情報
	 *
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Inherits the specified content and creates new index information.
	 *
	 * @param info Index information
	 *
	 * @throws NullPointerException when {@code null} is specified as argument
	 * </div>
	 */
	public IndexInfo(IndexInfo info) {
		final boolean sameClass;
		try {
			sameClass = (info.getClass() == IndexInfo.class);
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(info, "info", e);
		}

		if (sameClass) {
			type = info.type;
			name = info.name;
			columnList = info.columnList;
			columnNameList = info.columnNameList;
		}
		else {
			setType(info.getType());
			setName(info.getName());
			setColumnList(info.getColumnList());
			setColumnNameList(info.getColumnNameList());
		}
	}

	/**
	 * <div lang="ja">
	 * 必要に応じカラム名と索引種別を指定して索引情報を作成します。
	 *
	 * @param columnName カラム名。指定しない場合は{@code null}
	 * @param type 索引種別。指定しない場合は{@code null}
	 * </div><div lang="en">
	 * If necessary, create index information by specifying the column name and index type.
	 *
	 * @param columnName Column name. If not specified {@code null}
	 * @param type Index type. If not specified {@code null}
	 * </div>
	 */
	public static IndexInfo createByColumn(String columnName, IndexType type) {
		final IndexInfo info = new IndexInfo();
		info.setColumnName(columnName);
		info.setType(type);
		return info;
	}

	/**
	 * <div lang="ja">
	 * 必要に応じカラム名の列と索引種別を指定して索引情報を作成します。
	 *
	 * @param columnNames カラム名の列。長さ{@code 0}は許容するが、
	 * {@code null}は指定できない
	 * @param type 索引種別。指定しない場合は{@code null}
	 *
	 * @throws NullPointerException {@code columnNames}に{@code null}が
	 * 指定された場合
	 *
	 * @since 4.3
	 * </div><div lang="en">
	 * Creates index information by specifying the list of column names and index
	 * type as needed.
	 *
	 * @param columnNames List of column names. Length {@code 0} is allowed, but
	 * {@code null} cannot be specified
	 * @param type Index type. If not specified {@code null}
	 *
	 * @throws NullPointerException If {@code null} is specified in the argument
	 * {@code columnNames}
	 *
	 * @since 4.3
	 * </div>
	 */
	public static IndexInfo createByColumnList(
			List<String> columnNames, IndexType type) {
		final IndexInfo info = new IndexInfo();
		info.setColumnNameList(columnNames);
		info.setType(type);
		return info;
	}

	/**
	 * <div lang="ja">
	 * 必要に応じ索引名と索引種別を指定して索引情報を作成します。
	 *
	 * @param name 索引名。指定しない場合は{@code null}
	 * @param type 索引種別。指定しない場合は{@code null}
	 * </div><div lang="en">
	 * Creates index information by specifying the index name and index type as necessary.
	 *
	 * @param name Index name. If not specified {@code null}
	 * @param type Index type. If not specified {@code null}

	 * </div>
	 */
	public static IndexInfo createByName(String name, IndexType type) {
		final IndexInfo info = new IndexInfo();
		info.setName(name);
		info.setType(type);
		return info;
	}

	/**
	 * <div lang="ja">
	 * 索引種別を取得します。
	 *
	 * @return 索引種別。未設定の場合は{@code null}
	 * </div><div lang="en">
	 * Returns the index type.
	 *
	 * @return Index type. If not set {@code null}
	 * </div>
	 */
	public IndexType getType() {
		return type;
	}

	/**
	 * <div lang="ja">
	 * 索引種別を設定します。
	 *
	 * @param type 索引種別。{@code null}の場合は未設定状態になる
	 * </div><div lang="en">
	 * Sets the index type.
	 *
	 * @param type Index type. Not set when {@code null} is specified.
	 * </div>
	 */
	public void setType(IndexType type) {
		this.type = type;
	}

	/**
	 * <div lang="ja">
	 * 索引名を取得します。
	 *
	 * @return 索引名。未設定の場合は{@code null}
	 * </div><div lang="en">
	 * Returns the index name.
	 *
	 * @return index name. If not set {@code null}
	 * </div>
	 */
	public String getName() {
		return name;
	}

	/**
	 * <div lang="ja">
	 * 索引名を設定します。
	 *
	 * @param name 索引名。{@code null}の場合は未設定状態になる
	 * </div><div lang="en">
	 * Sets the index name.
	 *
	 * @param name index name. Not set when {@code null} is specified.
	 * </div>
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * <div lang="ja">
	 * 索引に対応する単一のカラムのカラム番号を取得します。
	 *
	 * @return カラム番号。未設定の場合は{@code null}
	 *
	 * @throws IllegalStateException 複合索引に関するカラム番号の列または
	 * カラム名の列が設定されていた場合
	 * </div><div lang="en">
	 * Returns the column number of the single column corresponding to the index.
	 *
	 * @return column number. If not set {@code null}
	 *
	 * @throws IllegalStateException If a list of column numbers or a list of
	 * column names is set for the composite index
	 * </div>
	 */
	public Integer getColumn() {
		if (columnList.size() > 1 || columnNameList.size() > 1) {
			throw new IllegalStateException(
					"This method cannot be used for composite index");
		}
		else if (columnList.isEmpty()) {
			return null;
		}
		else {
			return columnList.get(0);
		}
	}

	/**
	 * <div lang="ja">
	 * 索引に対応する、単一のカラムからなるカラム番号列を設定します。
	 *
	 * @param column カラム番号。{@code null}の場合は未設定状態になる
	 * </div><div lang="en">
	 * Sets the list of the column numbers consisting of a single column corresponding to
	 * the index.
	 *
	 * @param column Column number. Not set when {@code null} is specified.
	 * </div>
	 */
	public void setColumn(Integer column) {
		if (column == null) {
			this.columnList = Collections.emptyList();
		}
		else {
			this.columnList = Collections.singletonList(column);
		}
	}

	/**
	 * <div lang="ja">
	 * 索引に対応する単一のカラムのカラム名を取得します。
	 *
	 * @return カラム名。未設定の場合は{@code null}
	 *
	 * @throws IllegalStateException 複合索引に関するカラム番号の列または
	 * カラム名の列が設定されていた場合
	 * </div><div lang="en">
	 * Returns the column name of the single column corresponding to the index.
	 *
	 * @return column name. If not set {@code null}
	 *
	 * @throws IllegalStateException If a list of column numbers or a list of column
	 * names is set for the composite index
	 * </div>
	 */
	public String getColumnName() {
		if (columnList.size() > 1 || columnNameList.size() > 1) {
			throw new IllegalStateException(
					"This method cannot be used for composite index");
		}
		else if (columnNameList.isEmpty()) {
			return null;
		}
		else {
			return columnNameList.get(0);
		}
	}

	/**
	 * <div lang="ja">
	 * 索引に対応する、単一のカラムからなるカラム名列を設定します。
	 *
	 * @param columnName カラム名。{@code null}の場合は未設定状態になる
	 * </div><div lang="en">
	 * Sets the list of the column names consisting of a single column
	 * corresponding to the index.
	 *
	 * @param columnName column name. Not set when {@code null} is specified.
	 * </div>
	 */
	public void setColumnName(String columnName) {
		if (columnName == null) {
			this.columnNameList = Collections.emptyList();
		}
		else {
			this.columnNameList = Collections.singletonList(columnName);
		}
	}

	/**
	 * <div lang="ja">
	 * 索引に対応する任意個数のカラムのカラム番号の列を設定します。
	 *
	 * @param columns カラム番号の列。長さ{@code 0}は許容するが、
	 * {@code null}は指定できない
	 *
	 * @throws NullPointerException {@code columns}に{@code null}が
	 * 指定された場合
	 *
	 * @since 4.3
	 * </div><div lang="en">
	 * Sets the list of the column numbers of any number of columns corresponding
	 * to the index.
	 *
	 * @param columns List of column numbers. Length {@code 0} is allowed, but
	 * {@code null} cannot be specified
	 *
	 * @throws NullPointerException If {@code null} is specified in the argument
	 * {@code columns}
	 *
	 * @since 4.3
	 * </div>
	 */
	public void setColumnList(List<Integer> columns) {
		final List<Integer> dest;
		try {
			dest = Collections.unmodifiableList(
					new ArrayList<Integer>(columns));
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(columns, "columns", e);
		}
		this.columnList = checkListElements(dest, "columns");
	}

	/**
	 * <div lang="ja">
	 * 索引に対応する任意個数のカラムのカラム番号の列を取得します。
	 *
	 * @return 番号の列。未設定の場合は長さ{@code 0}の列
	 *
	 * @since 4.3
	 * </div><div lang="en">
	 * Returns the list of the column numbers of any number of columns corresponding
	 * to the index.
	 *
	 * @return List of column numbers. If not set, a list of length {@code 0}
	 *
	 * @since 4.3
	 * </div>
	 */
	public List<Integer> getColumnList() {
		return columnList;
	}

	/**
	 * <div lang="ja">
	 * 索引に対応する任意個数のカラムのカラム名の列を設定します。
	 *
	 * @param columnNames カラム名の列。長さ{@code 0}は許容するが、
	 * {@code null}は指定できない
	 *
	 * @throws NullPointerException {@code columnNames}に{@code null}が
	 * 指定された場合
	 *
	 * @since 4.3
	 * </div><div lang="en">
	 * Sets the list of the column names of any number of columns
	 * corresponding to the index.
	 *
	 * @param columnNames List of column names. Length {@code 0} is allowed, but
	 * {@code null} cannot be specified
	 *
	 * @throws NullPointerException If {@code null} is specified in the argument
	 * {@code columnNames}
	 *
	 * @since 4.3
	 * </div>
	 */
	public void setColumnNameList(List<String> columnNames) {
		final List<String> dest;
		try {
			dest = Collections.unmodifiableList(
					new ArrayList<String>(columnNames));
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(columnNames, "columnNames", e);
		}
		this.columnNameList = checkListElements(dest, "columnNames");
	}

	/**
	 * <div lang="ja">
	 * 索引に対応する任意個数のカラムのカラム名の列を取得します。
	 *
	 * @return カラム名の列。未設定の場合は長さ{@code 0}の列
	 *
	 * @since 4.3
	 * </div><div lang="en">
	 * Returns the list of the column names of any number of columns corresponding
	 * to the index.
	 *
	 * @return List of column names. If not set, a list of length 0
	 *
	 * @since 4.3
	 * </div>
	 */
	public List<String> getColumnNameList() {
		return columnNameList;
	}

	private static <T> List<T> checkListElements(List<T> list, String listName) {
		for (T elem : list) {
			GSErrorCode.checkNullParameter(
					elem, "element of " + listName, null);
		}
		return list;
	}

	static IndexInfo toImmutable(IndexInfo base) {
		if (base instanceof Immutable) {
			return (Immutable) base;
		}
		return new Immutable(base);
	}

	private static class Immutable extends IndexInfo {

		private Immutable(IndexInfo info) {
			super(info);
		}

		@Override
		public void setType(IndexType type) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void setName(String name) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void setColumn(Integer column) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void setColumnName(String columnName) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void setColumnList(List<Integer> columns) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void setColumnNameList(List<String> columnNames) {
			throw new UnsupportedOperationException();
		}

	}

}
