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

	private Integer column;

	private String columnName;

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
		try {
			type = info.type;
			name = info.name;
			column = info.column;
			columnName = info.columnName;
		}
		catch (NullPointerException e) {
			GSErrorCode.checkNullParameter(info, "info", e);
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
	 * 必要に応じ索引名と索引種別を指定して索引情報を作成します。
	 *
	 * @param name 索引名。指定しない場合は{@code null}
	 * @param type 索引種別。指定しない場合は{@code null}
	 * </div><div lang="en">
	 * Create index information by specifying the index name and index type as necessary.
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
	 * Acquire index type.
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
	 * Set index type.
	 *
	 * @param type Index type. When {@code null} is used, it becomes a not set state.
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
	 * get index name
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
	 * set index nam.
	 *
	 * @param name index name. When {@code null} is used, it becomes a not set state.
	 * </div>
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * <div lang="ja">
	 * 索引に対応するカラムのカラム番号を取得します。
	 *
	 * @return カラム番号。未設定の場合は{@code null}
	 * </div><div lang="en">
	 * Get the column number of the column corresponding to the index.
	 *
	 * @return column number. If not set {@code null}
	 * </div>
	 */
	public Integer getColumn() {
		return column;
	}

	/**
	 * <div lang="ja">
	 * 索引に対応するカラムのカラム番号を設定します。
	 *
	 * @param column 索引名。{@code null}の場合は未設定状態になる
	 * </div><div lang="en">
	 * Set the column number of the column corresponding to the index.
	 *
	 * @param column index name. When {@code null} is used, it becomes a not set state.
	 * </div>
	 */
	public void setColumn(Integer column) {
		this.column = column;
	}

	/**
	 * <div lang="ja">
	 * 索引に対応するカラムのカラム名を取得します。
	 *
	 * @return カラム名。未設定の場合は{@code null}
	 * </div><div lang="en">
	 * Get the column name of the column corresponding to the index.
	 *
	 * @return column name. If not set {@code null}
	 * </div>
	 */
	public String getColumnName() {
		return columnName;
	}

	/**
	 * <div lang="ja">
	 * 索引に対応するカラムのカラム名を設定します。
	 *
	 * @param columnName カラム名。{@code null}の場合は未設定状態になる
	 * </div><div lang="en">
	 * Set the column name of the column corresponding to the index.
	 *
	 * @param columnName column name. When {@code null} is used, it becomes a not set state.
	 * </div>
	 */
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	IndexInfo toUnmodifiable() {
		return new Unmodifiable(this);
	}

	private static class Unmodifiable extends IndexInfo {

		Unmodifiable(IndexInfo info) {
			super.setType(info.getType());
			super.setName(info.getName());
			super.setColumn(info.getColumn());
			super.setColumnName(info.getColumnName());
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
		IndexInfo toUnmodifiable() {
			return this;
		}

	}

}
