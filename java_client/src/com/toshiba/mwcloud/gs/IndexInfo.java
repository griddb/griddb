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
 * shows the index setting
 *
 * <p>The notation of column names and the validity of column numbers
 * are not inspected. </p>
 *
 */
public class IndexInfo {

	private IndexType type;

	private String name;

	private Integer column;

	private String columnName;

	/**
	 * Create empty index information.
	 *
	 * <p> Creates index information with no index type, index name,
	 * column number, column name. </p>
	 */
	public IndexInfo() {
	}

	/**
	 * Inherits the specified content and creates new index information.
	 *
	 * @param info Index information
	 *
	 * @throws NullPointerException when {@code null} is specified as argument
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
	 * If necessary, create index information by specifying the column name and index type.
	 *
	 * @param columnName Column name. If not specified {@code null}
	 * @param type Index type. If not specified {@code null}
	 */
	public static IndexInfo createByColumn(String columnName, IndexType type) {
		final IndexInfo info = new IndexInfo();
		info.setColumnName(columnName);
		info.setType(type);
		return info;
	}

	/**
	 * Create index information by specifying the index name and index type as necessary.
	 *
	 * @param name Index name. If not specified {@code null}
	 * @param type Index type. If not specified {@code null}

	 */
	public static IndexInfo createByName(String name, IndexType type) {
		final IndexInfo info = new IndexInfo();
		info.setName(name);
		info.setType(type);
		return info;
	}

	/**
	 * Acquire index type.
	 *
	 * @return Index type. If not set {@code null}
	 */
	public IndexType getType() {
		return type;
	}

	/**
	 * Set index type.
	 *
	 * @param type Index type. When {@code null} is used, it becomes a not set state.
	 */
	public void setType(IndexType type) {
		this.type = type;
	}

	/**
	 * get index name
	 *
	 * @return index name. If not set {@code null}
	 */
	public String getName() {
		return name;
	}

	/**
	 * set index nam.
	 *
	 * @param name index name. When {@code null} is used, it becomes a not set state.
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Get the column number of the column corresponding to the index.
	 *
	 * @return column number. If not set {@code null}
	 */
	public Integer getColumn() {
		return column;
	}

	/**
	 * Set the column number of the column corresponding to the index.
	 *
	 * @param column index name. When {@code null} is used, it becomes a not set state.
	 */
	public void setColumn(Integer column) {
		this.column = column;
	}

	/**
	 * Get the column name of the column corresponding to the index.
	 *
	 * @return column name. If not set {@code null}
	 */
	public String getColumnName() {
		return columnName;
	}

	/**
	 * Set the column name of the column corresponding to the index.
	 *
	 * @param columnName column name. When {@code null} is used, it becomes a not set state.
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
