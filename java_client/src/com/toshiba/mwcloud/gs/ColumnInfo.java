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

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * <div lang="ja">
 * カラムのスキーマに関する情報を表します。
 *
 * <p>カラム名の表記、もしくは、カラムの型と索引種別の対応関係などの内容の
 * 妥当性について、必ずしも検査するとは限りません。</p>
 * </div><div lang="en">
 * Represents the information about the schema of a Column.
 *
 * <p>It does not guarantee the validity of values e.g. the Column name and
 * the type of index for the type of a Column.</p>
 * </div>
 */
public class ColumnInfo {

	private final String name;

	private final GSType type;

	private final Boolean nullable;

	private final Set<IndexType> indexTypes;

	/**
	 * <div lang="ja">
	 * カラム名、型を指定してカラム情報を作成します。
	 *
	 * @param name カラム名。{@code null}を指定すると未設定状態となる
	 * @param type カラムの型。{@code null}も指定すると未設定状態となる
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Creates the information with the specified Column name and type of Column.
	 *
	 * @param name Column name. {@code null} indicates no specification of Column name.
	 * @param type Column type. {@code null} indicates no specification of Column type.
	 * </div>
	 */
	public ColumnInfo(String name, GSType type) {
		this(name, type, null);
	}

	/**
	 * <div lang="ja">
	 * カラム名、型、索引種別の集合を指定してカラム情報を作成します。
	 *
	 * <p>空ではない索引種別の集合が指定された場合、内容が複製されます。</p>
	 *
	 * @param name カラム名。{@code null}を指定すると未設定状態となる
	 * @param type カラムの型。{@code null}を指定すると未設定状態となる
	 * @param indexTypes 索引種別の集合。{@code null}を指定すると
	 * 未設定状態となる。空の場合は索引の設定が一つもないとみなされる
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Creates the information with the specified Column name and type of Column
	 * and set of Index type.
	 *
	 * <p>Creates a copy of a specified non-empty set of Index type copied.</p>
	 *
	 * @param name Column name. {@code null} indicates no specification of Column name.
	 * @param type Column type. {@code null} indicates no specification of Column type.
	 * @param indexTypes Set of index type. {@code null} indicates no specification.
	 * No index type is set if it is an empty set.
	 * </div>
	 */
	public ColumnInfo(String name, GSType type, Set<IndexType> indexTypes) {
		this(name, type, null, indexTypes);
	}

	/**
	 * <div lang="ja">
	 * カラム名、型、NOT NULL制約の状態、索引種別の集合を指定してカラム情報を
	 * 作成します。
	 *
	 * <p>空ではない索引種別の集合が指定された場合、内容が複製されます。</p>
	 *
	 * @param name カラム名。{@code null}を指定すると未設定状態となる
	 * @param type カラムの型。{@code null}を指定すると未設定状態となる
	 * @param nullable NOT NULL制約が設定されていない場合は{@code true}、
	 * 設定されている場合は{@code false}。{@code null}を指定すると
	 * 未設定状態となる
	 * @param indexTypes 索引種別の集合。{@code null}を指定すると
	 * 未設定状態となる。空の場合は索引の設定が一つもないとみなされる
	 *
	 * @since 3.5
	 * </div><div lang="en">
	 * Create column information by specifying the column name, type,
	 * NOT NULL constraint state, and index type set.
	 *
	 * <p>If a set of not empty index types is specified, the contents are duplicated. </p>
	 *
	 * @param name column name. Not set when {@code null} is specified.
	 * @param type column type. Not set when {@code null} is specified.
	 * @param nullable {@code true} if NOT NULL constraint is not set, {@code false} is set.
	 * Not set when {@code null} is specified.
	 * @param indexTypes A set of index types. Not set when {@code null} is specified.
	 * When empty, the index is considered as not set.
	 *
	 * @since 3.5
	 * </div>
	 */
	public ColumnInfo(
			String name, GSType type, Boolean nullable,
			Set<IndexType> indexTypes) {
		this.name = name;
		this.type = type;
		this.nullable = nullable;

		if (indexTypes == null) {
			this.indexTypes = null;
		}
		else if (indexTypes.isEmpty()) {
			this.indexTypes = Collections.emptySet();
		}
		else {
			this.indexTypes =
					Collections.unmodifiableSet(EnumSet.copyOf(indexTypes));
		}
	}

	/**
	 * <div lang="ja">
	 * カラム名を取得します。
	 *
	 * @return カラム名。未設定の場合は{@code null}
	 * </div><div lang="en">
	 * Returns a name of Column.
	 *
	 * @return Name of Column, or {@code null} if unspecified.
	 * </div>
	 */
	public String getName() {
		return name;
	}

	/**
	 * <div lang="ja">
	 * カラムの型、すなわち、カラムに対応する各フィールド値の型を取得します。
	 *
	 * @return カラムの型。未設定の場合は{@code null}
	 * </div><div lang="en">
	 * Returns the type of a Column, i.e., the type of each field value
	 * corresponding to a Column.
	 *
	 * @return Type of Column, or {@code null} if unspecified.
	 * </div>
	 */
	public GSType getType() {
		return type;
	}

	/**
	 * <div lang="ja">
	 * カラムにNOT NULL制約が設定されていないかどうかを取得します。
	 *
	 * @return NOT NULL制約が設定されていない場合は{@code true}、
	 * 設定されている場合は{@code false}、未設定の場合は{@code null}
	 *
	 * @since 3.5
	 * </div><div lang="en">
	 * Retrieve the value irrespective to NOT NULL constraint is set in the column or not.
	 *
	 * @return {@code true} if the NOT NULL constraint is not set,
	 * {@code false} if set. If not set {@code null}.
	 *
	 * @since 3.5
	 * </div>
	 */
	public Boolean getNullable() {
		return nullable;
	}

	/**
	 * <div lang="ja">
	 * このカラムのすべての索引種別を取得します。
	 *
	 * <p>返却された値に対して変更操作を行った場合、
	 * {@link UnsupportedOperationException}が発生することがあります。
	 * また、このオブジェクトに対する操作により、返却されたオブジェクトの内容が
	 * 変化することはありません。</p>
	 *
	 * @return 索引種別の集合。未設定の場合は{@code null}
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Returns all of set of index type.
	 *
	 * <p>{@link UnsupportedOperationException} can occur when value of the
	 * returned object is updated. And value of the returned object is not changed by
	 * updating this object.</p>
	 *
	 * @return Set of index type, or {@code null} if unspecified.
	 * </div>
	 */
	public Set<IndexType> getIndexTypes() {
		return indexTypes;
	}

}
