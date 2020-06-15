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

import com.toshiba.mwcloud.gs.common.GSErrorCode;

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

	private final Boolean defaultValueNull;

	private final Set<IndexType> indexTypes;

	ColumnInfo(ColumnInfo info) {
		final boolean sameClass;
		try {
			sameClass = (info.getClass() == ColumnInfo.class);
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(info, "info", e);
		}

		if (sameClass) {
			this.name = info.name;
			this.type = info.type;
			this.nullable = info.nullable;
			this.defaultValueNull = info.defaultValueNull;
			this.indexTypes = info.indexTypes;
		}
		else {
			this.name = info.getName();
			this.type = info.getType();
			this.nullable = info.getNullable();
			this.defaultValueNull = info.getDefaultValueNull();
			this.indexTypes = getImmutableIndexTypes(info.getIndexTypes());
		}
	}

	/**
	 * <div lang="ja">
	 * カラム名、型を指定してカラム情報を作成します。
	 *
	 * @param name カラム名。{@code null}を指定すると未設定状態となる
	 * @param type カラムの型。{@code null}を指定すると未設定状態となる
	 *
	 * @since 1.5
	 * </div><div lang="en">
	 * Constructs column information with the specified column name and type.
	 *
	 * @param name Column name. Not set when {@code null} is specified.
	 * @param type Column type. Not set when {@code null} is specified.
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
	 * Constructs column information with the specified column name, type,
	 * and set of index types.
	 *
	 * <p>If a set of not empty index types is specified, the contents are
	 * duplicated.</p>
	 *
	 * @param name Column name. Not set when {@code null} is specified.
	 * @param type Column type. Not set when {@code null} is specified.
	 * @param indexTypes A set of index types. Not set when {@code null}
	 * is specified. No index type is set if empty set is specified.
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
	 * Constructs column information with the column name, type,
	 * NOT NULL constraint state, and set of index types.
	 *
	 * <p>If a set of not empty index types is specified, the contents are
	 * duplicated.</p>
	 *
	 * @param name Column name. Not set when {@code null} is specified.
	 * @param type Column type. Not set when {@code null} is specified.
	 * @param nullable {@code true} if NOT NULL constraint is not set.
	 * {@code false} if it is set. Not set when {@code null} is specified.
	 * @param indexTypes A set of index types. Not set when {@code null}
	 * is specified. No index type is set if empty set is specified.
	 *
	 * @since 3.5
	 * </div>
	 */
	public ColumnInfo(
			String name, GSType type, Boolean nullable,
			Set<IndexType> indexTypes) {
		this(name, type, nullable, null, indexTypes);
	}

	/**
	 * <div lang="ja">
	 * カラム名、型、NOT NULL制約の状態、初期値でのNULL使用有無、索引種別の
	 * 集合を指定してカラム情報を作成します。
	 *
	 * <p>空ではない索引種別の集合が指定された場合、内容が複製されます。</p>
	 *
	 * @param name カラム名。{@code null}を指定すると未設定状態となる
	 * @param type カラムの型。{@code null}を指定すると未設定状態となる
	 * @param nullable NOT NULL制約が設定されていない場合は{@code true}、
	 * 設定されている場合は{@code false}。{@code null}を指定すると
	 * 未設定状態となる
	 * @param defaultValueNull 初期値としてNULLを使用する場合は{@code true}、
	 * 使用しない場合は{@code false}。{@code null}を指定すると
	 * 未設定状態となる
	 * @param indexTypes 索引種別の集合。{@code null}を指定すると
	 * 未設定状態となる。空の場合は索引の設定が一つもないとみなされる
	 *
	 * @since 4.1
	 * </div><div lang="en">
	 * Constructs column information with specifying the column name, type,
	 * whether to use of NULL for the initial value, NOT NULL constraint state,
	 * and set of index types.
	 *
	 * <p>If a set of not empty index types is specified, the contents are
	 * duplicated.</p>
	 *
	 * @param name Column name. Not set when {@code null} is specified.
	 * @param type Column type. Not set when {@code null} is specified.
	 * @param defaultValueNull {@code true} if the initial value is set to
	 * NULL. {@code false} if the initial value is set to non-NULL. Not set
	 * when {@code null} is specified.
	 * @param nullable {@code true} if NOT NULL constraint is not set.
	 * {@code false} if it is set. Not set when {@code null} is specified.
	 * @param indexTypes A set of index types. Not set when {@code null}
	 * is specified. No index type is set if empty set is specified.
	 *
	 * @since 4.1
	 * </div>
	 */
	public ColumnInfo(
			String name, GSType type, Boolean nullable,
			Boolean defaultValueNull, Set<IndexType> indexTypes) {
		this.name = name;
		this.type = type;
		this.nullable = nullable;
		this.defaultValueNull = defaultValueNull;
		this.indexTypes = getImmutableIndexTypes(indexTypes);
	}

	/**
	 * <div lang="ja">
	 * カラム名を取得します。
	 *
	 * @return カラム名。未設定の場合は{@code null}
	 * </div><div lang="en">
	 * Returns a name of column.
	 *
	 * @return Name of column, or {@code null} if unspecified.
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
	 * Returns the type of a column, i.e., the type of each field value
	 * corresponding to a column.
	 *
	 * @return Type of column, or {@code null} if unspecified.
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
	 * Returns the value irrespective to NOT NULL constraint is set in the
	 * column or not.
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
	 * カラムの初期値としてNULLを使用するかどうかを取得します。
	 *
	 * @return NULLを使用する場合は{@code true}、NULL以外の値を使用する場合は
	 * {@code false}、未設定の場合は{@code null}
	 *
	 * @since 4.1
	 * </div><div lang="en">
	 * Returns whether to use of NULL for the initial value.
	 *
	 * @return {@code true} if the initial value is set to NULL. {@code false}
	 * if the initial value is set to non-NULL. {@code null} if not specified.
	 *
	 * @since 4.1
	 * </div>
	 */
	public Boolean getDefaultValueNull() {
		return defaultValueNull;
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

	private static Set<IndexType> getImmutableIndexTypes(Set<IndexType> src) {
		if (src == null) {
			return null;
		}
		else if (src.isEmpty()) {
			return Collections.emptySet();
		}
		else {
			return Collections.unmodifiableSet(EnumSet.copyOf(src));
		}
	}

	static Immutable toImmutable(ColumnInfo base) {
		if (base instanceof Immutable) {
			return (Immutable) base;
		}
		return new Immutable(base);
	}

	private static class Immutable extends ColumnInfo {

		Immutable(ColumnInfo base) {
			super(base);
		}

	}

}
