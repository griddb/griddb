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

	private final TimeUnit timePrecision;

	ColumnInfo(Builder builder) {
		this(
				builder.name,
				builder.type,
				builder.nullable,
				builder.defaultValueNull,
				builder.indexTypes,
				builder.timePrecision);
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
		this(name, type, nullable, defaultValueNull, indexTypes, null);
	}

	private ColumnInfo(
			String name, GSType type, Boolean nullable,
			Boolean defaultValueNull, Set<IndexType> indexTypes,
			TimeUnit timePrecision) {
		this.name = name;
		this.type = type;
		this.nullable = nullable;
		this.defaultValueNull = defaultValueNull;
		this.indexTypes = getImmutableIndexTypes(indexTypes);
		this.timePrecision = timePrecision;
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

	/**
	 * <div lang="ja">
	 * このカラムの日時精度を取得します。
	 *
	 * <p>コンテナを定義する際に、現バージョンにて使用できる組み合わせは、以下
	 * のみです。</p>
	 * <table>
	 * <thead><tr><th>カラム型</th><th>日時精度</th></tr></thead>
	 * <tbody>
	 * <tr><td>TIMESTAMP型</td><td>
	 * <ul>
	 * <li>{@link TimeUnit#MILLISECOND}</li>
	 * <li>{@link TimeUnit#MICROSECOND}</li>
	 * <li>{@link TimeUnit#NANOSECOND}</li>
	 * <li>{@code null} ({@link TimeUnit#MILLISECOND}が指定されたものとして
	 * 解釈)</li>
	 * </ul>
	 * </td></tr>
	 * <tr><td>TIMESTAMP型以外</td><td>{@code null}</td></tr>
	 * </tbody>
	 * </table>
	 *
	 * <p>日時精度を指定した{@link ColumnInfo}を構築するには、{@link Builder}を
	 * 使用します。</p>
	 *
	 * @return 日時精度。未設定の場合は{@code null}
	 *
	 * @since 5.3
	 * </div><div lang="en">
	 * @since 5.3
	 * </div>
	 */
	public TimeUnit getTimePrecision() {
		return timePrecision;
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
		return new Immutable(new Builder(base));
	}

	private static class Immutable extends ColumnInfo {

		Immutable(Builder base) {
			super(base);
		}

	}

	/**
	 * <div lang="ja">
	 * {@link ColumnInfo}を構築するためのビルダーです。
	 *
	 * <p>{@link ColumnInfo}を通じて参照できる各項目のうち、必要とする項目
	 * のみを設定し、{@link ColumnInfo}を構築することができます。</p>
	 *
	 * @since 5.3
	 * </div><div lang="en">
	 * @since 5.3
	 * </div>
	 */
	public static class Builder {

		private ColumnInfo info;

		private String name;

		private GSType type;

		private Boolean nullable;

		private Boolean defaultValueNull;

		private Set<IndexType> indexTypes;

		private TimeUnit timePrecision;

		/**
		 * <div lang="ja">
		 * すべての項目が未設定状態のビルダーを構築します。
		 * </div><div lang="en">
		 * </div>
		 */
		public Builder() {
		}

		/**
		 * <div lang="ja">
		 * 指定の{@link ColumnInfo}に含まれる、すべての項目が設定された
		 * ビルダーを構築します。
		 *
		 * @param info ビルダーに設定する内容を含むカラム情報
		 * @throws NullPointerException 引数に{@code null}が指定された場合
		 * </div><div lang="en">
		 * </div>
		 */
		public Builder(ColumnInfo info) {
			set(info);
		}

		/**
		 * <div lang="ja">
		 * 指定の{@link ColumnInfo}に含まれる、すべての項目を設定します。
		 *
		 * <p>指定の{@link ColumnInfo}に未設定状態の項目が含まれる場合、この
		 * ビルダーでの対応する項目についても、元の設定内容に関係なく未設定
		 * 状態となります。</p>
		 *
		 * @param info ビルダーに設定する内容を含むカラム情報
		 * @throws NullPointerException 引数に{@code null}が指定された場合
		 * @return このビルダー
		 * </div><div lang="en">
		 * </div>
		 */
		public Builder set(ColumnInfo info) {
			GSErrorCode.checkNullParameter(info, "info", null);

			setName(info.getName());
			setType(info.getType());
			setNullable(info.getNullable());
			setDefaultValueNull(info.getDefaultValueNull());
			setIndexTypes(info.getIndexTypes());
			setTimePrecision(info.getTimePrecision());

			return this;
		}

		/**
		 * <div lang="ja">
		 * カラム名を設定します。
		 *
		 * <p>指定の引数が{@code null}の場合、未設定状態となります。</p>
		 *
		 * @param name カラム名
		 * @return このビルダー
		 * @see ColumnInfo#getName()
		 * </div><div lang="en">
		 * </div>
		 */
		public Builder setName(String name) {
			clearInfo();
			this.name = name;
			return this;
		}

		/**
		 * <div lang="ja">
		 * カラム型を設定します。
		 *
		 * <p>指定の引数が{@code null}の場合、未設定状態となります。</p>
		 *
		 * @param type カラム型
		 * @return このビルダー
		 * @see ColumnInfo#getType()
		 * </div><div lang="en">
		 * </div>
		 */
		public Builder setType(GSType type) {
			clearInfo();
			this.type = type;
			return this;
		}

		/**
		 * <div lang="ja">
		 * カラムにNOT NULL制約が設定されていないかどうかを設定します。
		 *
		 * <p>指定の引数が{@code null}の場合、未設定状態となります。</p>
		 *
		 * @param nullable カラムにNOT NULL制約が設定されていないかどうか
		 * @return このビルダー
		 * @see ColumnInfo#getNullable()
		 * </div><div lang="en">
		 * </div>
		 */
		public Builder setNullable(Boolean nullable) {
			clearInfo();
			this.nullable = nullable;
			return this;
		}

		/**
		 * <div lang="ja">
		 * カラムの初期値としてNULLを使用するかどうかを設定します。
		 *
		 * <p>指定の引数が{@code null}の場合、未設定状態となります。</p>
		 *
		 * @param defaultValueNull カラムの初期値としてNULLを使用するか
		 * @return このビルダー
		 * @see ColumnInfo#getDefaultValueNull()
		 * </div><div lang="en">
		 * </div>
		 */
		public Builder setDefaultValueNull(Boolean defaultValueNull) {
			clearInfo();
			this.defaultValueNull = defaultValueNull;
			return this;
		}

		/**
		 * <div lang="ja">
		 * 索引種別の集合を設定します。
		 *
		 * <p>指定の引数が{@code null}の場合、未設定状態となります。</p>
		 *
		 * @param indexTypes 索引種別の集合
		 * @return このビルダー
		 * @see ColumnInfo#getIndexTypes()
		 * </div><div lang="en">
		 * </div>
		 */
		public Builder setIndexTypes(Set<IndexType> indexTypes) {
			clearInfo();
			this.indexTypes = getImmutableIndexTypes(indexTypes);
			return this;
		}

		/**
		 * <div lang="ja">
		 * 日時精度を設定します。
		 *
		 * <p>指定の引数が{@code null}の場合、未設定状態となります。</p>
		 *
		 * @param timePrecision 日時精度
		 * @return このビルダー
		 * @see ColumnInfo#getTimePrecision()
		 * </div><div lang="en">
		 * </div>
		 */
		public Builder setTimePrecision(TimeUnit timePrecision) {
			clearInfo();
			this.timePrecision = timePrecision;
			return this;
		}

		/**
		 * <div lang="ja">
		 * このビルダーに設定された内容に基づき、{@link ColumnInfo}を構築します。
		 *
		 * <p>構築後にこのビルダーの設定が変更されたとしても、返却されるカラム
		 * 情報が変化することはありません。</p>
		 *
		 * @return カラム情報
		 * </div><div lang="en">
		 * </div>
		 */
		public ColumnInfo toInfo() {
			if (info == null) {
				info = new Immutable(this);
			}
			return info;
		}

		private void clearInfo() {
			info = null;
		}

	}

}
