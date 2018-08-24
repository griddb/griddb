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
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import com.toshiba.mwcloud.gs.common.GSErrorCode;

/**
 * <div lang="ja">
 * ロウキーの合致条件を表します。
 *
 * <p>{@link GridStore#multiGet(java.util.Map)}における取得条件を
 * 構成するために使用できます。</p>
 *
 * <p>条件の種別として、範囲条件と個別条件の2つの種別があります。
 * 両方の種別の条件を共に指定することはできません。
 * 条件の内容を何も指定しない場合、対象とするすべてのロウキーに
 * 合致することを表します。</p>
 *
 * @param <K> 合致条件の評価対象とするロウキーの型
 *
 * @since 1.5
 * </div><div lang="en">
 * Represents the condition that a {@link RowKey} satisfies.
 *
 * <p>This is used as the search condition in {@link GridStore#multiGet(java.util.Map)}</p>
 *
 * <p>There are two types of conditions, range condition and individual condition.
 * The two types of conditions cannot be specified at the same time.
 * If the condition is not specified, it means that the condition is satisfied in all the target row keys.</p>
 *
 * @param <K> type of {@link RowKey}
 *
 * @since 1.5
 * </div>
 */
public class RowKeyPredicate<K> {

	private static boolean stringRangeRestricted = false;

	private final Class<?> keyClass;

	private K start;

	private K finish;

	private Set<K> distinctKeys;

	private RowKeyPredicate(Class<?> keyClass) {
		this.keyClass = keyClass;
	}

	/**
	 * <div lang="ja">
	 * 指定の{@link GSType}をロウキーの型とする合致条件を作成します。
	 *
	 * <p>合致条件の評価対象とするコンテナは、ロウキーを持ち、かつ、
	 * ロウキーの型が指定の{@link GSType}と同一の型でなければなりません。</p>
	 *
	 * <p>{@link #create(Class)}とは異なり、アプリケーションのコンパイル時点で
	 * ロウキーの型が確定しない場合の使用に適します。ただし、
	 * 条件内容を設定する際のロウキーの型チェックの基準は同一です。</p>
	 *
	 * <p>設定可能なロウキーの型は、{@link Container}のいずれかの
	 * サブインタフェースにて許容されている型のみです。</p>
	 *
	 * @param keyType 合致条件の評価対象とするロウキーの型
	 *
	 * @return 新規に作成された{@link RowKeyPredicate}
	 *
	 * @throws GSException 指定された型がロウキーとして常にサポート外となる場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 *
	 * @see Container
	 * </div><div lang="en">
	 * Creates an instance of {@link RowKeyPredicate} with the specified {@link GSType} as the {@link RowKey} type.
	 *
	 * <p>The target Container must have a {@link RowKey}, and the type of the {@link RowKey} must be
	 * the specified {@link GSType}</p>
	 *
	 * <p>Unlike {@link #create(Class)}, this method is used when the type of {@link RowKey} is not specified
	 * when the application is compiled. However, the criteria for checking the RowKey type when setting
	 * the condition is the same as {@link #create(Class)}.</p>
	 *
	 * <p>The type of {@link RowKey} that can be set is only that allowed
	 * by either one of the subinterfaces of {@link Container}.</p>
	 *
	 * @param keyType type of {@link RowKey} used as a search condition
	 *
	 * @return {@link RowKeyPredicate} newly created
	 *
	 * @throws GSException if the specified type is not always supported as a {@link RowKey}
	 * @throws NullPointerException if {@code null} is specified in the argument.
	 *
	 * @see Container
	 * </div>
	 */
	public static RowKeyPredicate<Object> create(
			GSType keyType) throws GSException {
		try {
			switch (keyType) {
			case STRING:
				return new RowKeyPredicate<Object>(String.class);
			case INTEGER:
				return new RowKeyPredicate<Object>(Integer.class);
			case LONG:
				return new RowKeyPredicate<Object>(Long.class);
			case TIMESTAMP:
				return new RowKeyPredicate<Object>(Date.class);
			default:
				throw new GSException(GSErrorCode.UNSUPPORTED_KEY_TYPE,
						"Unsupported key type (type=" + keyType + ")");
			}
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(keyType, "keyType", e);
		}
	}

	/**
	 * <div lang="ja">
	 * 指定の{@link Class}に対応する{@link GSType}をロウキーの型とする
	 * 合致条件を作成します。
	 *
	 * <p>判定対象とするコンテナは、ロウキーを持ち、かつ、ロウキーの型が対応する
	 * {@link GSType}と同一の型でなければなりません。</p>
	 *
	 * <p>設定可能なロウキーの型は、{@link Container}のいずれかの
	 * サブインタフェースにて許容されている型のみです。
	 * {@link Class}と{@link GSType}との対応関係については、
	 * {@link Container}の定義を参照してください。</p>
	 *
	 * @param keyType 合致条件の判定対象とするロウキーの型に対応する、{@link Class}
	 *
	 * @return 新規に作成された{@link RowKeyPredicate}
	 *
	 * @throws GSException 指定された型がロウキーとして常にサポート外となる場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 *
	 * @see Container
	 * </div><div lang="en">
	 * Creates an instance of {@link RowKeyPredicate} with the {@link GSType} corresponding to
	 * the specified {@link Class} as the {@link RowKey} type.
	 *
	 * <p>The target Container must have a {@link RowKey}, and the type of the {@link RowKey} must be
	 * the specified {@link GSType}</p>
	 *
	 * <p>The type of {@link RowKey} that can be set is only that allowed
	 * by either one of the subinterfaces of {@link Container}.
	 * For the correspondence of {@link Class} to {@link GSType},
	 * see the definition of {@link Container}.</p>
	 *
	 * @param keyType {@link Class} corresponding to a {@link RowKey} used as a search condition
	 *
	 * @return {@link RowKeyPredicate} newly created
	 *
	 * @throws GSException if the specified type is not always supported as a {@link RowKey}
	 * @throws NullPointerException if {@code null} is specified in the argument.
	 *
	 * @see Container
	 * </div>
	 */
	public static <K> RowKeyPredicate<K> create(
			Class<K> keyType) throws GSException {
		if (keyType != String.class &&
				keyType != Integer.class &&
				keyType != Long.class &&
				keyType != Date.class) {

			if (keyType == null) {
				throw GSErrorCode.checkNullParameter(
						keyType, "keyType", null);
			}

			throw new GSException(GSErrorCode.UNSUPPORTED_KEY_TYPE,
					"Unsupported key type (type=" + keyType + ")");
		}

		return new RowKeyPredicate<K>(keyType);
	}

	/**
	 * <div lang="ja">
	 * 範囲条件の開始位置とするロウキーの値を設定します。
	 *
	 * <p>設定された値より小さな値のロウキーは合致しないものとみなされる
	 * ようになります。</p>
	 *
	 * <p>STRING型のように大小関係の定義されていない型の場合、
	 * 条件として設定はできるものの、実際の判定に用いることはできません。</p>
	 *
	 * @param startKey 開始位置とするロウキーの値。{@code null}の場合、
	 * 設定が解除される
	 *
	 * @throws GSException 個別条件がすでに設定されていた場合
	 * @throws ClassCastException 指定のロウキーの値の型が{@code null}
	 * ではなく、ロウキーに対応するクラスのインスタンスではない場合
	 * </div><div lang="en">
	 * Sets the value of the {@link RowKey} at the starting positionof the range condition.
	 *
	 * <p>A {@link RowKey} with a value smaller than the specified value is deemed as non-conforming.</p>
	 *
	 * <p>A type with an undefined magnitude relationship can be set as a condition
	 * but cannot be used in the actual judgment, e.g. STRING type</p>
	 *
	 * @param startKey value of {@link RowKey} at the starting position or {@code null}.
	 * For {@code null}, the setting is cancelled.
	 *
	 * @throws GSException if an individual condition had been set already
	 * @throws ClassCastException the specified RowKey is not NULL or the type is not supported as {@link RowKey}
	 * </div>
	 */
	public void setStart(K startKey) throws GSException {
		if (distinctKeys != null) {
			throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
					"Distinct key has already been specified");
		}

		if (keyClass == String.class && startKey != null &&
				stringRangeRestricted) {
			throw new GSException(GSErrorCode.UNSUPPORTED_OPERATION,
					"String range is not supported");
		}

		keyClass.cast(startKey);
		start = startKey;
	}

	/**
	 * <div lang="ja">
	 * 範囲条件の終了位置とするロウキーの値を設定します。
	 *
	 * <p>設定された値より大きな値のロウキーは合致しないものとみなされる
	 * ようになります。</p>
	 *
	 * <p>STRING型のように大小関係の定義されていない型の場合、
	 * 条件として設定はできるものの、実際の判定に用いることはできません。</p>
	 *
	 * @param finishKey 終了位置とするロウキーの値。{@code null}の場合、
	 * 設定が解除される
	 *
	 * @throws GSException 個別条件がすでに設定されていた場合
	 * @throws ClassCastException 指定のロウキーの値の型が{@code null}
	 * ではなく、ロウキーに対応するクラスのインスタンスではない場合
	 * </div><div lang="en">
	 * Sets the value of the {@link RowKey} at the last position of the range condition.
	 *
	 * <p>A {@link RowKey} with a value larger than the specified value is deemed as non-conforming.</p>
	 *
	 * <p>A type with an undefined magnitude relationship can be set as a condition
	 * but cannot be used in the actual judgment e.g. STRING type</p>
	 *
	 * @param finishKey the value of {@link RowKey} at the last position or {@code null}.
	 * For {@code null}, the setting is cancelled.
	 *
	 * @throws GSException if an individual condition had been set already
	 * @throws ClassCastException the value of specified key is not NULL
	 * or the type is not supported as {@link RowKey}
	 * </div>
	 */
	public void setFinish(K finishKey) throws GSException {
		if (distinctKeys != null) {
			throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
					"Distinct key has already been specified");
		}

		if (keyClass == String.class && finishKey != null &&
				stringRangeRestricted) {
			throw new GSException(GSErrorCode.UNSUPPORTED_OPERATION,
					"String range is not supported");
		}

		keyClass.cast(finishKey);
		finish = finishKey;
	}

	/**
	 * <div lang="ja">
	 * 個別条件の要素の一つとするロウキーの値を追加します。
	 *
	 * <p>追加された値と同一の値のロウキーは合致するものとみなされる
	 * ようになります。</p>
	 *
	 * @param key 個別条件の要素の一つとするロウキーの値。{@code null}は
	 * 指定できない
	 *
	 * @throws GSException 範囲条件がすでに設定されていた場合
	 * @throws ClassCastException 指定のロウキーの値の型が{@code null}
	 * ではなく、ロウキーに対応するクラスのインスタンスではない場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Appends the value of the {@link RowKey} as one of the elements of the individual condition.
	 *
	 * <p>A {@link RowKey} with the same value as the added value is deemed as conforming.</p>
	 *
	 * @param key value of {@link RowKey} to be appended as one of the elements
	 * of the individual condition. Must not be a {@code null} value.
	 *
	 * @throws GSException if a range condition had already been set
	 * @throws ClassCastException the value of the specified key is not NULL or the type is not supported as {@link RowKey}
	 * @throws NullPointerException when {@code null} is specified as an argument
	 * </div>
	 */
	public void add(K key) throws GSException {
		if (start != null || finish != null) {
			throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
					"Start or finish key has already been specified");
		}

		keyClass.cast(key);

		if (key == null) {
			throw GSErrorCode.checkNullParameter(key, "key", null);
		}

		if (distinctKeys == null) {
			distinctKeys = new HashSet<K>();
		}

		distinctKeys.add(key);
	}

	/**
	 * <div lang="ja">
	 * 合致条件の評価対象とするロウキーの型を取得します。
	 *
	 * @return 合致条件の評価対象とするロウキーの型
	 * </div><div lang="en">
	 * Returns the type of {@link RowKey} used as a search condition.
	 *
	 * @return the type of {@link RowKey} used as a search condition.
	 * </div>
	 */
	public GSType getKeyType() {
		if (keyClass == String.class) {
			return GSType.STRING;
		}
		else if (keyClass == Integer.class) {
			return GSType.INTEGER;
		}
		else if (keyClass == Long.class) {
			return GSType.LONG;
		}
		else if (keyClass == Date.class) {
			return GSType.TIMESTAMP;
		}
		else {
			throw new Error("Internal error by unknown key class (class=" +
					keyClass + ")");
		}
	}

	/**
	 * <div lang="ja">
	 * 範囲条件の開始位置とするロウキーの値を取得します。
	 *
	 * @return 開始位置とするロウキーの値。設定されていない場合は{@code null}
	 * </div><div lang="en">
	 * Returns the value of the {@link RowKey} at the starting position of the range condition.
	 *
	 * @return the value of {@link RowKey} at the starting position
	 * of the range condition, or {@code null} if it is not set.
	 * </div>
	 */
	public K getStart() {
		return start;
	}

	/**
	 * <div lang="ja">
	 * 範囲条件の終了位置とするロウキーの値を取得します。
	 *
	 * @return 終了位置とするロウキーの値。設定されていない場合は{@code null}
	 * </div><div lang="en">
	 * Returns the value of {@link RowKey} at the last position
	 * of the range condition.
	 *
	 * @return the value of {@link RowKey} at the last position
	 * of the range condition, or {@code null} if it is not set.
	 * </div>
	 */
	public K getFinish() {
		return finish;
	}

	/**
	 * <div lang="ja">
	 * 個別条件を構成するロウキーの値の集合を取得します。
	 *
	 * <p>返却された値に対して変更操作を行った場合に、
	 * {@link UnsupportedOperationException}などの実行時例外が発生するか
	 * どうかは未定義です。
	 * また、このオブジェクトに対する操作により、返却されたオブジェクトの内容が
	 * 変化するかどうかは未定義です。</p>
	 *
	 * @return 個別条件を構成するロウキーの値を要素とする
	 * {@link java.util.Collection}
	 * </div><div lang="en">
	 * Returns a Collection containing all of the values of the row keys
	 * that make up the individual condition.
	 *
	 * <p>It is not defined whether an exception like {@link UnsupportedOperationException}
	 * will occur during execution, when a returned object is updated.
	 * Moreover, after an object is returned, it is not defined
	 * whether an update of this object will change the contents of the returned object.</p>
	 *
	 * @return {@link java.util.Collection} containing all of the values of the row keys
	 * that make up the individual condition.
	 * </div>
	 */
	public java.util.Collection<K> getDistinctKeys() {
		if (distinctKeys == null) {
			return null;
		}

		return Collections.unmodifiableCollection(distinctKeys);
	}

}
