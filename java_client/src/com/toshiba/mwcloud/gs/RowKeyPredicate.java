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
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.toshiba.mwcloud.gs.common.GSErrorCode;
import com.toshiba.mwcloud.gs.common.RowMapper;

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

	private static final RowMapper.Config KEY_MAPPER_CONFIG =
			new RowMapper.Config(true, true, true, true);

	private static final Map<
			GSType, Map<Integer, RowMapper>> SINGLE_MAPPERS_BY_TYPE =
			makeSingleMapperMap();

	private static final Map<
			Class<?>, Map<Integer, RowMapper>> SINGLE_MAPPERS_BY_CLASS =
			makeSingleMapperMap(SINGLE_MAPPERS_BY_TYPE);

	private static final Map<Class<?>, GSType> SINGLE_TYPE_MAP =
			makeSingleTypeMap(SINGLE_MAPPERS_BY_CLASS);

	private static boolean stringRangeRestricted = false;

	private final RowMapper.ValueDuplicator<K> duplicator;

	private final boolean rangeAcceptable;

	private K start;

	private K finish;

	private Set<K> distinctKeys;

	private RowKeyPredicate(RowMapper.ValueDuplicator<K> duplicator)
			throws GSException {
		checkMapper(duplicator.getMapper());
		this.duplicator = duplicator;
		this.rangeAcceptable = isRangeKeyAcceptable(
				duplicator.getValueClass(), duplicator.getMapper());
	}

	/**
	 * <div lang="ja">
	 * 指定の{@link GSType}をロウキーの型とする合致条件を作成します。
	 *
	 * <p>合致条件の評価対象とするコンテナは、ロウキーを持ち、かつ、
	 * ロウキーの型が指定の{@link GSType}と同一の型でなければなりません。</p>
	 *
	 * <p>また、カラムの精度を合わせる必要があります。</p>
	 *
	 * <p>{@link #create(Class)}とは異なり、アプリケーションのコンパイル時点で
	 * ロウキーの型が確定しない場合の使用に適します。ただし、
	 * 条件内容を設定する際のロウキーの型・精度チェックの基準は同一です。</p>
	 *
	 * <p>設定可能なロウキーの型は、{@link Container}のいずれかの
	 * サブインタフェースにて許容されているもののみです。</p>
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
	public static RowKeyPredicate<Object> create(GSType keyType)
			throws GSException {
		return create(keyType, null);
	}

	/**
	 * <div lang="ja">
	 * 指定の{@link GSType}ならびに日時精度をロウキーの型とする合致条件を
	 * 作成します。
	 *
	 * <p>合致条件の評価対象とするコンテナは、ロウキーを持ち、かつ、
	 * ロウキーの型が指定の{@link GSType}と同一の型でなければなりません。</p>
	 *
	 * <p>また、カラムの精度を合わせる必要があります。</p>
	 *
	 * <p>{@link #create(Class)}とは異なり、アプリケーションのコンパイル時点で
	 * ロウキーの型が確定しない場合の使用に適します。ただし、
	 * 条件内容を設定する際のロウキーの型・精度チェックの基準は同一です。</p>
	 *
	 * <p>設定可能なロウキーの型は、{@link Container}のいずれかの
	 * サブインタフェースにて許容されているもののみです。</p>
	 *
	 * @param keyType 合致条件の評価対象とするロウキーの型
	 * @param timePrecision 合致条件の評価対象とするロウキーの日時精度
	 *
	 * @return 新規に作成された{@link RowKeyPredicate}
	 *
	 * @throws GSException 指定された型がロウキーとして常にサポート外となる場合
	 * @throws NullPointerException {@code keyType}引数に{@code null}が指定された場合
	 *
	 * @see Container
	 * @since 5.3
	 * </div><div lang="en">
	 * @since 5.3
	 * </div>
	 */
	public static RowKeyPredicate<Object> create(
			GSType keyType, TimeUnit timePrecision) throws GSException {
		final RowMapper mapper = findSingleMapper(keyType, timePrecision);
		if (mapper == null) {
			GSErrorCode.checkNullParameter(keyType, "keyType", null);
			throw new GSException(GSErrorCode.UNSUPPORTED_KEY_TYPE,
					"Unsupported key type (type=" + keyType + ")");
		}

		return new RowKeyPredicate<Object>(
				RowMapper.ValueDuplicator.createSingle(Object.class, mapper));
	}

	/**
	 * <div lang="ja">
	 * 指定の{@link Class}に対応する{@link GSType}をロウキーの型とする
	 * 合致条件を作成します。
	 *
	 * <p>合致条件の評価対象とするコンテナは、単一カラムからなるロウキーを
	 * 持ち、かつ、そのロウキーの型は指定の{@link GSType}と同一の型でなければ
	 * なりません。</p>
	 *
	 * <p>また、カラムの精度を合わせる必要があります。</p>
	 *
	 * <p>設定可能なロウキーの型・精度は、{@link Container}のいずれかの
	 * サブインタフェースにて許容されているもののみです。
	 * {@link Class}と{@link GSType}との対応関係については、
	 * {@link Container}の定義を参照してください。</p>
	 *
	 * <p>複合ロウキーなどロウキーを構成するカラムの個数によらずに合致条件を
	 * 作成するには、{@link #create(ContainerInfo)}を使用します。</p>
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
	 * <p>The container to be evaluated by the search condition must have a Row key
	 * with a single column, and the type of the Row key must be same as the specified
	 * {@link GSType}.</p>
	 *
	 * <p>The type of {@link RowKey} that can be set is only that allowed
	 * by either one of the subinterfaces of {@link Container}.
	 * For the correspondence of {@link Class} to {@link GSType},
	 * see the definition of {@link Container}.</p>
	 *
	 * <p>For a composite Row key, use {@link #create(ContainerInfo)} which allows to
	 * create a search condition regardless of the number of columns that configure
	 * a Row key.</p>
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
	public static <K> RowKeyPredicate<K> create(Class<K> keyType)
			throws GSException {
		return create(keyType, null);
	}

	/**
	 * <div lang="ja">
	 * 指定の{@link Class}ならびに日時精度と対応する{@link GSType}を
	 * ロウキーの型とする合致条件を作成します。
	 *
	 * <p>合致条件の評価対象とするコンテナは、単一カラムからなるロウキーを
	 * 持ち、かつ、そのロウキーの型は指定の{@link GSType}と同一の型でなければ
	 * なりません。</p>
	 *
	 * <p>また、カラムの精度を合わせる必要があります。</p>
	 *
	 * <p>設定可能なロウキーの型は、{@link Container}のいずれかの
	 * サブインタフェースにて許容されているもののみです。
	 * {@link Class}と{@link GSType}との対応関係については、
	 * {@link Container}の定義を参照してください。</p>
	 *
	 * <p>複合ロウキーなどロウキーを構成するカラムの個数によらずに合致条件を
	 * 作成するには、{@link #create(ContainerInfo)}を使用します。</p>
	 *
	 * @param keyType 合致条件の判定対象とするロウキーの型に対応する、{@link Class}
	 * @param timePrecision 合致条件の評価対象とするロウキーの日時精度
	 *
	 * @return 新規に作成された{@link RowKeyPredicate}
	 *
	 * @throws GSException 指定された型がロウキーとして常にサポート外となる場合
	 * @throws NullPointerException {@code keyType}引数に{@code null}が指定された場合
	 *
	 * @see Container
	 * @since 5.3
	 * </div><div lang="en">
	 * @since 5.3
	 * </div>
	 */
	public static <K> RowKeyPredicate<K> create(
			Class<K> keyType, TimeUnit timePrecision) throws GSException {
		final RowMapper mapper = findSingleMapper(keyType, timePrecision);
		if (mapper == null) {
			GSErrorCode.checkNullParameter(keyType, "keyType", null);
			throw new GSException(GSErrorCode.UNSUPPORTED_KEY_TYPE,
					"Unsupported key type (type=" + keyType + ")");
		}

		return new RowKeyPredicate<K>(
				RowMapper.ValueDuplicator.createSingle(keyType, mapper));
	}

	/**
	 * <div lang="ja">
	 * 指定の{@link ContainerInfo}のロウキーに関するカラム定義に基づく、
	 * 合致条件を作成します。
	 *
	 * <p>合致条件の評価対象とするコンテナは、ロウキーを持ち、かつ、指定の
	 * {@link ContainerInfo}のロウキーに関するカラム定義と対応づく
	 * 必要があります。ロウキー以外のカラム定義については対応関係の
	 * 判定に用いられません。</p>
	 *
	 * @param info 合致条件の判定対象とするロウキーのカラムレイアウトを含む、
	 * コンテナ情報。その他の内容は無視される
	 *
	 * @return 新規に作成された{@link RowKeyPredicate}
	 *
	 * @throws GSException 指定の情報がロウキーを含まないか、ロウキーとして
	 * 常にサポート外となる場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 *
	 * @since 4.3
	 * </div><div lang="en">
	 * Creates an instance of {@link RowKeyPredicate} based on the Row key column
	 * definition described in the specified {@link ContainerInfo}.
	 *
	 * <p>The container to be evaluated by the search condition must have a Row key
	 * and must correspond to the column definition described in the specified
	 * {@link ContainerInfo}. Column definitions other than that of the Row key are
	 * not used for the evaluation.</p>
	 *
	 * @param info Container information including the column layout of the Row key
	 * for which the search condition is determined. Other contents are ignored
	 *
	 * @return {@link RowKeyPredicate} newly created
	 *
	 * @throws GSException if the specified information does not include the Row key
	 * or is always not supported as a Row key
	 * @throws NullPointerException if {@code null} is specified in the argument.
	 *
	 * @since 4.3
	 * </div>
	 */
	public static RowKeyPredicate<Row.Key> create(
			ContainerInfo info) throws GSException {
		GSErrorCode.checkNullParameter(info, "info", null);
		final RowMapper mapper =
				RowMapper.getInstance(info.getType(), info, KEY_MAPPER_CONFIG);
		return new RowKeyPredicate<Row.Key>(
				RowMapper.ValueDuplicator.createForKey(mapper));
	}

	/**
	 * <div lang="ja">
	 * 範囲条件の開始位置とするロウキーの値を設定します。
	 *
	 * <p>設定された値より小さな値のロウキーは合致しないものとみなされる
	 * ようになります。</p>
	 *
	 * <p>STRING型のロウキーまたはその型を含む複合ロウキーのように、大小関係が
	 * 定義されていないロウキーの場合、条件として設定はできるものの、実際の
	 * 判定に用いることはできません。</p>
	 *
	 * @param startKey 開始位置とするロウキーの値。{@code null}の場合、
	 * 設定が解除される
	 *
	 * @throws GSException 個別条件がすでに設定されていた場合
	 * @throws ClassCastException 指定のロウキーの値の型が{@code null}
	 * ではなく、ロウキーに対応するクラスのインスタンスではない場合
	 * </div><div lang="en">
	 * Sets the value of the {@link RowKey} at the starting position of the range condition.
	 *
	 * <p>A {@link RowKey} with a value smaller than the specified value is deemed as non-conforming.</p>
	 *
	 * <p>A Row key with no magnitude relation defined, including a Row key of the
	 * STRING type or one with a composite Row key containing STRING type, are not
	 * used for the determination even though it can be set as a condition.</p>
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

		checkRangeKey(startKey);
		final K checkedKey = checkKeyType(startKey);

		start = duplicateKey(checkedKey, false);
	}

	/**
	 * <div lang="ja">
	 * 範囲条件の終了位置とするロウキーの値を設定します。
	 *
	 * <p>設定された値より大きな値のロウキーは合致しないものとみなされる
	 * ようになります。</p>
	 *
	 * <p>STRING型のロウキーまたはその型を含む複合ロウキーのように、大小関係が
	 * 定義されていないロウキーの場合、条件として設定はできるものの、実際の
	 * 判定に用いることはできません。</p>
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
	 * <p>A Row key with no magnitude relation defined, including a Row key of the
	 * STRING type or one with a composite Row key containing STRING type, are not
	 * used for the determination even though it can be set as a condition.</p>
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

		checkRangeKey(finishKey);
		final K checkedKey = checkKeyType(finishKey);

		finish = duplicateKey(checkedKey, false);
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

		GSErrorCode.checkNullParameter(key, "key", null);

		final K checkedKey = checkKeyType(key);

		Set<K> distinctKeys = this.distinctKeys;
		if (distinctKeys == null) {
			distinctKeys = new HashSet<K>();
		}

		distinctKeys.add(duplicateKey(checkedKey, true));
		this.distinctKeys = distinctKeys;
	}

	/**
	 * <div lang="ja">
	 * 複合ロウキーについての合致条件である場合を除いて、合致条件の評価対象
	 * とするロウキーの型を取得します。
	 *
	 * <p>複合ロウキーを含む任意のロウキーについてのスキーマを取得するには、
	 * {@link #getKeySchema()}を使用します。</p>
	 *
	 * @return 合致条件の評価対象とするロウキーの型
	 *
	 * @throws IllegalStateException 複合ロウキーについての合致条件である
	 * 場合に呼び出された場合
	 * </div><div lang="en">
	 * Returns the type of Row key used as a search condition, except when
	 * it is a search condition for the composite Row key.
	 *
	 * <p>Uses {@link #getKeySchema()} to get the schema for any Row key including
	 * the composite Row key.</p>
	 *
	 * @return the type of {@link RowKey} used as a search condition.
	 *
	 * @throws IllegalStateException if called for a search condition for a
	 * composite Row key.
	 * </div>
	 */
	public GSType getKeyType() {
		if (getRowMapper().getKeyCategory() != RowMapper.KeyCategory.SINGLE) {
			throw new IllegalStateException(
					"This method cannot be used for composite row key");
		}

		final GSType keyType = SINGLE_TYPE_MAP.get(getKeyClass());
		if (keyType == null) {
			throw new Error();
		}
		return keyType;
	}

	/**
	 * <div lang="ja">
	 * 合致条件の評価対象とするロウキーのスキーマを取得します。
	 *
	 * <p>この合致条件の作成に用いられた情報に、ロウキー以外のカラム情報や
	 * スキーマ以外のコンテナ情報が含まれていたとしても、返却されるスキーマ
	 * 情報には含まれません。</p>
	 *
	 * @return ロウキーのスキーマに関するコンテナ情報のみを持つ
	 * {@link ContainerInfo}
	 *
	 * @since 4.3
	 * </div><div lang="en">
	 * Returns the Row key schema used as a search condition.
	 *
	 * <p>The schema information to be returned does not have column information other
	 * than the Row key and container information other than the schema, even though it
	 * is in the information used to create the search condition. </p>
	 *
	 * @return {@link ContainerInfo} that has only container information related to
	 * the schema of the Row key
	 *
	 * @since 4.3
	 * </div>
	 */
	public ContainerInfo getKeySchema() {
		try {
			return getRowMapper().resolveKeyContainerInfo();
		}
		catch (GSException e) {
			throw new Error(e);
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
		return duplicateKey(start, false);
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
		return duplicateKey(finish, false);
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

		final java.util.Collection<K> baseKeys;
		if (duplicator.isImmutable()) {
			baseKeys = distinctKeys;
		}
		else {
			final List<K> keys = new ArrayList<K>(distinctKeys.size());
			for (K key : distinctKeys) {
				keys.add(duplicateKey(key, false));
			}
			baseKeys = keys;
		}
		return Collections.unmodifiableCollection(baseKeys);
	}

	private Class<K> getBindingClass() {
		return duplicator.getBindingClass();
	}

	private Class<?> getKeyClass() {
		return duplicator.getValueClass();
	}

	private RowMapper getRowMapper() {
		return duplicator.getMapper();
	}

	private K duplicateKey(K src, boolean identical) {
		if (src == null) {
			return null;
		}
		return duplicator.duplicate(src, identical);
	}

	private void checkRangeKey(Object obj) throws GSException {
		if (obj == null) {
			return;
		}

		if (!rangeAcceptable) {
			throw new GSException(GSErrorCode.UNSUPPORTED_OPERATION,
					"String range is not supported");
		}
	}

	private K checkKeyType(K obj) throws GSException {
		if (obj == null) {
			return null;
		}

		final Class<K> bindingClass = getBindingClass();
		if (bindingClass == Object.class) {
			if (obj instanceof Row.Key) {
				final Row.Key generalKey = (Row.Key) obj;
				final Object elemObj = generalKey.getValue(0);
				checkKeyType(elemObj, generalKey);
				return bindingClass.cast(elemObj);
			}
		}
		else if (bindingClass == Row.Key.class) {
			checkKeyType(obj, obj);
			return obj;
		}

		checkKeyType(obj, null);
		return obj;
	}

	private void checkKeyType(Object obj, Object generalObj)
			throws GSException {
		final Class<?> keyClass = getKeyClass();
		try {
			keyClass.cast(obj);
		}
		catch (ClassCastException cause) {
			final ClassCastException e =
					new ClassCastException("Row key class unmatched");
			e.initCause(cause);
			throw e;
		}

		if (generalObj != null) {
			final RowMapper objMapper;
			try {
				objMapper = RowMapper.getInstance(
						(Row) generalObj, KEY_MAPPER_CONFIG);
			}
			catch (GSException e) {
				throw new IllegalArgumentException(e);
			}

			try {
				objMapper.checkKeySchemaMatched(getRowMapper());
			}
			catch (GSException e) {
				throw new GSException(
						"Row key schema unmatched", e);
			}
		}
	}

	private static RowMapper findSingleMapper(
			GSType keyType, TimeUnit timePrecision) throws GSException {
		final Map<Integer, RowMapper> subMap =
				SINGLE_MAPPERS_BY_TYPE.get(keyType);
		if (subMap == null) {
			return null;
		}

		final int precisionLevel = RowMapper.TypeUtils.resolvePrecisionLevel(
				keyType, timePrecision);
		return subMap.get(precisionLevel);
	}

	private static RowMapper findSingleMapper(
			Class<?> keyClass, TimeUnit timePrecision) throws GSException {
		final Map<Integer, RowMapper> subMap =
				SINGLE_MAPPERS_BY_CLASS.get(keyClass);
		if (subMap == null) {
			return null;
		}

		final int precisionLevel = RowMapper.TypeUtils.resolvePrecisionLevel(
				keyClass, timePrecision);
		return subMap.get(precisionLevel);
	}

	private static Map<GSType, Map<Integer, RowMapper>> makeSingleMapperMap() {
		final Map<GSType, Map<Integer, RowMapper>> map =
				new EnumMap<GSType, Map<Integer, RowMapper>>(GSType.class);
		for (GSType type : RowMapper.TypeUtils.getKeyTypeSet()) {
			final Map<Integer, RowMapper> subMap =
					new HashMap<Integer, RowMapper>();
			for (int precisionLevel : RowMapper.TypeUtils.getPrecisionLevelSet(type)) {
				final ColumnInfo.Builder builder = new ColumnInfo.Builder();
				builder.setType(type);
				builder.setTimePrecision(
						RowMapper.TypeUtils.resolveTimePrecision(
								type, precisionLevel));

				final boolean rowKeyAssigned = true;
				final ContainerInfo info = new ContainerInfo(
						null, null,
						Collections.singletonList(builder.toInfo()),
						rowKeyAssigned);

				final RowMapper mapper;
				try {
					mapper = RowMapper.getInstance(
							null, info, KEY_MAPPER_CONFIG);
				}
				catch (GSException e) {
					throw new Error(e);
				}
				subMap.put(precisionLevel, mapper);
			}
			map.put(type, subMap);
		}
		return map;
	}

	private static Map<Class<?>, Map<Integer, RowMapper>> makeSingleMapperMap(
			Map<GSType, Map<Integer, RowMapper>> src) {
		final Map<Class<?>, Map<Integer, RowMapper>> map =
				new HashMap<Class<?>, Map<Integer, RowMapper>>();
		for (GSType type : src.keySet()) {
			final Map<Integer, RowMapper> srcSubMap = src.get(type);
			for (int precisionLevel : srcSubMap.keySet()) {
				final RowMapper mapper = srcSubMap.get(precisionLevel);
				final Class<?> keyClass =
						RowMapper.ValueDuplicator.createSingle(
								Object.class, mapper).getValueClass();
				Map<Integer, RowMapper> subMap = map.get(keyClass);
				if (subMap == null) {
					subMap = new HashMap<Integer, RowMapper>();
					map.put(keyClass, subMap);
				}
				subMap.put(precisionLevel, mapper);
			}
		}
		return map;
	}

	private static Map<Class<?>, GSType> makeSingleTypeMap(
			Map<Class<?>, Map<Integer, RowMapper>> src) {
		final Map<Class<?>, GSType> map = new HashMap<Class<?>, GSType>();
		for (Class<?> keyClass : src.keySet()) {
			final Map<Integer, RowMapper> srcSubMap = src.get(keyClass);
			final RowMapper mapper = srcSubMap.values().iterator().next();
			final GSType type =
					mapper.getContainerInfo().getColumnInfo(0).getType();
			map.put(keyClass, type);
		}
		return map;
	}

	private static void checkMapper(RowMapper mapper)
			throws GSException {
		if (!mapper.hasKey()) {
			throw new GSException(
					GSErrorCode.KEY_NOT_FOUND,
					"Row key does not exist on predicate for row key");
		}
	}

	private static boolean isRangeKeyAcceptable(
			Class<?> keyClass, RowMapper mapper) throws GSException {
		if (!stringRangeRestricted) {
			return true;
		}

		if (keyClass != Row.Key.class) {
			return (keyClass != String.class);
		}

		final ContainerInfo info = mapper.resolveKeyContainerInfo();
		final int columnCount = info.getColumnCount();
		for (int i = 0; i < columnCount; i++) {
			if (info.getColumnInfo(i).getType() == GSType.STRING) {
				return false;
			}
		}

		return true;
	}

}
