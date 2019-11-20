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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.toshiba.mwcloud.gs.common.GSErrorCode;
import com.toshiba.mwcloud.gs.common.RowMapper;

/**
 * <div lang="ja">
 * 時系列を新規作成または変更する際に使用される、オプションの構成情報を
 * 表します。
 *
 * <p>カラム名の表記、もしくは、個別に圧縮設定できるカラム数の上限などの
 * 内容の妥当性について、必ずしも検査するとは限りません。</p>
 * </div><div lang="en">
 * Represents the information about optional configuration settings used for newly creating or updating a TimeSeries.
 *
 * <p>It does not guarantee the validity of values e.g. the column
 * names and the upper limit of the column number that can be
 * individually compressed.</p>
 * </div>
 */
public class TimeSeriesProperties implements Cloneable {

	private int rowExpirationTime = -1;

	private TimeUnit rowExpirationTimeUnit;

	private int compressionWindowSize = -1;

	private TimeUnit compressionWindowSizeUnit;

	private CompressionMethod compressionMethod = CompressionMethod.NO;

	private Map<String, Entry> entryMap = new HashMap<String, Entry>();

	private int expirationDivisionCount = -1;

	private static class Entry {
		final String column;

		boolean relative;
		double rate;
		double span;
		double width;

		public Entry(String column) {
			this.column = column;
		}

	}

	/**
	 * <div lang="ja">
	 * 標準設定の{@link TimeSeriesProperties}を作成します。
	 * </div><div lang="en">
	 * Returns a default instance of {@link TimeSeriesProperties}.
	 * </div>
	 */
	public TimeSeriesProperties() {
		rowExpirationTime = -1;
		rowExpirationTimeUnit = null;
		compressionWindowSize = -1;
		compressionWindowSizeUnit = null;
		compressionMethod = CompressionMethod.NO;
		entryMap = new HashMap<String, Entry>();
		expirationDivisionCount = -1;
	}

	TimeSeriesProperties(TimeSeriesProperties props) {
		rowExpirationTime = props.rowExpirationTime;
		rowExpirationTimeUnit = props.rowExpirationTimeUnit;
		compressionWindowSize = props.compressionWindowSize;
		compressionWindowSizeUnit = props.compressionWindowSizeUnit;
		compressionMethod = props.compressionMethod;
		expirationDivisionCount = props.expirationDivisionCount;
		entryMap = props.entryMap;
		duplicateDeep();
	}

	/**
	 * <div lang="ja">
	 * 時系列圧縮方式の種別を設定します。
	 *
	 * <p>異なる圧縮方式に変更した場合、カラム別の設定はすべて解除されます。</p>
	 *
	 * @param compressionMethod 圧縮方式の種別
	 *
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Sets a type of TimeSeries compression method.
	 *
	 * <p>When changing the compression method, all settings of individual column are released.</p>
	 *
	 * @param compressionMethod A type of compression method
	 *
	 * @throws NullPointerException If {@code null} is specified in the argument
	 * </div>
	 */
	public void setCompressionMethod(CompressionMethod compressionMethod) {
		if (compressionMethod == null) {
			throw GSErrorCode.checkNullParameter(
					compressionMethod, "compressionMethod", null);
		}

		if (compressionMethod != this.compressionMethod) {
			entryMap.clear();
		}

		this.compressionMethod = compressionMethod;
	}

	/**
	 * <div lang="ja">
	 * 時系列圧縮方式の種別を取得します。
	 *
	 * @return 圧縮方式の種別。{@code null}は返却されない
	 * </div><div lang="en">
	 * Gets the type of compression method
	 *
	 * @return Type of compression method. {@code null} is not returned.
	 * </div>
	 */
	public CompressionMethod getCompressionMethod() {
		return compressionMethod;
	}

	/**
	 * <div lang="ja">
	 * 指定のカラムについて、相対誤差あり間引き圧縮のパラメータを設定します。
	 *
	 * <p>異なる圧縮方式が設定されていた場合、間引き圧縮設定に変更されます。</p>
	 *
	 * <p>{@code rate}と{@code span}の積は、絶対誤差あり間引き圧縮にて
	 * {@link #getCompressionWidth(String)}により得られる値と等価です。</p>
	 *
	 * <p>パラメータ設定できるカラムの型は、以下のいずれかに限定されます。</p>
	 * <ul>
	 * <li>{@link GSType#BYTE}</li>
	 * <li>{@link GSType#SHORT}</li>
	 * <li>{@link GSType#INTEGER}</li>
	 * <li>{@link GSType#LONG}</li>
	 * <li>{@link GSType#FLOAT}</li>
	 * <li>{@link GSType#DOUBLE}</li>
	 * </ul>
	 *
	 * <p>1つの時系列に対してパラメータ設定できるカラムの上限数については、
	 * GridDBテクニカルリファレンスを参照してください。上限を超えるオプションは作成できますが、
	 * 上限を超えたオプションを指定して時系列を作成することはできません。</p>
	 *
	 * @param column カラム名
	 * @param rate {@code span}を基準とした相対誤差境界値。{@code 0}
	 * 以上{@code 1}以下でなければならない
	 * @param span 対象のカラムの値がとりうる範囲の最大値と最小値の差
	 *
	 * @throws IllegalArgumentException 制限に反するカラム名が指定された
	 * ことを検知できた場合、また、{@code rate}に範囲外の値を指定した場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Sets parameters for the thinning compression method with
	 * relative error on the specified column.
	 *
	 * <p>If a different compression method had been set, it is
	 * changed to the thinning method.</p>
	 *
	 * <p>Multiplying {@code rate} and {@code span} together is
	 * equal to the value that is obtained by {@link #getCompressionWidth(String)}
	 * in the thinning compression with absolute error.</p>
	 *
	 * <p>The column types are limited to the followings. </p>
	 * <ul>
	 * <li>{@link GSType#BYTE}</li>
	 * <li>{@link GSType#SHORT}</li>
	 * <li>{@link GSType#INTEGER}</li>
	 * <li>{@link GSType#LONG}</li>
	 * <li>{@link GSType#FLOAT}</li>
	 * <li>{@link GSType#DOUBLE}</li>
	 * </ul>
	 *
	 * <p>See the GridDB Technical Reference for the upper limit
	 * of the number of columns on which the parameters can be
	 * specified. The number of options that is over the limit
	 * can be created, but the TimeSeries cannot be created for
	 * that case.</p>
	 *
	 * @param column Column name
	 * @param rate Boundary value of relative error based on
	 * {@code span}. It must be greater than or equal to 0.0 and
	 * less than or equal to 1.0.
	 * @param span The difference between maximum and minimum
	 * value of the column.
	 *
	 * @throws IllegalArgumentException When detecting an illegal
	 * column name against the limitations, or specifying value
	 * out of the range for {@code rate}.
	 * @throws NullPointerException If {@code null} is specified
	 * in the argument.
	 * </div>
	 */
	public void setRelativeHiCompression(
			String column, double rate, double span) {
		if (!(0 <= rate && rate <= 1)) {
			throw new IllegalArgumentException(
					"Rate out of range (value=" + rate + ")");
		}

		this.compressionMethod = CompressionMethod.HI;
		final Entry entry = new Entry(column);
		entry.relative = true;
		entry.rate = rate;
		entry.span = span;

		try {
			entryMap.put(normalizeColumn(column), entry);
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(column, "column", e);
		}
	}

	/**
	 * <div lang="ja">
	 * 指定のカラムについて、絶対誤差あり間引き圧縮のパラメータを設定します。
	 *
	 * <p>異なる圧縮方式が設定されていた場合、間引き圧縮設定に変更されます。</p>
	 *
	 * <p>パラメータ設定できるカラムの型、ならびに、カラム数の上限は、
	 * {@link #setRelativeHiCompression(String, double, double)}と同様です。</p>
	 *
	 * @param column カラム名
	 * @param width {@link #getCompressionWidth(String)}と対応する
	 * 誤差境界の幅
	 *
	 * @throws IllegalArgumentException 制限に反するカラム名が指定された
	 * ことを検知できた場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Sets parameters for the thinning compression method with
	 * absolute error on the specified column.
	 *
	 * <p>If a different compression method had been set, it is
	 * changed to the thinning method.</p>
	 *
	 * <p>The limitation of column type and the upper limit of the
	 * number of columns are the same as {@link #setRelativeHiCompression(String, double, double)}. </p>
	 *
	 * @param column Column name
	 * @param width Width of error boundary corresponding to
	 * {@link #getCompressionWidth(String)}
	 *
	 * @throws IllegalArgumentException When detecting an illegal
	 * column name against the limitations.
	 * @throws NullPointerException If {@code null} is specified
	 * in the argument.
	 * </div>
	 */
	public void setAbsoluteHiCompression(String column, double width) {
		this.compressionMethod = CompressionMethod.HI;

		final Entry entry = new Entry(column);
		entry.width = width;
		try {
			entryMap.put(normalizeColumn(column), entry);
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(column, "column", e);
		}
	}

	/**
	 * <div lang="ja">
	 * 指定のカラムについて、誤差あり間引き圧縮の誤差判定基準値が
	 * 相対値かどうかを返します。
	 *
	 * @param column カラム名
	 *
	 * @return 指定のカラム名に対応する設定がある場合はその設定値、
	 * ない場合は{@code null}
	 *
	 * @throws IllegalArgumentException 制限に反するカラム名が指定された
	 * ことを検知できた場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Returns value which indicates whether the error
	 * threshold of the thinning compression with error is
	 * relative or not on the specified column.
	 *
	 * @param column Column name
	 *
	 * @return If the specified column has the corresponding
	 * setting, the value is returned, otherwise {@code null}
	 *
	 * @throws IllegalArgumentException When detecting an illegal
	 * column name against the limitations.
	 * @throws NullPointerException If {@code null} is specified
	 * in the argument.
	 * </div>
	 */
	public Boolean isCompressionRelative(String column) {
		final Entry entry;
		try {
			entry = entryMap.get(normalizeColumn(column));
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(column, "column", e);
		}

		if (compressionMethod != CompressionMethod.HI || entry == null) {
			return null;
		}

		return entry.relative;
	}

	/**
	 * <div lang="ja">
	 * 指定のカラムについて、相対誤差あり間引き圧縮における、値がとりうる範囲を
	 * 基準とした誤差境界値の比率を取得します。
	 *
	 * <p>値がとりうる範囲は、{@link #getCompressionWidth(String)}により
	 * 取得できます。</p>
	 *
	 * @param column カラム名
	 *
	 * @return 指定のカラム名に対応する設定がある場合はその設定値、
	 * ない場合は{@code null}
	 *
	 * @throws IllegalArgumentException 制限に反するカラム名が指定された
	 * ことを検知できた場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Returns the ratio of error range based on the possible
	 * value range of the specified column for the thinning
	 * compression with relative error.
	 *
	 * <p>The possible value range can be obtained by {@link #getCompressionWidth(String)}.</p>
	 *
	 * @param column Column name
	 *
	 * @return If the specified column has the corresponding
	 * setting, the value is returned, otherwise {@code null}
	 *
	 * @throws IllegalArgumentException When detecting an illegal
	 * column name against the limitations.
	 * @throws NullPointerException If {@code null} is specified
	 * in the argument.
	 * </div>
	 */
	public Double getCompressionRate(String column) {
		final Entry entry;
		try {
			entry = entryMap.get(normalizeColumn(column));
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(column, "column", e);
		}

		if (compressionMethod != CompressionMethod.HI || entry == null ||
				!entry.relative) {
			return null;
		}

		return entry.rate;
	}

	/**
	 * <div lang="ja">
	 * 指定のカラムについての、相対誤差あり間引き圧縮で用いられる、
	 * 値がとりうる範囲の最大値と最小値の差を取得します。
	 *
	 * @param column カラム名
	 *
	 * @return 指定のカラム名に対応する設定がある場合はその設定値、
	 * ない場合は{@code null}
	 *
	 * @throws IllegalArgumentException 制限に反するカラム名が指定された
	 * ことを検知できた場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Returns the difference between maximum and minimum
	 * possible value of the specified column by the thinning
	 * compression with relative error.
	 *
	 * @param column Column name
	 *
	 * @return If the specified column has the corresponding
	 * setting, the value is returned, otherwise {@code null}
	 *
	 * @throws IllegalArgumentException When detecting an illegal
	 * column name against the limitations.
	 * @throws NullPointerException If {@code null} is specified
	 * in the argument.
	 * </div>
	 */
	public Double getCompressionSpan(String column) {
		final Entry entry;
		try {
			entry = entryMap.get(normalizeColumn(column));
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(column, "column", e);
		}

		if (compressionMethod != CompressionMethod.HI || entry == null ||
				!entry.relative) {
			return null;
		}

		return entry.span;
	}

	/**
	 * <div lang="ja">
	 * 指定のカラムについての、絶対誤差あり間引き圧縮における誤差境界の幅を取得します。
	 *
	 * <p>誤差境界の幅とは、間引き判定対象の値と間引きした場合に線形補間される
	 * 値との差として、許容される最大の値です。</p>
	 *
	 * @param column カラム名
	 *
	 * @return 指定のカラム名に対応する設定がある場合はその設定値、
	 * ない場合は{@code null}
	 *
	 * @throws IllegalArgumentException 制限に反するカラム名が指定された
	 * ことを検知できた場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Returns the width of error boundary on the specified column
	 * for the thinning compression with absolute error.
	 *
	 * @param column Column name
	 *
	 * @return If the specified column has the corresponding
	 * setting, the value is returned, otherwise {@code null}
	 *
	 * @throws IllegalArgumentException When detecting an illegal
	 * column name against the limitations.
	 * @throws NullPointerException if {@code null} is specified
	 * in the argument.
	 * </div>
	 */
	public Double getCompressionWidth(String column) {
		final Entry entry;
		try {
			entry = entryMap.get(normalizeColumn(column));
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(column, "column", e);
		}

		if (compressionMethod != CompressionMethod.HI || entry == null ||
				entry.relative) {
			return null;
		}

		return entry.width;
	}

	/**
	 * <div lang="ja">
	 * 追加設定のあるカラムの名前をすべて取得します。
	 *
	 * <p>返却されたオブジェクトの内容を呼び出し後に変更したとしても、
	 * このオブジェクトの内容は変化しません。
	 * また、このオブジェクトに対する操作により、返却されたオブジェクトの内容が
	 * 変化することはありません。</p>
	 *
	 * @return 追加設定のあるカラム名の集合
	 * </div><div lang="en">
	 * Returns all names of the columns which have additional setting.
	 *
	 * <p>This object is not changed by updating the returned
	 * object, and the returned object is not changed by updating
	 * this object.</p>
	 *
	 * @return Name set of columns which have additional setting.
	 * </div>
	 */
	public Set<String> getSpecifiedColumns() {
		final Set<String> columnNames = new HashSet<String>();

		for (Entry entry : entryMap.values()) {
			columnNames.add(entry.column);
		}

		return columnNames;
	}

	/**
	 * <div lang="ja">
	 * 指定のカラムを無圧縮に設定するために提供されていた、廃止済みのメソッドです。
	 *
	 * @deprecated {@link CompressionMethod#NO}指定による
	 * {@link #setCompressionMethod(CompressionMethod)}に
	 * 置き換えられました。
	 *
	 * @throws Error 呼び出した場合
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	void setNonCompression(String column) {
		throw new Error("Not supported");
	}

	/**
	 * <div lang="ja">
	 * 指定のカラムを可逆圧縮するために提供されていた、廃止済みのメソッドです。
	 *
	 * @deprecated {@link CompressionMethod#SS}指定による
	 * {@link #setCompressionMethod(CompressionMethod)}に
	 * 置き換えられました。
	 *
	 * @throws Error 呼び出した場合
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	public void setLosslessCompression(String column) {
		throw new Error("Not supported");
	}

	/**
	 * <div lang="ja">
	 * 指定のカラムに関する不可逆圧縮設定を返すために提供されていた、
	 * 廃止済みのメソッドです。
	 *
	 * @deprecated {@link #getCompressionMethod()}に
	 * 置き換えられました。
	 *
	 * @throws Error 呼び出した場合
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	public boolean isLosslessCompression(String column) {
		throw new Error("Not supported");
	}

	/**
	 * <div lang="ja">
	 * 指定のカラムを不可逆圧縮設定にするために提供されていた、廃止済みのメソッドです。
	 *
	 * @deprecated {@link CompressionMethod#HI}指定による
	 * {@link #setCompressionMethod(CompressionMethod)}などに
	 * 置き換えられました。
	 *
	 * @throws Error 呼び出した場合
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	public void setLossyCompression(
			String column, double threshold, boolean thresholdRelative) {
		throw new Error("Not supported");
	}

	/**
	 * <div lang="ja">
	 * 指定のカラムに関する不可逆圧縮設定を返すために提供されていた、
	 * 廃止済みのメソッドです。
	 *
	 * @deprecated {@link #getCompressionMethod()}に
	 * 置き換えられました。
	 *
	 * @throws Error 呼び出した場合
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	public boolean isLossyCompression(String column) {
		throw new Error("Not supported");
	}

	/**
	 * <div lang="ja">
	 * 指定のカラムに関して不可逆圧縮が指定されている場合の、
	 * 対象フィールドの間引き条件となる誤差の閾値を返すために提供されていた、
	 * 廃止済みのメソッドです。
	 *
	 * @deprecated {@link #getCompressionSpan(String)}もしくは
	 * {@link #getCompressionWidth(String)}に置き換えられました。
	 *
	 * @throws Error 呼び出した場合
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	public double getCompressionThreshold(String column) {
		throw new Error("Not supported");
	}

	/**
	 * <div lang="ja">
	 * 指定のカラムに関して不可逆圧縮が指定されている場合の、
	 * 対象フィールドの間引き条件となる誤差の閾値種別を返すために提供されていた、
	 * 廃止済みのメソッドです。
	 *
	 * @deprecated {@link #isCompressionRelative(String)}に
	 * 置き換えられました。
	 *
	 * @throws Error 呼び出した場合
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	public boolean isRelativeCompressionThreshold(String column) {
		throw new Error("Not supported");
	}

	/**
	 * <div lang="ja">
	 * ロウの有効期限の基準となる経過期間を設定します。
	 *
	 * <p>ロウの有効期限の時刻は、ロウキーの時刻から指定の経過期間を加算することで
	 * 求まります。有効期限の時刻がGridDB上の現在時刻よりも古いロウは、
	 * 有効期限の切れたロウとみなされます。
	 * 期限切れのロウは、検索や更新といったロウ操作の対象から外れ、存在しないものと
	 * みなされます。
	 * 対応するGridDB上の内部データは、随時削除されます。</p>
	 *
	 * <p>有効期限超過の判定に使用される現在時刻は、GridDBの各ノードの
	 * 実行環境に依存します。
	 * したがって、ネットワークの遅延や実行環境の時刻設定のずれなどにより、
	 * このVMの時刻より前に期限切れ前のロウにアクセスできなくなる場合や、
	 * このVMの時刻より後に期限切れロウにアクセスできる場合があります。
	 * 意図しないロウの喪失を避けるために、最低限必要な期間よりも大きな値を
	 * 設定することを推奨します。</p>
	 *
	 * <p>作成済みの時系列の設定を変更することはできません。</p>
	 *
	 * @param elapsedTime 基準とする経過期間。{@code 0}以下の値は指定できない
	 * @param timeUnit 経過期間の単位。{@link TimeUnit#YEAR}、
	 * {@link TimeUnit#MONTH}は指定できない
	 *
	 * @throws IllegalArgumentException 範囲外の{@code elapsedTime}、
	 * {@code timeUnit}が指定された場合
	 * @throws IllegalArgumentException 制限に反するカラム名が指定された
	 * ことを検知できた場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Sets the elapsed time period of a Row to be used as the basis of validity period.
	 *
	 * <p>The validity period of a Row can be obtained by adding the specified elapsed time period to the timestamp of
	 * the Row key. Rows whose expiration times are older than the current time on GridDB are treated as expired Rows.
	 * Expired Rows are treated as non existent and are not used for search, update or other Row operations.
	 * The corresponding internal data in GridDB will be deleted as needed.</p>
	 *
	 * <p>The current time used for expiry check is dependent on the runtime environment of each node of GridDB.
	 * Therefore, there may be cases where unexpired Rows cannot be accessed before the VM time,
	 * or expired Rows can be accessed after the VM time because of a network delay
	 * or a discrepancy in the time setting of the runtime environment.
	 * In order to avoid unintended loss of Rows, you
	 * should set a value larger than the minimum required period of time.</p>
	 *
	 * <p>The setting for an already-created TimeSeries cannot be changed.</p>
	 *
	 * @param elapsedTime the elapsed time period of a Row to be used as the basis of the validity period.
	 * Must not be {@code 0} or less.
	 * @param timeUnit the unit of the elapsed time period. {@link TimeUnit#YEAR} or {@link TimeUnit#MONTH}
	 * cannot be specified.
	 *
	 * @throws IllegalArgumentException if {@code elapsedTime} or {@code timeUnit} of out of range is specified
	 * @throws IllegalArgumentException when detecting an illegal column name against the limitations
	 * @throws NullPointerException if {@code null} is specified in the argument.
	 * </div>
	 */
	public void setRowExpiration(int elapsedTime, TimeUnit timeUnit) {
		if (elapsedTime <= 0) {
			throw new IllegalArgumentException(
					"Time must not be zero or negative " +
					"(time=" + elapsedTime + ")");
		}

		try {
			switch (timeUnit) {
			case YEAR:
			case MONTH:
				throw new IllegalArgumentException(
						"Time unit must not be YEAR or MONTH " +
						"(unit=" + timeUnit + ")");
			}
		}
		catch (NullPointerException e) {
			GSErrorCode.checkNullParameter(timeUnit, "timeUnit", e);
			throw e;
		}

		rowExpirationTime = elapsedTime;
		rowExpirationTimeUnit = timeUnit;
	}

	/**
	 * <div lang="ja">
	 * ロウの有効期限の基準となる経過期間を取得します。
	 *
	 * @return 有効期限の基準となる経過期間。無設定の場合は{@code -1}
	 * </div><div lang="en">
	 * Returns the elapsed time period of a Row to be used as the basis of the validity period.
	 *
	 * @return The elapsed time period to be used as the basis of the validity period. {@code -1} if unspecified.
	 * </div>
	 */
	public int getRowExpirationTime() {
		return rowExpirationTime;
	}

	/**
	 * <div lang="ja">
	 * ロウの有効期限の基準とする経過期間の単位を取得します。
	 *
	 * @return 有効期限の基準とする経過期間の単位。無設定の場合は{@code null}
	 * </div><div lang="en">
	 * Returns the unit of the elapsed time period of a Row to be used as the basis of the validity period.
	 *
	 * @return The unit of the elapsed time period to be used as the basis of the validity period. {@code null} if unspecified.
	 * </div>
	 */
	public TimeUnit getRowExpirationTimeUnit() {
		return rowExpirationTimeUnit;
	}

	/**
	 * <div lang="ja">
	 * 間引き圧縮において連続して間引きされるロウの最大期間を設定します。
	 *
	 * <p>この期間が設定された時系列のロウについて、前方のロウと指定の期間
	 * 以上時刻が離れていた場合、間引き圧縮として間引き可能な条件を満たして
	 * いたとしても、間引かれなくなります。</p>
	 *
	 * <p>時系列圧縮方式として{@link CompressionMethod#NO}が設定されていた
	 * 場合、この期間の設定は無視されます。</p>
	 *
	 * <p>時系列圧縮方式として{@link CompressionMethod#HI}または
	 * {@link CompressionMethod#SS}が設定されており、この期間について設定
	 * されなかった場合、TIMESTAMP型の取りうる値の範囲全体が期間として設定
	 * されたとみなされます。</p>
	 *
	 * <p>前方のロウと指定の期間以上時刻が離れておらず、かつ、間引き圧縮として
	 * 間引き可能な条件を満たしていたとしても、格納先の内部の配置などに
	 * よっては間引かれない場合があります。</p>
	 *
	 * @param compressionWindowSize 最大連続間引き期間。{@code 0}
	 * 以下の値は指定できない
	 * @param compressionWindowSizeUnit 最大連続間引き期間を表す単位。
	 * {@link TimeUnit#YEAR}、{@link TimeUnit#MONTH}は指定できない
	 *
	 * @throws IllegalArgumentException 範囲外の{@code compressionWindowSize}、
	 * {@code compressionWindowSizeUnit}が指定された場合
	 * @throws IllegalArgumentException 制限に反するカラム名が指定された
	 * ことを検知できた場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Sets a window size which means maximum time span of
	 * contiguous data thinned by the compression.
	 *
	 * <p>When the window size has been set to a time series and a
	 * row in the time series is apart over the window size from
	 * previous stored data, even if the row satisfies the other
	 * compression conditions, the row is not thinned.</p>
	 *
	 * <p>This setting is ignored if {@link CompressionMethod#NO}
	 * has been set.</p>
	 *
	 * <p>If {@link CompressionMethod#HI} or {@link CompressionMethod#SS}
	 * has been set and this setting is not specified, the maximum
	 * value of TIMESTAMP is set as the window size.</p>
	 *
	 * <p>Even if the row is in the window size from the previous
	 * data and it satisfies the conditions of the compression, it
	 * may not be thinned depending on its stored location, etc.</p>
	 *
	 * @param compressionWindowSize Window size. It is not
	 * allowed to set value less than or equal to {@code 0}.
	 * @param compressionWindowSizeUnit Unit of the window size.
	 * {@link TimeUnit#YEAR} and {@link TimeUnit#MONTH} cannot be
	 * specified.
	 *
	 * @throws IllegalArgumentException When {@code compressionWindowSize} is out of the range.
	 * @throws IllegalArgumentException when detecting an illegal column name against the limitations
	 * @throws NullPointerException if {@code null} is specified in the argument.
	 * </div>
	 */
	public void setCompressionWindowSize(
			int compressionWindowSize, TimeUnit compressionWindowSizeUnit) {
		if (compressionWindowSize <= 0) {
			throw new IllegalArgumentException(
					"Windows size must not be zero or negative " +
					"(size=" + compressionWindowSize + ")");
		}

		try {
			switch (compressionWindowSizeUnit) {
			case YEAR:
			case MONTH:
				throw new IllegalArgumentException(
						"Size unit must not be YEAR or MONTH " +
						"(unit=" + compressionWindowSizeUnit + ")");
			}
		}
		catch (NullPointerException e) {
			GSErrorCode.checkNullParameter(
					compressionWindowSizeUnit,
					"compressionWindowSizeUnit", e);
			throw e;
		}

		this.compressionWindowSize = compressionWindowSize;
		this.compressionWindowSizeUnit = compressionWindowSizeUnit;
	}

	/**
	 * <div lang="ja">
	 * 期限に到達したロウデータの解放単位と対応する、有効期間に対しての分割数を
	 * 取得します。
	 *
	 * @return 有効期間に対する分割数。無設定の場合は{@code -1}
	 *
	 * @see #setExpirationDivisionCount(int)
	 *
	 * @since 2.0
	 * </div><div lang="en">
	 * Return the division number for the validity period that corresponds
	 * to the number of expired rows data units to be released.
	 *
	 * @return Division number of the validity period. {@code -1} if unspecified.
	 *
	 * @see #setExpirationDivisionCount(int)
	 *
	 * @since 2.0
	 * </div>
	 */
	public int getExpirationDivisionCount() {
		return expirationDivisionCount;
	}

	/**
	 * <div lang="ja">
	 * 有効期間に対する分割数により、期限に到達したロウデータの解放単位を
	 * 設定します。
	 *
	 * <p>分割数を設定すると、期限に到達したロウデータの管理領域を
	 * 解放するための条件を制御できます。期限に到達したロウデータが
	 * 分割数に相当する期間だけ集まった時点で解放しようとします。</p>
	 *
	 * <p>分割数の上限については、GridDBテクニカルリファレンスを参照してください。
	 * 上限を超えたオプションを指定して時系列を作成することはできません。</p>
	 *
	 * <p>ロウの有効期限の基準となる経過期間の設定がない場合、この分割数の
	 * 設定は無視され無設定となります。
	 * 一方、ロウの有効期限の基準となる経過期間の設定がある場合にこの
	 * 分割数の設定を省略すると、作成される時系列にはGridDBクラスタ上の
	 * デフォルトの分割数が設定されます。</p>
	 *
	 * @param count 有効期間に対する分割数。{@code 0}以下の値は指定できない
	 *
	 * @throws IllegalArgumentException {@code 0}以下の分割数が指定された場合
	 *
	 * @since 2.0
	 * </div><div lang="en">
	 * Sets the division number for the validity period as the number of
	 * expired row data units to be released.
	 *
	 * <p>The division number set is used to control the conditions for releasing
	 * row data management areas that have expired in GridDB.
	 * Expired row data shall be released at the point they are collected only
	 * when the period equivalent to the division number is reached.</p>
	 *
	 * <p>See the GridDB Technical Reference for the upper limit
	 * of the division number. It will be failed if the division
	 * number exceeds the size limit.</p>
	 *
	 * <p>If the elapsed time period is not set, the setting of
	 * division number is ignored. If the elapsed time period has been
	 * set and the setting of division number is not set, the
	 * division number is set to the default value of the
	 * connected GridDB server.</p>
	 *
	 * @param count the division number for the validity period. Must not be {@code 0} or less.
	 *
	 * @throws IllegalArgumentException if the division number is specified as {@code 0} or less.
	 *
	 * @since 2.0
	 * </div>
	 */
	public void setExpirationDivisionCount(int count) {
		if (count <= 0) {
			throw new IllegalArgumentException(
					"Division count must not be zero or negative " +
					"(count=" + count + ")");
		}

		expirationDivisionCount = count;
	}

	/**
	 * <div lang="ja">
	 * 間引き圧縮において連続して間引きされるロウの最大期間を取得します。
	 *
	 * @return 最大連続間引き期間。無設定の場合は{@code -1}
	 * </div><div lang="en">
	 * Returns the window size for the thinning compression.
	 * The contiguous data can be thinned in the window size which
	 * represents a time span.
	 *
	 * @return Window size. {@code -1} if it has not been set.
	 * </div>
	 */
	public int getCompressionWindowSize() {
		return compressionWindowSize;
	}

	/**
	 * <div lang="ja">
	 * 間引き圧縮において連続して間引きされるロウの最大期間の単位を取得します。
	 *
	 * @return 最大連続間引き期間の単位。無設定の場合は{@code null}
	 * </div><div lang="en">
	 * Returns the unit of the window size for the thinning compression.
	 * The contiguous data can be thinned in the window size which
	 * represents a time span.
	 *
	 * @return Unit of window size. {@code -1} if it has not been set.
	 * </div>
	 */
	public TimeUnit getCompressionWindowSizeUnit() {
		return compressionWindowSizeUnit;
	}

	/**
	 * <div lang="ja">
	 * このオブジェクトと同一設定の{@link TimeSeriesProperties}を作成します。
	 *
	 * @return 作成された{@link TimeSeriesProperties}
	 * </div><div lang="en">
	 * Creates new {@link TimeSeriesProperties} with the same settings as this object.
	 *
	 * @return Creates and returns a copy of this object.
	 * </div>
	 */
	@Override
	public TimeSeriesProperties clone() {
		final TimeSeriesProperties props;
		try {
			props = (TimeSeriesProperties) super.clone();
		}
		catch (CloneNotSupportedException e) {
			throw new Error(e);
		}

		props.duplicateDeep();

		return props;
	}

	private void duplicateDeep() {
		final Map<String, Entry> entryMap = new HashMap<String, Entry>();
		entryMap.putAll(this.entryMap);
		this.entryMap = entryMap;
	}

	private static String normalizeColumn(String column) {
		try {
			return RowMapper.normalizeSymbol(column, "column name");
		}
		catch (GSException e) {
			throw new IllegalArgumentException(e);
		}
	}

}
