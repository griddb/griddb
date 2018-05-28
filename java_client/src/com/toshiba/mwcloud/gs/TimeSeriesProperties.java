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
 * Represents the information about optional configuration settings used for newly creating or updating a TimeSeries.
 *
 * <p>It does not guarantee the validity of values e.g. the column
 * names and the upper limit of the column number that can be
 * individually compressed.</p>
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
	 * Returns a default instance of {@link TimeSeriesProperties}.
	 */
	public TimeSeriesProperties() {
	}

	private TimeSeriesProperties(TimeSeriesProperties props) {
		rowExpirationTime = props.rowExpirationTime;
		rowExpirationTimeUnit = props.rowExpirationTimeUnit;
		compressionWindowSize = props.compressionWindowSize;
		compressionWindowSizeUnit = props.compressionWindowSizeUnit;
		compressionMethod = props.compressionMethod;
		expirationDivisionCount = props.expirationDivisionCount;
		entryMap.putAll(props.entryMap);
	}

	/**
	 * Sets a type of TimeSeries compression method.
	 *
	 * <p>When changing the compression method, all settings of individual column are released.</p>
	 *
	 * @param compressionMethod A type of compression method
	 *
	 * @throws NullPointerException If {@code null} is specified in the argument
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
	 * Gets the type of compression method
	 *
	 * @return Type of compression method. {@code null} is not returned.
	 */
	public CompressionMethod getCompressionMethod() {
		return compressionMethod;
	}

	/**
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
	 * Returns all names of the columns which have additional setting.
	 *
	 * <p>This object is not changed by updating the returned
	 * object, and the returned object is not changed by updating
	 * this object.</p>
	 *
	 * @return Name set of columns which have additional setting.
	 */
	public Set<String> getSpecifiedColumns() {
		final Set<String> columnNames = new HashSet<String>();

		for (Entry entry : entryMap.values()) {
			columnNames.add(entry.column);
		}

		return columnNames;
	}

	/**
	 * @deprecated
	 */
	@Deprecated
	void setNonCompression(String column) {
		throw new Error("Not supported");
	}

	/**
	 * @deprecated
	 */
	@Deprecated
	public void setLosslessCompression(String column) {
		throw new Error("Not supported");
	}

	/**
	 * @deprecated
	 */
	@Deprecated
	public boolean isLosslessCompression(String column) {
		throw new Error("Not supported");
	}

	/**
	 * @deprecated
	 */
	@Deprecated
	public void setLossyCompression(
			String column, double threshold, boolean thresholdRelative) {
		throw new Error("Not supported");
	}

	/**
	 * @deprecated
	 */
	@Deprecated
	public boolean isLossyCompression(String column) {
		throw new Error("Not supported");
	}

	/**
	 * @deprecated
	 */
	@Deprecated
	public double getCompressionThreshold(String column) {
		throw new Error("Not supported");
	}

	/**
	 * @deprecated
	 */
	@Deprecated
	public boolean isRelativeCompressionThreshold(String column) {
		throw new Error("Not supported");
	}

	/**
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
	 * Returns the elapsed time period of a Row to be used as the basis of the validity period.
	 *
	 * @return The elapsed time period to be used as the basis of the validity period. {@code -1} if unspecified.
	 */
	public int getRowExpirationTime() {
		return rowExpirationTime;
	}

	/**
	 * Returns the unit of the elapsed time period of a Row to be used as the basis of the validity period.
	 *
	 * @return The unit of the elapsed time period to be used as the basis of the validity period. {@code null} if unspecified.
	 */
	public TimeUnit getRowExpirationTimeUnit() {
		return rowExpirationTimeUnit;
	}

	/**
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
	 * Return the division number for the validity period that corresponds
	 * to the number of expired rows data units to be released.
	 *
	 * @return Division number of the validity period. {@code -1} if unspecified.
	 *
	 * @see #setExpirationDivisionCount(int)
	 *
	 */
	public int getExpirationDivisionCount() {
		return expirationDivisionCount;
	}

	/**
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
	 * Returns the window size for the thinning compression.
	 * The contiguous data can be thinned in the window size which
	 * represents a time span.
	 *
	 * @return Window size. {@code -1} if it has not been set.
	 */
	public int getCompressionWindowSize() {
		return compressionWindowSize;
	}

	/**
	 * Returns the unit of the window size for the thinning compression.
	 * The contiguous data can be thinned in the window size which
	 * represents a time span.
	 *
	 * @return Unit of window size. {@code -1} if it has not been set.
	 */
	public TimeUnit getCompressionWindowSizeUnit() {
		return compressionWindowSizeUnit;
	}

	/**
	 * Creates new {@link TimeSeriesProperties} with the same settings as this object.
	 *
	 * @return Creates and returns a copy of this object.
	 */
	@Override
	public TimeSeriesProperties clone() {
		return new TimeSeriesProperties(this);
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
