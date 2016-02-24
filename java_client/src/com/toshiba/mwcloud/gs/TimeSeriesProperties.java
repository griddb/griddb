/*
   Copyright (c) 2012 TOSHIBA CORPORATION.

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
 * <p>It does not guarantee the validity of values e.g. the upper limit of
 * the column number that can be individually compressed.</p>
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
	 * Not supported
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
	 * Not supported
	 */
	public CompressionMethod getCompressionMethod() {
		return compressionMethod;
	}

	/**
	 * Not supported
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
	 * Not supported
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
	 * Not supported
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
	 * Not supported
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
	 * Not supported
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
	 * Not supported
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
	 * Not supported
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
	 * @throws IllegalArgumentException if a column value with an incorrect format
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
	 * Not supported
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
	 * <p>It will be failed if the division number exceeds the size limit.
	 * See "System limiting values" in the GridDB API Reference for the division number upper limit.</p>
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
	 * Not supported
	 */
	public int getCompressionWindowSize() {
		return compressionWindowSize;
	}

	/**
	 * Not supported
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
			return RowMapper.normalizeSymbol(column);
		}
		catch (GSException e) {
			throw new IllegalArgumentException(
					"Illegal column format (reason=" + e.getMessage() + ")",
					e);
		}
	}

}
