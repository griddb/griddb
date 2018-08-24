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

import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import com.toshiba.mwcloud.gs.common.GSErrorCode;

/**
 * <div lang="ja">
 * 時刻データを操作するためのユーティリティ機能を提供します。
 * </div><div lang="en">
 * Provides the utilities for manipulating time data.
 * </div>
 */
public class TimestampUtils {

	private static final boolean TRIM_MILLISECONDS = false;

	@Deprecated
	public TimestampUtils() {
	}

	/**
	 * <div lang="ja">
	 * 現在時刻を求めます。
	 * </div><div lang="en">
	 * Returns the current time.
	 * </div>
	 */
	public static Date current() {
		return currentCalendar().getTime();
	}

	/**
	 * <div lang="ja">
	 * 現在時刻を{@link Calendar}として求めます。
	 *
	 * <p>現バージョンでは、タイムゾーンは常にUTCに設定されます。</p>
	 * </div><div lang="en">
	 * Returns the current time as a {@link Calendar} object.
	 *
	 * <p>The current version always uses the UTC timezone.</p>
	 * </div>
	 */
	public static Calendar currentCalendar() {
		final Calendar calendar =
				Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		if (TRIM_MILLISECONDS) {
			calendar.set(Calendar.MILLISECOND, 0);
		}
		return calendar;
	}

	/**
	 * <div lang="ja">
	 * 時刻に一定の値を加算します。
	 *
	 * <p>{@code amount}に負の値を指定することで、指定の時刻より
	 * 前の時刻を求めることができます。</p>
	 *
	 * <p>現バージョンでは、算出の際に使用されるタイムゾーンはUTCです。</p>
	 *
	 * @param timestamp 対象とする時刻
	 * @param amount 加算する値
	 * @param timeUnit 加算する値の単位
	 *
	 * @throws NullPointerException {@code timestamp}、{@code timeUnit}に
	 * {@code null}が指定された場合
	 * </div><div lang="en">
	 * Adds a specific value to the specified time.
	 *
	 * <p>An earlier time than the specified time can be obtained by specifying a negative value as {@code amount}.</p>
	 *
	 * <p>The current version uses the UTC timezone for calculation.</p>
	 *
	 * @param timestamp target time
	 * @param amount value to be added
	 * @param timeUnit unit of value to be added
	 *
	 * @throws NullPointerException when {@code null} is specified for {@code timestamp}, {@code timeUnit}
	 * </div>
	 */
	public static Date add(Date timestamp, int amount, TimeUnit timeUnit) {
		final Calendar calendar = currentCalendar();

		try {
			calendar.setTime(timestamp);
			calendar.add(toCalendarField(timeUnit), amount);
		}
		catch (NullPointerException e) {
			GSErrorCode.checkNullParameter(timestamp, "timestamp", e);
			GSErrorCode.checkNullParameter(timeUnit, "timeUnit", e);
			throw e;
		}

		if (TRIM_MILLISECONDS) {
			calendar.set(Calendar.MILLISECOND, 0);
		}

		return calendar.getTime();
	}

	private static int toCalendarField(TimeUnit timeUnit) {
		switch (timeUnit) {
		case YEAR:
			return Calendar.YEAR;
		case MONTH:
			return Calendar.MONTH;
		case DAY:
			return Calendar.DAY_OF_MONTH;
		case HOUR:
			return Calendar.HOUR;
		case MINUTE:
			return Calendar.MINUTE;
		case SECOND:
			return Calendar.SECOND;
		case MILLISECOND:
			return Calendar.MILLISECOND;
		default:
			throw new Error("Internal error by unknown time unit (timeUnit=" +
					timeUnit + ")");
		}
	}

	/**
	 * <div lang="ja">
	 * TQLのTIMESTAMP値表記に従い、時刻の文字列表現を求めます。
	 *
	 * <p>現バージョンでは、変換の際に使用されるタイムゾーンはUTCです。</p>
	 *
	 * @param timestamp 対象とする時刻
	 *
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Returns the string representing the specified time, according to the TIMESTAMP value notation of TQL.
	 *
	 * <p>The current version uses the UTC timezone for conversion.</p>
	 *
	 * @param timestamp target time
	 *
	 * @throws NullPointerException when {@code null} is specified as argument
	 * </div>
	 */
	public static String format(Date timestamp) {
		try {
			return getFormat().format(timestamp);
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(timestamp, "timestamp", e);
		}
	}

	/**
	 * <div lang="ja">
	 * TQLのTIMESTAMP値表記に従い、指定の文字列に対応する{@link Date}を
	 * 求めます。
	 *
	 * @param source 対象とする時刻の文字列表現
	 *
	 * @throws ParseException 時刻の文字列表記と一致しない文字列が指定された場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Returns a {@link Date} object corresponding to the specified string, according to the TIMESTAMP value notation of TQL.
	 *
	 * @param source string representation of target time
	 *
	 * @throws ParseException if the specified string does not match any time representation.
	 * @throws NullPointerException when {@code null} is specified as argument
	 * </div>
	 */
	public static Date parse(String source) throws ParseException {
		try {
			return getFormat().parse(source);
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(source, "source", e);
		}
	}

	/**
	 * <div lang="ja">
	 * TQLのTIMESTAMP値表記と対応する、日付フォーマットを求めます。
	 *
	 * <p>年の値が負となる時刻は扱えません。</p>
	 * </div><div lang="en">
	 * Returns the date format conforming to the TIMESTAMP value notation of TQL.
	 *
	 * <p>The time representation containing a negative year value is not supported.</p>
	 * </div>
	 */
	public static DateFormat getFormat() {
		return new CustomDateFormat();
	}

	private static class CustomDateFormat extends DateFormat {

		private static final long serialVersionUID = -5296445605735800942L;

		private static final int SECONDS_FORMAT_LENGTH = 20;

		private static final int MILLIS_FORMAT_LENGTH = 24;

		private static final int MILLIS_DOT_POSITION = 19;

		private DateFormat secondsFormat;

		private DateFormat millisFormat;

		private DateFormat getSecondsFormat() {
			if (secondsFormat == null) {
				final DateFormat format = new SimpleDateFormat(
						"yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US);
				format.setLenient(false);
				format.setTimeZone(TimeZone.getTimeZone("UTC"));
				secondsFormat = format;
			}
			return secondsFormat;
		}

		private DateFormat getMillisFormat() {
			if (millisFormat == null) {
				final DateFormat format = new SimpleDateFormat(
						"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
				format.setLenient(false);
				format.setTimeZone(TimeZone.getTimeZone("UTC"));
				millisFormat = format;
			}
			return millisFormat;
		}

		@Override
		public StringBuffer format(
				Date date, StringBuffer toAppendTo, FieldPosition fieldPosition) {
			return getMillisFormat().format(date, toAppendTo, fieldPosition);
		}

		@Override
		public Date parse(String source, ParsePosition pos) {
			if (source.length() > MILLIS_DOT_POSITION &&
					source.charAt(MILLIS_DOT_POSITION) == '.') {
				if (source.length() != MILLIS_FORMAT_LENGTH) {
					return null;
				}
				return getMillisFormat().parse(source, pos);
			}
			else {
				if (source.length() != SECONDS_FORMAT_LENGTH) {
					return null;
				}
				return getSecondsFormat().parse(source, pos);
			}
		}

		@Override
		public void setCalendar(Calendar newCalendar) {
			throw new UnsupportedOperationException(
					"Modification is currently not supported");
		}

		@Override
		public void setNumberFormat(NumberFormat newNumberFormat) {
			throw new UnsupportedOperationException(
					"Modification is currently not supported");
		}

		@Override
		public void setTimeZone(TimeZone zone) {
			throw new UnsupportedOperationException(
					"Modification is currently not supported");
		}

		@Override
		public void setLenient(boolean lenient) {
			throw new UnsupportedOperationException(
					"Modification is currently not supported");
		}

		@Override
		public int hashCode() {
			return CustomDateFormat.class.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			return (getClass() == obj.getClass());
		}

	}

}
