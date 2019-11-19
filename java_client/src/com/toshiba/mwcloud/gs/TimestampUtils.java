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
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.regex.Matcher;

import com.toshiba.mwcloud.gs.common.GSErrorCode;
import com.toshiba.mwcloud.gs.common.PropertyUtils;

/**
 * <div lang="ja">
 * 時刻データを操作するためのユーティリティ機能を提供します。
 *
 * <p>TQL文の構築や、TQLと同一表記のTIMESTAMP値の処理を補助する
 * ために使用できます。</p>
 *
 * <p>実行環境のタイムゾーン、ロケール設定には依存しません。</p>
 * </div><div lang="en">
 * TODO Provides the utilities for manipulating time data.
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
		final Calendar calendar = Calendar.getInstance(
				PropertyUtils.createTimeZoneOffset(0), Locale.ROOT);
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
	 * @return 加算された値
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
		return add(
				timestamp, amount, timeUnit,
				PropertyUtils.createTimeZoneOffset(0));
	}

	/**
	 * <div lang="ja">
	 * 指定のタイムゾーン設定を用い、時刻に一定の値を加算します。
	 *
	 * <p>演算に用いる時間の単位によっては、タイムゾーン設定の影響を受けない
	 * 場合があります。</p>
	 *
	 * @param timestamp 対象とする時刻
	 * @param amount 加算する値
	 * @param timeUnit 加算する値の単位
	 * @param zone 演算に用いるタイムゾーン設定
	 *
	 * @return 加算された値
	 *
	 * @throws NullPointerException {@code timestamp}、{@code timeUnit}に
	 * {@code null}が指定された場合
	 *
	 * @see #add(Date, int, TimeUnit)
	 *
	 * @since 4.3
	 * </div><div lang="en">
	 * TODO
	 *
	 * @since 4.3
	 * </div>
	 */
	public static Date add(
			Date timestamp, int amount, TimeUnit timeUnit, TimeZone zone) {
		final Calendar calendar = Calendar.getInstance(zone, Locale.ROOT);

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
		return format(timestamp, PropertyUtils.createTimeZoneOffset(0));
	}

	/**
	 * <div lang="ja">
	 * 指定のタイムゾーン設定を用い、TQLのTIMESTAMP値表記に従って時刻の
	 * 文字列表現を求めます。
	 *
	 * @param timestamp 対象とする時刻
	 * @param zone 演算に用いるタイムゾーン設定
	 *
	 * @return 対応する文字列表現
	 *
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 *
	 * @see #format(Date)
	 *
	 * @since 4.3
	 * </div><div lang="en">
	 * TODO
	 *
	 * @since 4.3
	 * </div>
	 */
	public static String format(Date timestamp, TimeZone zone) {
		final DateFormat format = getFormat(zone);
		try {
			return format.format(timestamp);
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
	 * @return 指定の文字列に対応する{@link Date}
	 *
	 * @throws ParseException 時刻の文字列表記と一致しない文字列が指定された場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * TODO Returns a {@link Date} object corresponding to the specified string, according to the TIMESTAMP value notation of TQL.
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
	 * TQLのTIMESTAMP値表記と対応する、日付フォーマットを取得します。
	 *
	 * <p>現バージョンでは、タイムゾーンは常にUTCに設定されます。</p>
	 *
	 * @return 日付フォーマット
	 *
	 * <p>年の値が負となる時刻は扱えません。</p>
	 * </div><div lang="en">
	 * TODO Returns the date format conforming to the TIMESTAMP value notation of TQL.
	 *
	 * <p>The time representation containing a negative year value is not supported.</p>
	 * </div>
	 */
	public static DateFormat getFormat() {
		return getFormat(PropertyUtils.createTimeZoneOffset(0));
	}

	/**
	 * <div lang="ja">
	 * 指定のタイムゾーン設定が適用され、TQLのTIMESTAMP値表記と対応する、
	 * 日付フォーマットを取得します。
	 *
	 * @param zone 適用対象のタイムゾーン設定
	 *
	 * @return 日付フォーマット
	 *
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 *
	 * @since 4.3
	 * </div><div lang="en">
	 * TODO
	 *
	 * @since 4.3
	 * </div>
	 */
	public static DateFormat getFormat(TimeZone zone) {
		try {
			return new CustomDateFormat(zone);
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(zone, "zone", e);
		}
	}

	private static class CustomDateFormat extends DateFormat {

		private static final long serialVersionUID = -5296445605735800942L;

		private static final int MILLIS_DOT_MIN_POSITION = 19;

		private final TimeZone zone;

		private DateFormat secondsFormat;

		private DateFormat millisFormat;

		CustomDateFormat(TimeZone zone) {
			if (zone.useDaylightTime()) {
				throw new IllegalArgumentException(
						"Daylight time is currently not supported " +
						"(timeZone=" + zone.getDisplayName(Locale.ROOT) + ")");
			}
			this.zone = (TimeZone) zone.clone();
		}

		private DateFormat getSecondsFormat(TimeZone zone) {
			DateFormat format = secondsFormat;
			if (format == null || !zone.equals(format.getTimeZone())) {
				format = new SimpleDateFormat(
						"yyyy-MM-dd'T'HH:mm:ss", Locale.ROOT);
				format.setLenient(false);
				format.setTimeZone(zone);
				secondsFormat = format;
			}
			return format;
		}

		private DateFormat getMillisFormat(TimeZone zone) {
			DateFormat format = millisFormat;
			if (format == null || !zone.equals(format.getTimeZone())) {
				format = new SimpleDateFormat(
						"yyyy-MM-dd'T'HH:mm:ss.SSS", Locale.ROOT);
				format.setLenient(false);
				format.setTimeZone(zone);
				millisFormat = format;
			}
			return format;
		}

		private DateFormat getBaseFormat(String source, TimeZone zone) {
			DateFormat format;
			if (source.length() > MILLIS_DOT_MIN_POSITION &&
					source.indexOf('.', MILLIS_DOT_MIN_POSITION) >= 0) {
				format = getMillisFormat(zone);
			}
			else {
				format = getSecondsFormat(zone);
			}

			if (!format.getTimeZone().equals(zone)) {
				format = (DateFormat) format.clone();
				format.setTimeZone(zone);
			}

			return format;
		}

		@Override
		public StringBuffer format(
				Date date, StringBuffer toAppendTo, FieldPosition fieldPosition) {
			final StringBuffer sb = getMillisFormat(this.zone).format(
					date, toAppendTo, fieldPosition);
			sb.append(PropertyUtils.formatTimeZoneOffset(
					this.zone.getRawOffset(), false));
			return sb;
		}

		@Override
		public Date parse(String source, ParsePosition pos) {
			final String[] elems = splitZonePart(source);
			if (elems == null) {
				return null;
			}

			final TimeZone zone;
			try {
				zone = PropertyUtils.parseTimeZoneOffset(elems[1], false);
			}
			catch (GSException e) {
				return null;
			}

			final DateFormat baseFormat = getBaseFormat(source, zone);
			return parseStrict(baseFormat, elems[0], pos);
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
		public TimeZone getTimeZone() {
			return (TimeZone) this.zone.clone();
		}

		@Override
		public int hashCode() {
			return Arrays.hashCode(new int[] {
					CustomDateFormat.class.hashCode(),
					super.hashCode()
			});
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null || !(obj instanceof CustomDateFormat)) {
				return false;
			}
			final CustomDateFormat another = (CustomDateFormat) obj;
			if (!zone.equals(another.zone)) {
				return false;
			}
			return super.equals(obj);
		}

		private static String[] splitZonePart(String str) {
			final Matcher matcher =
					PropertyUtils.getTimeZoneOffsetPattern().matcher(str);

			final String mainPart;
			final String zonePart;
			if (matcher.find() && matcher.end() == str.length()) {
				mainPart = str.substring(0, matcher.start());
				zonePart = matcher.group();
			}
			else {
				final String utcZone = "Z";
				if (!str.endsWith(utcZone)) {
					return null;
				}
				mainPart = str.substring(0, str.length() - utcZone.length());
				zonePart = utcZone;
			}
			return new String[] { mainPart, zonePart };
		}

		private Date parseStrict(
				DateFormat format, String source, ParsePosition pos) {
			final Date parsed = format.parse(source, pos);
			if (parsed == null) {
				return null;
			}
			final String reformatted = format.format(parsed);
			if (!reformatted.equals(source)) {
				pos.setIndex(0);
				pos.setErrorIndex(0);
				return null;
			}
			return parsed;
		}

	}

}
