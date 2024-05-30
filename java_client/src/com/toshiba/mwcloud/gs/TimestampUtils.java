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

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Formatter;
import java.util.Locale;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * Provides the utilities for manipulating time data.
 *
 * <p>Used to help construct TQL statements and to process TIMESTAMP values with
 * the same notation as TQL.</p>
 *
 * <p>Does not depend on the time zone and locale settings of the execution
 * environment.</p>
 * </div>
 */
public class TimestampUtils {

	private static final boolean TRIM_MILLISECONDS = false;

	/**
	 * @deprecated
	 */
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
		final Calendar calendar = createCalendar(null);
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
	 * <p>マイクロ・ナノ秒精度の単位は未サポートです。</p>
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
	 * @throws IllegalArgumentException 未サポートの{@code timeUnit}が指定された
	 * 場合
	 * </div><div lang="en">
	 * Adds a specific value to the specified time.
	 *
	 * <p>An earlier time than the specified time can be obtained by specifying a negative value as {@code amount}.</p>
	 *
	 * <p>Microsecond/nanosecond precision is not supported.</p>
	 *
	 * <p>The current version uses the UTC timezone for calculation.</p>
	 *
	 * @param timestamp target time
	 * @param amount value to be added
	 * @param timeUnit unit of value to be added
	 *
	 * @return The added value
	 *
	 * @throws NullPointerException when {@code null} is specified for {@code timestamp}, {@code timeUnit}
	 * </div>
	 */
	public static Date add(Date timestamp, int amount, TimeUnit timeUnit) {
		return add(timestamp, amount, timeUnit, null);
	}

	/**
	 * <div lang="ja">
	 * 指定のタイムゾーン設定を用い、時刻に一定の値を加算します。
	 *
	 * <p>マイクロ・ナノ秒精度の単位は未サポートです。</p>
	 *
	 * <p>演算に用いる時間の単位によっては、タイムゾーン設定の影響を受けない
	 * 場合があります。</p>
	 *
	 * @param timestamp 対象とする時刻
	 * @param amount 加算する値
	 * @param timeUnit 加算する値の単位
	 * @param zone 演算に用いるタイムゾーン設定、または、{@code null}
	 *
	 * @return 加算された値
	 *
	 * @throws NullPointerException {@code timestamp}、{@code timeUnit}に
	 * {@code null}が指定された場合
	 * @throws IllegalArgumentException 未サポートの{@code timeUnit}が指定された
	 * 場合
	 *
	 * @see #add(Date, int, TimeUnit)
	 *
	 * @since 4.3
	 * </div><div lang="en">
	 * Adds a fixed value to the time using the specified time zone setting.
	 *
	 * <p>If unsupported timeUnit is specified</p>
	 *
	 * <p>Depending on the unit of time used for calculation, the time zone
	 * setting may not be affected.</p>
	 *
	 * @param timestamp target time
	 * @param amount value to be added
	 * @param timeUnit unit of value to be added
	 * @param zone time zone setting used for calculation
	 *
	 * @return The added value
	 *
	 * @throws NullPointerException when {@code null} is specified for
	 * {@code timestamp}, {@code timeUnit}
	 * @throws IllegalArgumentException If unsupported {@code timeUnit} is specified
	 *
	 * @since 4.3
	 * </div>
	 */
	public static Date add(
			Date timestamp, int amount, TimeUnit timeUnit, TimeZone zone) {
		final Calendar calendar = createCalendar(zone);

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
		case MICROSECOND:
			break;
		case NANOSECOND:
			break;
		default:
			throw new Error("Internal error by unknown time unit (timeUnit=" +
					timeUnit + ")");
		}
		throw new IllegalArgumentException(
				"Unsupported time unit for the timestamp operation (" +
				"timeUnit=" + timeUnit + ")");
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
		return format(timestamp, null);
	}

	/**
	 * <div lang="ja">
	 * 指定のタイムゾーン設定を用い、TQLの通常精度のTIMESTAMP値表記に従って
	 * 時刻の文字列表現を求めます。
	 *
	 * @param timestamp 対象とする時刻
	 * @param zone 演算に用いるタイムゾーン設定、または、{@code null}
	 *
	 * @return 対応する文字列表現
	 *
	 * @throws NullPointerException {@code timestamp}引数に{@code null}が
	 * 指定された場合
	 *
	 * @see #format(Date)
	 *
	 * @since 4.3
	 * </div><div lang="en">
	 * Using the specified time zone setting, returns the string representation 
	 * of time, according to the normal-precision TIMESTAMP value notation of TQL.
	 *
	 * @param timestamp target time
	 * @param zone time zone setting used for calculation, or null
	 *
	 * @return corresponding string representation
	 *
	 * @throws NullPointerException If {@code null} is specified as a timestamp argument
	 *
	 * @see #format(Date)
	 *
	 * @since 4.3
	 * </div>
	 */
	public static String format(Date timestamp, TimeZone zone) {
		final DateFormat format = getFormat(zone);
		return format.format(checkTimestamp(timestamp));
	}

	/**
	 * <div lang="ja">
	 * 指定のタイムゾーン設定を用い、高精度のTIMESTAMP値表記に従って
	 * 時刻の文字列表現を求めます。
	 *
	 * @param timestamp 対象とする時刻
	 * @param zone 演算に用いるタイムゾーン設定、または、{@code null}
	 *
	 * @return 対応する文字列表現
	 *
	 * @throws NullPointerException {@code timestamp}引数に{@code null}が
	 * 指定された場合
	 *
	 * @see #format(Date)
	 *
	 * @since 5.3
	 * </div><div lang="en">
	 * Using the specified time zone setting, returns the string representation 
	 * of time, according to the high-precision TIMESTAMP value notation.
	 *
	 * @param timestamp the target time
	 * @param zone time zone setting used for calculation, or {@code null}
	 *
	 * @return the corresponding string representation
	 *
	 * @throws NullPointerException If {@code null} is specified as a 
	 * {@code timestamp} argument
	 *
	 * @see #format(Date)
	 *
	 * @since 5.3
	 * </div>
	 */
	public static String formatPrecise(Timestamp timestamp, TimeZone zone) {
		final DateFormat format = getFormat(zone, getDefaultPrecision(true));
		return format.format(checkTimestamp(timestamp));
	}

	/**
	 * <div lang="ja">
	 * TQLの通常精度のTIMESTAMP値表記に従い、指定の文字列に対応する
	 * {@link Date}を求めます。
	 *
	 * @param source 対象とする時刻の文字列表現
	 *
	 * @return 指定の文字列に対応する{@link Date}
	 *
	 * @throws ParseException 時刻の文字列表記と一致しない文字列が指定された場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Returns {@link Date} corresponding to the specified string, according 
	 * to the normal-precision TIMESTAMP value notation of TQL.
	 *
	 * @param source string representation of target time
	 *
	 * @return {@link Date} object corresponding to the specified string
	 *
	 * @throws ParseException if the specified string does not match any time representation.
	 * @throws NullPointerException when {@code null} is specified as argument
	 * </div>
	 */
	public static Date parse(String source) throws ParseException {
		return getFormat().parse(checkSource(source));
	}

	/**
	 * <div lang="ja">
	 * 通常精度もしくは高精度のTIMESTAMP値表記に従い、指定の文字列に
	 * 対応する{@link Timestamp}を求めます。
	 *
	 * @param source 対象とする時刻の文字列表現
	 *
	 * @return 指定の文字列に対応する{@link Timestamp}
	 *
	 * @throws ParseException 時刻の文字列表記と一致しない文字列が指定された場合
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * @since 5.3
	 * </div><div lang="en">
	 * Returns {@link Timestamp} corresponding to the specified string, according 
	 * to the normal-precision or high-precision TIMESTAMP value notation.
	 *
	 * @param source string representation of the target time
	 *
	 * @return {@link Timestamp} corresponding to the specified string
	 *
	 * @throws ParseException If a string that does not match the string notation of time.
	 * @throws NullPointerException If {@code null} is specified as an argument
	 * @since 5.3
	 * </div>
	 */
	public static Timestamp parsePrecise(String source) throws ParseException {
		final DateFormat format = getFormat(null, getDefaultPrecision(true));
		return (Timestamp) format.parse(checkSource(source));
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
	 * Returns the date format conforming to the TIMESTAMP value notation of TQL.
	 *
	 * <p>The current version always uses the UTC timezone.</p>
	 *
	 * @return date format
	 *
	 * <p>The time representation containing a negative year value is not supported.</p>
	 * </div>
	 */
	public static DateFormat getFormat() {
		return getFormat(null);
	}

	/**
	 * <div lang="ja">
	 * 指定のタイムゾーン設定が適用され、TQLのTIMESTAMP値表記と対応する、
	 * 日付フォーマットを取得します。
	 *
	 * @param zone 適用対象のタイムゾーン設定、または、{@code null}
	 *
	 * @return 日付フォーマット
	 *
	 * @since 4.3
	 * </div><div lang="en">
	 * Using the specified time zone setting, returns the string representing the
	 * specified time, according to the TIMESTAMP value notation of TQL.
	 *
	 * @param zone applicable time zone setting, or {@code null}
	 *
	 * @return date format
	 *
	 * @throws NullPointerException when {@code null} is specified as argument
	 *
	 * @since 4.3
	 * </div>
	 */
	public static DateFormat getFormat(TimeZone zone) {
		return getFormat(zone, null);
	}

	/**
	 * <div lang="ja">
	 * 指定のタイムゾーン設定・日時精度が適用され、TQLのTIMESTAMP値表記と
	 * 対応する、日付フォーマットを取得します。
	 *
	 * <p>日時精度の指定は、次のいずれかのみをサポートします。</p>
	 * <ul>
	 * <li>{@link TimeUnit#SECOND}</li>
	 * <li>{@link TimeUnit#MILLISECOND}</li>
	 * <li>{@link TimeUnit#MICROSECOND}</li>
	 * <li>{@link TimeUnit#NANOSECOND}</li>
	 * <li>{@code null} ({@link TimeUnit#MILLISECOND}が指定されたものとして
	 * 解釈)</li>
	 * </ul>
	 *
	 * @param zone 適用対象のタイムゾーン設定、または、{@code null}
	 * @param timePrecision 適用対象の日時精度、または、{@code null}
	 *
	 * @return 日付フォーマット
	 *
	 * @throws IllegalArgumentException 未サポートの日時精度が指定された場合
	 *
	 * @since 5.3
	 * </div><div lang="en">
	 * Using the specified time zone setting and date/time precision, retrieves 
	 * the date format according to the TIMESTAMP value notation of TQL.
	 *
	 * <p>Specification of date/time precision is supported only in the following cases.</p>
	 * <ul>
	 * <li>{@link TimeUnit#SECOND}</li>
	 * <li>{@link TimeUnit#MILLISECOND}</li>
	 * <li>{@link TimeUnit#MICROSECOND}</li>
	 * <li>{@link TimeUnit#NANOSECOND}</li>
	 * <li>{@code null} (interpreted as if {@link TimeUnit#MILLISECOND} 
	 * is specified)</li>
	 * </ul>
	 *
	 * @param zone applicable time zone setting, or {@code null}
	 * @param timePrecision applicable date/time precision, or {@code null}
	 *
	 * @return date format
	 *
	 * @throws IllegalArgumentException If unsupported date/time precision is specified
	 *
	 * @since 5.3
	 * </div>
	 */
	public static DateFormat getFormat(TimeZone zone, TimeUnit timePrecision) {
		return new CustomDateFormat(
				resolveTimeZone(zone), resolvePrecision(timePrecision));
	}

	private static Date checkTimestamp(Date timestamp) {
		GSErrorCode.checkNullParameter(timestamp, "timestamp", null);
		return timestamp;
	}

	private static String checkSource(String source) {
		GSErrorCode.checkNullParameter(source, "source", null);
		return source;
	}

	private static TimeZone resolveTimeZone(TimeZone base) {
		if (base == null) {
			return createDefaultTimeZone();
		}
		return base;
	}

	private static TimeUnit resolvePrecision(TimeUnit base) {
		if (base == null) {
			return getDefaultPrecision(false);
		}
		return base;
	}

	private static Calendar createCalendar(TimeZone zone) {
		return Calendar.getInstance(resolveTimeZone(null), Locale.ROOT);
	}

	private static TimeZone createDefaultTimeZone() {
		return PropertyUtils.createTimeZoneOffset(0);
	}

	private static TimeUnit getDefaultPrecision(boolean precise) {
		if (precise) {
			return TimeUnit.NANOSECOND;
		}
		else {
			return TimeUnit.MILLISECOND;
		}
	}

	private static class CustomDateFormat extends DateFormat {

		private static final long serialVersionUID = -5296445605735800942L;

		private static final int MILLIS_DOT_MIN_POSITION = 19;

		private static final Pattern FRACTION_PATTERN =
				Pattern.compile("[.][0-9]{3}([0-9]{6}|[0-9]{3})");

		private final TimeZone zone;

		private final TimeUnit timePrecision;

		private DateFormat secondsFormat;

		private DateFormat millisFormat;

		CustomDateFormat(TimeZone zone, TimeUnit timePrecision) {
			checkTimeZone(zone);
			checkTimePrecision(timePrecision);
			this.zone = (TimeZone) zone.clone();
			this.timePrecision = timePrecision;
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

		private DateFormat getBaseOutputFormat(TimeZone zone) {
			if (isFractionAllowed()) {
				return getMillisFormat(zone);
			}
			else {
				return getSecondsFormat(zone);
			}
		}

		private DateFormat getBaseInputFormat(String source, TimeZone zone) {
			DateFormat format;
			if (source.length() > MILLIS_DOT_MIN_POSITION &&
					source.indexOf('.', MILLIS_DOT_MIN_POSITION) >= 0 &&
					isFractionAllowed()) {
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
			final StringBuffer sb = getBaseOutputFormat(zone).format(
					date, toAppendTo, fieldPosition);
			if (checkTimePrecision(timePrecision)) {
				formatPrecisePart(date, sb);
			}
			sb.append(PropertyUtils.formatTimeZoneOffset(
					zone.getRawOffset(), false));
			return sb;
		}

		@Override
		public Date parse(String source, ParsePosition pos) {
			final String[] elems = splitToElements(source);
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

			final DateFormat baseFormat = getBaseInputFormat(source, zone);
			final Date date = parseStrict(baseFormat, elems[0], pos);
			if (checkTimePrecision(timePrecision)) {
				return parsePrecisePart(date, elems[2]);
			}
			return date;
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

		private static void checkTimeZone(TimeZone zone) {
			if (zone.useDaylightTime()) {
				throw new IllegalArgumentException(
						"Daylight time is currently not supported " +
						"(timeZone=" + zone.getDisplayName(Locale.ROOT) + ")");
			}
		}

		private static boolean checkTimePrecision(TimeUnit timePrecision) {
			switch (timePrecision) {
			case SECOND:
				return false;
			case MILLISECOND:
				return false;
			case MICROSECOND:
				return true;
			case NANOSECOND:
				return true;
			default:
				throw new IllegalArgumentException(
						"Illegal time precision (precision=" + timePrecision +")");
			}
		}

		private boolean isFractionAllowed() {
			switch (timePrecision) {
			case SECOND:
				return false;
			default:
				return true;
			}
		}

		private String[] splitToElements(String str) {
			final Matcher matcher =
					PropertyUtils.getTimeZoneOffsetPattern().matcher(str);

			final int zonePos;
			final String zonePart;
			if (matcher.find() && matcher.end() == str.length()) {
				zonePos = matcher.start();
				zonePart = matcher.group();
			}
			else {
				final String utcZone = "Z";
				if (!str.endsWith(utcZone)) {
					return null;
				}
				zonePos = str.length() - utcZone.length();
				zonePart = utcZone;
			}

			final int precisePos = findPrecisePosition(str, zonePos);
			final String mainPart;
			final String precisePart;
			if (precisePos < 0) {
				mainPart = str.substring(0, zonePos);
				precisePart = null;
			}
			else {
				mainPart = str.substring(0, precisePos);
				precisePart = str.substring(precisePos, zonePos);
			}
			return new String[] { mainPart, zonePart, precisePart };
		}

		private int findPrecisePosition(String str, int zonePos) {
			final int maxLength;
			switch (timePrecision) {
			case MICROSECOND:
				maxLength = 3;
				break;
			case NANOSECOND:
				maxLength = 6;
				break;
			default:
				return -1;
			}

			final Matcher matcher = FRACTION_PATTERN.matcher(str);
			if (!matcher.find() || matcher.end() != zonePos ||
					matcher.end(1) - matcher.start(1) > maxLength) {
				return -1;
			}

			return matcher.start(1);
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

		private void formatPrecisePart(Date date, StringBuffer sb) {
			final int nanos;
			if (date instanceof Timestamp) {
				nanos = ((Timestamp) date).getNanos() %  (1000 * 1000);
			}
			else {
				nanos = 0;
			}

			final int unit;
			final String format;
			switch (timePrecision) {
			case MICROSECOND:
				unit = 1000;
				format = "%03d";
				break;
			case NANOSECOND:
				unit = 1;
				format = "%06d";
				break;
			default:
				throw new Error();
			}

			new Formatter(sb, Locale.ROOT).format(format, nanos / unit);
		}

		private static Timestamp parsePrecisePart(
				Date mainDate, String precisePart) {
			if (mainDate == null) {
				return null;
			}

			final long timeMillis = mainDate.getTime();
			int nanos = (int) (timeMillis % 1000) * (1000 * 1000);
			if (precisePart != null) {
				int sub;
				try {
					sub = Integer.parseInt(precisePart);
				}
				catch (NumberFormatException e) {
					throw new IllegalArgumentException(e);
				}
				if (precisePart.length() <= 3) {
					sub *= 1000;
				}
				nanos += sub;
			}

			final Timestamp ts = new Timestamp(timeMillis);
			ts.setNanos(nanos);
			return ts;
		}

	}

}
