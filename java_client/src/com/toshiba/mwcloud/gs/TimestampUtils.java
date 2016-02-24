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
 * Provides the utilities for manipulating time data.
 */
public class TimestampUtils {

	private static final boolean TRIM_MILLISECONDS = false;

	/**
	 * Returns the current time.
	 */
	public static Date current() {
		return currentCalendar().getTime();
	}

	/**
	 * Returns the current time as a {@link Calendar} object.
	 *
	 * <p>The current version always uses the UTC timezone.</p>
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
	 * Adds a specific value to the specified time.
	 *
	 * <p>An earlier time than the specified time can be obtained by specifying a negative value as {@code amount}.</p>
	 *
	 * <p>The current version uses the UTC timezone for calculation.</p>
	 */
	public static Date add(Date timeStamp, int amount, TimeUnit timeUnit) {
		final Calendar calendar = currentCalendar();

		try {
			calendar.setTime(timeStamp);
			calendar.add(toCalendarField(timeUnit), amount);
		}
		catch (NullPointerException e) {
			GSErrorCode.checkNullParameter(timeStamp, "timeStamp", e);
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
	 * Returns the string representing the specified time, according to the TIMESTAMP value notation of TQL.
	 *
	 * <p>The current version uses the UTC timezone for conversion.</p>
	 */
	public static String format(Date timeStamp) {
		try {
			return getFormat().format(timeStamp);
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(timeStamp, "timeStamp", e);
		}
	}

	/**
	 * Returns a {@link Date} object corresponding to the specified string, according to the TIMESTAMP value notation of TQL.
	 *
	 * @throws ParseException if the specified string does not match any time representation.
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
	 * Returns the date format conforming to the TIMESTAMP value notation of TQL.
	 *
	 * <p>The time representation containing a negative year value is not supported.</p>
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
