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
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

/**
 * <div lang="ja">
 * @deprecated {@link TimestampUtils}に置き換えられました。
 * </div><div lang="en">
 * @deprecated
 * </div>
 */
@Deprecated
public class TimestampUtil {

	@Deprecated
	public TimestampUtil() {
	}

	/**
	 * <div lang="ja">
	 * @deprecated {@link TimestampUtils#current()}に置き換えられました。
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	public static Date current() {
		return TimestampUtils.current();
	}

	/**
	 * <div lang="ja">
	 * @deprecated {@link TimestampUtils#currentCalendar()}に置き換えられました。
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	public static Calendar currentCalendar() {
		return TimestampUtils.currentCalendar();
	}

	/**
	 * <div lang="ja">
	 * @deprecated {@link TimestampUtils#add(Date, int, TimeUnit)}
	 * に置き換えられました。
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	public static Date add(Date timeStamp, int amount, TimeUnit timeUnit) {
		return TimestampUtils.add(timeStamp, amount, timeUnit);
	}

	/**
	 * <div lang="ja">
	 * @deprecated {@link TimestampUtils#format(Date)}に置き換えられました。
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	public static String format(Date timeStamp) {
		return TimestampUtils.format(timeStamp);
	}

	/**
	 * <div lang="ja">
	 * @deprecated {@link TimestampUtils#parse(String)}に置き換えられました。
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	public static Date parse(String source) throws ParseException {
		return TimestampUtils.parse(source);
	}

	/**
	 * <div lang="ja">
	 * @deprecated {@link TimestampUtils#getFormat()}に置き換えられました。
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	public static DateFormat getFormat() {
		return TimestampUtils.getFormat();
	}

}
