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
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

/**
 * @deprecated
 */
@Deprecated
public class TimestampUtil {

	/**
	 * @deprecated
	 */
	@Deprecated
	public static Date current() {
		return TimestampUtils.current();
	}

	/**
	 * @deprecated
	 */
	@Deprecated
	public static Calendar currentCalendar() {
		return TimestampUtils.currentCalendar();
	}

	/**
	 * @deprecated
	 */
	@Deprecated
	public static Date add(Date timeStamp, int amount, TimeUnit timeUnit) {
		return TimestampUtils.add(timeStamp, amount, timeUnit);
	}

	/**
	 * @deprecated
	 */
	@Deprecated
	public static String format(Date timeStamp) {
		return TimestampUtils.format(timeStamp);
	}

	/**
	 * @deprecated
	 */
	@Deprecated
	public static Date parse(String source) throws ParseException {
		return TimestampUtils.parse(source);
	}

	/**
	 * @deprecated
	 */
	@Deprecated
	public static DateFormat getFormat() {
		return TimestampUtils.getFormat();
	}

}
