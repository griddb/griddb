/*
   Copyright (c) 2019 TOSHIBA Digital Solutions Corporation

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
package com.toshiba.mwcloud.gs.call_logging;

import java.lang.reflect.Method;
import java.util.List;
import java.util.ResourceBundle;

public class CallLoggerLoggingUtils {

	private static final ResourceBundle CALL_LOGGING_BUNDLE =
			ResourceBundle.getBundle(CallLoggerLoggingUtils.class.getName());

	private static final BaseCallLoggingLogger CALL_LOGGER = new CallLoggingLogger();

	private static final LoggingTargetManager loggingTargetManager = new LoggingTargetManager();

	public static String getFormatString(String key) {
		return CALL_LOGGING_BUNDLE.getString(key);
	}

	public static abstract class BaseCallLoggingLogger {
		public abstract void debug(String key, Object... args);
		public abstract void info(String key, Object... args);
		public abstract void warn(String key, Object... args);
		public abstract boolean isDebugEnabled();
		public abstract boolean isInfoEnabled();
		public abstract boolean isWarnEnabled();
	}

	public static BaseCallLoggingLogger getLogger() {
		return CALL_LOGGER;
	}

	public static CallLogInfo getLogInfo(Class<?> cls, Method method) {
		return loggingTargetManager.getLogInfo(cls, method);
	}

	public static String stringListToString(List<String> values) {
		StringBuilder builder = new StringBuilder();
		builder.append("{");
		boolean first = true;
		for (String val : values) {
			if (first) {
				first = false;
			} else {
				builder.append(", ");
			}
			builder.append(val);
		}
		builder.append("}");
		return builder.toString();
	}

}