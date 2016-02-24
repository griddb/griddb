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
package com.toshiba.mwcloud.gs.common;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

import com.toshiba.mwcloud.gs.GridStoreFactory;

public class LoggingUtils {

	public static final String DEFAULT_LOGGER_NAME =
			"com.toshiba.mwcloud.gs.GridStoreLogger";

	public static final String[] SUB_LOGGER_NAMES = {
		"Config",
		"Heartbeat",
		"Discovery",
		"Connection",
		"Transaction",
		"StatementIO",
		"StatementExec"
	};

	private static final Map<String, BaseGridStoreLogger> SUB_LOGGERS =
			Collections.synchronizedMap(
					new HashMap<String, BaseGridStoreLogger>());

	private static final BaseGridStoreLogger EMPTY_LOGGER =
			new BaseGridStoreLogger() {
				public void debug(String format, Object... arguments) {}
				public void info(String format, Object... arguments) {}
				public void warn(String format, Object... arguments) {}
				public boolean isDebugEnabled() { return false; }
				public boolean isInfoEnabled() { return false; }
				public boolean isWarnEnabled() { return false; }
	};

	private static final ResourceBundle BUNDLE =
			ResourceBundle.getBundle(LoggingUtils.class.getName());

	static {
		GridStoreFactory.getInstance();
	}

	public static abstract class BaseGridStoreLogger {
		public abstract void debug(String key, Object... args);
		public abstract void info(String key, Object... args);
		public abstract void warn(String key, Object... args);
		public abstract boolean isDebugEnabled();
		public abstract boolean isInfoEnabled();
		public abstract boolean isWarnEnabled();
	}

	public static void registerLogger(
			String subName, BaseGridStoreLogger logger) {
		if (SUB_LOGGERS.containsKey(subName)) {
			throw new Error();
		}
		SUB_LOGGERS.put(subName, (logger == null ? EMPTY_LOGGER : logger));
	}

	public static BaseGridStoreLogger getLogger(String subName) {
		final BaseGridStoreLogger logger = SUB_LOGGERS.get(subName);
		if (logger == null) {
			throw new Error();
		}
		return logger;
	}

	public static String getFormatString(String key) {
		return BUNDLE.getString(key);
	}

}
