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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.toshiba.mwcloud.gs.common.LoggingUtils;
import com.toshiba.mwcloud.gs.common.LoggingUtils.BaseGridStoreLogger;

abstract class GridStoreLogger extends BaseGridStoreLogger {

	private static int SUITABLE_LOGGER_MAJOR = 1;

	private static int SUITABLE_LOGGER_MINOR = 6;

	final Logger base;

	GridStoreLogger() {
		LoggerFactory.getLogger(GridStoreLogger.class);
		base = LoggerFactory.getLogger(getClass().getName().replace('$', '.'));
	}

	public boolean isDebugEnabled() {
		return base.isDebugEnabled();
	}

	public boolean isInfoEnabled() {
		return base.isInfoEnabled();
	}

	public boolean isWarnEnabled() {
		return base.isWarnEnabled();
	}

	static class Config extends GridStoreLogger {
		Config() {
			checkVersion(this);
		}
		public void debug(String key, Object... args) { base.debug(fmt(key), args); }
		public void info(String key, Object... args) { base.info(fmt(key), args); }
		public void warn(String key, Object... args) { base.warn(fmt(key), args); }
	}

	static class Heartbeat extends GridStoreLogger {
		public void debug(String key, Object... args) { base.debug(fmt(key), args); }
		public void info(String key, Object... args) { base.info(fmt(key), args); }
		public void warn(String key, Object... args) { base.warn(fmt(key), args); }
	}

	static class Discovery extends GridStoreLogger {
		public void debug(String key, Object... args) { base.debug(fmt(key), args); }
		public void info(String key, Object... args) { base.info(fmt(key), args); }
		public void warn(String key, Object... args) { base.warn(fmt(key), args); }
	}

	static class Connection extends GridStoreLogger {
		public void debug(String key, Object... args) { base.debug(fmt(key), args); }
		public void info(String key, Object... args) { base.info(fmt(key), args); }
		public void warn(String key, Object... args) { base.warn(fmt(key), args); }
	}

	static class Transaction extends GridStoreLogger {
		public void debug(String key, Object... args) { base.debug(fmt(key), args); }
		public void info(String key, Object... args) { base.info(fmt(key), args); }
		public void warn(String key, Object... args) { base.warn(fmt(key), args); }
	}

	static class StatementIO extends GridStoreLogger {
		public void debug(String key, Object... args) { base.debug(fmt(key), args); }
		public void info(String key, Object... args) { base.info(fmt(key), args); }
		public void warn(String key, Object... args) { base.warn(fmt(key), args); }
	}

	static class StatementExec extends GridStoreLogger {
		public void debug(String key, Object... args) { base.debug(fmt(key), args); }
		public void info(String key, Object... args) { base.info(fmt(key), args); }
		public void warn(String key, Object... args) { base.warn(fmt(key), args); }
	}

	private static String fmt(String key) {
		return LoggingUtils.getFormatString(key);
	}

	private static void checkVersion(BaseGridStoreLogger logger) {
		BufferedReader reader = null;
		try {
			final Pattern versionPattern = Pattern.compile(
					"^Bundle-Version: (([0-9]+)\\.([0-9]+)\\.[0-9]+)");
			for (URL url : Collections.list(
					LoggerFactory.class.getClassLoader().getResources(
							"META-INF/MANIFEST.MF"))) {
				reader = new BufferedReader(
						new InputStreamReader(url.openStream(), "UTF-8"));
				boolean libraryFound = false;
				String version = null;
				boolean versionSuitable = false;
				for (String line; (line = reader.readLine()) != null;) {
					do {
						if (line.equals("Bundle-SymbolicName: slf4j.api")) {
							libraryFound = true;
							break;
						}

						final Matcher matcher = versionPattern.matcher(line);
						if (!matcher.find()) {
							break;
						}

						version = matcher.group(1);
						final int major = Integer.parseInt(matcher.group(2));
						final int minor = Integer.parseInt(matcher.group(3));

						if (major > SUITABLE_LOGGER_MAJOR ||
								major == SUITABLE_LOGGER_MAJOR &&
								minor >= SUITABLE_LOGGER_MINOR) {
							versionSuitable = true;
						}
					}
					while (false);

					if (libraryFound && version != null) {
						break;
					}
				}

				if (libraryFound && version != null && !versionSuitable) {
					logger.warn("config.slf4jVersionNotSuitable",
							SUITABLE_LOGGER_MAJOR + "." +
							SUITABLE_LOGGER_MINOR + ".0", version);
				}
				reader.close();
				reader = null;
			}
		}
		catch (IOException e) {
			logger.warn("config.slf4jVersionCheckFailed", e);
		}
		finally {
			if (reader != null) {
				try {
					reader.close();
				}
				catch (IOException e) {
				}
			}
		}
	}

}
