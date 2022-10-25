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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.toshiba.mwcloud.gs.call_logging.CallLoggerLoggingUtils.BaseCallLoggingLogger;

public class CallLoggingLogger extends BaseCallLoggingLogger {

	final Logger base;

	CallLoggingLogger() {
		base = LoggerFactory.getLogger("com.toshiba.mwcloud.gs.GridStoreLogger.ApiCall");
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

	public void debug(String key, Object... args) {
		base.debug(fmt(key),args);
	}

	public void info(String key, Object... args) {
		base.info(fmt(key),args);
	}

	public void warn(String key, Object... args) {
		base.warn(fmt(key),args);
	}

	private String fmt(String key) {
		return CallLoggerLoggingUtils.getFormatString(key);
	}

}
