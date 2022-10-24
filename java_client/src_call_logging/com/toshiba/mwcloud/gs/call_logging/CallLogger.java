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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import com.toshiba.mwcloud.gs.call_logging.CallLoggerLoggingUtils.BaseCallLoggingLogger;
import com.toshiba.mwcloud.gs.common.GSInterceptorProvider;

class CallLogger extends GSInterceptorProvider.Chain {

	private static final BaseCallLoggingLogger CALL_LOGGER =
			CallLoggerLoggingUtils.getLogger();

	private static final String PID = java.lang.management.ManagementFactory.getRuntimeMXBean().getName().split("@")[0];

	CallLogger(GSInterceptorProvider.Chain chain) {
		super(chain);
	}

	@Override
	public Object invoke(
			Class<?> targetType, Object target, Method method, Object[] args)
			throws IllegalAccessException, InvocationTargetException {
		logStarted(targetType, target, method, args);

		final Object returnObj;
		try {
			returnObj = super.invokeDefault(targetType, target, method, args);
		}
		catch (InvocationTargetException e) {
			logFailed(targetType, target, method, args, e.getCause());
			throw e;
		}

		logFinished(targetType, target, method, args);
		return returnObj;
	}

	private void logStarted(
			Class<?> targetType, Object target, Method method, Object[] args) {
		if (CALL_LOGGER.isDebugEnabled()) {
			CallLogInfo loginfo = CallLoggerLoggingUtils.getLogInfo(targetType, method);
			if (loginfo.isLogOutput()) {
				CALL_LOGGER.debug(loginfo.getMsgKey(), loginfo.getLogOutputValues(PID, args));
			}
		}
	}

	private void logFinished(
			Class<?> targetType, Object target, Method method, Object[] args) {
	}

	private void logFailed(
			Class<?> targetType, Object target, Method method, Object[] args,
			Throwable cause) {
	}

}
