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
import java.util.ArrayList;
import java.util.List;

import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.Query;

public class LoggingTargetManager {

	private List<LogggingTargetInfo> targetInfoList;
	private CallLogInfo NOLOGINFO = new CallLogInfo.CallLogInfoNoLog();

	LoggingTargetManager() {
		targetInfoList = new ArrayList<LogggingTargetInfo>();

		LogggingTargetInfo targetInfo;
		targetInfo = new LogggingTargetInfo(GridStore.class, "multiGet", new CallLogInfo.CallLogInfoMultiGet());
		targetInfoList.add(targetInfo);
		targetInfo = new LogggingTargetInfo(GridStore.class, "multiPut", new CallLogInfo.CallLogInfoMultiPut());
		targetInfoList.add(targetInfo);
		targetInfo = new LogggingTargetInfo(GridStore.class, "fetchAll", new CallLogInfo.CallLogInfoFetchAll());
		targetInfoList.add(targetInfo);
		targetInfo = new LogggingTargetInfo(Query.class, "setFetchOption", new CallLogInfo.CallLogInfoSetFetchOption());
		targetInfoList.add(targetInfo);
	}

	public CallLogInfo getLogInfo(Class<?> cls, Method method) {
		String checkMethodName = method.getName();

		for (LogggingTargetInfo targetInfo:targetInfoList) {

			boolean classDecision = targetInfo.getCls().isAssignableFrom(cls);
			boolean methodDecision = targetInfo.getMethodName().equals(checkMethodName);

			if (classDecision && methodDecision) {
				return targetInfo.getLogInfo();
			}
		}
		return NOLOGINFO;
	}

	private class LogggingTargetInfo {
		Class<?> cls;
		String methodName;

		CallLogInfo logInfo;

		LogggingTargetInfo(Class<?> cls, String methodName, CallLogInfo logInfo) {
			this.cls = cls;
			this.methodName = methodName;
			this.logInfo = logInfo;
		}

		public Class<?> getCls() {
			return cls;
		}
		public String getMethodName() {
			return methodName;
		}
		public CallLogInfo getLogInfo() {
			return logInfo;
		}
	}


}
