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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.common.ContainerKeyConverter.ContainerKey;
import com.toshiba.mwcloud.gs.common.Extensibles.AsQuery;
import com.toshiba.mwcloud.gs.common.Extensibles.QueryInfo;

public abstract class CallLogInfo {

	public abstract Object[] getLogOutputValues(String pid, Object[] args) ;

	public abstract boolean isLogOutput ();

	public abstract String getMsgKey ();

	public static class CallLogInfoNoLog extends CallLogInfo {

		@Override
		public String getMsgKey () {
			return "";
		}

		@Override
		public Object[] getLogOutputValues(String pid, Object[] args) {
			return null;
		}

		@Override
		public boolean isLogOutput() {
			return false;
		}

	}

	public static class CallLogInfoMultiGet extends CallLogInfo {

		@Override
		public String getMsgKey () {
			return "callLog.multiGet";
		}

		@Override
		public Object[] getLogOutputValues(String pid, Object[] args) {
			ArrayList<Object> retValues = new ArrayList<Object>();
			retValues.add(pid);
			if (args.length > 0) {
				try {
					Map<?, ?> map = (Map<?, ?>)args[0];
					Set<?> containerNames = map.keySet();
					ArrayList<String> containerNameList = new ArrayList<String>();

					int max = 10;
					int count = 0;
					for (Object containerName: containerNames) {
						if (count < max) {
							containerNameList.add(containerName.toString());
							count++;
						} else {
							int otherCount = containerNames.size() - max;
							containerNameList.add("... " + otherCount + " containers omitted");
							break;
						}
					}
					retValues.add(CallLoggerLoggingUtils.stringListToString(containerNameList));
				} catch (Exception e) {
				}
			}
			return retValues.toArray();
		}

		@Override
		public boolean isLogOutput() {
			return true;
		}

	}

	public static class CallLogInfoMultiPut extends CallLogInfo {

		@Override
		public String getMsgKey () {
			return "callLog.multiPut";
		}

		@Override
		public Object[] getLogOutputValues(String pid, Object[] args) {
			ArrayList<Object> retValues = new ArrayList<Object>();
			retValues.add(pid);
			if (args.length > 0) {
				try {
					Map<?, ?> map = (Map<?, ?>)args[0];
					Set<?> containerNames = map.keySet();
					ArrayList<String> containerNameList = new ArrayList<String>();

					int max = 10;
					int count = 0;
					for (Object containerName: containerNames) {
						if (count < max) {
							containerNameList.add(containerName.toString());
							count++;
						} else {
							int otherCount = containerNames.size() - max;
							containerNameList.add("... " + otherCount + " containers omitted");
							break;
						}
					}
					retValues.add(CallLoggerLoggingUtils.stringListToString(containerNameList));
				} catch (Exception e) {
				}
			}
			return retValues.toArray();
		}

		@Override
		public boolean isLogOutput() {
			return true;
		}

	}

	public static class CallLogInfoFetchAll extends CallLogInfo {

		@Override
		public String getMsgKey () {
			return "callLog.fetchAll";
		}

		@Override
		public Object[] getLogOutputValues(String pid, Object[] args) {
			ArrayList<Object> retValues = new ArrayList<Object>();
			retValues.add(pid);
			ArrayList<String> queryValStrList = new ArrayList<String>();
			if (args.length > 0 && args[0] instanceof List) {
				try {
					int max = 10;
					int count = 0;

					@SuppressWarnings("unchecked")
					List<? extends Query<?>> queryList = (List<? extends Query<?>>)args[0];
					for (Query<?> query: queryList) {

						String containerName = null;
						String queryStr = null;

						if (query instanceof AsQuery) {
							AsQuery<?> asQuery = (AsQuery<?>)query;

							ContainerKey containerKey = asQuery.getContainer().getKey();
							containerName = containerKey.toString();

							QueryInfo queryInfo = asQuery.getInfo();
							if (queryInfo != null) {
								queryStr = queryInfo.getQueryString();
							} else {
								queryStr = query.toString();
							}
						} else {
							queryStr = query.toString();
						}

						if (count < max) {
							String queryValStr = "{container=" + containerName + ", query=" + queryStr + "}";
							queryValStrList.add(queryValStr);
							count++;
						} else {
							int otherCount = queryList.size() - max;
							String queryValStr = "... " + otherCount + " queries omitted";
							queryValStrList.add(queryValStr);
							break;
						}
					}
					retValues.add(CallLoggerLoggingUtils.stringListToString(queryValStrList));
				} catch (Exception e) {
				}
			}
			return retValues.toArray();
		}

		@Override
		public boolean isLogOutput() {
			return true;
		}

	}

	public static class CallLogInfoSetFetchOption extends CallLogInfo {

		@Override
		public String getMsgKey () {
			return "callLog.setFetchOption";
		}

		@Override
		public Object[] getLogOutputValues(String pid, Object[] args) {
			ArrayList<Object> retValues = new ArrayList<Object>();
			retValues.add(pid);
			if (args.length > 1) {
				retValues.add(args[0]);
				retValues.add(args[1]);
			}
			return retValues.toArray();
		}

		@Override
		public boolean isLogOutput() {
			return true;
		}

	}

}
