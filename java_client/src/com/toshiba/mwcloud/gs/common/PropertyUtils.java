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
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.toshiba.mwcloud.gs.GSException;

public class PropertyUtils {

	private PropertyUtils() {
	}

	public static int timeoutPropertyToIntSeconds(long value) {
		if (value < 0) {
			return -1;
		}
		else if (value >= (long) Integer.MAX_VALUE * 1000) {
			return Integer.MAX_VALUE;
		}
		else {
			return (int) (value / 1000);
		}
	}

	public static int timeoutPropertyToIntMillis(long value) {
		if (value < 0) {
			return -1;
		}
		else if (value >= Integer.MAX_VALUE) {
			return Integer.MAX_VALUE;
		}
		else {
			return (int) value;
		}
	}

	public static void checkExclusiveProperties(
			Properties props, String ...nameList) throws GSException {
		int count = 0;
		for (final String name : nameList) {
			if (props.containsKey(name)) {
				count++;
			}
		}

		if (count > 1) {
			final StringBuilder builder = new StringBuilder();

			builder.append("Either one of following properties ");
			builder.append("can be specified (");

			boolean found = false;
			for (final String name : nameList) {
				if (props.containsKey(name)) {
					if (!found) {
						builder.append(", ");
						found = true;
					}
					builder.append(name);
					builder.append("=");
					builder.append(props.getProperty(name));
				}
			}

			builder.append(")");

			throw new GSException(
					GSErrorCode.ILLEGAL_PROPERTY_ENTRY, builder.toString());
		}
	}

	public static class WrappedProperties {

		private final Properties base;

		private final Map<String, Boolean> visited =
				new HashMap<String, Boolean>();

		public WrappedProperties(Properties base) {
			this.base = base;
		}

		public Properties getBase() {
			return base;
		}

		public Long getTimeoutProperty(String name, Long defaultValue,
				boolean deprecated)
				throws GSException {
			final String millisecName = name + "Millis";
			final Integer secValue = getIntProperty(name, deprecated);
			final Integer millisecValue = getIntProperty(millisecName, true);

			if (secValue != null && millisecValue != null) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PROPERTY_ENTRY,
						"Property name conflicted: \"" +
						name + "\", \"" + millisecName + "\"");
			}

			final Long value;
			if (secValue != null) {
				if (secValue == Integer.MAX_VALUE) {
					value = Long.MAX_VALUE;
				}
				else {
					value = (long) secValue * 1000;
				}
			}
			else if (millisecValue != null) {
				value = (long) millisecValue;
			}
			else {
				value = defaultValue;
			}

			if (value != null && value < 0) {
				return (long) -1;
			}
			return value;
		}

		public Integer getIntProperty(
				String name, boolean deprecated) throws GSException {
			final String strValue = getProperty(name, deprecated);
			if (strValue == null) {
				return null;
			}

			try {
				return Integer.valueOf(strValue);
			}
			catch (NumberFormatException e) {
				throw new GSException(
						GSErrorCode.ILLEGAL_VALUE_FORMAT,
						"Failed to parse as integer (value=" + strValue +
						", propertyName=" + name +
						", reason=" + e.getMessage() + ")", e);
			}
		}

		public Boolean getBooleanProperty(
				String name, Boolean defaultValue,
				boolean deprecated) throws GSException {
			final String strValue = getProperty(name, deprecated);
			if (strValue == null) {
				return defaultValue;
			}

			return Boolean.valueOf(strValue);
		}

		public String getProperty(String name, boolean deprecated) {
			return getProperty(name, null, deprecated);
		}

		public String getProperty(String name, String defaultValue,
				boolean deprecated) {
			final String value = base.getProperty(name, defaultValue);
			visited.put(name, (value == null ? false : deprecated));

			return (value == null ? defaultValue : value);
		}

		public Set<String> getUnknowNames() {
			final Set<String> names = new HashSet<String>(
					base.stringPropertyNames());
			names.removeAll(visited.keySet());

			return names;
		}

		public Set<String> getDeprecatedNames() {
			Set<String> names = Collections.emptySet();
			for (Map.Entry<String, Boolean> entry : visited.entrySet()) {
				if (entry.getValue()) {
					if (names.isEmpty()) {
						names = new HashSet<String>();
					}
					names.add(entry.getKey());
				}
			}
			return names;
		}

	}

}
