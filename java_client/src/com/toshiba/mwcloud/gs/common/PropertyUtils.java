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
package com.toshiba.mwcloud.gs.common;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SimpleTimeZone;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.toshiba.mwcloud.gs.GSException;

public class PropertyUtils {

	private static final Pattern TIME_ZONE_OFFSET_PATTERN =
			Pattern.compile("([+\\-])([0-9]{2}):?([0-9]{2})");

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

	public static SimpleTimeZone parseTimeZoneOffset(
			String str, boolean withAuto) throws GSException {
		if (str.equals("Z")) {
			return createTimeZoneOffset(0);
		}
		else if (withAuto && str.equals("auto")) {
			return getLocalTimeZoneOffset();
		}

		final Matcher matcher = getTimeZoneOffsetPattern().matcher(str);
		if (!matcher.find() ||
				matcher.start() != 0 || matcher.end() != str.length()) {
			throw new GSException(
					GSErrorCode.ILLEGAL_PROPERTY_ENTRY,
					"Illegal time zone format (value=" + str + ")");
		}

		final int sign = matcher.group(1).equals("+") ? 1 : -1;
		int hour = 0;
		int min = 0;

		for (int i = 0; i < 2; i++) {
			final String strElem = matcher.group(i + 2);
			final int elem = Integer.valueOf(strElem);
			if (i == 0) {
				hour = elem;
			}
			else {
				min = elem;
			}
		}

		if (hour > 23 || min > 59) {
			throw new GSException(
					GSErrorCode.ILLEGAL_PROPERTY_ENTRY,
					"Illegal time zone format (value=" + str + ")");
		}

		final int offset = sign * (hour * 60 + min) * 60 * 1000;
		return createTimeZoneOffset(offset);
	}

	public static String formatTimeZoneOffset(long offsetMillis, boolean forId) {
		final String gmtStr = "GMT";
		if (offsetMillis == 0) {
			if (forId) {
				return gmtStr;
			}
			else {
				return "Z";
			}
		}

		final long absMillis = (offsetMillis >= 0 ? offsetMillis : -offsetMillis);

		final String elemFormat = "%02d";
		final StringBuilder sb = new StringBuilder();
		if (forId) {
			sb.append(gmtStr);
		}
		sb.append(offsetMillis >= 0 ? "+" : "-");
		sb.append(String.format(
				Locale.ROOT, elemFormat, absMillis / (60 * 60 * 1000) % 24));
		sb.append(":");
		sb.append(String.format(
				Locale.ROOT, elemFormat, absMillis / (60 * 1000) % 60));
		return sb.toString();
	}

	public static SimpleTimeZone getLocalTimeZoneOffset()
			throws GSException {
		final TimeZone src = TimeZone.getDefault();
		if (src.useDaylightTime() || src.getDSTSavings() != 0) {
			throw new GSException(
					GSErrorCode.UNSUPPORTED_OPERATION,
					"Daylight time is not supported (" +
					"zoneId=" + src.getID() +
					", zoneName=" + src.getDisplayName(Locale.ROOT) +
					", zoneDetail=" + src + ")");
		}
		return createTimeZoneOffset(src.getRawOffset());
	}

	public static SimpleTimeZone createTimeZoneOffset(int offsetMillis) {
		return new SimpleTimeZone(
				offsetMillis, formatTimeZoneOffset(offsetMillis, true));
	}

	public static Pattern getTimeZoneOffsetPattern() {
		return TIME_ZONE_OFFSET_PATTERN;
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

		public Double getDoubleProperty(
				String name, boolean deprecated) throws GSException {
			final String strValue = getProperty(name, deprecated);
			if (strValue == null) {
				return null;
			}

			try {
				return Double.valueOf(strValue);
			}
			catch (NumberFormatException e) {
				throw new GSException(
						GSErrorCode.ILLEGAL_VALUE_FORMAT,
						"Failed to parse as double (value=" + strValue +
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

		public Set<String> getUnknownNames() {
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

		public void addVisitedNames(Set<String> nameSet, boolean deprecated) {
			for (String name : nameSet) {
				visited.put(name, deprecated);
			}
		}

	}

}
