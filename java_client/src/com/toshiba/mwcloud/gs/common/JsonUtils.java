/*
   Copyright (c) 2015 TOSHIBA CORPORATION.

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

import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Map;

import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.contrib.org.json.simple.JSONValue;
import com.toshiba.mwcloud.gs.contrib.org.json.simple.parser.ParseException;

public class JsonUtils {

	public static Object parse(String value) throws GSException {
		try {
			return JSONValue.parseWithException(value);
		}
		catch (ParseException e) {
			throw fromParseException(e);
		}
	}

	public static Object parse(Reader reader) throws GSException {
		try {
			return JSONValue.parseWithException(reader);
		}
		catch (ParseException e) {
			throw fromParseException(e);
		}
		catch (IOException e) {
			throw new GSException(e);
		}
	}

	private static GSException fromParseException(ParseException e) {
		final String description = e.getMessage();
		return new GSException(
				GSErrorCode.JSON_INVALID_SYNTAX,
				"Failed to parse JSON (reason=" + description + ")", e);
	}

	public static <T> T as(Class<T> type, Object value) throws GSException {
		if (!type.isInstance(value)) {
			throw new GSException(
					GSErrorCode.JSON_UNEXPECTED_TYPE,
					"Json type unmatched (expected=" + type.getName() +
					", actual=" + (value == null ?
							"null" : value.getClass().getName()) + ")");
		}
		return type.cast(value);
	}

	@SuppressWarnings("unchecked")
	public static Map<String, Object> asObject(
			Object value) throws GSException {
		return as(Map.class, value);
	}

	@SuppressWarnings("unchecked")
	public static List<Object> asArray(Object value) throws GSException {
		return as(List.class, value);
	}

	public static long asLong(Object value) throws GSException {
		if (value instanceof Number) {
			return ((Number) value).longValue();
		}

		final double base = as(Double.class, value);
		final long min = Long.MIN_VALUE;
		final long max = Long.MAX_VALUE;
		final long result = (long) base;
		if (!((double) min <= base && base <= (double) max &&
				min <= result && result <= max)) {
			throw new GSException(
					GSErrorCode.JSON_VALUE_OUT_OF_RANGE,
					"Json value out of range (value=" + value +
					", min=" + min + ", max=" + max + ")");
		}
		return result;
	}

	public static <T> T as(
			Class<T> type, Object value, String key) throws GSException {
		final Map<String, Object> object = asObject(value);
		final Object found = object.get(key);
		if (found == null) {
			throw new GSException(
					GSErrorCode.JSON_KEY_NOT_FOUND,
					"Json object does not contain the specified key (" +
					"key=" + key + ")");
		}

		return as(type, found);
	}

	public static Map<String, Object> asObject(
			Object value, String key) throws GSException {
		return asObject(as(Object.class, value, key));
	}

	public static List<Object> asArray(
			Object value, String key) throws GSException {
		return asArray(as(Object.class, value, key));
	}

	public static long asLong(
			Object value, String key) throws GSException {
		return asLong(as(Object.class, value, key));
	}

}
