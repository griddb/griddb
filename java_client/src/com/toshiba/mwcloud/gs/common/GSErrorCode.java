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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class GSErrorCode {

	

	public static final int INTERNAL_ERROR = 145000;

	public static final int EMPTY_PARAMETER = 145001;

	public static final int ILLEGAL_PARAMETER = 145002;

	public static final int UNSUPPORTED_OPERATION = 145003;

	public static final int SIZE_VALUE_OUT_OF_RANGE = 145004;

	public static final int ILLEGAL_PROPERTY_ENTRY = 145005;

	public static final int ILLEGAL_VALUE_FORMAT = 145006;

	public static final int ILLEGAL_SYMBOL_CHARACTER = 145007;

	public static final int UNKNOWN_COLUMN_NAME = 145008;

	public static final int UNSUPPORTED_KEY_TYPE = 145009;

	public static final int UNSUPPORTED_FIELD_TYPE = 145010;

	public static final int UNSUPPORTED_ROW_MAPPING = 145011;

	public static final int UNKNOWN_ELEMENT_TYPE_OPTION = 145012;

	public static final int UNKNOWN_GEOMETRY_OPERATOR = 145013;

	public static final int UNKNOWN_AGGREGATION = 145014;

	public static final int UNKNOWN_TIME_OPERATOR = 145015;

	public static final int UNKNOWN_INDEX_FLAG = 145016;

	public static final int UNKNOWN_FETCH_OPTION = 145017;

	public static final int UNKNOWN_TIME_UNIT = 145018;

	public static final int UNSUPPORTED_DEFAULT_INDEX = 145019;

	public static final int BINDING_ENTRY_NOT_FOUND = 145020;

	public static final int MULTIPLE_KEYS_FOUND = 145021;

	public static final int COLUMN_NAME_CONFLICTED = 145022;

	public static final int ILLEGAL_SCHEMA = 145023;

	public static final int KEY_NOT_FOUND = 145024;

	public static final int KEY_NOT_ACCEPTED = 145025;

	public static final int EMPTY_ROW_FIELD = 145026;

	public static final int BAD_STATEMENT = 145027;

	public static final int BAD_CONNECTION = 145028;

	public static final int CONNECTION_TIMEOUT = 145029;

	public static final int WRONG_NODE = 145030;

	public static final int MESSAGE_CORRUPTED = 145031;

	public static final int PARTITION_NOT_AVAILABLE = 145032;

	public static final int ILLEGAL_PARTITION_COUNT = 145033;

	public static final int CONTAINER_NOT_OPENED = 145034;

	public static final int ILLEGAL_COMMIT_MODE = 145035;

	public static final int TRANSACTION_CLOSED = 145036;

	public static final int NO_SUCH_ELEMENT = 145037;

	public static final int CONTAINER_CLOSED = 145038;

	public static final int NOT_LOCKED = 145039;

	public static final int RESOURCE_CLOSED = 145040;

	
	public static final int ALLOCATION_FAILED = 145041;

	public static final int RECOVERABLE_CONNECTION_PROBLEM = 145042;

	public static final int RECOVERABLE_ROW_SET_LOST = 145043;

	public static final int ILLEGAL_CONFIG = 145044;

	public static final int DATABASE_NOT_EMPTY = 145045;

	public static final int JSON_INVALID_SYNTAX = 121500;

	public static final int JSON_KEY_NOT_FOUND = 121501;

	public static final int JSON_VALUE_OUT_OF_RANGE = 121502;

	public static final int JSON_UNEXPECTED_TYPE = 121503;

	public static final int HTTP_UNEXPECTED_MESSAGE = 122503;

	public static final int SA_INVALID_CONFIG = 123502;

	public static final int SA_ADDRESS_CONFLICTED = 123503;

	public static final int SA_ADDRESS_NOT_ASSIGNED = 123504;

	public static final int SA_INVALID_ADDRESS = 123505;

	private static final Map<Integer, String> NAME_MAP = makeNameMap();

	public static NullPointerException checkNullParameter(
			Object parameter, String name, NullPointerException cause) {
		if (parameter == null) {
			final NullPointerException e = new NullPointerException(
					"The parameter \"" + name + "\" must not be null" +
					(cause == null || cause.getMessage() == null ?
							"" : " (reason=" + cause.getMessage() + ")"));
			e.initCause(cause);
			throw e;
		}

		return cause;
	}

	private static Map<Integer, String> makeNameMap() {
		final Map<Integer, String> map = new HashMap<Integer, String>();
		for (Field field : GSErrorCode.class.getFields()) {
			if (field.getType() != Integer.TYPE) {
				continue;
			}
			try {
				map.put(field.getInt(null), "JC_" + field.getName());
			}
			catch (IllegalAccessException e) {
				throw new Error(e);
			}
		}
		return map;
	}

	public static String getName(int code) {
		return NAME_MAP.get(code);
	}

}
