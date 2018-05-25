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

public enum Statement {
	CONNECT,
	DISCONNECT,
	LOGIN,
	LOGOUT,
	GET_PARTITION_ADDRESS,
	GET_CONTAINER,
	GET_TIME_SERIES,		
	PUT_CONTAINER,
	PUT_TIME_SERIES,		
	DROP_COLLECTION,		
	DROP_TIME_SERIES,		
	CREATE_SESSION,
	CLOSE_SESSION,
	CREATE_INDEX,
	DROP_INDEX,
	CREATE_EVENT_NOTIFICATION,
	DROP_EVENT_NOTIFICATION,
	FLUSH_LOG,
	COMMIT_TRANSACTION,
	ABORT_TRANSACTION,
	GET_ROW,
	QUERY_TQL,
	QUERY_COLLECTION_GEOMETRY_RELATED,
	QUERY_COLLECTION_GEOMETRY_WITH_EXCLUSION,
	PUT_ROW,
	PUT_MULTIPLE_ROWS,
	UPDATE_ROW_BY_ID,
	DELETE_ROW,
	DELETE_ROW_BY_ID,
	APPEND_TIME_SERIES_ROW,
	GET_TIME_SERIES_ROW,		
	GET_TIME_SERIES_ROW_RELATED,
	INTERPOLATE_TIME_SERIES_ROW,
	AGGREGATE_TIME_SERIES,
	QUERY_TIME_SERIES_TQL,		
	QUERY_TIME_SERIES_RANGE,
	QUERY_TIME_SERIES_SAMPLING,
	PUT_TIME_SERIES_ROW,		
	PUT_TIME_SERIES_MULTIPLE_ROWS,		
	DELETE_TIME_SERIES_ROW,		
	GET_CONTAINER_PROPERTIES,
	GET_MULTIPLE_ROWS,
	GET_TIME_SERIES_MULTIPLE_ROWS,		

	
	GET_PARTITION_CONTAINER_NAMES,
	DROP_CONTAINER,
	CREATE_MULTIPLE_SESSIONS,
	CLOSE_MULTIPLE_SESSIONS,
	EXECUTE_MULTIPLE_QUERIES,
	GET_MULTIPLE_CONTAINER_ROWS,
	PUT_MULTIPLE_CONTAINER_ROWS,
	CLOSE_ROW_SET,
	FETCH_ROW_SET,
	CREATE_TRIGGER,
	DROP_TRIGGER,

	
	GET_USERS,
	PUT_USER,
	DROP_USER,
	GET_DATABASES,
	PUT_DATABASE,
	DROP_DATABASE,
	PUT_PRIVILEGE,
	DROP_PRIVILEGE,

	
	CREATE_INDEX_DETAIL,
	DROP_INDEX_DETAIL

	;

	private final GeneralStatement general;

	private Statement() {
		this.general = new GeneralStatement(name(), ordinal());
	}

	public GeneralStatement generalize() {
		return general;
	}

	public static class GeneralStatement {

		private final String name;

		private final int ordinal;

		private GeneralStatement(String name, int ordinal) {
			this.name = name;
			this.ordinal = ordinal;
		}

		public static GeneralStatement getInstance(String name, int ordinal) {
			if (name == null) {
				throw new NullPointerException();
			}
			if (ordinal < Constants.PRESET_STATEMENTS.length) {
				throw new IllegalArgumentException();
			}
			return new GeneralStatement(name, ordinal);
		}

		public int ordinal() {
			return ordinal;
		}

		@Override
		public String toString() {
			return name;
		}

	}

	private static class Constants {
		private static final Statement[] PRESET_STATEMENTS = values();
	}

}
