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

/**
 * The options for fetching the result of a query.
 */
public enum FetchOption {

	/**
	 * Used to set the maximum number of Rows to be fetched.
	 *
	 * <p>If the number of Rows in a result exceeds the maximum, the maximum number of
	 * Rows counting from the 0-th in {@link RowSet} are fetched. The rest of the Rows
	 * in the result cannot be fetched. </p>
	 *
	 * <p>The supported types of values are {@link Integer} and {@link Long}. Negative values are not available.
	 * If the setting is omitted, the limit is not defined. </p>
	 */
	LIMIT,

	/**
	 * @deprecated
	 */
	@Deprecated
	SIZE

}
