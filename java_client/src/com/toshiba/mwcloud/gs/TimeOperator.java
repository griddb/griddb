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
package com.toshiba.mwcloud.gs;

/**
 * Represents how to specify a Row based on a key timestamp of {@link TimeSeries}</a>.
 *
 * <p>It can be used in combination with the timestamp (specified separately) to specify 
 * a Row with the nearest timestamp etc. When no relevant Row exists, 
 * it behaves differently depending on the function using this enumeration type.</p>
 */
public enum TimeOperator {

	/**
	 * Returns the newest among the Rows whose timestamp are identical with or earlier than the specified time.
	 */
	PREVIOUS,

	/**
	 * Returns the newest among the Rows whose timestamp are earlier than the specified time.
	 */
	PREVIOUS_ONLY,

	/**
	 * Returns the oldest among the Rows whose timestamp are identical with or later than the specified time.
	 */
	NEXT,

	/**
	 * Returns the oldest among the Rows whose timestamp are later than the specified time.
	 */
	NEXT_ONLY,

}
