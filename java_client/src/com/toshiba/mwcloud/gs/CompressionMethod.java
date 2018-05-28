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
 * Represents types of compression methods.
 *
 * <p>These types are used when setting a time series compression.</p>
 */
public enum CompressionMethod {

	/**
	 * Represents no compression.
	 */
	NO,

	/**
	 * Represents a thinning compression without error
	 *
	 * <p>In this type of compression, rows that have the same
	 * value registered immediately before and after are thinned.
	 * The thinned values are recovered in the interpolation or sampling
	 * processing without information loss.</p>
	 */
	SS,

	/**
	 * Represents a thinning compression with error
	 *
	 * <p>In this type of compression, values that represent
	 * gradient same as before and immediately after may be thinned.
	 * The algorithm to determine the equality of the gradients
	 * can be specified by users.
	 * Only if specified column meets the above conditions and
	 * other columns are the same as the last registered data, the
	 * row is thinned.
	 * The thinned value is recovered in an interpolation or
	 * sampling processing within the specified error range.</p>
	 */
	HI

}
