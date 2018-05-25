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
 * Represents the type of interpolation of Rows.
 *
 * <p>It is used by the function of interpolating TimeSeries Rows.</p>
 */
public enum InterpolationMode {

	/**
	 * Indicates performing linear interpolation or interpolation with the value of
	 * a privious Row on Columns.
	 *
	 * <p>The Column specified the interpolation function is linearly interpolated
	 * with the Row values before and after the target time of interpolation.
	 * The target Column must be of numeric type. </p>
	 *
	 * <p>The Columns not specified as interpolation targets are interpolated
	 * with adjacent Row values just before the target time of interpolation.
	 * Those Columns can be of any type. </p>
	 */
	LINEAR_OR_PREVIOUS,

	/**
	 * Indicates using an empty value as an interpolated value.
	 *
	 * <p>It indicates that an empty value defined for each type is used as
	 * an interpolated value for all Row fields except Row keys. The empty value is
	 * the same as the initial value of a Column newly created when changing
	 * a Column layout using {@link GridStore#putCollection(String, Class, boolean)}. </p>
	 */
	EMPTY

}
