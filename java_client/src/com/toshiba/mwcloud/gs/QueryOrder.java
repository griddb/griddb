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
 * Represents the order of Rows requested by a query.
 *
 * <p>It is used to specify the order of Rows targeted by each query function. Specific targets differ with individual
 * functions.</p>
 */
public enum QueryOrder {

	/**
	 * Indicates the requested order of Rows is an ascending order.
	 */
	ASCENDING,

	/**
	 * Indicates the requested order of Rows is a descending order.
	 */
	DESCENDING

}
