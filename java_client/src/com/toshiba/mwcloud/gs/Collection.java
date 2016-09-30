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
 * A general-purpose Container for managing a set of Rows.
 *
 * <p>The following types are available as a Row key.</p>
 * <ul>
 * <li>String type ({@link String})</li>
 * <li>INTEGER type ({@link Integer})</li>
 * <li>LONG type ({@link Long})</li>
 * <li>TIMESTAMP type ({@link java.util.Date})</li>
 * </ul>
 * <p>It is not mandatory to set a Row key.</p>
 *
 * <p>There is no Container-specific constraint on Row operations. </p>
 *
 * <p>A set of Rows retuned by {@link #query(String)} or
 * {@link GridStore#multiGet(java.util.Map)} etc. in no particular order,
 * when order is not specified.</p>
 *
 * <p>The granularity of locking is a Row. </p>
 */
public interface Collection<K, R> extends Container<K, R> {

	/**
	 * Not supported
	 */
	public Query<R> query(
			String column, Geometry geometry, GeometryOperator geometryOp)
			throws GSException;

	/**
	 * Not supported
	 */
	public Query<R> query(
			String column, Geometry geometryIntersection, Geometry geometryDisjoint)
			throws GSException;

}
