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
	 * Creates a query to obtain a set of Rows which are matched to specified
	 * geometry range conditions.
	 *
	 * <p>When obtaining a set of Rows using {@link Query#fetch(boolean)}, the
	 * option of locking for update can be enabled.</p>
	 *
	 * <p>{@link GSException} will not be thrown in the current version.
	 * If there is an error such as column names, exception will be thrown when
	 * fetching the obtained query.</p>
	 *
	 * <p>In the current version, {@link NullPointerException} will not be dispatched
	 * when {@link GSException} and {@code null} cannot be specified as {@code null}.
	 * If there is an error in the column name, etc., an exception is thrown when
	 * the obtained query is fetched. </p>
	 *
	 * @param column A name of the geometry type column to be compared. {@code null} cannot be specified
	 * @param geometry Geometry structure to be compared. {@code null} cannot be specified
	 * @param geometryOp Comparison method. {@code null} cannot be specified
	 *
	 * @throws GSException It will not be thrown in the current version.
	 */
	public Query<R> query(
			String column, Geometry geometry, GeometryOperator geometryOp)
			throws GSException;

	/**
	 * Creates a query to obtain a set of Rows which are matched to specified
	 * geometry range conditions with exclusion range.
	 *
	 * <p>Obtains a set of Rows which has the column values that intersect with
	 * {@code geometryIntersection} and do not intersect with
	 * {@code geometryDisjoint}. Conditions of the intersection determination is
	 * the same as the {@link GeometryOperator#INTERSECT}.
	 *
	 * <p>When obtaining a set of Rows using {@link Query#fetch(boolean)}, the
	 * option of locking for update can be enabled.</p>
	 *
	 * <p>In the current version, {@link NullPointerException} will not be dispatched
	 * when {@link GSException} and {@code null} cannot be specified as {@code null}.
	 * If there is an error in the column name, etc., an exception is thrown when
	 * the obtained query is fetched. </p>
	 *
	 * @param column A name of the geometry type column to be compared. {@code null} cannot be specified
	 * @param geometryIntersection Geometry structure indicating a range that
	 * intersects with the value on the column. {@code null} cannot be specified
	 * @param geometryDisjoint Geometry structure indicating a range that does
	 * not intersect with the values on the column. {@code null} cannot be specified
	 *
	 * @throws GSException It will not be thrown in the current version.
	 */
	public Query<R> query(
			String column, Geometry geometryIntersection, Geometry geometryDisjoint)
			throws GSException;

}
