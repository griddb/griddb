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

import java.util.Date;
import java.util.Set;

/**
 * A specialized Container with a time-type Row key for TimeSeries data operation.
 *
 * <p>Generally, in extraction of a specific range and aggregation operations on TimeSeries,
 * more efficient implementation is selected than on {@link Collection}.</p>
 *
 * <p>There are some limitations on row operations unlike {@link Collection}.
 * If a compression option based on {@link TimeSeriesProperties}
 * has been set, the following operations cannot be performed.</p>
 * <ul>
 * <li>Update of the specified row</li>
 * <li>Deletion of the specified row</li>
 * <li>Creation of a new row that has an older timestamp key than the latest one.</li>
 * </ul>
 *
 * <p>If the order of Rows requested by {@link #query(String)} or {@link GridStore#multiGet(java.util.Map)}
 * is not specified, the Rows in a result set are sorted in ascending order of Row key.</p>
 *
 * <p>The granularity of locking is an internal storage unit, i.e., a set of one or more Rows.
 * Accordingly, when locking a specific Row, GridDB will attempt to lock other Rows in the
 * same internal storage unit as the Row.
</p>
 */
public interface TimeSeries<R> extends Container<Date, R> {

	/**
	 * Newly creates or updates a Row with a Row key of the current time on GridDB.
	 *
	 * <p>It behaves in the same way as {@link #put(Date, Object)}, except that it sets as a Row key the TIMESTAMP
	 * value equivalent to the current time on GridDB. The Row key in the specified Row object is ignored.</p>
	 *
	 * <p>In the manual commit mode, the target Row is locked.
	 * Other Rows in the same internal storage unit are also locked.</p>
	 *
	 * @param row A Row object representing the content of a Row to be newly created or updated.
	 *
	 * @return TRUE if a Row is found whose timestamp is identical with the current time on GridDB.
	 *
	 * @throws GSException if a timeout occurs during this operation or the transaction,
	 * this Container is deleted, its schema is changed or a connection failure occurs;
	 * or if called after the connection is closed; or if an unsupported value is set
	 * in the Row object.
	 * @throws ClassCastException if the specified key or Row object does not completely match the types used
	 * in mapping operation.
	 * @throws NullPointerException if {@code null} is specified as {@code row}; or if the Row object
	 * lacks some object corresponding to a Row field.
	 *
	 * @see #put(Date, Object)
	 * @see TimeSeriesProperties#getCompressionMethod()
	 */
	public boolean append(R row) throws GSException;

	/**
	 * Newly creates or updates a Row, based on the specified Row object and also the Row key specified as needed.
	 *
	 * <p>It newly creates a Row, based on the value of the Row key specified as {@code key} or the Row key in
	 * the specified Row object if {@code key} is not specified.</p>
	 *
	 * <p>Only rows that have a newer timestamp key than any
	 * existing row can be created. If the timestamp is equal to
	 * the newest one, no update occurs and the existing row
	 * is held.</p>
	 *
	 * <p>In the manual commit mode, the target Row is locked. Other Rows in the same internal storage unit
	 * are also locked.</p>
	 *
	 * @return TRUE if a Row is found whose timestamp is identical with the specified time.
	 *
	 * @throws GSException if a timeout occurs during this operation or the transaction,
	 * this Container is deleted, its schema is changed or a connection failure occurs;
	 * or if called after the connection is closed; or if an unsupported value is set
	 * in the Row object.
	 * @throws ClassCastException if the specified key or Row object does not completely match the types used
	 * in mapping operation.
	 * @throws NullPointerException if {@code null} is specified as {@code row}; or if the Row object
	 * lacks some object corresponding to a Row field.
	 *
	 * @see Container#put(Object, Object)
	 * @see TimeSeriesProperties#getCompressionMethod()
	 */
	public boolean put(Date key, R row) throws GSException;

	/**
	 * Returns the content of a Row corresponding to the Row key, based on the specified option.
	 *
	 * <p>It behaves in the same way as {@link Container#get(Object)}. However, since the type of a Row key is
	 * predetermined, {@link ClassCastException} will not be thrown.</p>
	 *
	 * @return TRUE if a corresponding Row exists.
	 *
	 * @throws GSException if a timeout occurs during this operation or the transaction, this Container is deleted,
	 * its schema is changed or a connection failure occurs; or if called after the connection is closed;
	 * or if an unsupported value is specified as {@code key}.
	 * @throws NullPointerException if a {@code null} parameter is specified.
	 *
	 * @see Container#get(Object)
	 */
	public R get(Date key) throws GSException;

	/**
	 * Deletes a Row corresponding to the specified Row key.
	 *
	 * <p>It can be used only if a Column exists which corresponds to a specified Row key.
	 * If no corresponding Row exists, nothing is changed.</p>
	 *
	 * <p>It can not be used for time series that has a setting of
	 * compression option.</p>
	 *
	 * <p>In the manual commit mode, the target Row is locked.</p>
	 *
	 * @return TRUE if a corresponding Row exists.
	 *
	 * @throws GSException if no Column exists which corresponds to the specified Row key.
	 * @throws GSException if a timeout occurs during this operation or the transaction, this Container is deleted,
	 * its schema is changed or a connection failure occurs; or if called after the connection is closed;
	 * or if an unsupported value is specified as {@code key}.
	 * @throws ClassCastException if the specified Row key does not match the type of a Row key used in mapping operation.
	 * @throws NullPointerException if a {@code null} parameter is specified.
	 *
	 * @see Container#remove(Object)
	 * @see TimeSeriesProperties#getCompressionMethod()
	 */
	public boolean remove(Date key) throws GSException;

	/**
	 * Returns one Row related with the specified time.
	 *
	 * @param base a base time point
	 * @param timeOp what to obtain
	 *
	 * @return The Row(s) satisfying the conditions; or {@code null} if no such Row is found.
	 *
	 * @throws GSException if a timeout occurs during this operation or the transaction, this Container is deleted,
	 * its schema is changed or a connection failure occurs; or if called after the connection is closed;
	 * or if an unsupported value is specified as the base time point.
	 * @throws NullPointerException if a {@code null} parameter is specified.
	 */
	public R get(Date base, TimeOperator timeOp) throws GSException;

	/**
	 * Performs linear interpolation etc. of a Row object corresponding to the specified time.
	 *
	 * <p>It creates a Row object, based on the value of the specified Column of the TimeSeries Row identical with
	 * the base time or the value obtained by linearly interpolating the values of the specified Columns of adjacent
	 * Rows around the base time. If there is no Row with the same timestamp as the base time nor no Row with an earlier
	 * or later timestamp, no Row object is created.</p>
	 *
	 * <p>Only Columns of numeric type can be interpolated. The field values of the Row whose timestamp is identical
	 * with the specified timestamp or the latest among those with earlier timestamps are set on the specified Column
	 * and the fields other than a Row key.</p>
	 *
	 * @param base a base time point
	 * @param column A Column to be interpolated
	 *
	 * @throws GSException if no Column has the specified name; or if an unsupported type of Column is specified.
	 * @throws GSException if a timeout occurs during this operation or the transaction, this Container is deleted,
	 * its schema is changed or a connection failure occurs; or if called after the connection is closed; or if
	 * an unsupported value is specified as the base time point.
	 * @throws NullPointerException if a {@code null} parameter is specified.
	 */
	public R interpolate(Date base, String column) throws GSException;

	/**
	 * Creates a query to obtain a set of Rows within a specific range between the specified start and end times.
	 *
	 * <p>The boundary Rows whose timestamps are identical with the start or end time are included in the range of
	 * fetch. If the specified start time is later than the end time, all Rows are excluded from the range. The Rows
	 * in a result set are sorted in ascending order, i.e., from an earlier timestamp to a later timestamp.</p>
	 *
	 * <p>The option of locking for update can be enabled when obtaining a set of Rows using
	 * {@link Query#fetch(boolean)}.</p>
	 *
	 * @param start start time or {@code null}. A {@code null} value indicates the timestamp of the oldest Row in
	 * this TimeSeries.
	 * @param end end time or {@code null}. A {@code null} value indicates the timestamp of the newest Row in this TimeSeries.
	 *
	 * @throws GSException It will not be thrown in the current version.
	 */
	public Query<R> query(Date start, Date end) throws GSException;

	/**
	 * Creates a query to obtain a set of Rows sorted in the specified order within a specific range between
	 * the specified start and end times.
	 *
	 * <p>The boundary Rows whose timestamps are identical with the start or end time are included in the range
	 * of fetch. If the specified start time is later than the end time, all Rows are excluded from the range.</p>
	 *
	 * <p>The option of locking for update can be enabled when obtaining a set of Rows using
	 * {@link Query#fetch(boolean)}.</p>
	 *
	 * <p>For arguments that cannot specify {@code NULL}, depending on the {@code NULL} status,
	 * {@link NullPointerException} may not be dispatched. If there is an error in the argument,
	 * an exception will be thrown when the query result is fetched. </p>
	 *
	 * @param start start time or {@code null}. A {@code null} value indicates the timestamp of the oldest Row in
	 * this TimeSeries.
	 * @param end end time or {@code null}. A value indicates the timestamp of the newest Row in this TimeSeries.
	 * @param order the time order of Rows in a result set. {@code null} cannot be specified
	 * {@link QueryOrder#ASCENDING} indicates the order from older to newer, and {@link QueryOrder#DESCENDING} indicates
	 * the order from newer to older.
	 *
	 * @throws GSException It will not be thrown in the current version.
	 */
	public Query<R> query(
			Date start, Date end, QueryOrder order) throws GSException;

	/**
	 * Creates a query to take a sampling of Rows within a specific range.
	 *
	 * <p>Each sampling time point is defined by adding a sampling interval multiplied by a non-negative integer to
	 * the start time, excluding the time points later than the end time. If the specified start time is later than
	 * the end time, all Rows are excluded from the range.</p>
	 *
	 * <p>If executed, the query creates a set of Rows, using the values of the Rows whose timestamps are identical
	 * with each sampling time point, or the interpolated values according to the parameters {@code columnSet} and
	 * {@code mode} if such a Row is not found. For specific interpolation methods, see the description of
	 * {@link InterpolationMode}.
	 *
	 * <p>If there is no Rows to be referenced for interpolation at a specific sampling time point, a corresponding
	 * Row is not generated, and thus the number of results returned is reduced by the number of such time points.
	 * A shorter sampling interval increases the likelihood that identical Row field values will be used even at
	 * different sampling time points, depending on the interpolation method.</p>
	 *
	 * <p>The option of locking for update cannot be enabled when obtaining a set of Rows using {@link Query#fetch(boolean)}.
	 *
	 * <p>In the current version, for arguments that cannot specify {@link GSException} or {@code NULL},
	 * depending on the {@code NULL} status, {@link NullPointerException} may not be dispatched.
	 * If there is an error in the argument, an exception will be thrown when the query result is fetched. </p>
	 *
	 * @param start start time. {@code null} cannot be specified
	 * @param end end time. {@code null} cannot be specified
	 * @param columnSet a set of names of the Columns to be interpolated according to {@code mode}. An empty set
	 * indicates no specification of target Columns. A {@code null} value indicates the same as an empty set.
	 * @param mode an interpolation method. {@code null} cannot be specified
	 * @param interval a sampling interval.{@code 0} or a negative value cannot be specified.
	 * @param intervalUnit the time unit of the sampling interval.
	 * {@link TimeUnit#YEAR} and {@link TimeUnit#MONTH} cannot be specified. {@code null} cannot be specified
	 *
	 * @throws GSException It will not be thrown in the current version.
	 */
	public Query<R> query(
			Date start, Date end,
			Set<String> columnSet, InterpolationMode mode,
			int interval, TimeUnit intervalUnit)
			throws GSException;

	/**
	 * @deprecated
	 */
	@Deprecated
	public Query<R> query(
			Date start, Date end, Set<String> columnSet,
			int interval, TimeUnit intervalUnit)
			throws GSException;

	/**
	 * Performs an aggregation operation on a Row set or its specific Columns, based on the specified start and end times.
	 *
	 * <p>The parameter {@code column} might be ignored depending on the parameter {@code aggregation}.
	 *
	 * The boundary Rows whose timestamps are identical with the start or end time are included in the range of operation.
	 * If the specified start time is later than the end time, all Rows are excluded from the range.</p>
	 *
	 * @param start Start time
	 * @param end End time
	 * @param column The name of a target Column.
	 * Specify {@code null} if the specified aggregation method does not target a specific Column.
	 * @param aggregation An aggregation method
	 *
	 * @return {@link AggregationResult} if the aggregation result is stored in it, or {@code null} if not.
	 * See the description of {@link AggregationResult} for more information.
	 *
	 * @throws GSException if the type of the specified Column is not supported by the specified aggregation method.
	 * @throws GSException if a timeout occurs during this operation or the transaction, this Container is deleted,
	 * its schema is changed, or a connection failure occurs; or if called after the connection is closed.
	 * @throws NullPointerException if {@code null} is specified as {@code start}, {@code end}, or {@code aggregation}.
	 *
	 * @see Aggregation
	 */
	public AggregationResult aggregate(
			Date start, Date end, String column, Aggregation aggregation)
			throws GSException;

}
