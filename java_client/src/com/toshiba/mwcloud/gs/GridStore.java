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

import java.io.Closeable;
import java.util.List;
import java.util.Map;


/**
 * A function is provided for processing the data in the connected GridDB system.
 *
 * <p>A function to add, delete, or change the composition of Collection
 * and TimeSeries Containers as well as to process the Rows constituting
 * a Container is provided.</p>
 *
 * <p>Rows cannot be stored with duplicated names, regardless of
 * whether it is a TimeSeries or Collection Container.
 * Where necessary, the Container name can be specified by appending
 * the node affinity name after the base Container name with the at mark "@".
 * Only ASCII alphanumeric characters and the underscore mark ( "_" ) can be used
 * in the base Container name and node affinity name.
 * ASCII characters are not case-sensitive. Numbers cannot be used in front of the
 * base Container name.
 * Refer to "System limiting values" in the GridDB API Reference for the upper limit of the length
 * of the Container name.</p>
 *
 * <p>Thread safety of each method is not guaranteed. </p>
 *
 * @see Collection
 * @see TimeSeries
 * @see Container
 */
public interface GridStore extends Closeable {

	/**
	 * @deprecated
	 */
	@Deprecated
	public boolean put(String pathKey, Object rowObject) throws GSException;

	/**
	 * @deprecated
	 */
	@Deprecated
	public Object get(String pathKey) throws GSException;

	/**
	 * @deprecated
	 */
	@Deprecated
	public <R> R get(String pathKey, Class<R> rowType) throws GSException;

	/**
	 * @deprecated
	 */
	@Deprecated
	public boolean remove(String pathKey) throws GSException;

	/**
	 * Get information related to a Container with the specified name.
	 *
	 * <p>A name stored in GridDB is set for the Container name to be included
	 * in a returned {@link ContainerInfo}. Therefore, compared to the specified
	 * Container name, the notation of the ASCII uppercase characters and
	 * lowercase characters may differ.</p>
	 *
	 * <p>The column sequence is set to Do Not Ignore.
	 * This setting can be verified through
	 * {@link ContainerInfo#isColumnOrderIgnorable()}.</p>
	 *
	 * @param name the target Container name
	 *
	 * @return Container Info of the specific Container.
	 *
	 * @return The relevant {@link ContainerInfo} if the Container exists, otherwise
	 * {@code null}.
	 * @throws GSException if a timeout occurs during this operation or a connection
	 * failure occurs; or if called after the connection is closed.
	 * @throws NullPointerException if {@code null} is specified as the parameter.
	 */
	public ContainerInfo getContainerInfo(String name) throws GSException;

	/**
	 * Newly creates or update a Collection.
	 *
	 * <p>If a Container with the specified name does not exist, it newly creates
	 * a Collection based on the Column layout defined by the specified class.
	 * If a Container with the specified name already exists and its whole Column
	 * layout matches the specified type, it behaves in the same way as
	 * {@link #getCollection(String, Class)}, except that it waits for active
	 * transactions to complete. </p>
	 *
	 * <p>For the correspondence between a specified type and a Column layout,
	 * see the description of {@link Container}</p>
	 *
	 * <p>If a Collection with the specified name exists and if a transaction(s)
	 * is active in the Collection, it does the operation after waiting for the
	 * transaction(s) to complete. </p>
	 *
	 * @param name Name of a Collection subject to processing
	 * @param rowType Type of Row object corresponding to the column layout
	 * of the Collection subject to processing
	 *
	 * @return A {@link Collection} object created or updated
	 *
	 * @throws GSException if a Collection with the specified name exists; or if
	 * the Column layout and additional settings conflict with the specified type
	 * with regard to the existing TimeSeries with the specified name.
	 * @throws GSException if a timeout occurs during this operation or a connection
	 * failure occurs; or if called after the connection is closed.
	 * @throws NullPointerException a {@code null} parameter(s) is specified.
	 */
	public <K, R> Collection<K, R> putCollection(
			String name, Class<R> rowType) throws GSException;

	/**
	 * Newly creates or update a Collection with the specified option.
	 *
	 * <p>If a Container with the specified name does not exist, it newly creates
	 * a Collection based on the Column layout defined by the specified class.
	 * If a Collection with the specified name already exists and its whole
	 * Column layout matches the specified type, it behaves in the same way as
	 * {@link #getCollection(String, Class)}, except that it waits for active
	 * transactions to complete. </p>
	 *
	 * <p>If {@code modifiable} is {@code true} and a Collection with the
	 * specified name exists, it changes its layout as necessary. When changing
	 * the layout, it leaves unchanged the existing Columns with the same name
	 * and type as defined by the specified class. If a Column name defined by
	 * the class is not found in the existing Collection, it creates a Column
	 * with the name; and it deletes other Columns in the existing Collection
	 * as well as their data. It fails if a Column with the same name but of a
	 * different type exists. It is not possible to create or delete a Column
	 * corresponding to a Row key. </p>
	 *
	 * <p>If a trigger is set in a Container, and if a column whose trigger
	 * is subject to notification is deleted due to a change in the column layout,
	 * the column will be deleted from the list of triggers subject to notification.</p>
	 *
	 * <p>The values of Columns to be newly created are initialized according
	 * to the type as follows:
	 * <table>
	 * <thead><tr><th>Column type</th><th>Initial value</th></tr></thead>
	 * <tbody>
	 * <tr><td>STRING</td><td>{@code ""}(a string of length 0)</td></tr>
	 * <tr><td>BOOL</td><td>({@code false})</td></tr>
	 * <tr><td>Numeric type</td><td>{@code 0}</td></tr>
	 * <tr><td>TIMESTAMP</td><td>{@code 1970-01-01T00:00:00Z}</td></tr>
	 * <tr><td>BLOB</td><td>BLOB data of length 0</td></tr>
	 * <tr><td>Array type</td><td>An array with no element</td></tr>
	 * </tbody>
	 * </table>
	 * </p>
	 *
	 * <p>For the correspondence between a specified type and a Column layout,
	 * see the description of {@link Container}</p>
	 *
	 * <p>If a Collection with the specified name exists and if a transaction(s)
	 * is active in the Collection, it does the operation after waiting for the
	 * transaction(s) to complete. </p>
	 *
	 * <p>When creating a Collection with Row key, an index of default type of
	 * the Row key which is defined by {@link Container#createIndex(String)}
	 * is created. The index is removable.</p>
	 *
	 * @param name Name of a Collection subject to processing
	 * @param rowType Type of Row object corresponding to the column layout
	 * of the Collection subject to processing
	 * @param modifiable Indicates whether the column layout of the existing
	 * Collection can be changed or not
	 *
	 * @return A {@link Collection} object created or updated
	 *
	 * @throws GSException if a TimeSeries with the same name exists; or if
	 * {@code modifiable} is {@code false} and the Column layout of the
	 * existing Collection with the specified name conflicts with the requested
	 * layout; or if {@code modifiable} is {@code true} and it attempted to
	 * change the unchangeable items in the existing Collection with the
	 * specified name.
	 * @throws GSException if the specified type is not proper as a type of a
	 * Row object. For more information, see the description of {@link Container}.
	 * @throws GSException if a timeout occurs during this operation or a
	 * connection failure occurs; or if called after the connection is closed.
	 * @throws NullPointerException if a {@code null} parameter(s) is specified.
	 */
	public <K, R> Collection<K, R> putCollection(
			String name, Class<R> rowType,
			boolean modifiable) throws GSException;

	/**
	 * Newly creates or updates a TimeSeries.
	 *
	 * <p>If a Container with the specified name does not exist, it newly
	 * creates a TimeSeries based on the Column layout defined by the
	 * specified class. If a TimeSeries with the specified name already
	 * exists and its whole Column layout matches the specified type, it
	 * behaves in the same way as {@link #getTimeSeries(String, Class)},
	 * except that it waits for active transactions to complete. </p>
	 *
	 * <p>For the correspondence between a specified type and a Column
	 * layout, see the description of {@link Container}.</p>
	 *
	 * <p>If a TimeSeries with the specified name exists and if a
	 * transaction(s) is active in the TimeSeries, it does the operation
	 * after waiting for the transaction(s) to complete. </p>
	 *
	 * @param name Name of TimeSeries subject to processing
	 * @param rowType Type of Row object corresponding to the column layout
	 * of the TimeSeries subject to processing
	 *
	 * @return A {@link TimeSeries} object created or updated
	 *
	 * @throws GSException if a Collection with the same name exists; or if
	 * the Column layout and additional settings conflict with the specified
	 * type with regard to the existing TimeSeries with the specified name.
	 * @throws GSException if the specified type is not proper as a type of
	 * a Row object. For more information, see the description of {@link Container}.
	 * @throws GSException if a timeout occurs during this operation or a
	 * connection failure occurs; or if called after the connection is closed.
	 * @throws NullPointerException if a {@code null} parameter(s) is specified.
	 */
	public <R> TimeSeries<R> putTimeSeries(
			String name, Class<R> rowType) throws GSException;

	/**
	 * Newly creates or updates a TimeSeries with the specified additional settings
	 * and update option.
	 *
	 * <p>If a Container with the specified name does not exist, it newly creates a
	 * TimeSeries based on the Column layout and additional settings defined by the
	 * specified class. If a TimeSeries with the specified name already exists and
	 * its whole Column layout and additional settings match the specified type,
	 * it behaves in the same way as {@link #getTimeSeries(String, Class)}, except
	 * that it waits for active transactions to complete. </p>
	 *
	 * <p>If {@code modifiable} is {@code true} and a TimeSeries with the specified
	 * name exists, it changes its layout as necessary. When changing the layout,
	 * it leaves unchanged the existing Columns with the same name and type as
	 * defined by the specified class. If a Column name defined by the class is
	 * not found in the existing TimeSeries, it creates a Column with the name;
	 * and it deletes other Columns in the existing Collection as well as their
	 * data. It fails if a Column with the same name but of a different type exists.
	 * It is not possible to create or delete a Column corresponding to a Row key
	 * or change the options for configuring a TimeSeries. When specifying some
	 * options for configuring a TimeSeries, specified values must be the same as
	 * the current settings. </p>
	 *
	 * <p>If a trigger is set in a Container, and if a column whose trigger
	 * is subject to notification is deleted due to a change in the column layout,
	 * the column will be deleted from the list of triggers subject to notification.</p>
	 *
	 * <p>For the initial values for newly created Columns, see the description of
	 * {@link #putCollection(String, Class, boolean)}.</p>
	 *
	 * <p>For the correspondence between a specified type and a Column layout,
	 * see the description of {@link Container}</p>
	 *
	 * <p>If a TimeSeries with the specified name exists and if a transaction(s)
	 * is active in the TimeSeries, it does the operation after waiting for the
	 * transaction(s) to complete. </p>
	 *
	 * @param name Name of TimeSeries subject to processing
	 * @param rowType Type of Row object corresponding to the column layout
	 * of the TimeSeries subject to processing
	 * @param props Composition option of TimeSeries. If {@code null} is specified,
	 * the existing settings will be inherited if a TimeSeries with the same name
	 * exists. If not, they will be deemed as specified in the initial status
	 * of {@link TimeSeriesProperties}.
	 * @param modifiable To permit a change in the column layout of an existing
	 * TimeSeries or not
	 *
	 * @return A {@link TimeSeries} object to created or deleted
	 *
	 * @throws GSException if a Collection with the same name exists; or if
	 * {@code modifiable} is {@code false} and the Column layout and additional
	 * settings conflict with the specified type with regard to the existing
	 * TimeSeries with the specified name; or if {@code modifiable} is {@code true}
	 * and it attempted to change the unchangeable items in the existing TimeSeries
	 * with the specified name.
	 * @throws GSException if the specified type is not proper as a type of a
	 * Row object. For more information, see the description of {@link Container}.
	 * @throws GSException if a timeout occurs during this operation or a
	 * connection failure occurs; or if called after the connection is closed.
	 * @throws NullPointerException if a {@code null} parameter(s) is specified.
	 */
	public <R> TimeSeries<R> putTimeSeries(
			String name, Class<R> rowType,
			TimeSeriesProperties props, boolean modifiable)
			throws GSException;

	/**
	 * Return a {@link Collection} object to manipulate a Collection
	 * with the specified name.
	 *
	 * <p>For the correspondence between a specified type and a Column layout,
	 * see the description of {@link Container}.</p>
	 *
	 * @param name Name of a Collection subject to processing
	 * @param rowType Type of Row object corresponding to the column layout
	 * of the Collection subject to processing
	 *
	 * @return A Collection object if a {@link Collection} with the specified
	 * name exist; or {@code null} if not.
	 *
	 * @throws GSException if a TimeSeries with the same name exists.
	 * @throws GSException if the specified type and the existing Column layout
	 * conflict each other.
	 * @throws GSException if the specified type is not proper as a type of a
	 * Row object. For more information, see the description of {@link Container}.
	 * @throws GSException if a timeout occurs during this operation or a
	 * connection failure occurs; or if called after the connection is closed.
	 * @throws NullPointerException if a {@code null} parameter(s) is specified.
	 */
	public <K, R> Collection<K, R> getCollection(
			String name, Class<R> rowType) throws GSException;

	/**
	 * Returns a {@link TimeSeries} object to manipulate a TimeSeries with
	 * the specified name.
	 *
	 * <p>For the correspondence between a specified type and a Column layout,
	 * see the description of {@link Container}.</p>
	 *
	 * @param name Name of TimeSeries subject to processing
	 * @param rowType Type of Row object corresponding to the column layout
	 * of the TimeSeries subject to processing
	 *
	 * @return A {@link TimeSeries} object if a TimeSeries with the specified
	 * name exists; or {@code null} if not.
	 *
	 * @throws GSException if a Collection with the same name exists.
	 * @throws GSException if the specified type and the existing Column layout
	 * conflict each other.
	 * @throws GSException if the specified type is not proper as a type of a
	 * Row object. For more information, see the description of {@link Container}.
	 * @throws GSException if a timeout occurs during this operation or a
	 * connection failure occurs; or if called after the connection is closed.
	 * @throws NullPointerException if a {@code null} parameter(s) is specified.
	 */
	public <R> TimeSeries<R> getTimeSeries(
			String name, Class<R> rowType) throws GSException;

	/**
	 * Deletes a Collection with the specified name.
	 *
	 * <p>If the specified Collection is already deleted, nothing is changed. </p>
	 *
	 * <p>When a transaction(s) is active in a target Collection, it deletes
	 * the Collection after waiting for the transaction(s) to complete. </p>
	 *
 	 * @param name Name of Collection subject to processing
	 *
	 * @throws GSException if the Container type is unmatched.
	 * @throws GSException if a timeout occurs during this operation or
	 * a connection failure occurs; or if called after the connection is
	 * closed.
	 * @throws NullPointerException if a {@code null} parameter is specified.
	 */
	public void dropCollection(String name) throws GSException;

	/**
	 * Deletes a TimeSeries with the specified name.
	 *
	 * <p>If the specified TimeSeries is already deleted, nothing is changed. </p>
	 *
	 * <p>When a transaction(s) is active in a target TimeSeries, it deletes
	 * the TimeSeries after waiting for the transaction(s) to complete. </p>
	 *
	 * @param name Name of TimeSeries subject to processing
	 *
	 * @throws GSException if the Container type is unmatched.
	 * @throws GSException if a timeout occurs during this operation or
	 * a connection failure occurs; or if called after the connection is
	 * closed.
	 * @throws NullPointerException if a {@code null} parameter is specified.
	 */
	public void dropTimeSeries(String name) throws GSException;

	/**
	 * Disconnects with GridDB and releases related resources as necessary.
	 *
	 * <p>Even if {@link GSException} is thrown, the connection and local resources
	 * are released properly. However, the transaction state etc. might remain in
	 * GridDB. If the connection is already closed, this method will not work
	 * effectively. </p>
	 *
	 * @throws GSException if a connection failure occurs
	 */
	public void close() throws GSException;

	/**
	 * Specify {@link ContainerInfo} and create a new Container or update a Container.
	 *
	 * <p>Excluding the next point, the behavior will be the same
	 * as {@link #putCollection(String, Class, boolean)}
	 * or {@link #putTimeSeries(String, Class, TimeSeriesProperties, boolean)}.
	 * <ul>
	 * <li>Use {@link ContainerInfo} to specify the Container type, column layout,
	 * as well as the TimeSeries composition option where necessary</li>
	 * <li>The Row object type of the {@link Container} returned will always
	 * be {@link Row}</li>
	 * </ul>
	 * Arguments {@code modifiable} with the same respective name are used
	 * in the same way as well.</p>
	 *
	 * <p>A list of the methods to specify Container-related information is
	 * given below.
	 * <table>
	 * <thead><tr><th>item</th><th>argument</th><th>description</th></tr></thead>
	 * <tbody>
	 *
	 * <tr><td>Container name</td><td>{@code name} or {@code info}</td>
	 * <td>Specify a value that is not {@code null} in at least one of the arguments.
	 * A different value must be specified when specifying both sides. </td></tr>
	 * <tr><td>Container type</td><td>{@code info}</td>
	 * <td>Specify a value that is not {@code null}.
	 * If {@link ContainerType#COLLECTION} is specified, the behavior
	 * will be the same as {@link #putCollection(String, Class, boolean)}.
	 * If {@link ContainerType#TIME_SERIES} is specified, the behavior will be the same as
	 * {@link #putTimeSeries(String, Class, TimeSeriesProperties, boolean)}. </td></tr>
	 * <tr><ts>column layout</td><td>{@code info}</td>
	 * <td>Set the {@link ColumnInfo} list and whether there is any Row key
	 * so as to conform to the restrictions stipulated in {@link Container}. </td></tr>
	 * <tr><td>ignore column sequence</td><td>{@code info}</td>
	 * <td>If ignored, no verification of the conformance with the column sequence
	 * of existing Containers with the same name will be carried out. </td></tr>
	 * <tr><td>TimeSeries composition option</td><td>{@code info}</td>
	 * <td>A value that is not {@code null} can be specified only
	 * if the Container type is {@link ContainerType#TIME_SERIES}. </td></tr>
	 * <tr><td>index setting</td><td>{@code info}</td>
	 * <td>Ignored in the current version.
	 * In future versions, if settings that do not conform to the rules of
	 * {@link Container#createIndex(String, IndexType)} are included,
	 * an exception may be sent out. </td></tr>
	 * <tr><td>trigger setting</td><td>{@code info}</td>
	 * <td>Ignored in the current version. In future versions, if settings
	 * that do not conform to the rules of
	 * {@link Container#createTrigger(TriggerInfo)} are included,
	 * an exception may be sent out. </td></tr>
	 * <tr><td>Container similarity</td><td>{@code info}</td>
	 * <td>The specified contents will be reflected if a setting other than
	 * {@code null} is specified and newly created. The settings
	 * of an existing Container cannot be changed. The settings are ignored
	 * if {@code null} is specified. </td></tr>
	 * </tbody>
	 * </table>
	 * </p>
	 *
	 * @param name Name of a Container subject to processing
	 * @param info Information of a Container subject to processing
	 * @param modifiable To permit a change in the column layout of existing
	 * Container data or not
	 *
	 * @return Corresponding {@link Container}. If {@link ContainerType#COLLECTION}
	 * is specified as the Container type,
	 * the instance created will be a {@link Collection} instance.
	 * If {@link ContainerType#TIME_SERIES} is specified,
	 * the instance created will be a {@link TimeSeries} instance.
	 *
	 * @throws GSException If the contents of the arguments {@code name} and
	 * {@code info} do not conform to the rules.
	 * If the contents also do not conform to the rules of the new Container
	 * creation and update method for the specified Container type
	 * @throws GSException If this process times out, a connection failure were
	 * to occur, or if it is invoked after being closed
	 * @throws NullPointerException If {@code null} is specified in the argument
	 * {@code info}
	 *
	 * @see #putCollection(String, Class, boolean)
	 * @see #putTimeSeries(String, Class, TimeSeriesProperties, boolean)
	 * @see Container
	 */
	public <K> Container<K, Row> putContainer(String name,
			ContainerInfo info, boolean modifiable) throws GSException;

	/**
	 * Get a {@link Container} object whose Rows can be processed using
	 * a {@link Row}.
	 *
	 * <p>Excluding the next point, the behavior will be the same as
	 * {@link #getCollection(String, Class)} or {@link #getTimeSeries(String, Class)}.
	 * <ul>
	 * <li>Return a {@link Container} object based on the existing Container type
	 * and column layout</li>
	 * <li>No error accompanying these non-conformances will occur as the Container
	 * type and column layout are not specified</li>
	 * <li>The Row object type of the {@link Container} returned will always be
	 * {@link Row}</li>
	 * </ul>
	 * Arguments {@code name} with the same respective name are used in the same way
	 * as well.</p>
	 *
	 * @param name Name of a Container subject to processing
	 *
	 * @return Corresponding {@link Container} if a Container exists and {@code null} if not.
	 * If a Container exists and the type is {@link ContainerType#COLLECTION},
	 * the instance created will be a {@link Collection} instance. If the type
	 * is {@link ContainerType#TIME_SERIES}, the instance created will be a
	 * {@link TimeSeries} instance.
	 *
	 * @throws GSException If this process times out, a connection failure were
	 * to occur, or if it is invoked after being closed
	 * @throws NullPointerException If {@code null} is specified in the argument
	 *
	 * @see #getCollection(String, Class)
	 * @see #getTimeSeries(String, Class)
	 */
	public <K> Container<K, Row> getContainer(
			String name) throws GSException;

	/**
	 * Specify {@link ContainerInfo} and create a new Collection or update
	 * a Collection.
	 *
	 * <p>Except for points where the Container type is limited to
	 * {@link ContainerType#COLLECTION} and the returned type is {@link Collection},
	 * the behavior will be the same as {@link #putContainer(String, ContainerInfo,
	 * boolean)}.</p>
	 *
	 * @param name Name of Collection subject to processing
	 * @param info Collection information subject to processing Specify either
	 * {@code null} or {@link ContainerType#COLLECTION} in the Container type
	 * @param modifiable To permit a change in the column layout of an existing
	 * Collection or not
	 *
	 * @return Corresponding {@link Collection}.
	 *
	 * @throws GSException If specifications other than the Container type do not
	 * conform to the rules of {@link #putContainer(String, ContainerInfo, boolean)}.
	 * If the specifications do not conform to the restrictions related
	 * to the Container type as well
	 * @throws GSException If this process times out, a connection failure were
	 * to occur, or if it is invoked after being closed
	 * @throws NullPointerException If {@code null} is specified in the argument
	 * {@code info}
	 *
	 * @see #putContainer(String, ContainerInfo, boolean)
	 */
	public <K> Collection<K, Row> putCollection(String name,
			ContainerInfo info, boolean modifiable) throws GSException;

	/**
	 * Get a {@link Collection} object whose Rows can be processed using a {@link Row}.
	 *
	 * <p>Except for points where the expected Container type is limited to
	 * {@link ContainerType#COLLECTION} and the returned type is {@link Collection},
	 * the behavior will be the same as {@link #getContainer(String)}.</p>
	 *
	 * @return Corresponding {@link Collection} if a Collection exists
	 * and {@code null} if not.
	 *
	 * @throws GSException If a TimeSeries with the same name exists
	 * @throws GSException If this process times out, a connection failure were
	 * to occur, or if it is invoked after being closed
	 * @throws NullPointerException If {@code null} is specified in the argument
	 *
	 * @see #getContainer(String)
	 */
	public <K> Collection<K, Row> getCollection(
			String name) throws GSException;

	/**
	 * Specify {@link ContainerInfo} and create a new or update TimeSeries.
	 *
	 * <p>Except for points where the Container type is limited
	 * to {@link ContainerType#TIME_SERIES} and the returned type
	 * is {@link TimeSeries}, the behavior will be the same as
	 * {@link #putContainer(String, ContainerInfo, boolean)}.</p>
	 *
	 * @param name Name of TimeSeries subject to processing
	 * @param info Information of TimeSeries subject to processing. Specify
	 * either {@code null} or {@link ContainerType#TIME_SERIES} in the Container type
	 * @param modifiable To permit a change in the column layout of an existing
	 * TimeSeries or not
	 *
	 * @return Corresponding {@link TimeSeries}
	 *
	 * @throws GSException If specifications other than the Container type do not
	 * conform to the rules of {@link #putContainer(String, ContainerInfo, boolean)}.
	 * If the specifications do not conform to the restrictions related
	 * to the Container type as well
	 * @throws GSException If this process times out, a connection failure were
	 * to occur, or if it is invoked after being closed
	 * @throws NullPointerException If {@code null} is specified in the argument
	 * {@code info}
	 *
	 * @see #putContainer(String, ContainerInfo, boolean)
	 */
	public TimeSeries<Row> putTimeSeries(String name,
			ContainerInfo info, boolean modifiable) throws GSException;

	/**
	 * Get a {@link TimeSeries} object whose Rows can be processed using
	 * a {@link Row}.
	 *
	 * <p>Except for points where the expected Container type is limited
	 * to {@link ContainerType#TIME_SERIES} and the returned type is
	 * {@link TimeSeries}, the behavior will be the same as
	 * {@link #getTimeSeries(String)}.</p>
	 *
	 * @return Corresponding {@link TimeSeries} if a TimeSeries exists
	 * and {@code null} if not.
	 *
	 * @throws GSException If a Collection with the same name exists
	 * @throws GSException If this process times out, a connection
	 * failure were to occur, or if it is invoked after being closed
	 * @throws NullPointerException If {@code null} is specified in the
	 * argument
	 *
	 * @see #getContainer(String)
	 */
	public TimeSeries<Row> getTimeSeries(String name) throws GSException;

	/**
	 * Delete a Container having the specified name
	 *
	 * <p>Nothing is changed if the Container has already been deleted.</p>
	 *
	 * <p>If a transaction under execution exists in a Container subject
	 * to processing, the system will wait for these to be completed
	 * before deleting the data.</p>
	 *
	 * @param name Name of a Container subject to processing
	 *
	 * @throws GSException Name of a Container subject to processing
	 * @throws NullPointerException If {@code null} is specified in the argument
	 *
	 * @see #dropCollection(String)
	 * @see #dropTimeSeries(String)
	 */
	public void dropContainer(String name) throws GSException;

	/**
	 * Specify {@link ContainerInfo} and create a new {@link Row}.
	 *
	 * <p>Include the {@link ColumnInfo} list and whether there is any Row key
	 * so as to conform to the restrictions stipulated in {@link Container}.
	 * Specify the column layout in {@link ContainerInfo}.</p>
	 *
	 * <p>In addition, by including the Container type in {@link ContainerInfo},
	 * it can be verified whether the restrictions unique to a specific Container
	 * type are conformed to or not.
	 * However, the Container type will not be included even if a
	 * {@link Row#getSchema()} is invoked against the created {@link Row}.</p>
	 *
	 * <p>The existing initial value is set in each field of the created {@link Row}.
	 * This initial value is the same as the value of each field in the
	 * column newly added in {@link #putContainer(String, ContainerInfo, boolean)}.
	 * Refer to the definition of {@link #putCollection(String, Class, boolean)}
	 * for a list of the specific values.</p>
	 *
	 * <p>The operation on the created {@link Row} also does not affect whether this
	 * {@link GridStore} object is closed or not.</p>
	 *
	 * @param info Container information including the column layout. Other contents
	 * are ignored
	 *
	 * @return Created {@link Row}
	 *
	 * @throws GSException If the Container type or restrictions of the column layout
	 * are not conformed to
	 * @throws GSException If invoked after being closed
	 * @throws NullPointerException If {@code null} is specified in the argument
	 *
	 * @see Container
	 */
	public Row createRow(ContainerInfo info) throws GSException;

	/**
	 * Query execution and fetch is carried out on a specified arbitrary number
	 * of {@link Query}, with the request unit enlarged as much as possible.
	 *
	 * <p>For each {@link Query} included in a specified list, perform
	 * a similar query execution and fetch as when {@link Query#fetch()}
	 * is performed individually and set the {@link RowSet} in the results.
	 * Use {@link Query#getRowSet()} to extract the execution results of each
	 * {@link Query}. However, unlike the case when carried out individually,
	 * the target node is requested for the same storage destination, etc.
	 * with a unit that is as large as possible. Based on this, the larger
	 * the number of elements in the list, the higher is the possibility
	 * that the number of correspondences with the target node will be reduced.
	 * {@link Query} in a list are not executed in any particular order.</p>
	 *
	 * <p>Only a {@link Query} that has not been closed, including corresponding
	 * {@link Container} acquired via this {@link GridStore} object, can be included
	 * in a specified list.
	 * Like a {@link Query#fetch()}, the {@link RowSet} formed last and held
	 * by each {@link Query} will be closed.
	 * If the same instance is included multiple times in a list, the behavior
	 * will be the same as the case in which the respective instances differ.</p>
	 *
	 * <p>Like other Container or Row operations, consistency between Containers
	 * is not guaranteed.
	 * Therefore, the processing results for a certain Container may be affected
	 * by other operation commands that have been completed prior to the start
	 * of the process.</p>
	 *
	 * <p>The commit mode of each {@link Container} corresponding to the specified
	 * {@link Query} can be used in either the auto commit mode or manual
	 * commit mode.
	 * The transaction status is reflected in the execution results of the query.
	 * If the operation is completed normally, the corresponding transaction of
	 * each {@link Container} will not be aborted so long as the transaction
	 * timeout time has not been reached.</p>
	 *
	 * <p>If an exception occurs in the midst of processing each {@link Query},
	 * a new {@link RowSet} may be set for only some of the {@link Query}. In addition,
	 * uncommitted transactions of each {@link Query} corresponding to the designated
	 * {@link Container} may be aborted.</p>
	 *
	 * <p>If the system tries to acquire a large number of Rows all at once,
	 * the upper limit of the communication buffer size managed by the GridDB
	 * node may be reached, possibly resulting in a failure.
	 * Refer to "System limiting values" in the GridDB API Reference for the upper limit size.</p>
	 *
	 * @param queryList List of {@link Query} targeted
	 *
	 * @throws If a {@link Query} other than a {@link Query} obtained via this
	 * {@link GridStore} object is included
	 * @throws GSException If the system tries to execute a query containing
	 * a wrong parameter, syntax or command. For example, when a column type
	 * that is not compatible with the argument of the function is specified
	 * in the TQL. Refer to the various definitions of the function to create
	 * this query for the specific restrictions
	 * @throws GSException If the execution results do not conform to the expected
	 * type of each component of the {@link RowSet}, when a TQL is executed.
	 * @throws GSException If this process or related transaction times out,
	 * if the corresponding Container is deleted or the schema is changed,
	 * if a connection failure occurs, or if the corresponding Container is
	 * invoked after being closed
	 * @throws NullPointerException If {@code null} is specified in the argument
	 * {@code queryList}, or if {@code null} is included as a component
	 * the argument {@code queryList}
	 * @throws NullPointerException If a non-permitted {@code null} is included
	 * in the parameter given when creating this query. To be executed in GridDB
	 * The evaluation results of the TQL text will not be sent out
	 *
	 * @see Query#fetch()
	 */
	public void fetchAll(
			List<? extends Query<?>> queryList) throws GSException;

	/**
	 * New creation or update operation is carried out on an arbitrary number
	 * of rows of a Container, with the request unit enlarged as much as possible.
	 *
	 * <p>For each Row object included in a specified map, a new creation
	 * or update operation is carried out just like the case when
	 * {@link Container#put(Object)} is invoked individually.
	 * However, unlike the case when carried out individually, the target node
	 * is requested for the same storage destination, etc. with a unit
	 * that is as large as possible. Based on this, the larger the total number
	 * of Row objects specified and the larger the total number of target Containers,
	 * the higher is the possibility that the number of correspondences
	 * with the target node will be reduced.</p>
	 *
	 * <p>A specified map is composed of an arbitrary number of entries that adopt
	 * the Container name as its key and list of Row objects as its value.
	 * A subject Container may be a mixture of different Container types and
	 * column layouts. However, the Containers must already exist. The Container
	 * cannot include {@code null} as a key or value of the map.</p>
	 *
	 * <p>An arbitrary number of {@link Row} with the same column layout as the
	 * subject Container can be included in each list of Row objects.
	 * In the current version, all the column sequences must also be the same.
	 * The Container cannot include {@code null} as a component of the list.</p>
	 *
	 * <p>Depending on the Container type and setting, the same restrictions
	 * as {@link Container#put(Object)} are established for the contents of Rows
	 * that can be operated.
	 * Refer to the sub-interface definition of {@link Container} for the specific
	 * restrictions.</p>
	 *
	 * <p>If there are multiple Row objects having the same Row key targeting
	 * the same Container in the designated map, the contents of the rear-most
	 * Row object having a Row key with the same value will be reflected using
	 * the take-out sequence from the iterator of the map entry group as
	 * a reference if it is between different lists, or the component sequence
	 * of the list as a reference if it is within the same list.</p>
	 *
	 * <p>The transaction cannot be maintained and the lock cannot continue
	 * to be retained.
	 * However, if the lock that affects the target Row is secured by an existing
	 * transaction, the system will continue to wait for all the locks
	 * to be released.</p>
	 *
	 * <p>Like other Container or Row operations, consistency between Containers
	 * is not guaranteed.
	 * Therefore, the processing results for a certain Container may be affected
	 * by other operation commands that have been completed prior to the start
	 * of the process.</p>
	 *
	 * <p>If an exclusion occurs in the midst of processing a Container
	 * and its Rows, only the results for some of the Rows of some of the
	 * Containers may remain reflected.</p>
	 *
	 * @param containerRowsMap A map made up of a list of Row objects
	 * and target Container names
	 *
	 * @throws GSException If the target Container does not exist, or if the column
	 * layouts of the target Container and Row object do not match
	 * @throws GSException When an operation violating the restrictions unique
	 * to a specific Container type is carried out
	 * @throws GSException If this process or transaction times out, a connection
	 * failure were to occur, or if it is invoked after being closed, or if a value
	 * outside the supported range is included in the Row object
	 * @throws NullPointerException If {@code null} is specified as an argument
	 * {@code containerRowsMap}, if {@code null} is included as a key or value
	 * of this map, or if {@code null} is included as a component of the list
	 * constituting the map
	 *
	 * @see Container#put(Object)
	 */
	public void multiPut(
			Map<String, List<Row>> containerRowsMap) throws GSException;

	/**
	 * Get an arbitrary number and range of Rows of a Container based on the
	 * specified conditions, with the request unit enlarged as much as possible.
	 *
	 * <p>Get the Row contents in accordance with the conditions included in the
	 * specified map, similar to invoking {@link Container#get(Object)}
	 * or {@link Query#fetch()} individually. However, unlike the case
	 * when carried out individually, the target node is requested for the same
	 * storage destination, etc. with a unit that is as large as possible.
	 * Based on this, the larger the total number of Rows conforming
	 * to the conditions and the larger the total number of target Containers,
	 * the higher is the possibility that the number of correspondences
	 * with the target node will be reduced.</p>
	 *
	 * <p>A specified map is composed of an arbitrary number of entries
	 * that adopt the Container name as the key and the acquisition condition
	 * represented by {@link RowKeyPredicate} as the value.
	 * Multiple instances with the same {@link RowKeyPredicate} can also be included.
	 * In addition, a subject Container may be a mixture of different Container
	 * types and column layouts.
	 * However, there are some acquisition conditions that cannot be evaluated
	 * due to the composition of the Container. Refer to the definitions of
	 * the various setting functions for {@link RowKeyPredicate} for the specific
	 * restrictions.
	 * In addition, the specified Container name must be a real Container.
	 * The Container cannot include {@code null} as a key or value of the map.</p>
	 *
	 * <p>A returned map is composed of entries that adopt the Container name
	 * as its key and list of Row objects as its value. All the Container names
	 * included in a specified map as acquisition conditions are included
	 * in a returned map.
	 * If the same Container is specified and multiple entries in which
	 * Container names with different notations in uppercase and lowercase
	 * letters are set are included in a specified map, a single entry
	 * consolidating these is stored in the returned map.
	 * If multiple Row objects are included in the same list, the stored sequence
	 * follows the Container type and the definition of the sub-interface
	 * of the corresponding {@link Container}.
	 * If not a single Row corresponding to the specified Container exists,
	 * the list of corresponding Row objects is blank.</p>
	 *
	 * <p>When a returned map or list included in a map is updated, whether
	 * an exception will occur or not when {@link UnsupportedOperationException},
	 * etc. is executed is still not defined.</p>
	 *
	 * <p>Like other Container or Row operations, consistency between Containers
	 * is not guaranteed.
	 * Therefore, the processing results for a certain Container may be affected
	 * by other operation commands that have been completed prior to the start
	 * of the process.</p>
	 *
	 * <p>Like {@link Container#get(Object, boolean)} or {@link Query#fetch(boolean)},
	 * a transaction cannot be maintained and requests for updating locks cannot
	 * be made.</p>
	 *
	 * <p>If the system tries to acquire a large number of Rows all at once,
	 * the upper limit of the communication buffer size managed by the GridDB
	 * node may be reached, possibly resulting in a failure.
	 * Refer to "System limiting values" in the GridDB API Reference for the upper limit size.</p>
	 *
	 * @param containerPredicateMap Map made up of targeted Container names
	 * and conditions
	 *
	 * @return Map that maintains Row groups conforming to the conditions
	 * by Container
	 *
	 * @throws GSException If the subject Container does not exist.
	 * If acquisition conditions concerning a specified Container that cannot be
	 * evaluated are specified
	 * @throws GSException If this process or transaction times out, a connection
	 * failure were to occur, or if it is invoked after being closed
	 * @throws NullPointerException If {@code null} is specified as an argument
	 * {@code containerRowsPredicate}, if {@code null} is included as a key or
	 * value of this map
	 *
	 * @see Container#get(Object)
	 * @see Query#fetch()
	 * @see RowKeyPredicate
	 */
	public Map<String, List<Row>> multiGet(
			Map<String, ? extends RowKeyPredicate<?>> containerPredicateMap)
					throws GSException;

	/**
	 * Get {@link PartitionController} on the corresponding GridDB cluster.
	 *
	 * <p>Use is no longer possible once this {@link GridStore} is closed.</p>
	 *
	 * @return {@link PartitionController} on the corresponding GridDB cluster
	 *
	 * @throws GSException If invoked after being closed
	 *
	 * @see PartitionController
	 */
	public PartitionController getPartitionController() throws GSException;

	/**
	 * Specify the Row object type and {@link ContainerInfo} and create a new
	 * Container or update a Container.
	 *
	 * <p>Mainly used when specifying the type of Row object and creating a new
	 * Container with additional settings.</p>
	 *
	 * <p>Same as {@link #putContainer(String, ContainerInfo, boolean)}
	 * if points for which the ignore setting of the column layout and column
	 * sequence cannot be specified are excluded.</p>
	 *
	 * @param name Name of a Container subject to processing
	 * @param rowType Type of Row object corresponding to the column layout
	 * of the Collection subject to processing
	 * @param info Information of a Container subject to processing. Ignored
	 * if {@code null} is specified
	 * @param modifiable To permit a change in the column layout of existing
	 * Container data or not
	 *
	 * @return Corresponding {@link Container}. If {@link ContainerType#COLLECTION}
	 * is specified as the Container type, the instance created will be a
	 * {@link Collection} instance. If {@link ContainerType#TIME_SERIES}
	 * is specified, the instance created will be a {@link TimeSeries} instance.
	 *
	 * @throws GSException If the contents of the arguments {@code name}
	 * and {@code info} do not conform to the rules. If the contents also do not
	 * conform to the rules of the new Container creation and update method
	 * for the specified Container type
	 * @throws GSException If the specified type is not suitable as the Row
	 * object type.
	 * Refer to the definition of {@link Container} for details.
	 *
	 * @throws GSException If this process times out, a connection failure
	 * were to occur, or if it is invoked after being closed
	 * @throws NullPointerException If this process times out, a connection
	 * failure were to occur, or if it is invoked after being closed
	 *
	 * @see #putContainer(String, ContainerInfo, boolean)
	 * @see #putCollection(String, Class, boolean)
	 * @see #putTimeSeries(String, Class, TimeSeriesProperties, boolean)
	 */
	public <K, R> Container<K, R> putContainer(
			String name, Class<R> rowType,
			ContainerInfo info, boolean modifiable)
			throws GSException;

}
