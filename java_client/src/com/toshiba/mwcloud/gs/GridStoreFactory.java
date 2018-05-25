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

import java.io.Closeable;
import java.util.Properties;
import java.util.ServiceLoader;

import com.toshiba.mwcloud.gs.common.GridStoreFactoryProvider;
import com.toshiba.mwcloud.gs.common.LoggingUtils;
import com.toshiba.mwcloud.gs.common.LoggingUtils.BaseGridStoreLogger;

/**
 * Manages a {@link GridStore} instance.
 *
 * <p>It manages the client settings shared by {@link GridStore} instances and used connections.</p>
 *
 * <p>To access GridDB, you need to get a {@link GridStore} instance using this Factory.</p>
 *
 * <p>All the public methods of this class are thread safe.</p>
 *
 * <p>Client Logging and Client Properties File are available.</p>
 *
 * <p><b>Client Logging</b></p>
 * <p>Logging is enabled by adding the logging library SLF4J to the classpath.</p>
 * <p>The name of the logger begins with "com.toshiba.mwcloud.gs.GridStoreLogger". The recommended version of SLF4J
 * is 1.6.0 or above.</p>
 *
 * <p><b>Client Properties File</b></p>
 * <p>By including both of the directory including the properties file "gs_client.properties"
 * and the configuration library "gridstore-conf.jar" in the classpath,
 * properties of the file are applied to GridStoreFactory.
 * Connection properties can be changed without editing application codes
 * by using the properties file.
 * The priority of applying the properties of the file is higher than
 * the properties of Properties object.
 * The following properties are available for the Client Properties File.
 * <dl><dt>Factory Category Property</dt>
 * <dd>Properties conform to {@link GridStoreFactory#setProperties(Properties)} specifications are available.</dd>
 * <dd>Add "factory." before each property name as following.
 * <pre>factory.maxConnectionPoolSize = 10</pre>
 * </dd>
 * <dt>Store Category Property</dt>
 * <dd>Properties conform to {@link GridStoreFactory#getGridStore(Properties)} specifications are available.</dd>
 * <dd>Add "store." before each property name as following.
 * <pre>store.clusterName = Project1</pre>
 * </dd>
 * <dd>
 * <p>Exceptions will be thrown in the cases as following.</p>
 * <ul><li>If two or more directories of the properties files are included in the classpath</li>
 * <li>If only the configuration library is included in the classpath</li>
 * <li>If an unavailable property name is specified</li>
 * <li>If a specified property name is made up of only the category name</li></ul>
 * <p>By including only the directory including the properties file, properties
 * of the file are not applied to GridStoreFactory.</p>
 *
 */
public abstract class GridStoreFactory implements Closeable {

	static {
		registerLoggers();
	}

	private static final String DEFAULT_CLASS_NAME =
			"com.toshiba.mwcloud.gs.subnet.SubnetGridStoreFactory";

	private static final GridStoreFactory INSTANCE = newInstance();

	protected GridStoreFactory() {
	}

	private static GridStoreFactory newInstance() {
	
		for (ClassLoader cl : new ClassLoader[] {
				GridStoreFactory.class.getClassLoader(),
				Thread.currentThread().getContextClassLoader(),
				ClassLoader.getSystemClassLoader()
		}) {
			for (GridStoreFactoryProvider provider : ServiceLoader.load(
					GridStoreFactoryProvider.class, cl)) {
				return provider.getFactory();
			}
		}

		try {
			return (GridStoreFactory)
					Class.forName(DEFAULT_CLASS_NAME).newInstance();
		}
		catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
		catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		catch (InstantiationException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Returns a default instance.
	 *
	 * <p>When loading this class, a default subclass of the {@link GridStoreFactory}
	 *  class is loaded and an instance is created.</p>
	 */
	public static GridStoreFactory getInstance() {
		return INSTANCE;
	}

	/**
	 * Returns a {@link GridStore} with the specified properties.
	 *
	 * <p>When obtaining {@link GridStore}, it just searches for the name of a master node
	 * (hereafter, a master) administering each {@link Container} as necessary, but
	 * authentication is not performed. When a client really needs to connect to
	 * a node corresponding to each Container, authentication is performed.</p>
	 *
	 * <p>The following properties can be specified. Unsupported property
	 * names are ignored. See "System limiting values" in the GridDB API Reference for upper limit values
	 * of some properties.</p>
	 * <table>
	 * <thead><tr><th>Property</th><th>Description</th></tr></thead>
	 * <tbody>
	 * <tr><td>host</td><td>A destination host name. An IP address (IPV4 only) is
	 * also available. Mandatory for manually setting a master. For autodetection
	 * of a master, omit the setting.</td></tr>
	 * <tr><td>port</td><td>A destination port number. A string representing of
	 * a number from {@code 0} to {@code 65535}. Mandatory for manually setting a master.
	 * For autodetection of a master, omit the setting.</td></tr>
	 * <tr><td>notificationAddress</td><td>An IP address (IPV4 only) for receiving
	 * a notification used for autodetection of a master. A default address is
	 * used if omitted.
	 * This property cannot be specified with neither notificationMember nor
	 * notificationProvider properties at the same time.
	 * </td></tr>
	 * <tr><td>notificationPort</td><td>A port number for receiving a notification
	 * used for autodetection of a master. A string representing of a number
	 * from {@code 0} to {@code 65535}. A default port number is used if omitted.</td></tr>
	 * <tr><td>clusterName</td><td>A cluster name. It is used to verify whether it
	 * matches the cluster name assigned to the destination cluster. If it is omitted
	 * or an empty string is specified, cluster name verification is not performed.
	 * If a cluster name of over the upper length limit is specified, connection to the
	 * cluster node will fail. </td></tr>
	 * <tr><td>user</td><td>A user name</td></tr>
	 * <tr><td>password</td><td>A password for user authentication</td></tr>
	 * <tr><td>consistency</td><td>Either one of the following consistency levels:
	 * <dl>
	 * <dt>{@code "IMMEDIATE"}</dt><dd>The updates by other clients are committed
	 * immediately after a relevant transaction completes.</dd>
	 * <dt>{@code "EVENTUAL"}</dt><dd>The updates by other clients may not be
	 * committed even after a relevant transaction completes. No update operation
	 * cannot be applied to {@link Container}.</dd>
	 * </dl>
	 * By default, "IMMEDIATE" is selected.
	 * </td></tr>
	 * <tr><td>transactionTimeout</td><td>The minimum value of transaction timeout
	 * time. The transaction timeout is counted from the beginning of each
	 * transaction in a relevant {@link Container}. A string representing of a number from {@code 0}
	 * to {@link Integer#MAX_VALUE} in seconds. The value {@code 0} indicates
	 * that it is always uncertain whether a timeout error will occur during
	 * a subsequent transaction. If a value specified over the internal upper limit
	 * of timeout, timeout will occur at the internal upper limit value. If omitted,
	 * the default value used by a destination GridDB is applied.
	 * </td></tr>
	 * <tr><td>failoverTimeout</td><td>The minimum value of waiting time until
	 * a new destination is found in a failover. A numeric string representing
	 * from {@code 0} to {@link Integer#MAX_VALUE} in seconds. The value {@code 0}
	 * indicates that no failover is performed. If omitted, the default value
	 * used by this Factory is applied. </td></tr>
	 * <tr><td>containerCacheSize</td><td>The maximum number of ContainerInfos
	 * on the Container cache. A string representing of a number from
	 * {@code 0} to {@link Integer#MAX_VALUE}. The Container cache is not used if
	 * the value is {@code 0}. To obtain a {@link Container}, its ContainerInfo
	 * might be obtained from the Container cache instead of request to GridDB.
	 * A default number is used if omitted. </td></tr>
	 * <tr><td>dataAffinityPattern</td><td>Patterns of Container names and data
	 * affinity strings in order to set data affinity strings to their {@link Container}s
	 * automatically. Format is as following.
	 * <pre>(ContainerNamePattern1)=(DataAffinityString1),(ContainerNamePattern2)=(DataAffinityString2),...</pre>
	 * The dataAffinityPattern is applied to the new Container when it is created
	 * with the ContainerInfo which does not include a data affinity string.
	 * If a Container name matches one of the ContainerNamePattern of the dataAffinityPattern,
	 * the corresponding data affinity string is applied to the new Container.
	 * The priority order of matching and applying patterns is the order of specified
	 * patterns. ContainerNamePatterns maintain the general contract for Container
	 * names, except a wild card character "%" is also acceptable additionally. Data
	 * affinity patterns maintain the general contract for
	 * {@link ContainerInfo#setDataAffinity(String)}.</td></tr>
	 * <tr><td>notificationMember</td><td>
	 * A list of address and port pairs in cluster. It is used to connect to
	 * cluster which is configured with FIXED_LIST mode, and specified as
	 * follows.
	 * <pre>(Address1):(Port1),(Address2):(Port2),...</pre>
	 * This property cannot be specified with neither notificationAddress nor
	 * notificationProvider properties at the same time.
	 * This property is supported on version 2.9 or later.
	 * </td></tr>
	 * <tr><td>notificationProvider</td><td>
	 * A URL of address provider. It is used to connect to cluster which is
	 * configured with PROVIDER mode.
	 * This property cannot be specified with neither notificationAddress nor
	 * notificationMember properties at the same time.
	 * This property is supported on version 2.9 or later.
	 * 
	 * </td></tr>
	 * </tbody>
	 * </table>
	 *
	 * <p>A new {@link GridStore} instance is created by each call of this method.
	 * Operations on different {@link GridStore} instances and related objects are thread
	 * safe. That is, if some two objects are each created based on {@link GridStore}
	 * instances or they are just {@link GridStore} instances, and if they are related to
	 * different {@link GridStore} instances respectively, any method of one object can be
	 * called, no matter when a method of the other object may be called from any
	 * thread. However, since thread safety is not guaranteed for {@link GridStore} itself,
	 * it is not allowed to call a method of a single {@link GridStore} instance from two or
	 * more threads at an arbitrary time. </p>
	 *
	 * @param properties Properties specifying the settings for the object to be
	 * obtained.
	 *
	 * @throws GSException if host name resolution fails.
	 * @throws GSException if any specified property does not match the format
	 * shown above. If the properties match the format, no GSException is thrown
	 * even if connection or authentication will not succeed with their values.
	 * @throws GSException if the connection is closed.
	 * @throws NullPointerException {@code null} is specified as {@code properties}.
	 */
	public abstract GridStore getGridStore(
			Properties properties) throws GSException;

	/**
	 * Changes the settings for this Factory.
	 *
	 * <p>The changed settings are reflected in {@link GridStore} already created
	 * by this Factory and {@link GridStore} to be created by this Factory later. </p>
	 *
	 * <p>The following properties can be specified. Unsupported property names are ignored.</p>
	 * <table>
	 * <thead><tr><th>Property</th><th>Description</th></tr></thead>
	 * <tbody>
	 * <tr><td>maxConnectionPoolSize</td><td>The maximum number of connections in the
	 * connection pool used inside. A numeric string representing {@code 0} to
	 * {@link Integer#MAX_VALUE}. The value {@code 0} indicates no use of the
	 * connection pool. If omitted, the default value is used.</td></tr>
	 * <tr><td>failoverTimeout</td><td>The minimum value of waiting time until a new
	 * destination is found in a failover. A numeric string representing {@code 0}
	 * to {@link Integer#MAX_VALUE} in seconds. The value {@code 0} indicates
	 * that no failover is performed. If omitted, the default value is used.
	 * </td></tr>
	 * </tbody>
	 * </table>
	 *
	 * @throws GSException if any specified property does not match the format shown above.
	 * @throws GSException if the connection is closed.
	 * @throws NullPointerException {@code null} is specified as {@code properties}.
	 */
	public abstract void setProperties(Properties properties) throws GSException;

	/**
	 * Closes all {@link GridStore} instances created by this Factory and release
	 * related resources as necessary.
	 *
	 * <p>Even if {@link GSException} is thrown, all related connection resources
	 * are released. If the connection is already closed, this method will not work
	 * effectively. It is also called when stopping the current VM.</p>
	 *
	 * @throws GSException if an connection failure etc. occurs while closing.
	 *
	 * @see Closeable#close()
	 */
	public abstract void close() throws GSException;

	private static void registerLoggers() {
		boolean loggerAvailable = false;
		try {
			Class.forName("org.slf4j.Logger");
			Class.forName("org.slf4j.LoggerFactory");
			loggerAvailable = true;
		}
		catch (ClassNotFoundException e) {
		}

		for (String subName : LoggingUtils.SUB_LOGGER_NAMES) {
			final BaseGridStoreLogger logger;
			if (loggerAvailable) {
				try {
				
				
					logger = (BaseGridStoreLogger) Class.forName(
							LoggingUtils.DEFAULT_LOGGER_NAME + "$" +
							subName).newInstance();
				}
				catch (Exception e) {
					throw new Error(e);
				}
			}
			else {
				logger = null;
			}
			LoggingUtils.registerLogger(subName, logger);
		}
	}

}
