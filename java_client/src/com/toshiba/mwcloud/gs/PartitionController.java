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
import java.net.InetAddress;
import java.util.List;

/**
 * Controller for acquiring and processing the partition status.
 *
 * <p>A partition is a theoretical region where data is stored.
 * It is used to perform operations based on the data arrangement in a GridDB cluster.</p>
 */
public interface PartitionController extends Closeable {

	/**
	 * Get the number of partitions in the target GridDB cluster.
	 *
	 * <p>Get the value of the number of partitions set in the target GridDB cluster.
	 * Results are cached once acquired and until the next cluster failure and
	 * cluster node failure is detected, no inquiry will be sent to the GridDB cluster again.</p>
	 *
	 * @return Number of partitions
	 *
	 * @throws GSException If this process times out or when a connection error occurs,
	 * or if the process is invoked after this object or corresponding {@link GridStore} is closed
	 */
	public int getPartitionCount() throws GSException;

	/**
	 * Get the total number of containers belonging to a specified partition.
	 *
	 * <p>The calculated quantity when determining the number of containers is generally not dependent
	 * on the number of containers.</p>
	 *
	 * @param partitionIndex Partition index.  A value of {@code 0} or above and less than the number of partitions.
	 *
	 * @return Number of containers
	 *
	 * @throws GSException If a partition index outside the range is specified
	 * @throws GSException If this process times out, when a connection error occurs,
	 * or if the process is invoked after this object or corresponding {@link GridStore} is closed
	 *
	 * @see #getPartitionIndexOfContainer(String)
	 */
	public long getContainerCount(int partitionIndex) throws GSException;

	/**
	 * Get a list of the Container names belonging to a specified partition.
	 *
	 * <p>For the specified partition, the sequence of the list of acquisition results
	 * before and after will not be changed when the relevant Container is excluded
	 * even if a Container is newly created, its composition changed or the Container is deleted.
	 * All other lists are compiled in no particular order.
	 * No duplicate names will be included.</p>
	 *
	 * <p>If the upper limit of the number of acquisition cases is specified,
	 * the cases will be cut off starting from the ones at the back if the upper limit is exceeded.
	 * If no relevant specified condition exists, a blank list is returned.</p>
	 *
	 * <p>When a returned list is updated, whether an exception will occur or not
	 * when {@link UnsupportedOperationException}, etc. is executed is still not defined.</p>
	 *
	 * @param partitionIndex Partition index.  A value of {@code 0} or above and less than the number of partitions.
	 * @param start Start position of the acquisition range. A value of {@code 0} and above
	 * @param limit Upper limit of the number of cases acquired. If {@code null}, no upper limit is assumed
	 *
	 * @return Assuming the Container name as a component {@link List}
	 *
	 * @throws GSException If a partition index outside the range is specified
	 * @throws GSException If this process times out, when a connection error occurs,
	 * or if the process is invoked after this object or corresponding {@link GridStore} is closed
	 */
	public List<String> getContainerNames(
			int partitionIndex, long start, Long limit) throws GSException;

	/**
	 * Get a list of the addresses of the nodes corresponding to a specified partition.
	 *
	 * <p>The list will be compiled in no particular order. No duplicate address will be included.</p>
	 *
	 * <p>When a returned list is updated, whether an exception will occur or not
	 * when {@link UnsupportedOperationException}, etc. is executed is still not defined.</p>
	 *
	 * @param partitionIndex Partition index.  A value of {@code 0} or above and less than the number of partitions.
	 *
	 * @return Assuming {@link InetAddress}, which represents the address of the node, as a component, {@link List}
	 *
	 * @throws GSException If a partition index outside the range is specified
	 * @throws GSException If this process times out, when a connection error occurs,
	 * or if the process is invoked after this object or corresponding {@link GridStore} is closed
	 */
	public List<InetAddress> getHosts(int partitionIndex) throws GSException;

	/**
	 * Get the address of the owner node corresponding to a specified partition.
	 *
	 * <p>An owner node is a node that is always selected when {@code "IMMEDIATE"} is specified as a consistency level
	 * in {@link GridStoreFactory#getGridStore(java.util.Properties)}.</p>
	 *
	 * @param partitionIndex Partition index.  A value of {@code 0} or above and less than the number of partitions.
	 *
	 * @return Represents the address of the owner node {@link InetAddress}
	 *
	 * @throws GSException If a partition index outside the range is specified
	 * @throws GSException If this process times out, when a connection error occurs,
	 * or if the process is invoked after this object or corresponding {@link GridStore} is closed
	 */
	public InetAddress getOwnerHost(int partitionIndex) throws GSException;

	/**
	 * Get a list of the addresses of the backup nodes corresponding to a specified partition.
	 *
	 * <p>A backup node is a node that is selected with a higher priority when {@code "EVENTUAL"} is
	 * specified as a consistency level in {@link GridStoreFactory#getGridStore(java.util.Properties)}.</p>
	 *
	 * <p>The list will be compiled in no particular order. No duplicate address will be included.</p>
	 *
	 * <p>When a returned list is updated, whether an exception will occur or not
	 * when {@link UnsupportedOperationException}, etc. is executed is still not defined.</p>
	 *
	 * @param partitionIndex Partition index.  A value of {@code 0} or above and less than the number of partitions.
	 *
	 * @return Assuming {@link InetAddress}, which represents the address of the backup node,
	 * as a component, {@link List}
	 *
	 * @throws GSException If a partition index outside the range is specified
	 * @throws GSException If this process times out, when a connection error occurs,
	 * or if the process is invoked after this object or corresponding {@link GridStore} is closed
	 */
	public List<InetAddress> getBackupHosts(
			int partitionIndex) throws GSException;

	/**
	 * Set the address of the host to be prioritized in the selection.
	 *
	 * <p>If multiple possible destinations exist e.g. connections to backup nodes, etc.,
	 * the address set will always be selected if it is included in the candidate destination.
	 * The setting is ignored otherwise.</p>
	 *
	 * @param partitionIndex Partition index.  A value of {@code 0} or above and less than the number of partitions.
	 *
	 * @param host Address of the host to be prioritized in the selection. 
	 * For {@code null}, the setting is cancelled
	 *
	 * @throws GSException If a partition index outside the range is specified
	 * @throws GSException If invoked after this object or corresponding {@link GridStore} is closed.
	 *
	 * @see #getBackupHosts(int)
	 */
	public void assignPreferableHost(
			int partitionIndex, InetAddress host) throws GSException;

	/**
	 * Get the partition index corresponding to the specified Container name.
	 *
	 * <p>Once a GridDB cluster is constructed, there will not be any changes in the partitions of the destination
	 * that the Container belongs to and the partition index will also be fixed.
	 * Whether there is a Container corresponding to the specified name or not does not depend on the results</p>
	 *
	 * <p>Information required in the computation of the partition index is cached and
	 * until the next cluster failure and cluster node failure is detected,
	 * no inquiry will be sent to the GridDB cluster again.</p>
	 *
	 * @param containerName Container name. {@code null} cannot be specified
	 *
	 * @return Partition index corresponding to the specified Container name
	 *
	 * @throws GSException If a character string allowed as a Container name is specified
	 * @throws GSException If this process times out, when a connection error occurs,
	 * or if the process is invoked after this object or corresponding {@link GridStore} is closed
	 * @throws NullPointerException If {@code null} is specified in the argument
	 */
	public int getPartitionIndexOfContainer(
			String containerName) throws GSException;

	/**
	 * The connection status with GridDB is released and related resources are released where necessary.
	 *
	 * @throws GSException Not sent out in the current version
	 */
	@Override
	public void close() throws GSException;

}
