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
package com.toshiba.mwcloud.gs.partitioned;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.PartitionController;
import com.toshiba.mwcloud.gs.common.ContainerKeyPredicate;
import com.toshiba.mwcloud.gs.common.Extensibles;
import com.toshiba.mwcloud.gs.common.ContainerKeyConverter.ContainerKey;
import com.toshiba.mwcloud.gs.common.Extensibles.AsPartitionController;
import com.toshiba.mwcloud.gs.subnet.SubnetGridStore;

class PartPartitionController
implements PartitionController, Extensibles.AsPartitionController {

	private static final ContainerKeyPredicate DEFAULT_PREDICATE =
			createContainerKeyPredicate(true);

	private static final ContainerKeyPredicate COMPATIBLE_PREDICATE =
			createContainerKeyPredicate(false);

	private final Extensibles.AsPartitionController base;

	public PartPartitionController(AsPartitionController base) {
		this.base = base;
	}

	@Override
	public int getPartitionCount() throws GSException {
		return base.getPartitionCount();
	}

	@Override
	public long getContainerCount(int partitionIndex) throws GSException {
		final ContainerKeyPredicate pred = getContainerKeyPredicate();
		final boolean internalMode = true;
		return base.getContainerCount(partitionIndex, pred, internalMode);
	}

	@Override
	public List<String> getContainerNames(
			int partitionIndex, long start, Long limit) throws GSException {
		final ContainerKeyPredicate pred = getContainerKeyPredicate();
		final boolean internalMode = true;
		return base.getContainerNames(
				partitionIndex, start, limit, pred, internalMode);
	}

	@Override
	public List<InetAddress> getHosts(int partitionIndex) throws GSException {
		return base.getHosts(partitionIndex);
	}

	@Override
	public InetAddress getOwnerHost(int partitionIndex) throws GSException {
		return base.getOwnerHost(partitionIndex);
	}

	@Override
	public List<InetAddress> getBackupHosts(int partitionIndex)
			throws GSException {
		return base.getBackupHosts(partitionIndex);
	}

	@Override
	public void assignPreferableHost(int partitionIndex, InetAddress host)
			throws GSException {
		base.assignPreferableHost(partitionIndex, host);
	}

	@Override
	public int getPartitionIndexOfContainer(String containerName)
			throws GSException {
		final boolean internalMode = PartStore.WORKAROUND_WRONG_API_TEST;
		return base.getPartitionIndexOfContainer(containerName, internalMode);
	}

	@Override
	public void close() throws GSException {
		// TODO
		base.close();
	}

	@Override
	public long getContainerCount(
			int partitionIndex, ContainerKeyPredicate pred,
			boolean internalMode) throws GSException {
		return base.getContainerCount(partitionIndex, pred, internalMode);
	}

	@Override
	public List<String> getContainerNames(
			int partitionIndex, long start, Long limit,
			ContainerKeyPredicate pred, boolean internalMode)
			throws GSException {
		return base.getContainerNames(
				partitionIndex, start, limit, pred, internalMode);
	}

	@Override
	public int getPartitionIndexOfContainer(
			String containerName, boolean internalMode) throws GSException {
		return base.getPartitionIndexOfContainer(containerName, internalMode);
	}

	@Override
	public int getPartitionIndexOfContainer(
			ContainerKey containerKey, boolean internalMode)
			throws GSException {
		return base.getPartitionIndexOfContainer(containerKey, internalMode);
	}

	private static ContainerKeyPredicate createContainerKeyPredicate(
			boolean unified) {

		final Set<Integer> attributeSet = new HashSet<Integer>();
		for (int attribute : ContainerKeyPredicate.ofDefaultAttributes(
				unified).getAttributes()) {
			attributeSet.add(attribute);
		}
		attributeSet.add(PartContainer.Attributes.LARGE);

		final int[] attributeList = new int[attributeSet.size()];
		int i = 0;
		for (int attribute : attributeSet) {
			attributeList[i] = attribute;
			i++;
		}

		return new ContainerKeyPredicate(attributeList);
	}

	private static ContainerKeyPredicate getContainerKeyPredicate() {
		if (SubnetGridStore.isContainerAttributeUnified()) {
			return DEFAULT_PREDICATE;
		}
		else {
			return COMPATIBLE_PREDICATE;
		}
	}

}
