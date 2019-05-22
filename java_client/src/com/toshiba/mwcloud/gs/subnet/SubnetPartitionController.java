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
package com.toshiba.mwcloud.gs.subnet;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.PartitionController;
import com.toshiba.mwcloud.gs.common.BasicBuffer;
import com.toshiba.mwcloud.gs.common.ContainerKeyConverter;
import com.toshiba.mwcloud.gs.common.ContainerKeyConverter.ContainerKey;
import com.toshiba.mwcloud.gs.common.ContainerKeyPredicate;
import com.toshiba.mwcloud.gs.common.Extensibles;
import com.toshiba.mwcloud.gs.common.GSErrorCode;
import com.toshiba.mwcloud.gs.common.Statement;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.Context;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.FeatureVersion;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.OptionalRequest;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.OptionalRequestSource;

public class SubnetPartitionController
implements PartitionController, Extensibles.AsPartitionController {

	
	static final boolean WORKAROUND_WRONG_API_TEST = false;

	private static final ContainerKeyPredicate DEFAULT_PREDICATE =
			ContainerKeyPredicate.ofDefaultAttributes(true);

	private static final ContainerKeyPredicate COMPATIBLE_PREDICATE =
			ContainerKeyPredicate.ofDefaultAttributes(false);

	private static final OptionalRequestSource CONTAINER_NAMES_REQUEST_SOURCE =
			createContaierNamesRequestSource();

	private final GridStoreChannel channel;

	private final Context context;

	private boolean closed;

	public SubnetPartitionController(GridStoreChannel channel, Context context) {
		this.channel = channel;
		this.context = context;
	}

	private void checkOpened() throws GSException {
		if (closed) {
			throw new GSException(GSErrorCode.RESOURCE_CLOSED,
					"This controller already closed");
		}

		channel.checkContextAvailable(context);
	}

	private void checkPartitionIndex(int partitionIndex) throws GSException {
		final int partitionCount = channel.getPartitionCount(context);

		if (partitionIndex < 0 || partitionIndex >= partitionCount) {
			throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
					"Partition index out of range (" +
					"partitionIndex=" + partitionIndex + ", " +
					"partitionCount=" + partitionCount + ")");
		}
	}

	private static ContainerKeyPredicate getContainerKeyPredicate() {
		if (SubnetGridStore.isContainerAttributeUnified()) {
			return DEFAULT_PREDICATE;
		}
		else {
			return COMPATIBLE_PREDICATE;
		}
	}

	private static void putContainerKeyPredicate(
			BasicBuffer req, ContainerKeyPredicate pred) throws GSException {
		final int[] attributes = pred.getAttributes();
		if (attributes.length > 0) {
			req.putInt(attributes.length);
			for (int attribute : attributes) {
				req.putInt(attribute);
			}
		}
		else {
			throw new GSException(GSErrorCode.EMPTY_PARAMETER,
					"No container attribute specified");
		}
	}

	@Override
	public int getPartitionCount() throws GSException {
		checkOpened();
		return channel.getPartitionCount(context);
	}

	@Override
	public long getContainerCount(int partitionIndex) throws GSException {
		final ContainerKeyPredicate pred = getContainerKeyPredicate();
		return getContainerCount(partitionIndex, pred, false);
	}

	@Override
	public long getContainerCount(
			int partitionIndex, ContainerKeyPredicate pred,
			boolean internalMode) throws GSException {

		checkOpened();
		checkPartitionIndex(partitionIndex);

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);

		SubnetGridStore.tryPutSystemOptionalRequest(
				req, context, internalMode, false, null,
				CONTAINER_NAMES_REQUEST_SOURCE);

		final long start = 0;
		final long limit = 0;

		req.putLong(start);
		req.putLong(limit);
		putContainerKeyPredicate(req, pred);

		channel.executeStatement(
				context, Statement.GET_PARTITION_CONTAINER_NAMES.generalize(),
				partitionIndex, 0, req, resp, null);

		final long totalCount = resp.base().getLong();
		if (totalCount < 0) {
			throw new GSException(GSErrorCode.MESSAGE_CORRUPTED,
					"Negative result by protocol error");
		}

		return totalCount;
	}

	@Override
	public List<String> getContainerNames(
			int partitionIndex, long start, Long limit) throws GSException {
		final ContainerKeyPredicate pred = getContainerKeyPredicate();
		return getContainerNames(partitionIndex, start, limit, pred, false);
	}

	@Override
	public List<String> getContainerNames(
			int partitionIndex, long start, Long limit,
			ContainerKeyPredicate pred, boolean internalMode)
			throws GSException {

		checkOpened();
		checkPartitionIndex(partitionIndex);

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		SubnetGridStore.tryPutSystemOptionalRequest(
				req, context, internalMode, false, null,
				CONTAINER_NAMES_REQUEST_SOURCE);

		req.putLong(start);
		req.putLong(limit == null ? Long.MAX_VALUE : limit);
		putContainerKeyPredicate(req, pred);

		channel.executeStatement(
				context, Statement.GET_PARTITION_CONTAINER_NAMES.generalize(),
				partitionIndex, 0, req, resp, null);

		final ContainerKeyConverter keyConverter =
				context.getKeyConverter(internalMode, false);

		
		resp.base().getLong();

		final int entryCount = resp.base().getInt();
		final List<String> list = new ArrayList<String>();
		for (int i = 0; i < entryCount; i++) {
			list.add(keyConverter.get(resp, false, true).toString());
		}

		return list;
	}

	private List<InetAddress> toAddressList(InetSocketAddress[] sockAddrList) {
		final List<InetAddress> addrList = new ArrayList<InetAddress>();
		for (InetSocketAddress sockAddr : sockAddrList) {
			if (sockAddr != null) {
				if (!addrList.contains(sockAddr.getAddress())) {
					addrList.add(sockAddr.getAddress());
				}
			}
		}
		return addrList;
	}

	@Override
	public List<InetAddress> getHosts(int partitionIndex) throws GSException {
		checkOpened();
		checkPartitionIndex(partitionIndex);

		final InetSocketAddress[] baseList =
				channel.getNodeAddressList(context, partitionIndex);

		return toAddressList(baseList);
	}

	@Override
	public InetAddress getOwnerHost(int partitionIndex) throws GSException {
		checkOpened();
		checkPartitionIndex(partitionIndex);

		final InetSocketAddress[] baseList =
				channel.getNodeAddressList(context, partitionIndex);

		if (baseList.length == 0 || baseList[0] == null) {
			return null;
		}

		return baseList[0].getAddress();
	}

	@Override
	public List<InetAddress> getBackupHosts(int partitionIndex)
			throws GSException {
		checkOpened();
		checkPartitionIndex(partitionIndex);

		final InetSocketAddress[] baseList =
				channel.getNodeAddressList(context, partitionIndex);
		if (baseList.length > 0) {
			baseList[0] = null;
		}

		return toAddressList(baseList);
	}

	@Override
	public void assignPreferableHost(int partitionIndex, InetAddress host)
			throws GSException {
		checkOpened();
		checkPartitionIndex(partitionIndex);

		context.setPreferableHost(partitionIndex, host);
	}

	@Override
	public int getPartitionIndexOfContainer(String containerName)
			throws GSException {
		final boolean internalMode = WORKAROUND_WRONG_API_TEST;
		return getPartitionIndexOfContainer(containerName, internalMode);
	}

	@Override
	public int getPartitionIndexOfContainer(
			String containerName, boolean internalMode) throws GSException {
		checkOpened();
		final ContainerKeyConverter keyConverter =
				context.getKeyConverter(internalMode, false);
		final ContainerKey containerKey;
		try {
			containerKey = keyConverter.parse(containerName, internalMode);
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(
					containerName, "containerName", e);
		}
		return getPartitionIndexOfContainer(containerKey, internalMode);
	}

	@Override
	public int getPartitionIndexOfContainer(
			ContainerKey containerKey, boolean internalMode) throws GSException {
		checkOpened();
		try {
			return channel.resolvePartitionId(context, containerKey, internalMode);
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(
					containerKey, "containerKey", e);
		}
	}

	@Override
	public void close() throws GSException {
		closed = true;
	}

	private static OptionalRequestSource createContaierNamesRequestSource() {
		return new OptionalRequestSource() {
			@Override
			public void putOptions(OptionalRequest optionalRequest) {
				optionalRequest.putAcceptableFeatureVersion(
						FeatureVersion.V4_2);
			}

			@Override
			public boolean hasOptions() {
				return true;
			}
		};
	}

}
