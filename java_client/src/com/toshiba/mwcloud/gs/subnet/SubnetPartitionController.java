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
package com.toshiba.mwcloud.gs.subnet;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.PartitionController;
import com.toshiba.mwcloud.gs.common.BasicBuffer;
import com.toshiba.mwcloud.gs.common.GSErrorCode;
import com.toshiba.mwcloud.gs.common.Statement;
import com.toshiba.mwcloud.gs.experimental.ContainerAttribute;
import com.toshiba.mwcloud.gs.experimental.ContainerCondition;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.Context;

public class SubnetPartitionController implements PartitionController {

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

	@Override
	public int getPartitionCount() throws GSException {
		checkOpened();
		return channel.getPartitionCount(context);
	}

	@Override
	public long getContainerCount(int partitionIndex) throws GSException {
		ContainerCondition cond = new ContainerCondition();
		cond.setAttributes(EnumSet.of(ContainerAttribute.BASE));
		return getContainerCount(partitionIndex, cond, false);
	}

	public long getContainerCount(
			int partitionIndex, ContainerCondition cond,
			boolean systemMode) throws GSException {

		checkOpened();
		checkPartitionIndex(partitionIndex);

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);

		SubnetGridStore.tryPutSystemOptionalRequest(req, context, systemMode);

		final long start = 0;
		final long limit = 0;

		req.putLong(start);
		req.putLong(limit);

		req.putInt(cond.getAttributes().size());
		if (0 < cond.getAttributes().size()) {
			for (ContainerAttribute attribute : cond.getAttributes()) {
				req.putInt(SubnetGridStore.getContainerAttributeFlag(attribute));
			}
		} else {
			throw new GSException(GSErrorCode.EMPTY_PARAMETER,
					"Container attribute is not specified.");
		}

		channel.executeStatement(
				context, Statement.GET_PARTITION_CONTAINER_NAMES,
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
		ContainerCondition cond = new ContainerCondition();
		cond.setAttributes(EnumSet.of(ContainerAttribute.BASE));
		return getContainerNames(partitionIndex, start, limit, cond, false);
	}

	public List<String> getContainerNames(
			int partitionIndex, long start, Long limit,
			ContainerCondition cond, boolean systemMode) throws GSException {

		checkOpened();
		checkPartitionIndex(partitionIndex);

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		SubnetGridStore.tryPutSystemOptionalRequest(req, context, systemMode);

		req.putLong(start);
		req.putLong(limit == null ? Long.MAX_VALUE : limit);

		req.putInt(cond.getAttributes().size());
		if (0 < cond.getAttributes().size()) {
			for (ContainerAttribute attribute : cond.getAttributes()) {
				req.putInt(SubnetGridStore.getContainerAttributeFlag(attribute));
			}
		} else {
			throw new GSException(GSErrorCode.EMPTY_PARAMETER,
					"Container attribute is not specified.");
		}

		channel.executeStatement(
				context, Statement.GET_PARTITION_CONTAINER_NAMES,
				partitionIndex, 0, req, resp, null);

		
		resp.base().getLong();

		final int entryCount = resp.base().getInt();
		final List<String> list = new ArrayList<String>();
		for (int i = 0; i < entryCount; i++) {
			list.add(resp.getString());
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
		checkOpened();
		try {
			return channel.resolvePartitionId(context, containerName);
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(
					containerName, "containerName", e);
		}
	}

	@Override
	public void close() throws GSException {
		closed = true;
	}

}
