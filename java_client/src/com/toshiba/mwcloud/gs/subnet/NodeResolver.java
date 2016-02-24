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

import java.io.Closeable;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.common.BasicBuffer;
import com.toshiba.mwcloud.gs.common.GSConnectionException;
import com.toshiba.mwcloud.gs.common.GSErrorCode;
import com.toshiba.mwcloud.gs.common.InternPool;
import com.toshiba.mwcloud.gs.common.LoggingUtils;
import com.toshiba.mwcloud.gs.common.LoggingUtils.BaseGridStoreLogger;
import com.toshiba.mwcloud.gs.common.PropertyUtils;
import com.toshiba.mwcloud.gs.common.Statement;

public class NodeResolver implements Closeable {

	public static final int NOTIFICATION_RECEIVE_TIMEOUT = 10 * 1000;

	private static final int DEFAULT_NOTIFICATION_STATEMENT_TYPE = 5000;

	public static final ProtocolConfig DEFAULT_PROTOCOL_CONFIG =
			new DefaultProtocolConfig();

	private static final BaseGridStoreLogger LOGGER =
			LoggingUtils.getLogger("Discovery");

	private final NodeConnectionPool pool;

	private final boolean ipv6Enabled;

	private final InetSocketAddress notificationAddress;

	private InetSocketAddress masterAddress;

	private InetSocketAddress prevMasterAddress;

	private final NodeConnection.Config connectionConfig =
			new NodeConnection.Config();

	private final NodeConnection.LoginInfo loginInfo;

	private int partitionCount;

	private int notifiedPartitionCount;

	private ContainerHashMode containerHashMode;

	private NodeConnection masterConnection;

	private final BasicBuffer req = new BasicBuffer(64);

	private final BasicBuffer resp = new BasicBuffer(64);

	private long notificationReceiveTimeoutMillis = NOTIFICATION_RECEIVE_TIMEOUT;

	private volatile long connectionTrialCounter;

	private boolean connectionFailedPreviously;

	private final InternPool<InetSocketAddress> addressCache =
			new InternPool<InetSocketAddress>();

	
	private final Map<Integer, InetSocketAddress[]> nodeAddressMap =
			new HashMap<Integer, InetSocketAddress[]>();

	static final String PUBLIC_DATABASE_NAME = "public";

	private int preferableConnectionPoolSize;

	private final Random random = new Random();

	private ProtocolConfig protocolConfig;

	public enum ContainerHashMode {
		CRC32
	}

	public NodeResolver(
			NodeConnectionPool pool,
			boolean passive, InetSocketAddress address,
			NodeConnection.Config connectionConfig,
			NodeConnection.LoginInfo loginInfo,
			int partitionCount) throws GSException {
		this.pool = pool;
		this.ipv6Enabled = (address.getAddress() instanceof Inet6Address);
		this.notificationAddress = (passive ? address : null);
		this.masterAddress = (passive ? null : address);
		this.connectionConfig.set(connectionConfig);
		this.loginInfo = new NodeConnection.LoginInfo(loginInfo);
		this.loginInfo.setDatabase(PUBLIC_DATABASE_NAME);
		this.loginInfo.setOwnerMode(false);
		this.partitionCount = partitionCount;
		this.containerHashMode = (passive ? null : ContainerHashMode.CRC32);
		this.preferableConnectionPoolSize = pool.getMaxSize();
		this.protocolConfig = DEFAULT_PROTOCOL_CONFIG;
		NodeConnection.fillRequestHead(ipv6Enabled, req);
	}

	public synchronized void setUser(String user) {
		loginInfo.setUser(user);
	}

	public synchronized void setPassword(String password) {
		loginInfo.setPassword(password);
	}

	public synchronized void setConnectionConfig(
			NodeConnection.Config connectionConfig) {
		this.connectionConfig.set(connectionConfig);
	}

	public synchronized void setNotificationReceiveTimeoutMillis(long timeout) {
		notificationReceiveTimeoutMillis = timeout;
	}

	public synchronized void setPreferableConnectionPoolSize(int size) {
		if (size >= 0 && preferableConnectionPoolSize != size) {
			preferableConnectionPoolSize = size;
			updateConnectionPoolSize();
		}
	}

	public synchronized void setProtocolConfig(ProtocolConfig protocolConfig) {
		this.protocolConfig = protocolConfig;
	}

	public int getPartitionCount() throws GSException {
		final long startTrialCount = connectionTrialCounter;
		synchronized (this) {
			if (partitionCount <= 0) {
				prepareMasterConnection(startTrialCount);
			}

			return partitionCount;
		}
	}

	public ContainerHashMode getContainerHashMode()
			throws GSException {
		final long startTrialCount = connectionTrialCounter;
		synchronized (this) {
			if (containerHashMode == null) {
				prepareMasterConnection(startTrialCount);
			}

			return containerHashMode;
		}
	}

	public InetSocketAddress getMasterAddress()
			throws GSException {
		final long startTrialCount = connectionTrialCounter;
		synchronized (this) {
			if (masterAddress == null) {
				prepareMasterConnection(startTrialCount);
			}

			return masterAddress;
		}
	}

	public InetSocketAddress getNodeAddress(
			int partitionId, boolean backupPreferred) throws GSException {
		return getNodeAddress(partitionId, backupPreferred, -1, null);
	}

	public InetSocketAddress getNodeAddress(
			int partitionId, boolean backupPreferred,
			int partitionCount, InetAddress preferableHost)
			throws GSException {
		final long startTrialCount = connectionTrialCounter;

		final InetSocketAddress[] addressList;
		synchronized (this) {
			addressList = getNodeAddressList(
					partitionId, backupPreferred, partitionCount, startTrialCount,
					false);
		}

		final int backupCount = addressList.length - 1;
		if (backupPreferred && backupCount > 0) {
			final int ownerCount = 1;
			final InetSocketAddress address =
					addressList[ownerCount + random.nextInt(backupCount)];

			if (preferableHost != null &&
					!address.getAddress().equals(preferableHost)) {
				for (int i = ownerCount; i < addressList.length; i++) {
					if (addressList[i].getAddress().equals(preferableHost)) {
						return addressList[i];
					}
				}
			}

			return address;
		}
		else {
			return addressList[0];
		}
	}

	public InetSocketAddress[] getNodeAddressList(
			int partitionId) throws GSException {
		final long startTrialCount = connectionTrialCounter;

		final InetSocketAddress[] addressList;
		synchronized (this) {
			addressList = getNodeAddressList(
					partitionId, true, partitionCount, startTrialCount, false);
		}

		return Arrays.copyOf(addressList, addressList.length);
	}

	private InetSocketAddress[] getNodeAddressList(
			int partitionId, boolean backupPreferred,
			int partitionCount, long startTrialCount, boolean allowEmpty)
			throws GSException {
		final Integer partitionIdKey = partitionId;
		InetSocketAddress[] addressList;
		addressList = nodeAddressMap.get(partitionId);
		if (addressList != null) {
			return addressList;
		}

		prepareMasterConnection(startTrialCount);

		if (partitionCount > 0 && this.partitionCount > 0 &&
				partitionCount != this.partitionCount) {
			throw new GSConnectionException(
					GSErrorCode.INTERNAL_ERROR,
					"Internal error by inconsistent partition count (" +
					"expected=" + partitionCount +
					", current=" + this.partitionCount +
					", currentMaster=" + masterAddress +
					", previousMaster=" + prevMasterAddress + ")");
		}

		if (partitionId < 0 ||
				this.partitionCount > 0 && partitionId >= this.partitionCount) {
			throw new GSException(
					GSErrorCode.INTERNAL_ERROR,
					"Internal error by invalid partition ID (" +
					"partitionId=" + partitionId +
					", partitionCount=" + partitionCount +
					", masterAddress=" + masterAddress + ")");
		}

		NodeConnection.fillRequestHead(ipv6Enabled, req);
		boolean succeeded = false;
		try {
			NodeConnection.tryPutEmptyOptionalRequest(req);
			masterConnection.executeStatementDirect(
					protocolConfig.getNormalStatementType(
							Statement.GET_PARTITION_ADDRESS),
					partitionId, 0, req, resp, null);

			acceptPartitionCount(resp.base().getInt(), false);

			final byte[] addressBuffer = new byte[ipv6Enabled ? 16 : 4];
			final int sockAddrSize =
					addressBuffer.length + Integer.SIZE / Byte.SIZE;

			
			
			

			final byte ownerCount = resp.base().get();
			final int ownerPosition = resp.base().position();
			resp.base().position(ownerPosition + sockAddrSize * ownerCount);

			final byte backupCount = resp.base().get();
			final int backupPosition = resp.base().position();
			resp.base().position(backupPosition + sockAddrSize * backupCount);

			if (ownerCount < 0 || backupCount < 0) {
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by negative address count (" +
						", masterAddress=" + masterAddress +
						", partitionId=" + partitionId +
						", ownerCount=" + ownerCount +
						", backupCount=" + backupCount + ")");
			}

			if (ownerCount == 0 && (backupCount == 0 || !backupPreferred)) {
				if (allowEmpty) {
					return new InetSocketAddress[0];
				}

				throw new GSConnectionException(
						GSErrorCode.PARTITION_NOT_AVAILABLE,
						"Specified partition is currently not available (" +
						"masterAddress=" + masterAddress +
						", partitionId=" + partitionId +
						", ownerCount=" + ownerCount +
						", backupCount=" + backupCount +
						", backupPreferred=" + backupPreferred + ")");
			}

			addressList = new InetSocketAddress[1 + backupCount];

			resp.base().position(ownerPosition);
			if (ownerCount > 0) {
				addressList[0] = decodeSocketAddress(resp, addressBuffer);
			}

			resp.base().position(backupPosition);
			for (int i = 1; i < addressList.length; i++) {
				addressList[i] = decodeSocketAddress(resp, addressBuffer);
			}

			if (ownerCount > 0) {
				nodeAddressMap.put(partitionIdKey, addressList);
			}

			updateConnectionPoolSize();

			succeeded = true;
			connectionFailedPreviously = false;
			return addressList;
		}
		finally {
			if (!succeeded) {
				connectionFailedPreviously = true;
				try {
					masterConnection.close();
				}
				catch (GSException e) {
				}
				finally {
					masterConnection = null;
					nodeAddressMap.remove(partitionIdKey);
				}
			}
		}
	}

	private InetSocketAddress decodeSocketAddress(
			BasicBuffer in, byte[] addressBuffer)
			throws GSConnectionException {
		in.base().get(addressBuffer);
		final InetSocketAddress address;
		try {
			address = new InetSocketAddress(
					InetAddress.getByAddress(addressBuffer),
					in.base().getInt());
		}
		catch (UnknownHostException e) {
			throw new GSConnectionException(
					GSErrorCode.MESSAGE_CORRUPTED,
					"Prorotol error by invalid address (" +
					"reason=" + e.getMessage() + ")", e);
		}
		catch (IllegalArgumentException e) {
			throw new GSConnectionException(
					GSErrorCode.MESSAGE_CORRUPTED,
					"Prorotol error by invalid address (" +
					"reason=" + e.getMessage() + ")", e);
		}

		return addressCache.intern(address);
	}

	private void prepareMasterConnection(long startTrialCount) throws GSException {
		boolean succeeded = false;
		try {
			if (connectionFailedPreviously &&
					startTrialCount != connectionTrialCounter) {
				throw new GSConnectionException(
						GSErrorCode.CONNECTION_TIMEOUT,
						"Previously failed in the other thread");
			}

			if (masterAddress == null) {
				connectionTrialCounter++;
				updateMasterInfo();
			}

			boolean connectionUpdated = false;
			if (masterConnection == null) {
				connectionTrialCounter++;
				masterConnection = pool.resolve(
						masterAddress, req, resp, connectionConfig, loginInfo,
						!connectionFailedPreviously);
				connectionUpdated = true;
			}

			if (connectionUpdated) {
				NodeConnection.fillRequestHead(ipv6Enabled, req);
				NodeConnection.tryPutEmptyOptionalRequest(req);

				final int partitionId = 0;
				masterConnection.executeStatementDirect(
						protocolConfig.getNormalStatementType(
								Statement.GET_PARTITION_ADDRESS),
						partitionId, 0, req, resp, null);

				acceptPartitionCount(resp.base().getInt(), false);
			}

			succeeded = true;
			connectionFailedPreviously = false;
		}
		finally {
			if (!succeeded) {
				connectionFailedPreviously = true;
				invalidateMaster();
			}
		}
	}

	private void updateMasterInfo() throws GSException {
		MulticastSocket socket = null;
		try {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(
						"discovery.updatingMaster",
						notificationAddress,
						notificationReceiveTimeoutMillis);
			}

			socket = new MulticastSocket(notificationAddress.getPort());
			socket.setSoTimeout(PropertyUtils.timeoutPropertyToIntMillis(
					notificationReceiveTimeoutMillis));
			socket.setReuseAddress(true);
			socket.joinGroup(notificationAddress.getAddress());

			
			
			
			
			
			
			
			
			
			
			final int length = 4 * 7 + (ipv6Enabled ? 16 : 4) * 2 + 1;
			final DatagramPacket packet =
					new DatagramPacket(new byte[length], length);
			socket.receive(packet);
			if (packet.getLength() != length) {
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by invalid packet length (" +
						"expected=" + length +
						", actual=" + packet.getLength() + ")");
			}
			resp.clear();
			resp.prepare(length);
			resp.base().put(packet.getData());
			resp.base().flip();

			
			resp.base().position(4 * 4 + (ipv6Enabled ? 16 : 4));

			final int statementType = resp.base().getInt();
			if (statementType !=
					protocolConfig.getNotificationStatementType()) {
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by illegal statement type (" +
						"type=" + resp.base().getInt() + ")");
			}

			final byte[] addressData = new byte[ipv6Enabled ? 16 : 4];
			resp.base().get(addressData);
			final InetSocketAddress masterAddress = new InetSocketAddress(
					InetAddress.getByAddress(addressData),
					resp.base().getInt());
			final int partitionCount = resp.base().getInt();
			if (partitionCount <= 0) {
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by negative partition count");
			}
			final ContainerHashMode containerHashMode;
			try {
				containerHashMode = resp.getByteEnum(ContainerHashMode.class);
			}
			catch (IllegalStateException e) {
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by illegal hash mode (" +
						"reason=" + e + ")", e);
			}

			acceptPartitionCount(partitionCount, true);
			this.masterAddress = masterAddress;
			this.containerHashMode = containerHashMode;

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(
						"discovery.masterUpdated",
						notificationAddress,
						masterAddress,
						containerHashMode,
						partitionCount);
			}
		}
		catch (IOException e) {
			throw new GSConnectionException(
					GSErrorCode.BAD_CONNECTION,
					"Failed to update by notification (" +
					"address=" + notificationAddress +
					", reason=" + e.getMessage() + ")", e);
		}
		finally {
			if (socket != null) {
				socket.close();
			}
		}
	}

	public synchronized void invalidateMaster() {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(
					"discovery.invalidatingMaster",
					notificationAddress,
					masterAddress,
					containerHashMode,
					partitionCount,
					(masterConnection != null));
		}

		nodeAddressMap.clear();
		addressCache.clear();

		notifiedPartitionCount = 0;

		if (notificationAddress != null) {
			masterAddress = null;
			containerHashMode = null;
		}

		if (masterConnection != null) {
			try {
				masterConnection.close();
			}
			catch (GSException e) {
			}
			masterConnection = null;
		}

		updateConnectionPoolSize();
	}

	private void acceptPartitionCount(
			int partitionCount, boolean fromNotification)
			throws GSConnectionException {
		if (partitionCount <= 0) {
			throw new GSConnectionException(
					GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by negative partition count");
		}

		if (partitionCount == this.partitionCount) {
			return;
		}

		do {
			if (this.partitionCount > 0) {
				break;
			}

			if (fromNotification) {
				notifiedPartitionCount = partitionCount;
			}
			else {
				if (notifiedPartitionCount > 0 &&
						partitionCount != notifiedPartitionCount) {
					break;
				}

				this.partitionCount = partitionCount;
				prevMasterAddress = masterAddress;
			}
			return;
		}
		while (false);

		final String clusterName = (loginInfo.getClusterName() == null ||
				loginInfo.getClusterName().isEmpty() ? null :
				loginInfo.getClusterName());
		throw new GSConnectionException(
				GSErrorCode.ILLEGAL_PARTITION_COUNT,
				"Multiple cluster detected with same network configuration " +
				"or fundamental cluster configuration has been changed " +
				"by receiving different partition count. " +
				"Same cluster name might be assigned at the different " +
				"cluster. " +
				"Please check cluster configuration (" +
				"specifiedPartitionCount=" + partitionCount +
				", prevPartitionCount=" + (this.partitionCount <= 0 ?
						"(not yet set)" : this.partitionCount) +
				", prevMasterAddress=" + (prevMasterAddress == null ?
						"(not yet set)" : prevMasterAddress) +
				", fromNotification=" + fromNotification +
				(fromNotification ?
						", notificationAddress=" + notificationAddress :
						", masterAddress=" + masterAddress) +
				", clusterName=" +(clusterName == null ?
						"(not specified)" : clusterName) +
				")");
	}

	private void updateConnectionPoolSize() {
		pool.setMaxSize(
				Math.max(preferableConnectionPoolSize, addressCache.size()));
	}

	public synchronized void close() throws GSException {
		if (masterConnection != null) {
			pool.add(masterConnection);
			masterConnection = null;
		}
	}

	public static abstract class ProtocolConfig {

		public abstract int getNotificationStatementType();

		public abstract int getNormalStatementType(Statement statement);

	}

	private static class DefaultProtocolConfig extends ProtocolConfig {

		@Override
		public int getNotificationStatementType() {
			return DEFAULT_NOTIFICATION_STATEMENT_TYPE;
		}

		@Override
		public int getNormalStatementType(Statement statement) {
			if (statement == Statement.GET_PARTITION_ADDRESS) {
				return NodeConnection.statementToNumber(statement);
			}
			else {
				throw new Error();
			}
		}

	}

}
