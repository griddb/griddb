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

import java.io.Closeable;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
import com.toshiba.mwcloud.gs.common.PropertyUtils.WrappedProperties;
import com.toshiba.mwcloud.gs.common.ServiceAddressResolver;
import com.toshiba.mwcloud.gs.common.Statement;

public class NodeResolver implements Closeable {

	private static boolean ipv6EnabledDefault = false;

	public static final int NOTIFICATION_RECEIVE_TIMEOUT = 10 * 1000;

	private static final int DEFAULT_NOTIFICATION_STATEMENT_TYPE = 5000;

	private static final int DEFAULT_NOTIFICATION_PORT = 31999;

	private static final String DEFAULT_NOTIFICATION_ADDRESS = "239.0.0.1";

	private static final String DEFAULT_NOTIFICATION_ADDRESS_V6 = "ff12::1";

	private static final String DEFAULT_SERVICE_TYPE = "transaction";

	private static final AddressConfig DEFAULT_ADDRESS_CONFIG =
			new AddressConfig();

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

	private int preferableConnectionPoolSize;

	private final ServiceAddressResolver serviceAddressResolver;

	private final Random random = new Random();

	private int lastSelectedMember = -1;

	private boolean alwaysMaster = false;

	private ProtocolConfig protocolConfig;

	public enum ContainerHashMode {
		CRC32
	}

	public NodeResolver(
			NodeConnectionPool pool,
			boolean passive, InetSocketAddress address,
			NodeConnection.Config connectionConfig,
			NodeConnection.LoginInfo loginInfo,
			int partitionCount,
			ServiceAddressResolver.Config sarConfig,
			List<InetSocketAddress> memberList,
			AddressConfig addressConfig) throws GSException {
		this.pool = pool;
		this.ipv6Enabled = (sarConfig == null ?
				(address.getAddress() instanceof Inet6Address) :
					sarConfig.isIPv6Expected());
		this.notificationAddress = (passive ? address : null);
		this.masterAddress = (passive ? null : address);
		this.connectionConfig.set(connectionConfig);
		this.loginInfo = new NodeConnection.LoginInfo(loginInfo);
		this.loginInfo.setDatabase(
				NodeConnection.LoginInfo.DEFAULT_DATABASE_NAME);
		this.loginInfo.setOwnerMode(false);
		this.partitionCount = partitionCount;
		this.containerHashMode = (passive ? null : ContainerHashMode.CRC32);
		this.preferableConnectionPoolSize = pool.getMaxSize();
		this.serviceAddressResolver = makeServiceAddressResolver(
				sarConfig, memberList, addressConfig);
		this.protocolConfig = DEFAULT_PROTOCOL_CONFIG;
		if (addressConfig != null) {
			alwaysMaster = addressConfig.alwaysMaster;
		}
		NodeConnection.fillRequestHead(ipv6Enabled, req);
	}

	private static ServiceAddressResolver makeServiceAddressResolver(
			ServiceAddressResolver.Config sarConfig,
			List<InetSocketAddress> memberList,
			AddressConfig addressConfig) throws GSException {
		if (sarConfig == null) {
			return null;
		}

		if (addressConfig == null) {
			return makeServiceAddressResolver(
					sarConfig, memberList, DEFAULT_ADDRESS_CONFIG);
		}

		final ServiceAddressResolver resolver =
				new ServiceAddressResolver(sarConfig);
		resolver.initializeType(0, addressConfig.serviceType);

		if (sarConfig.getProviderURL() == null && memberList.isEmpty()) {
			throw new IllegalArgumentException();
		}

		if (sarConfig.getProviderURL() == null) {
			for (int i = 0; i < memberList.size(); i++) {
				resolver.setAddress(i, 0, memberList.get(i));
			}
			resolver.validate();
		}

		return resolver;
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

			final byte[] addressBuffer = new byte[getInetAddressSize()];
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

	private int getInetAddressSize() {
		return ipv6Enabled ? 16 : 4;
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

	private void prepareMasterConnection(
			long startTrialCount) throws GSException {
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
				if (serviceAddressResolver == null) {
					updateMasterInfo();
				}
				else {
					updateNotificationMember();
				}
			}

			while (!tryUpdateMasterConnection()) {
				connectionTrialCounter++;
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

	private void updateNotificationMember() throws GSException {
		final URL url = serviceAddressResolver.getConfig().getProviderURL();
		if (url == null) {
			return;
		}

		LOGGER.debug(
				"discovery.updatingMember",
				url, serviceAddressResolver.isAvailable());

		try {
			serviceAddressResolver.update();
			serviceAddressResolver.validate();
		}
		catch (GSException e) {
			if (serviceAddressResolver.isAvailable()) {
				LOGGER.info("discovery.previousMember", url, e);
				return;
			}
			throw new GSConnectionException(e);
		}

		if (LOGGER.isDebugEnabled()) {
			final StringBuilder builder = new StringBuilder();
			builder.append("[");
			for (int i = 0; i < serviceAddressResolver.getEntryCount(); i++) {
				if (builder.length() > 0) {
					builder.append(", ");
				}
				final InetSocketAddress address =
						serviceAddressResolver.getAddress(i, 0);
				builder.append(address.getAddress().getHostAddress());
				builder.append(":");
				builder.append(address.getPort());
			}
			builder.append("]");

			LOGGER.debug(
					"discovery.memberUpdated",
					url, serviceAddressResolver.isChanged(),
					builder.toString());
		}
	}

	private boolean tryUpdateMasterConnection() throws GSException {
		if (masterConnection != null) {
			return true;
		}

		NodeConnection pendingConnection = null;
		try {
			final InetSocketAddress address;
			if (masterAddress == null) {
				final int count = serviceAddressResolver.getEntryCount();
				if (count == 0) {
					final ServiceAddressResolver.Config config =
							serviceAddressResolver.getConfig();
					throw new GSConnectionException(
							GSErrorCode.SA_ADDRESS_NOT_ASSIGNED,
							"No address found in provider (url=" +
									config.getProviderURL() + ")");
				}

				int index = lastSelectedMember;
				if (index < 0) {
					index = random.nextInt(count);
				}
				else if (++index >= count) {
					index = 0;
				}
				lastSelectedMember = index;

				address = serviceAddressResolver.getAddress(index, 0);
				if (alwaysMaster) {
					masterAddress = address;
					containerHashMode = ContainerHashMode.CRC32;
				}
			}
			else {
				address = masterAddress;
			}
			final boolean masterResolving = (masterAddress == null);

			LOGGER.debug(
					"discovery.checkingMaster",
					masterAddress,
					address);

			pendingConnection = pool.resolve(
					address, req, resp, connectionConfig,
					loginInfo, !connectionFailedPreviously);

			NodeConnection.fillRequestHead(ipv6Enabled, req);
			NodeConnection.tryPutEmptyOptionalRequest(req);

			if (masterResolving) {
				req.putBoolean(true);
			}

			final int partitionId = 0;
			pendingConnection.executeStatementDirect(
					protocolConfig.getNormalStatementType(
							Statement.GET_PARTITION_ADDRESS),
					partitionId, 0, req, resp, null);

			final int partitionCount = resp.base().getInt();
			if (masterResolving) {
				final byte ownerCount = resp.base().get();
				final byte backupCount = resp.base().get();
				if (ownerCount != 0 || backupCount != 0 ||
						resp.base().remaining() == 0) {
					throw new GSConnectionException(
							GSErrorCode.MESSAGE_CORRUPTED,
							"Protocol error by invalid master location");
				}

				final boolean masterMatched = resp.getBoolean();
				containerHashMode = decodeContainerHashMode(resp);
				masterAddress = decodeSocketAddress(
						resp, new byte[getInetAddressSize()]);

				LOGGER.debug(
						"discovery.masterFound",
						masterAddress,
						pendingConnection.getRemoteSocketAddress(),
						containerHashMode,
						partitionCount);

				if (!masterMatched) {
					final NodeConnection connection = pendingConnection;
					pendingConnection = null;
					pool.add(connection);
				}
			}

			if (pendingConnection != null) {
				LOGGER.debug(
						"discovery.masterUpdated",
						notificationAddress,
						masterAddress,
						containerHashMode,
						partitionCount);

				acceptPartitionCount(partitionCount, false);

				masterConnection = pendingConnection;
				pendingConnection = null;
			}
		}
		finally {
			if (pendingConnection != null) {
				pool.add(pendingConnection);
			}
		}

		return (masterConnection != null);
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

			
			
			
			
			
			
			
			
			
			
			final int length = 4 * 7 + getInetAddressSize() * 2 + 1;
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

			
			resp.base().position(4 * 4 + getInetAddressSize());

			final int statementType = resp.base().getInt();
			if (statementType !=
					protocolConfig.getNotificationStatementType()) {
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by illegal statement type (" +
						"type=" + resp.base().getInt() + ")");
			}

			final byte[] addressData = new byte[getInetAddressSize()];
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
			final ContainerHashMode containerHashMode =
					decodeContainerHashMode(resp);

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

	private static ContainerHashMode decodeContainerHashMode(
			BasicBuffer in) throws GSException {
		try {
			return in.getByteEnum(ContainerHashMode.class);
		}
		catch (IllegalStateException e) {
			throw new GSConnectionException(
					GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by illegal hash mode (" +
					"reason=" + e + ")", e);
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

		if (notificationAddress != null || serviceAddressResolver != null) {
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

	public static class AddressConfig {
		public int notificationPort = DEFAULT_NOTIFICATION_PORT;
		public String notificationAddress = DEFAULT_NOTIFICATION_ADDRESS;
		public String notificationAddressV6 = DEFAULT_NOTIFICATION_ADDRESS_V6;
		public String serviceType = DEFAULT_SERVICE_TYPE;
		public boolean alwaysMaster = false;
	}

	public static InetSocketAddress getAddressProperties(
			WrappedProperties props, boolean[] passive,
			ServiceAddressResolver.Config sarConfig,
			List<InetSocketAddress> memberList,
			AddressConfig addressConfig) throws GSException {
		if (addressConfig == null) {
			return getAddressProperties(
					props, passive, sarConfig, memberList,
					DEFAULT_ADDRESS_CONFIG);
		}

		final String host = props.getProperty("host", false);
		passive[0] = (host == null);

		final String ipProtocol = props.getProperty("ipProtocol", true);
		boolean ipv6Enabled = ipv6EnabledDefault;
		if (ipProtocol != null) {
			if (ipProtocol.equals("IPV6")) {
				ipv6Enabled = true;
			}
			if (ipProtocol.equals("IPV4")) {
				ipv6Enabled = false;
			}
			else {
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"Illegal IP type (type=" + ipProtocol + ")");
			}
		}

		final InetAddress address = getNotificationProperties(
				props, host, ipv6Enabled, sarConfig, memberList,
				addressConfig);

		final String portKey = (passive[0] ? "notificationPort" : "port");
		Integer port = props.getIntProperty(portKey, false);
		if (port == null) {
			if (passive[0]) {
				port = addressConfig.notificationPort;
			}
			else if (address != null) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"Port must be specified");
			}
		}

		if (port != null && (port < 0 || port >= 1 << Short.SIZE)) {
			throw new GSException(
					GSErrorCode.ILLEGAL_PARAMETER,
					"Port out of range (" +
					"port=" + port + ", propertyName=" + portKey +  ")");
		}

		if (address == null) {
			return null;
		}

		return new InetSocketAddress(address, port);
	}

	public static InetAddress getNotificationProperties(
			WrappedProperties props, String host, Boolean ipv6Expected,
			ServiceAddressResolver.Config sarConfig,
			List<InetSocketAddress> memberList,
			AddressConfig addressConfig) throws GSException {

		PropertyUtils.checkExclusiveProperties(
				props.getBase(),
				"notificationProvider",
				"notificationMember",
				"notificationAddress");
		PropertyUtils.checkExclusiveProperties(
				props.getBase(), "notificationProvider", "host");
		PropertyUtils.checkExclusiveProperties(
				props.getBase(), "notificationMember", "host");

		final String notificationProvider =
				props.getProperty("notificationProvider", false);
		final String notificationMember =
				props.getProperty("notificationMember", false);
		final String notificationAddress =
				props.getProperty("notificationAddress", false);

		if (notificationProvider != null) {
			sarConfig.setProviderURL(notificationProvider);
		}
		sarConfig.setIPv6Expected(ipv6Expected != null && ipv6Expected);
		sarConfig.setTimeoutMillis((int) Math.min(props.getTimeoutProperty(
				"notificationProviderTimeout", (long) -1, false),
				Integer.MAX_VALUE));

		memberList.addAll(parseNotificationMember(
				notificationMember, ipv6Expected));

		if (notificationProvider != null || notificationMember != null) {
			return null;
		}

		if (host == null) {
			return getNotificationAddress(
					notificationAddress, ipv6Expected, addressConfig,
					"notificationAddress");
		}
		else {
			return getNotificationAddress(
					host, ipv6Expected, addressConfig, "host");
		}
	}

	private static InetAddress getNotificationAddress(
			String host, Boolean ipv6Expected, AddressConfig config, String key)
			throws GSException {
		if (host == null) {
			final String alternative;
			if (ipv6Expected != null && ipv6Expected) {
				alternative = config.notificationAddressV6;
			}
			else {
				alternative = config.notificationAddress;
			}
			return ServiceAddressResolver.resolveAddress(
					alternative, ipv6Expected, key);
		}
		else {
			return ServiceAddressResolver.resolveAddress(
					host, ipv6Expected, key);
		}
	}

	public static List<InetSocketAddress> parseNotificationMember(
			String value, Boolean ipv6Expected) throws GSException {
		if (value == null) {
			return Collections.emptyList();
		}
		else if (value.isEmpty()) {
			throw new GSException(
					GSErrorCode.EMPTY_PARAMETER,
					"Notification member is empty");
		}

		final List<InetSocketAddress> list =
				new ArrayList<InetSocketAddress>();

		for (String addrStr : value.split(",", -1)) {
			if (addrStr.isEmpty()) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"One or more element in notifications member " +
						"are empty (" +
						"element=" + addrStr + ", list=" + value + ")");
			}

			final int portPos = addrStr.lastIndexOf(":");
			final int v6AddrEnd = addrStr.lastIndexOf("]");
			if (portPos < 0 || portPos < v6AddrEnd) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"Port not found in notification member (" +
						"element=" + addrStr + ", list=" + value + ")");
			}

			final int port;
			try {
				port = Integer.parseInt(addrStr.substring(portPos + 1));
			}
			catch (NumberFormatException e) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"Failed to parse port in notification member (" +
						"element=" + addrStr + ", list=" + value +
						", reason=" + e.getMessage() + ")", e);
			}

			final InetAddress inetAddr;
			try {
				inetAddr = ServiceAddressResolver.resolveAddress(
						addrStr.substring(0, portPos), ipv6Expected, null);
			}
			catch (GSException e) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"Failed to resolve host in notification member (" +
						"element=" + addrStr + ", list=" + value +
						", reason=" + e.getMessage() + ")", e);
			}

			try {
				list.add(new InetSocketAddress(inetAddr, port));
			}
			catch (IllegalArgumentException e) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"Illegal port in notification member (" +
						"element=" + addrStr + ", list=" + value +
						", reason=" + e.getMessage() + ")", e);
			}
		}

		return list;
	}

}
