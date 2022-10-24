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
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.net.ssl.SSLSocketFactory;

import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.common.BasicBuffer;
import com.toshiba.mwcloud.gs.common.GSConnectionException;
import com.toshiba.mwcloud.gs.common.GSErrorCode;
import com.toshiba.mwcloud.gs.common.GSStatementException;
import com.toshiba.mwcloud.gs.common.InternPool;
import com.toshiba.mwcloud.gs.common.LoggingUtils;
import com.toshiba.mwcloud.gs.common.LoggingUtils.BaseGridStoreLogger;
import com.toshiba.mwcloud.gs.common.PropertyUtils;
import com.toshiba.mwcloud.gs.common.PropertyUtils.WrappedProperties;
import com.toshiba.mwcloud.gs.common.ServiceAddressResolver;
import com.toshiba.mwcloud.gs.common.Statement;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.SocketType;

public class NodeResolver implements Closeable {

	private static boolean ipv6EnabledDefault = false;

	public static final int NOTIFICATION_RECEIVE_TIMEOUT = 10 * 1000;

	private static final int DEFAULT_NOTIFICATION_STATEMENT_TYPE = 5000;

	private static final int DEFAULT_NOTIFICATION_PORT = 31999;

	private static final String DEFAULT_NOTIFICATION_ADDRESS = "239.0.0.1";

	private static final String DEFAULT_NOTIFICATION_ADDRESS_V6 = "ff12::1";

	private static final String DEFAULT_SERVICE_TYPE = "transaction";
	private static final String DEFAULT_PUBLIC_SERVICE_TYPE = "transactionPublic";

	private static AddressConfig DEFAULT_ADDRESS_CONFIG =
			new AddressConfig(DEFAULT_SERVICE_TYPE);

	private static AddressConfig DEFAULT_PUBLIC_ADDRESS_CONFIG =
			new AddressConfig(DEFAULT_PUBLIC_SERVICE_TYPE);

	public static final ProtocolConfig DEFAULT_PROTOCOL_CONFIG =
			new DefaultProtocolConfig();

	private static final BaseGridStoreLogger LOGGER =
			LoggingUtils.getLogger("Discovery");

	private final NodeConnectionPool pool;

	private final boolean ipv6Enabled;

	private final InetSocketAddress notificationAddress;

	private final NetworkInterface notificationInterface;

	private InetSocketAddress masterAddress;

	private final NodeConnection.Config connectionConfig =
			new NodeConnection.Config();

	private NodeConnection masterConnection;

	private final BasicBuffer req = new BasicBuffer(64);

	private final BasicBuffer resp = new BasicBuffer(64);

	private long notificationReceiveTimeoutMillis = NOTIFICATION_RECEIVE_TIMEOUT;

	private long masterCacheCounter = 1;

	private volatile long connectionTrialCounter;

	private boolean connectionFailedPreviously;

	private Throwable lastProblem;

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
		COMPATIBLE1,
		MD5
	}

	public NodeResolver(
			NodeConnectionPool pool,
			boolean passive, InetSocketAddress address,
			NodeConnection.Config connectionConfig,
			ServiceAddressResolver.Config sarConfig,
			List<InetSocketAddress> memberList,
			AddressConfig addressConfig,
			InetAddress notificationInterfaceAddress,
			boolean isPublicConnection) throws GSException {

		this.DEFAULT_ADDRESS_CONFIG = new AddressConfig(
			isPublicConnection ? DEFAULT_PUBLIC_SERVICE_TYPE : DEFAULT_SERVICE_TYPE);
		this.pool = pool;
		this.ipv6Enabled = (sarConfig == null ?
				(address.getAddress() instanceof Inet6Address) :
					sarConfig.isIPv6Expected());
		this.notificationAddress = (passive ? address : null);
		this.notificationInterface =
				findNotificationInterface(notificationInterfaceAddress);
		this.masterAddress = (passive ? null : address);
		this.connectionConfig.set(connectionConfig, true);
		this.preferableConnectionPoolSize = pool.getMaxSize();
		this.serviceAddressResolver = makeServiceAddressResolver(
				makeServiceAddressResolverConfig(sarConfig, connectionConfig),
				memberList, addressConfig);
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

	private static ServiceAddressResolver.Config
	makeServiceAddressResolverConfig(
			ServiceAddressResolver.Config src,
			NodeConnection.Config connectionConfig) {
		if (src == null) {
			return null;
		}

		final ServiceAddressResolver.Config dest =
				new ServiceAddressResolver.Config(src);
		dest.setSecureSocketFactory(
				(SSLSocketFactory) connectionConfig.getSocketFactories().get(
						SocketType.SECURE));
		return dest;
	}

	public synchronized void setConnectionConfig(
			NodeConnection.Config connectionConfig) {
		this.connectionConfig.set(connectionConfig, false);
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

	public int getPartitionCount(ClusterInfo clusterInfo) throws GSException {
		if (clusterInfo.getPartitionCount() == null) {
			final long startTrialCount = connectionTrialCounter;
			synchronized (this) {
				prepareConnectionAndClusterInfo(clusterInfo, startTrialCount);
				applyMasterCacheCounter(clusterInfo);
			}
		}
		return clusterInfo.getPartitionCount();
	}

	public ContainerHashMode getContainerHashMode(ClusterInfo clusterInfo)
			throws GSException {
		if (clusterInfo.getHashMode() == null) {
			final long startTrialCount = connectionTrialCounter;
			synchronized (this) {
				prepareConnectionAndClusterInfo(clusterInfo, startTrialCount);
				applyMasterCacheCounter(clusterInfo);
			}
		}
		return clusterInfo.getHashMode();
	}

	public long getDatabaseId(ClusterInfo clusterInfo) throws GSException {
		if (clusterInfo.getDatabaseId() == null) {
			final long startTrialCount = connectionTrialCounter;
			synchronized (this) {
				prepareConnectionAndClusterInfo(clusterInfo, startTrialCount);
				applyMasterCacheCounter(clusterInfo);
			}
		}
		return clusterInfo.getDatabaseId();
	}

	public void acceptDatabaseId(
			ClusterInfo clusterInfo, long databaseId,
			InetSocketAddress address) throws GSException {
		final boolean byConnection = true;
		acceptClusterInfo(
				clusterInfo, null, null, databaseId, address, byConnection);
	}

	public Set<InetSocketAddress> getActiveNodeAddressSet(
			ClusterInfo clusterInfo) throws GSException {
		final Set<InetSocketAddress> set = new HashSet<InetSocketAddress>();

		final int partitionCount = getPartitionCount(clusterInfo);
		for (int i = 0; i < partitionCount; i++) {
			final int partitionId = i;
			final long startTrialCount = connectionTrialCounter;

			final InetSocketAddress[] addressList;
			synchronized (this) {
				addressList = getNodeAddressList(
						clusterInfo, partitionId, true, startTrialCount, false,
						false);
				applyMasterCacheCounter(clusterInfo);
			}
			for (InetSocketAddress address : addressList) {
				set.add(address);
			}
		}

		return set;
	}

	public InetSocketAddress getMasterAddress(ClusterInfo clusterInfo)
			throws GSException {
		final long startTrialCount = connectionTrialCounter;
		synchronized (this) {
			if (masterAddress == null) {
				prepareConnectionAndClusterInfo(clusterInfo, startTrialCount);
			}
			applyMasterCacheCounter(clusterInfo);

			return masterAddress;
		}
	}

	public InetSocketAddress getNodeAddress(
			ClusterInfo clusterInfo, int partitionId, boolean backupPreferred)
			throws GSException {
		return getNodeAddress(
				clusterInfo, partitionId, backupPreferred, null);
	}

	public InetSocketAddress getNodeAddress(
			ClusterInfo clusterInfo, int partitionId, boolean backupPreferred,
			InetAddress preferableHost) throws GSException {
		final long startTrialCount = connectionTrialCounter;

		final InetSocketAddress[] addressList;
		synchronized (this) {
			addressList = getNodeAddressList(
					clusterInfo, partitionId, backupPreferred, startTrialCount,
					false, true);
			applyMasterCacheCounter(clusterInfo);
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
			ClusterInfo clusterInfo, int partitionId) throws GSException {
		final long startTrialCount = connectionTrialCounter;

		final InetSocketAddress[] addressList;
		synchronized (this) {
			addressList = getNodeAddressList(
					clusterInfo, partitionId, true, startTrialCount, false,
					true);
			applyMasterCacheCounter(clusterInfo);
		}

		return Arrays.copyOf(addressList, addressList.length);
	}

	public synchronized void invalidateMaster(ClusterInfo clusterInfo) {
		invalidateMasterInternal(clusterInfo, false);
	}

	private InetSocketAddress[] getNodeAddressList(
			ClusterInfo clusterInfo, int partitionId, boolean backupPreferred,
			long startTrialCount, boolean allowEmpty, boolean cacheReading)
			throws GSException {
		final Integer partitionIdKey = partitionId;
		InetSocketAddress[] addressList;
		addressList = (cacheReading ? nodeAddressMap.get(partitionId) : null);
		if (addressList != null) {
			return addressList;
		}

		if (masterConnection == null ||
				clusterInfo.partitionCount.get() == null) {
			prepareConnectionAndClusterInfo(clusterInfo, startTrialCount);
		}

		NodeConnection.fillRequestHead(ipv6Enabled, req);
		try {
			clusterInfo.loginInfo.putConnectionOption(req);
			masterConnection.executeStatementDirect(
					protocolConfig.getNormalStatementType(
							Statement.GET_PARTITION_ADDRESS),
					partitionId, 0, req, resp, null);

			final int partitionCount = resp.base().getInt();
			acceptClusterInfo(
					clusterInfo, partitionCount, null, null, masterAddress,
					true);

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

			clearLastProblem();
			return addressList;
		}
		catch (Throwable t) {
			try {
				throw acceptProblem(t, true);
			}
			finally {
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
					"Protocol error by invalid address (" +
					"reason=" + e.getMessage() + ")", e);
		}
		catch (IllegalArgumentException e) {
			throw new GSConnectionException(
					GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by invalid address (" +
					"reason=" + e.getMessage() + ")", e);
		}

		return addressCache.intern(address);
	}

	private void checkLastProblem(long startTrialCount) throws GSException {
		if (connectionFailedPreviously &&
				startTrialCount != connectionTrialCounter) {
			int errorCode = GSErrorCode.BAD_CONNECTION;
			String reason = "(unknown)";

			if (lastProblem != null) {
				errorCode = 0;
				reason = lastProblem.getMessage();
			}

			throw new GSConnectionException(
					errorCode,
					"Previously failed in the other thread (reason=" +
					reason + ")");
		}
	}

	private GSException acceptProblem(
			Throwable cause, boolean lastProblemChecked) {
		connectionFailedPreviously = true;
		if (lastProblemChecked) {
			lastProblem = cause;
		}
		return GSErrorCode.acceptGSException(cause);
	}

	private void clearLastProblem() {
		connectionFailedPreviously = false;
		lastProblem = null;
	}

	private void prepareConnectionAndClusterInfo(
			ClusterInfo clusterInfo, long startTrialCount) throws GSException {
		boolean lastProblemChecked = false;
		try {
			checkLastProblem(startTrialCount);
			lastProblemChecked = true;

			if (masterAddress == null) {
				connectionTrialCounter++;
				if (serviceAddressResolver == null) {
					updateMasterInfo(clusterInfo);
				}
				else {
					updateNotificationMember();
				}
			}

			while (!updateConnectionAndClusterInfo(clusterInfo)) {
				connectionTrialCounter++;
			}

			clearLastProblem();
		}
		catch (Throwable t) {
			final boolean invalidated = clusterInfo.invalidate();
			if (invalidated || !(t instanceof GSStatementException)) {
				try {
					invalidateMasterInternal(clusterInfo, true);
				}
				catch (Throwable t2) {
				}
				throw acceptProblem(t, lastProblemChecked);
			}
			else {
				throw GSErrorCode.acceptGSException(t);
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

	private boolean updateConnectionAndClusterInfo(ClusterInfo clusterInfo)
			throws GSException {
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
				}
			}
			else {
				address = masterAddress;
			}

			final boolean masterUnresolved = (masterAddress == null);

			final boolean masterResolvable =
					GridStoreChannel.isMasterResolvableByConnection();
			if (masterUnresolved && !masterResolvable) {
				throw new GSException(GSErrorCode.INTERNAL_ERROR, "");
			}

			LOGGER.debug(
					"discovery.checkingMaster", masterAddress, address);

			final NodeConnection.LoginInfo loginInfo = clusterInfo.loginInfo;
			final long[] databaseIdRef = new long[1];
			final NodeConnection connection;
			if (masterConnection == null) {
				connection = pool.resolve(
						address, req, resp, connectionConfig, loginInfo,
						databaseIdRef, !connectionFailedPreviously);
				pendingConnection = connection;
			}
			else {
				connection = masterConnection;
				connection.login(req, resp, loginInfo, databaseIdRef);
			}

			NodeConnection.fillRequestHead(ipv6Enabled, req);
			clusterInfo.loginInfo.putConnectionOption(req);

			if (masterResolvable) {
				req.putBoolean(true);
			}

			final int partitionId = 0;
			connection.executeStatementDirect(
					protocolConfig.getNormalStatementType(
							Statement.GET_PARTITION_ADDRESS),
					partitionId, 0, req, resp, null);

			final int partitionCount = resp.base().getInt();
			final boolean masterMatched;
			final ContainerHashMode hashMode;
			if (masterResolvable) {
				final byte ownerCount = resp.base().get();
				final byte backupCount = resp.base().get();
				if (ownerCount != 0 || backupCount != 0 ||
						resp.base().remaining() == 0) {
					throw new GSConnectionException(
							GSErrorCode.MESSAGE_CORRUPTED,
							"Protocol error by invalid master location");
				}

				masterMatched = resp.getBoolean();
				hashMode = decodeContainerHashMode(resp);
				masterAddress = decodeSocketAddress(
						resp, new byte[getInetAddressSize()]);

				if (masterUnresolved) {
					LOGGER.debug(
							"discovery.masterFound",
							masterAddress,
							connection.getRemoteSocketAddress(),
							hashMode,
							partitionCount);
				}
			}
			else {
				masterMatched = true;
				hashMode = GridStoreChannel.getDefaultContainerHashMode();
			}

			if (masterMatched && pendingConnection != null) {
				masterConnection = pendingConnection;
				pendingConnection = null;
			}

			final long databaseId = databaseIdRef[0];
			if (masterConnection != null) {
				acceptClusterInfo(
						clusterInfo, partitionCount, hashMode, databaseId,
						masterAddress, true);

				LOGGER.debug(
						"discovery.masterUpdated",
						notificationAddress,
						masterAddress,
						hashMode,
						partitionCount);
			}
		}
		finally {
			if (pendingConnection != null) {
				pool.add(pendingConnection);
			}
		}

		return (masterConnection != null);
	}

	private void updateMasterInfo(ClusterInfo clusterInfo) throws GSException {
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
			if (notificationInterface != null) {
				socket.setNetworkInterface(notificationInterface);
			}
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
						"type=" + statementType + ")");
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
			final ContainerHashMode hashMode =
					decodeContainerHashMode(resp);

			acceptClusterInfo(
					clusterInfo, partitionCount, hashMode, null,
					notificationAddress, false);

			this.masterAddress = masterAddress;

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(
						"discovery.masterUpdated",
						notificationAddress,
						masterAddress,
						hashMode,
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

	private void invalidateMasterInternal(
			ClusterInfo clusterInfo, boolean inside) {

		clusterInfo.invalidate();
		if (!clusterInfo.acceptMasterInvalidation(masterCacheCounter) &&
				!inside) {
			return;
		}

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(
					"discovery.invalidatingMaster",
					notificationAddress,
					masterAddress,
					clusterInfo.hashMode.get(),
					clusterInfo.partitionCount.get(),
					(masterConnection != null));
		}

		if (notificationAddress != null || serviceAddressResolver != null) {
			masterAddress = null;
		}

		try {
			releaseMasterCache(true);
		}
		catch (GSException e) {
		}

		updateConnectionPoolSize();

		while (++masterCacheCounter == 0) {
		}
	}

	private synchronized void releaseMasterCache(boolean forceClose)
			throws GSException {
		nodeAddressMap.clear();
		addressCache.clear();

		if (masterConnection != null) {
			final NodeConnection connection = masterConnection;
			masterConnection = null;

			if (forceClose) {
				connection.close();
			}
			else {
				pool.add(connection);
			}
		}
	}

	private void applyMasterCacheCounter(ClusterInfo clusterInfo) {
		clusterInfo.lastMasterCacheCounter = masterCacheCounter;
	}

	private void acceptClusterInfo(
			ClusterInfo clusterInfo, Integer partitionCount,
			ContainerHashMode hashMode, Long databaseId,
			InetSocketAddress address, boolean byConnection)
			throws GSException {
		if (partitionCount != null && partitionCount <= 0) {
			throw new GSConnectionException(
					GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by non positive partition count (" +
					"value=" + partitionCount + ")");
		}

		if (databaseId != null && databaseId < 0) {
			throw new GSConnectionException(
					GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by negative database ID (" +
					"value=" + databaseId + ")");
		}

		for (boolean checkOnly = true;; checkOnly = false) {
			acceptClusterInfoEntry(
					clusterInfo, clusterInfo.partitionCount, "partition count",
					partitionCount, address, byConnection, checkOnly);
			acceptClusterInfoEntry(
					clusterInfo, clusterInfo.hashMode, "container hash mode",
					hashMode, address, byConnection, checkOnly);
			acceptClusterInfoEntry(
					clusterInfo, clusterInfo.databaseId, "database ID",
					databaseId, address, byConnection, checkOnly);

			if (!checkOnly) {
				break;
			}
		}
	}

	private <T> void acceptClusterInfoEntry(
			ClusterInfo clusterInfo, ClusterInfoEntry<T> entry,
			String name, T value, InetSocketAddress address,
			boolean byConnection, boolean checkOnly)
			throws GSException {
		if (entry.tryAccept(value, address, byConnection, checkOnly)) {
			return;
		}

		final StringBuilder builder = new StringBuilder();
		final NodeConnection.LoginInfo loginInfo = clusterInfo.loginInfo;

		if (entry == clusterInfo.databaseId) {
			builder.append(
					"Database may be dropped and created again with same " +
					"database name ");
		}
		else {
			builder.append(
					"Multiple cluster may have been existent with same " +
					"network configuration ");
		}

		builder.append(
				"or fundamental cluster configuration may have been " +
				"changed ");

		builder.append("detected by receiving different ");
		builder.append(name);
		builder.append(". ");

		builder.append(
				"Please check previous operations or cluster configuration (");

		builder.append("receivedValue=");
		builder.append(value);
		builder.append(", previousValue=");
		builder.append(entry.get());

		builder.append(", ");
		formatAddressParameter(builder, "receivedAddress", address);
		builder.append(", ");
		formatAddressParameter(
				builder, "previousAddress", entry.acceptedAddress);

		builder.append(", ");
		formatStringParameter(
				builder, "clusterName", loginInfo.getClusterName());

		if (entry == clusterInfo.databaseId) {
			builder.append(", ");
			formatStringParameter(
					builder, "database", loginInfo.getDatabase());
		}

		builder.append(")");

		final int errorCode = (entry == clusterInfo.partitionCount ?
				GSErrorCode.ILLEGAL_PARTITION_COUNT :
				GSErrorCode.ILLEGAL_CONFIG);
		if (byConnection) {
			throw new GSException(errorCode, builder.toString());
		}
		else {
			throw new GSConnectionException(errorCode, builder.toString());
		}
	}

	private static void formatAddressParameter(
			StringBuilder builder, String name, InetSocketAddress value) {
		builder.append(name);
		builder.append("=");
		if (value == null) {
			builder.append("(manually specified)");
		}
		else {
			builder.append(value);
		}
	}

	private static void formatStringParameter(
			StringBuilder builder, String name, String value) {
		builder.append(name);
		builder.append("=");
		if (value == null) {
			builder.append("(not specified)");
		}
		else {
			builder.append("\"");
			builder.append(value);
			builder.append("\"");
		}
	}

	private void updateConnectionPoolSize() {
		pool.setMaxSize(
				Math.max(preferableConnectionPoolSize, addressCache.size()));
	}

	private static NetworkInterface findNotificationInterface(
			InetAddress address) throws GSException {
		if (address == null) {
			return null;
		}

		final NetworkInterface networkInterface;
		try {
			networkInterface = NetworkInterface.getByInetAddress(address);
		}
		catch (SocketException e) {
			throw new GSException(
					GSErrorCode.ILLEGAL_PARAMETER,
					"Failed to resolve network interface (" +
					"address=" + address +
					", reason=" + e.getMessage() + ")", e);
		}

		if (networkInterface == null) {
			throw new GSException(
					GSErrorCode.ILLEGAL_PARAMETER,
					"Network interface not found (address=" + address + ")");
		}

		return networkInterface;
	}

	public synchronized void close() throws GSException {
		releaseMasterCache(false);
	}

	private static class ClusterInfoEntry<T> {

		T value;

		boolean acceptedByConnection;

		InetSocketAddress acceptedAddress;

		T get() {
			return value;
		}

		boolean tryAccept(
				T value, InetSocketAddress address, boolean byConnection,
				boolean checkOnly) {
			if (value == null) {
				return true;
			}

			if (this.value == null) {
				if (!checkOnly) {
					this.value = value;
				}
			}
			else if (!this.value.equals(value)) {
				return false;
			}

			if (!checkOnly) {
				acceptedByConnection |= byConnection;
				acceptedAddress = address;
			}

			return true;
		}

		boolean invalidate() {
			if (!acceptedByConnection && value != null) {
				value = null;
				return true;
			}
			return false;
		}

	}

	public static class ClusterInfo {

		final NodeConnection.LoginInfo loginInfo;

		final ClusterInfoEntry<Integer> partitionCount =
				new ClusterInfoEntry<Integer>();

		final ClusterInfoEntry<ContainerHashMode> hashMode =
				new ClusterInfoEntry<ContainerHashMode>();

		final ClusterInfoEntry<Long> databaseId = new ClusterInfoEntry<Long>();

		long lastMasterCacheCounter;

		public ClusterInfo(NodeConnection.LoginInfo loginInfo) {
			this.loginInfo = new NodeConnection.LoginInfo(loginInfo);
			this.loginInfo.setOwnerMode(false);
		}

		boolean invalidate() {
			boolean invalidated = false;
			invalidated |= partitionCount.invalidate();
			invalidated |= hashMode.invalidate();
			invalidated |= databaseId.invalidate();
			return invalidated;
		}

		boolean acceptMasterInvalidation(long masterCacheCounter) {
			if (lastMasterCacheCounter != masterCacheCounter) {
				return false;
			}
			lastMasterCacheCounter = 0;
			return true;
		}

		public Integer getPartitionCount() {
			return partitionCount.get();
		}

		public ContainerHashMode getHashMode() {
			return hashMode.get();
		}

		public Long getDatabaseId() {
			return databaseId.get();
		}

		public void setPartitionCount(Integer partitionCount) {
			if (partitionCount != null && partitionCount <= 0) {
				return;
			}

			this.partitionCount.tryAccept(partitionCount, null, true, false);
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
				return NodeConnection.statementToNumber(
						statement.generalize());
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
		AddressConfig(String serviceType) {
			this.serviceType = serviceType;
		}
	}

	public static InetSocketAddress getAddressProperties(
			WrappedProperties props, boolean[] passive,
			ServiceAddressResolver.Config sarConfig,
			List<InetSocketAddress> memberList,
			AddressConfig addressConfig,
			InetAddress[] notificationInterfaceAddress) throws GSException {
		if (addressConfig == null) {
			return getAddressProperties(
					props, passive, sarConfig, memberList,
					DEFAULT_ADDRESS_CONFIG, notificationInterfaceAddress);
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
				addressConfig, notificationInterfaceAddress);

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
					"port=" + port + ", propertyName=" + portKey + ")");
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
			AddressConfig addressConfig,
			InetAddress[] notificationInterfaceAddress) throws GSException {

		PropertyUtils.checkExclusiveProperties(
				props.getBase(),
				"notificationProvider",
				"notificationMember",
				"notificationAddress");
		PropertyUtils.checkExclusiveProperties(
				props.getBase(),
				"notificationProvider",
				"notificationMember",
				"notificationInterfaceAddress");
		PropertyUtils.checkExclusiveProperties(
				props.getBase(), "notificationProvider", "host");
		PropertyUtils.checkExclusiveProperties(
				props.getBase(), "notificationMember", "host");
		PropertyUtils.checkExclusiveProperties(
				props.getBase(), "notificationInterfaceAddress", "host");

		final String notificationProvider =
				props.getProperty("notificationProvider", false);
		final String notificationMember =
				props.getProperty("notificationMember", false);
		final String notificationAddress =
				props.getProperty("notificationAddress", false);
		final String notificationInterfaceAddressStr =
				props.getProperty("notificationInterfaceAddress", false);

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
			if (notificationInterfaceAddressStr != null) {
				notificationInterfaceAddress[0] = getNotificationAddress(
						notificationInterfaceAddressStr, null, addressConfig,
						"notificationInterfaceAddress");
			}

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
