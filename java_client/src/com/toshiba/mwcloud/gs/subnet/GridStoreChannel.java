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
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

import com.toshiba.mwcloud.gs.ContainerType;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GSTimeoutException;
import com.toshiba.mwcloud.gs.common.BasicBuffer;
import com.toshiba.mwcloud.gs.common.ContainerKeyConverter;
import com.toshiba.mwcloud.gs.common.ContainerKeyConverter.ContainerKey;
import com.toshiba.mwcloud.gs.common.ContainerProperties.ContainerVisibility;
import com.toshiba.mwcloud.gs.common.ContainerProperties.MetaNamingType;
import com.toshiba.mwcloud.gs.common.GSConnectionException;
import com.toshiba.mwcloud.gs.common.GSErrorCode;
import com.toshiba.mwcloud.gs.common.GSStatementException;
import com.toshiba.mwcloud.gs.common.GSWrongNodeException;
import com.toshiba.mwcloud.gs.common.LoggingUtils;
import com.toshiba.mwcloud.gs.common.LoggingUtils.BaseGridStoreLogger;
import com.toshiba.mwcloud.gs.common.PropertyUtils.WrappedProperties;
import com.toshiba.mwcloud.gs.common.RowMapper;
import com.toshiba.mwcloud.gs.common.ServiceAddressResolver;
import com.toshiba.mwcloud.gs.common.ServiceAddressResolver.SocketAddressComparator;
import com.toshiba.mwcloud.gs.common.Statement.GeneralStatement;
import com.toshiba.mwcloud.gs.common.Statement;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.LoginInfo;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.MessageDigestFactory;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.OptionalRequest;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.OptionalRequestSource;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.OptionalRequestType;
import com.toshiba.mwcloud.gs.subnet.NodeResolver.ClusterInfo;
import com.toshiba.mwcloud.gs.subnet.NodeResolver.ContainerHashMode;

public class GridStoreChannel implements Closeable {

	public static Boolean v1ProtocolCompatible = null;

	public static boolean v1ProtocolCompatible_1_1_103x = false;

	public static boolean v10ResourceCompatible = false;

	public static boolean v15TSPropsCompatible = false;

	public static boolean v15DDLCompatible = true;

	public static boolean v20AffinityCompatible = false;

	public static boolean v21StatementIdCompatible = false;

	public static boolean v40QueryCompatible = false;

	public static boolean v40ContainerHashCompatible = true;

	public static boolean v40SchemaCompatible = false;

	private static int FAILOVER_TIMEOUT_DEFAULT = 2 * 60 * 1000;

	private static int FAILOVER_RETRY_INTERVAL = 1 * 1000;

	public static int INITIAL_BUFFER_SIZE = 256;

	public static int MAPPER_CACHE_SIZE = 32;

	private static int MAX_REF_SCAN_COUNT = 15;

	private static final BaseGridStoreLogger LOGGER =
			LoggingUtils.getLogger("Connection");

	private static final BaseGridStoreLogger STATEMENT_LOGGER =
			LoggingUtils.getLogger("StatementExec");

	private final Config config = new Config();

	private int statementRetryMode;

	private final NodeConnectionPool pool;

	private final NodeResolver nodeResolver;

	private final int requestHeadLength;

	private final Key key;

	private final Set<ContextReference> contextRefSet =
			new LinkedHashSet<ContextReference>();

	public static class Config {

		private NodeConnection.Config connectionConfig =
				new NodeConnection.Config();

		private long failoverTimeoutMillis = FAILOVER_TIMEOUT_DEFAULT;

		private long failoverRetryIntervalMillis = FAILOVER_RETRY_INTERVAL;

		private long notificationReceiveTimeoutMillis =
				NodeResolver.NOTIFICATION_RECEIVE_TIMEOUT;

		private int maxConnectionPoolSize = -1;

		public synchronized void set(Config config) {
			connectionConfig.set(config.connectionConfig);
			failoverTimeoutMillis = config.failoverTimeoutMillis;
			failoverRetryIntervalMillis = config.failoverRetryIntervalMillis;
			notificationReceiveTimeoutMillis =
					config.notificationReceiveTimeoutMillis;
			maxConnectionPoolSize =
					config.maxConnectionPoolSize;
		}

		public synchronized boolean set(WrappedProperties props) throws GSException {

			final long failoverTimeoutMillis = props.getTimeoutProperty(
					"failoverTimeout", this.failoverTimeoutMillis, false);
			final long failoverRetryIntervalMillis = props.getTimeoutProperty(
					"failoverRetryInterval", this.failoverRetryIntervalMillis, true);
			final long notificationReceiveTimeoutMillis =
					props.getTimeoutProperty(
							"notificationReceiveTimeout",
							this.notificationReceiveTimeoutMillis, true);

			if (failoverTimeoutMillis < 0 ||
					failoverRetryIntervalMillis < 0 ||
					notificationReceiveTimeoutMillis < 0) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PROPERTY_ENTRY,
						"Negative timeout parameter");
			}

			boolean updated = connectionConfig.set(props);

			if (failoverTimeoutMillis != this.failoverTimeoutMillis ||
					failoverRetryIntervalMillis != this.failoverRetryIntervalMillis ||
					notificationReceiveTimeoutMillis !=
					this.notificationReceiveTimeoutMillis) {
				this.failoverTimeoutMillis = failoverTimeoutMillis;
				this.failoverRetryIntervalMillis = failoverRetryIntervalMillis;
				this.notificationReceiveTimeoutMillis =
						notificationReceiveTimeoutMillis;
				updated = true;
			}

			final Integer maxConnectionPoolSize = props.getIntProperty(
					"maxConnectionPoolSize", false);
			if (maxConnectionPoolSize != null &&
					maxConnectionPoolSize != this.maxConnectionPoolSize) {
				if (maxConnectionPoolSize < 0) {
					throw new GSException(
							GSErrorCode.ILLEGAL_PROPERTY_ENTRY,
							"Negative connection pool size");
				}
				this.maxConnectionPoolSize = maxConnectionPoolSize;
				updated = true;
			}

			return updated;
		}

		public synchronized NodeConnection.Config getConnectionConfig() {
			final NodeConnection.Config connectionConfig =
					new NodeConnection.Config();
			connectionConfig.set(this.connectionConfig);
			return connectionConfig;
		}

		public synchronized long getFailoverTimeoutMillis() {
			return failoverTimeoutMillis;
		}

		public synchronized long getFailoverRetryIntervalMillis() {
			return failoverRetryIntervalMillis;
		}

		public synchronized long getNotificationReceiveTimeoutMillis() {
			return notificationReceiveTimeoutMillis;
		}

		public synchronized int getMaxConnectionPoolSize() {
			return maxConnectionPoolSize;
		}

	}

	static class LocalConfig {

		final long failoverTimeoutMillis;

		final long transactionTimeoutMillis;

		final int fetchBytesSize;

		final int containerCacheSize;

		final EnumSet<ContainerVisibility> containerVisibility;

		final MetaNamingType metaNamingType;

		public LocalConfig(WrappedProperties props) throws GSException {
			this.failoverTimeoutMillis = props.getTimeoutProperty(
					"failoverTimeout", (long) -1, false);
			this.transactionTimeoutMillis = props.getTimeoutProperty(
					"transactionTimeout", (long) -1, false);

			final Integer containerCacheSize =
					props.getIntProperty("containerCacheSize", false);
			if (containerCacheSize != null && containerCacheSize < 0) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"Negative container cache size (size=" +
						containerCacheSize + ")");
			}
			this.containerCacheSize =
					(containerCacheSize == null ? 0 : containerCacheSize);

			final Integer fetchBytesSize =
					props.getIntProperty("internal.fetchBytesSize", true);
			this.fetchBytesSize =
					(fetchBytesSize == null ? 0 : fetchBytesSize);

			containerVisibility = EnumSet.noneOf(ContainerVisibility.class);
			setVisibility(
					props, "experimental.metaContainerVisible",
					ContainerVisibility.META);
			setVisibility(
					props, "internal.internalMetaContainerVisible",
					ContainerVisibility.INTERNAL_META);
			setVisibility(
					props, "internal.systemToolContainerEnabled",
					ContainerVisibility.SYSTEM_TOOL);

			final String metaNaming = props.getProperty("experimental.metaNaming", false);
			if (metaNaming == null ||
					metaNaming.equals(MetaNamingType.CONTAINER.name())) {
				this.metaNamingType = null;
			}
			else if (metaNaming.equals(MetaNamingType.TABLE.name())) {
				this.metaNamingType = MetaNamingType.TABLE;
			}
			else {
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"Unknown meta naming (type=" + metaNaming + ")");
			}
		}

		void setVisibility(
				WrappedProperties props, String name,
				ContainerVisibility visibility) throws GSException {
			if (props.getBooleanProperty(name, false, false)) {
				containerVisibility.add(visibility);
			}
		}

	}

	public static class Source {

		private final Key key;

		private final Integer partitionCount;

		private final LocalConfig localConfig;

		private final NodeConnection.LoginInfo loginInfo;

		private final ContainerKeyConverter keyConverter;

		private final DataAffinityPattern dataAffinityPattern;

		public Source(WrappedProperties props) throws GSException {
			final boolean[] passive = new boolean[1];
			ServiceAddressResolver.Config sarConfig =
					new ServiceAddressResolver.Config();
			List<InetSocketAddress> memberList =
					new ArrayList<InetSocketAddress>();

			final InetSocketAddress address =
					NodeResolver.getAddressProperties(
							props, passive, sarConfig, memberList, null);
			if (address != null) {
				sarConfig = null;
				memberList = null;
			}

			final String clusterName =
					props.getProperty("clusterName", "", false);
			final String user = props.getProperty("user", "", false);
			final String password = props.getProperty("password", "", false);
			final String database = props.getProperty("database", null, false);

			if (!clusterName.isEmpty()) {
				RowMapper.checkSymbol(clusterName, "cluster name");
			}
			RowMapper.checkString(user, "user name");
			RowMapper.checkString(password, "password");
			if (database != null) {
				RowMapper.checkSymbol(database, "database name");
			}

			this.key = new Key(
					passive[0], address, clusterName, sarConfig, memberList);
			this.partitionCount = props.getIntProperty("partitionCount", true);

			final String consistency = props.getProperty("consistency", false);
			final boolean backupPreferred;
			if (consistency == null || consistency.equals("IMMEDIATE")) {
				backupPreferred = false;
			}
			else if (consistency.equals("EVENTUAL")) {
				backupPreferred = true;
			}
			else {
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"Unknown consistency type (consistency=" +
						consistency + ")");
			}

			this.localConfig = new LocalConfig(props);

			this.loginInfo = new LoginInfo(
					user, password, !backupPreferred, database, clusterName,
					localConfig.transactionTimeoutMillis);
			this.keyConverter = ContainerKeyConverter.getInstance(
					NodeConnection.getProtocolVersion(), false, false);
			this.dataAffinityPattern = new DataAffinityPattern(
					props.getProperty("dataAffinityPattern", false),
					keyConverter);
		}

		public NodeConnection.LoginInfo getLoginInfo() {
			return loginInfo;
		}

		public Key getKey() {
			return key;
		}

		public ClusterInfo createClusterInfo() {
			final ClusterInfo clusterInfo = new ClusterInfo(loginInfo);
			clusterInfo.setPartitionCount(partitionCount);
			return clusterInfo;
		}

		public Context createContext() {
			final BasicBuffer req = createRequestBuffer();
			final BasicBuffer resp = new BasicBuffer(INITIAL_BUFFER_SIZE);
			final BasicBuffer syncReq = createRequestBuffer();
			final BasicBuffer syncResp = new BasicBuffer(INITIAL_BUFFER_SIZE);

			return new Context(
					localConfig, loginInfo, createClusterInfo(), req, resp,
					syncReq, syncResp, keyConverter, dataAffinityPattern);
		}

		private BasicBuffer createRequestBuffer() {
			final int requestHeadLength =
					NodeConnection.getRequestHeadLength(key.isIPV6Enabled());
			final BasicBuffer req = new BasicBuffer(
					Math.max(requestHeadLength, INITIAL_BUFFER_SIZE));
			NodeConnection.fillRequestHead(key.isIPV6Enabled(), req);
			return req;
		}

	}

	public static class Key {

		private final boolean passive;

		private final InetSocketAddress address;

		private final String clusterName;

		private final ServiceAddressResolver.Config sarConfig;

		private final List<InetSocketAddress> memberList;

		public Key(
				boolean passive, InetSocketAddress address, String clusterName,
				ServiceAddressResolver.Config sarConfig,
				List<InetSocketAddress> memberList) {
			this.passive = passive;
			this.address = address;
			this.clusterName = clusterName;
			this.sarConfig = (sarConfig == null ?
					null : new ServiceAddressResolver.Config(sarConfig));
			this.memberList = (sarConfig == null ?
					Collections.<InetSocketAddress>emptyList() :
					new ArrayList<InetSocketAddress>(memberList));
			if (sarConfig != null) {
				Collections.sort(this.memberList, new SocketAddressComparator());
			}
		}

		public boolean isIPV6Enabled() {
			if (address == null) {
				return sarConfig.isIPv6Expected();
			}
			return (address.getAddress() instanceof Inet6Address);
		}

		public boolean isPassive() {
			return passive;
		}

		public InetSocketAddress getAddress() {
			return address;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result +
					((address == null) ? 0 : address.hashCode());
			result = prime * result +
					((clusterName == null) ? 0 : clusterName.hashCode());
			result = prime * result +
					((memberList == null) ? 0 : memberList.hashCode());
			result = prime * result + (passive ? 1231 : 1237);
			result = prime * result +
					((sarConfig == null) ? 0 : sarConfig.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Key other = (Key) obj;
			if (address == null) {
				if (other.address != null)
					return false;
			}
			else if (!address.equals(other.address))
				return false;
			if (clusterName == null) {
				if (other.clusterName != null)
					return false;
			}
			else if (!clusterName.equals(other.clusterName))
				return false;
			if (memberList == null) {
				if (other.memberList != null)
					return false;
			}
			else if (!memberList.equals(other.memberList))
				return false;
			if (passive != other.passive)
				return false;
			if (sarConfig == null) {
				if (other.sarConfig != null)
					return false;
			}
			else if (!sarConfig.equals(other.sarConfig))
				return false;
			return true;
		}

	}

	public static abstract class RemoteReference<T> extends WeakReference<T> {

		protected final Class<?> targetClass;

		protected final int partitionId;

		protected final long containerId;

		public RemoteReference(T target, Class<?> targetClass,
				Context context, int partitionId, long containerId) {
			super(target, context.remoteRefQueue);
			this.targetClass = targetClass;
			this.partitionId = partitionId;
			this.containerId = containerId;
		}

		public abstract void close(
				GridStoreChannel channel, Context context) throws GSException;

	}

	private static abstract class ResolverExecutor<T> {

		protected T result;

		public abstract void execute() throws GSException;

	}

	public static class Context {

		private final ContextMonitor contextMonitor =
				createContextMonitorIfAvailable();

		private final LocalConfig localConfig;

		private int partitionId = -1;

		private NodeConnection lastConnection;

		private long lastHeartbeatCount;

		private final Map<SocketAddress, NodeConnection> activeConnections =
				new HashMap<SocketAddress, NodeConnection>();

		private final NodeConnection.LoginInfo loginInfo;

		private final ClusterInfo clusterInfo;

		private boolean closed;

		private int failoverCount;

		private final BasicBuffer req;

		private BasicBuffer resp;

		private final BasicBuffer syncReq;

		private final BasicBuffer syncResp;

		private final OptionalRequest optionalRequest = new OptionalRequest();

		private final List<InetSocketAddress> addressCache =
				new ArrayList<InetSocketAddress>();

		private final UUID sessionUUID;

		private long lastSessionId;

		private final Map<Class<?>, Set<RemoteReference<?>>> remoteRefTable =
				new HashMap<Class<?>, Set<RemoteReference<?>>>();

		private final ReferenceQueue<Object> remoteRefQueue =
				new ReferenceQueue<Object>();

		private final ContainerCache containerCache;

		private final Map<Integer, InetAddress> preferableHosts =
				new HashMap<Integer, InetAddress>();

		private final DataAffinityPattern dataAffinityPattern;

		private ResolverExecutor<?> activeResolverExecutor;

		private ContainerKeyConverter keyConverter;

		private ContainerKeyConverter systemKeyConverter;

		private ContainerKeyConverter internalKeyConverter;

		private Context(
				LocalConfig localConfig, NodeConnection.LoginInfo loginInfo,
				ClusterInfo clusterInfo, BasicBuffer req, BasicBuffer resp,
				BasicBuffer syncReq, BasicBuffer syncResp,
				ContainerKeyConverter keyConverter,
				DataAffinityPattern dataAffinityPattern) {
			this.localConfig = localConfig;
			this.loginInfo = new NodeConnection.LoginInfo(loginInfo);
			this.loginInfo.setClientId(NodeConnection.ClientId.generate(0));
			this.clusterInfo = clusterInfo;
			this.req = req;
			this.resp = resp;
			this.syncReq = syncReq;
			this.syncResp = syncResp;
			this.containerCache = (localConfig.containerCacheSize > 0 ?
					new ContainerCache(localConfig.containerCacheSize) : null);
			this.sessionUUID = this.loginInfo.getClientId().getUUID();
			this.keyConverter = keyConverter;
			this.dataAffinityPattern = dataAffinityPattern;
		}

		public boolean isClosedAsync() {
			return closed;
		}

		public long getTransactionTimeoutMillis() {
			return localConfig.transactionTimeoutMillis;
		}

		public OptionalRequestSource bindQueryOptions(
				final OptionalRequestSource source) {
			final int fetchBytesSize = localConfig.fetchBytesSize;
			if (fetchBytesSize <= 0) {
				return source;
			}

			return new OptionalRequestSource() {
				@Override
				public boolean hasOptions() {
					return true;
				}

				@Override
				public void putOptions(OptionalRequest optionalRequest) {
					optionalRequest.put(
							OptionalRequestType.FETCH_BYTES_SIZE,
							fetchBytesSize);

					if (source != null) {
						source.putOptions(optionalRequest);
					}
				}
			};
		}

		public int getFailoverCount() {
			return failoverCount;
		}

		public SocketAddress getLastConnectionAddress() {
			if (lastConnection == null) {
				return null;
			}
			return lastConnection.getRemoteSocketAddress();
		}

		public BasicBuffer getRequestBuffer() {
			return req;
		}

		public BasicBuffer getResponseBuffer() {
			return resp;
		}

		public BasicBuffer replaceResponseBuffer(BasicBuffer newResp) {
			final BasicBuffer orgResp = resp;
			resp = newResp;
			return orgResp;
		}

		public BasicBuffer getSynchronizedRequestBuffer() {
			return syncReq;
		}

		public BasicBuffer getSynchronizedResponseBuffer() {
			return syncResp;
		}

		public OptionalRequest getOptionalRequest() {
			optionalRequest.clear();
			return optionalRequest;
		}

		public UUID getSessionUUID() {
			return sessionUUID;
		}

		public long generateSessionId() {
			long newId = lastSessionId + 1;
			while (newId == 0) {
				newId++;
			}
			lastSessionId = newId;

			return newId;
		}

		public NodeConnection.ClientId generateClientId() {
			return new NodeConnection.ClientId(
					loginInfo.getClientId().getUUID(), generateSessionId());
		}

		public Set<Class<?>> getReferenceTargetClasses() {
			return remoteRefTable.keySet();
		}

		public Set<RemoteReference<?>> getRemoteReferences(
				Class<?> targetClass) {
			final Set<RemoteReference<?>> set =
					remoteRefTable.get(targetClass);

			return (set == null ?
					Collections.<RemoteReference<?>>emptySet() : set);
		}

		public void addRemoteReference(RemoteReference<?> reference) {
			Set<RemoteReference<?>> set =
					remoteRefTable.get(reference.targetClass);

			if (set == null) {
				set = new LinkedHashSet<RemoteReference<?>>();
				remoteRefTable.put(reference.targetClass, set);
			}

			set.add(reference);
		}

		public void removeRemoteReference(RemoteReference<?> reference) {
			final Set<RemoteReference<?>> set =
					remoteRefTable.get(reference.targetClass);
			if (set == null) {
				return;
			}

			try {
				set.remove(reference);
			}
			finally {
				reference.clear();
			}
		}

		public ContainerCache getContainerCache() {
			return containerCache;
		}

		public void setPreferableHost(int partitionId, InetAddress host) {
			if (host == null) {
				preferableHosts.remove(partitionId);
			}
			else {
				preferableHosts.put(partitionId, host);
			}

			if (partitionId >= 0 && partitionId < addressCache.size() &&
					(host == null || !host.equals(addressCache.get(partitionId)))) {
				addressCache.set(partitionId, null);
			}
		}

		private SocketAddress getLastRemoteAddress() {
			if (lastConnection == null) {
				return null;
			}
			return lastConnection.getRemoteSocketAddress();
		}

		public DataAffinityPattern getDataAffinityPattern() {
			return dataAffinityPattern;
		}

		public String getUser() {
			return loginInfo.getUser();
		}

		public String getDatabaseName() {
			return loginInfo.getDatabase();
		}

		public boolean isVisible(ContainerVisibility visibility) {
			return localConfig.containerVisibility.contains(visibility);
		}

		public ContainerKeyConverter getKeyConverter(
				boolean internalMode, boolean forModification) {
			final boolean systemAllowed =
					(!forModification &&
							(isVisible(ContainerVisibility.META) ||
							(isVisible(ContainerVisibility.INTERNAL_META)) ||
					isVisible(ContainerVisibility.SYSTEM_TOOL)));

			ContainerKeyConverter converter;
			if (internalMode) {
				converter = internalKeyConverter;
			}
			else if (systemAllowed) {
				converter = systemKeyConverter;
			}
			else {
				converter = keyConverter;
			}

			if (converter == null) {
				converter = ContainerKeyConverter.getInstance(
						NodeConnection.getProtocolVersion(),
						internalMode, systemAllowed);
				if (internalMode) {
					internalKeyConverter = converter;
				}
				else if (systemAllowed) {
					systemKeyConverter = converter;
				}
				else {
					keyConverter = converter;
				}
			}

			return converter;
		}

		public LocalConfig getConfig() {
			return localConfig;
		}

	}

	public static class ContextReference extends WeakReference<Object> {

		final Context context;

		ContextReference(Object target, Context context) {
			super(target);
			this.context = context;
		}

	}

	public static class ContainerCache {

		private final int cacheSize;

		private final Map<ContainerKey, LocatedSchema> schemaCache =
				new LinkedHashMap<ContainerKey, LocatedSchema>();

		private final Map<SessionInfo.Key, SessionInfo> sessionCache =
				new LinkedHashMap<SessionInfo.Key, SessionInfo>();

		public ContainerCache(int cacheSize) {
			this.cacheSize = cacheSize;
		}

		public LocatedSchema findSchema(
				ContainerKey normalizedContainerKey,
				Class<?> rowType, ContainerType containerType) {
			final LocatedSchema schema =
					schemaCache.get(normalizedContainerKey);
			if (schema == null ||
					rowType != null &&
					rowType != schema.mapper.getRowType() ||
					containerType != null &&
					containerType != schema.mapper.getContainerType()) {
				return null;
			}

			return schema;
		}

		public void cacheSchema(
				ContainerKey normalizedContainerKey, LocatedSchema schema) {
			schemaCache.put(normalizedContainerKey, schema);
			if (schemaCache.size() > cacheSize) {
				final Iterator<LocatedSchema> it =
						schemaCache.values().iterator();
				it.next();
				it.remove();
			}
		}

		public void removeSchema(ContainerKey normalizedContainerKey) {
			schemaCache.remove(normalizedContainerKey);
		}

		public SessionInfo takeSession(
				Context context, int partitionId, long containerId) {
			synchronized (context) {
				return sessionCache.remove(
						new SessionInfo.Key(partitionId, containerId));
			}
		}

		public SessionInfo cacheSession(
				Context context, SessionInfo sessionInfo) {
			synchronized (context) {
				final SessionInfo old =
						sessionCache.put(sessionInfo.key, sessionInfo);
				if (old != null) {
					return old;
				}

				if (sessionCache.size() > cacheSize) {
					final Iterator<SessionInfo> it =
							sessionCache.values().iterator();
					final SessionInfo earliest = it.next();
					it.remove();
					return earliest;
				}

				return null;
			}
		}

		public List<SessionInfo> takeAllSessions(Context context) {
			synchronized (context) {
				final List<SessionInfo> list = new ArrayList<SessionInfo>(
						sessionCache.values());
				sessionCache.clear();
				return list;
			}
		}

	}

	public static class LocatedSchema {

		private final RowMapper mapper;

		private final long containerId;

		private final int versionId;

		private final ContainerKey containerKey;

		public LocatedSchema(
				RowMapper mapper, long containerId, int versionId,
				ContainerKey containerKey) {
			this.mapper = mapper;
			this.containerId = containerId;
			this.versionId = versionId;
			this.containerKey = containerKey;
		}

		public RowMapper getMapper() {
			return mapper;
		}

		public long getContainerId() {
			return containerId;
		}

		public int getVersionId() {
			return versionId;
		}

		public ContainerKey getContainerKey() {
			return containerKey;
		}

	}

	public static class SessionInfo {

		private final Key key;

		private final long sessionId;

		private final long lastStatementId;

		public SessionInfo(
				int partitionId, long containerId,
				long sessionId, long lastStatementId) {
			this.key = new Key(partitionId, containerId);
			this.sessionId = sessionId;
			this.lastStatementId = lastStatementId;
		}

		public int getPartitionId() {
			return key.partitionId;
		}

		public long getContainerId() {
			return key.containerId;
		}

		public long getSessionId() {
			return sessionId;
		}

		public long getLastStatementId() {
			return lastStatementId;
		}

		private static class Key {

			private final int partitionId;

			private final long containerId;

			public Key(int partitionId, long containerId) {
				this.partitionId = partitionId;
				this.containerId = containerId;
			}

			@Override
			public int hashCode() {
				final int prime = 31;
				int result = 1;
				result = prime * result +
						(int) (containerId ^ (containerId >>> 32));
				result = prime * result + partitionId;
				return result;
			}

			@Override
			public boolean equals(Object obj) {
				if (this == obj)
					return true;
				if (obj == null)
					return false;
				if (getClass() != obj.getClass())
					return false;
				Key other = (Key) obj;
				if (containerId != other.containerId)
					return false;
				if (partitionId != other.partitionId)
					return false;
				return true;
			}

		}
	}

	public static class DataAffinityPattern {

		private static final Pattern MATCH_PATTERN =
				Pattern.compile("([^%]+)|((?<!\\\\)%)");

		private static final Pattern WILDCARD_PATTERN =
				Pattern.compile("(?<!\\\\)%");

		private static final Pattern ENTRY_SEPARATOR_PATTERN =
				Pattern.compile("(?<!\\\\),");

		private static final Pattern PAIR_SEPARATOR_PATTERN =
				Pattern.compile("(?<!\\\\)=");

		private static final Pattern DUPLICATE_WILDCARD_PATTERN =
				Pattern.compile("(?<!\\\\)%%");

		private static final Pattern ESCAPED_CHAR_PATTERN =
				Pattern.compile("\\\\(.)");

		private static final Pattern ESCAPED_NODE_AFFINITY_SEPARATOR_PATTERN =
				Pattern.compile("\\\\@");

		private static class Entry {
			final Pattern basePattern;
			final Pattern nodeAffinityStrPattern;
			final long nodeAffinityNum;
			final boolean nodeAffinityIgnorable;
			final String affinity;

			private Entry(
					Pattern basePattern, Pattern nodeAffinityStrPattern,
					long nodeAffinityNum, boolean nodeAffinityIgnorable,
					String affinity) {
				this.basePattern = basePattern;
				this.nodeAffinityStrPattern = nodeAffinityStrPattern;
				this.nodeAffinityNum = nodeAffinityNum;
				this.nodeAffinityIgnorable = nodeAffinityIgnorable;
				this.affinity = affinity;
			}
		}

		private List<Entry> entryList = new ArrayList<Entry>();

		public DataAffinityPattern(
				String patternStr, ContainerKeyConverter keyConverter)
				throws GSException {
			if (patternStr == null || patternStr.isEmpty()) {
				return;
			}

			for (String entryStr :
					ENTRY_SEPARATOR_PATTERN.split(patternStr, -1)) {
				final String[] pair = PAIR_SEPARATOR_PATTERN.split(entryStr);
				if (pair.length != 2) {
					throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
							"Illegal data affinity pattern entry (" +
							"entry=\"" + entryStr +
							"\", pattern=\"" + patternStr + "\")");
				}

				final ContainerKeyConverter.Components components =
						new ContainerKeyConverter.Components();
				final String keyPattern = pair[0];
				try {
					final String modKeyPattern =
							unescapeNodeAffinity(keyPattern);
					keyConverter.parse(
							unescape(WILDCARD_PATTERN.matcher(
									modKeyPattern).replaceAll("_")),
							new ContainerKeyConverter.Components(), true);
					keyConverter.parse(modKeyPattern, components, false);
				}
				catch (GSException e) {
					throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
							"Illegal character found or empty key pattern " +
							"found in data affinity pattern (" +
							"keyPattern=\"" + keyPattern +
							"\", entry=\"" + entryStr +
							"\", pattern=\"" + patternStr + "\")");
				}

				if (DUPLICATE_WILDCARD_PATTERN.matcher(keyPattern).find()) {
					throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
							"Duplicate wildcard found in data affinity pattern (" +
							"keyPattern=\"" + keyPattern +
							"\", entry=\"" + entryStr +
							"\", pattern=\"" + patternStr + "\")");
				}

				final String dataAffinity = unescape(pair[1]);
				try {
					RowMapper.checkSymbol(dataAffinity, "data affinity");
				}
				catch (GSException e) {
					throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
							"Illegal data affinity specified " +
							"in data affinity pattern (" +
							"affinity=\"" + dataAffinity +
							"\", entry=\"" + entryStr +
							"\", pattern=\"" + patternStr + "\"" +
							", reason=" + e + ")", e);
				}

				final boolean[] wildCardEnd = new boolean[1];
				final Pattern basePattern = createPattern(
						components.base, keyConverter, wildCardEnd);

				final boolean nodeAffinityFound =
						(components.affinityStr != null ||
						components.affinityNum >= 0);

				final Pattern nodeAffinityStrPattern;
				if (components.affinityStr == null) {
					nodeAffinityStrPattern = null;
				}
				else {
					nodeAffinityStrPattern = createPattern(
							components.affinityStr, keyConverter, null);
				}

				final boolean nodeAffinityIgnorable =
						!nodeAffinityFound && wildCardEnd[0];

				entryList.add(new Entry(
						basePattern, nodeAffinityStrPattern,
						components.affinityNum, nodeAffinityIgnorable,
						dataAffinity));
			}
		}

		private static Pattern createPattern(
				String src, ContainerKeyConverter keyConverter,
				boolean[] wildCardEndRef) {
			final Matcher matcher = MATCH_PATTERN.matcher(src);
			final StringBuilder patternBuilder = new StringBuilder();
			patternBuilder.append("^");
			boolean wildCardEnd = false;
			while (matcher.find()) {
				if (matcher.group(2) == null) {
					final String elem = unescape(matcher.group(1));
					try {
						keyConverter.parse("_" + elem, false);
					}
					catch (GSException e) {
						throw new Error(e);
					}
					patternBuilder.append(Pattern.quote(normalize(elem)));
					wildCardEnd = false;
				}
				else {
					patternBuilder.append(".*");
					wildCardEnd = true;
				}
			}
			patternBuilder.append("$");

			if (wildCardEndRef != null) {
				wildCardEndRef[0] = wildCardEnd;
			}

			return Pattern.compile(patternBuilder.toString());
		}

		public String match(
				ContainerKey containerKey, String defaultAffinity,
				ContainerKeyConverter keyConverter)
				throws GSException {
			if (!entryList.isEmpty()) {
				final ContainerKeyConverter.Components components =
						new ContainerKeyConverter.Components();
				keyConverter.decompose(containerKey, components);

				for (Entry entry : entryList) {
					if (!entry.basePattern.matcher(
							normalize(components.base)).find()) {
						continue;
					}

					if (entry.nodeAffinityIgnorable) {
						return entry.affinity;
					}

					final long nodeAffinityNum = components.affinityNum;
					if (entry.nodeAffinityNum >= 0 &&
							nodeAffinityNum == entry.nodeAffinityNum) {
						return entry.affinity;
					}

					final String nodeAffinityStr = (nodeAffinityNum >= 0 ?
							Long.toString(nodeAffinityNum) :
							components.affinityStr);
					if (entry.nodeAffinityStrPattern != null) {
						if (nodeAffinityStr == null) {
							continue;
						}
						if (!entry.nodeAffinityStrPattern.matcher(
								normalize(nodeAffinityStr)).find()) {
							continue;
						}
					}
					else if (nodeAffinityStr != null) {
						continue;
					}

					return entry.affinity;
				}
			}

			return defaultAffinity;
		}

		private static String unescape(String src) {
			return ESCAPED_CHAR_PATTERN.matcher(src).replaceAll("$1");
		}

		private static String unescapeNodeAffinity(String src) {
			return ESCAPED_NODE_AFFINITY_SEPARATOR_PATTERN.matcher(
					src).replaceAll("@");
		}

		private static String normalize(String src) {
			return RowMapper.normalizeSymbolUnchecked(src);
		}
	}

	public GridStoreChannel(Config config, Source source)
			throws GSException {
		this.pool = new NodeConnectionPool();
		this.key = source.key;
		this.nodeResolver = new NodeResolver(
				pool, key.passive, key.address,
				config.getConnectionConfig(),
				key.sarConfig, key.memberList, null);
		this.requestHeadLength =
				NodeConnection.getRequestHeadLength(key.isIPV6Enabled());
		apply(config);
	}

	public NodeConnection.Config getConnectionConfig() {
		return config.getConnectionConfig();
	}

	public NodeConnectionPool getConnectionPool() {
		return pool;
	}

	public void apply(Config config) {
		this.config.set(config);
		nodeResolver.setConnectionConfig(this.config.connectionConfig);
		nodeResolver.setNotificationReceiveTimeoutMillis(
				config.getNotificationReceiveTimeoutMillis());
		nodeResolver.setPreferableConnectionPoolSize(
				config.getMaxConnectionPoolSize());
	}

	public ContextReference registerContext(
			Context context, Object target) throws GSException {
		synchronized (contextRefSet) {
			final List<ContextReference> pendingList =
					new ArrayList<ContextReference>(MAX_REF_SCAN_COUNT);
			try {
				for (Iterator<ContextReference> it = contextRefSet.iterator();
						it.hasNext();) {

					final ContextReference ref = it.next();
					if (ref.get() == null) {
						closeContext(ref.context);
					}
					else {
						pendingList.add(ref);
					}

					it.remove();

					if (pendingList.size() >= MAX_REF_SCAN_COUNT) {
						break;
					}
				}
			}
			finally {
				if (pendingList != null) {
					contextRefSet.addAll(pendingList);
				}
			}

			final ContextReference newRef =
					new ContextReference(target, context);
			contextRefSet.add(newRef);

			return newRef;
		}
	}

	public void unregisterContext(ContextReference ref) {
		synchronized (contextRefSet) {
			contextRefSet.remove(ref);
		}
	}

	public void closeAllContext(boolean silent) throws GSException {
		synchronized (contextRefSet) {
			boolean succeeded = false;
			try {
				for (Iterator<ContextReference> it = contextRefSet.iterator();
						it.hasNext();) {

					final Context context = it.next().context;
					it.remove();

					synchronized (context) {
						if (!context.closed) {
							if (silent) {
								try {
									closeContext(context);
								}
								catch (Throwable t) {
								}
							}
							else {
								closeContext(context);
							}
						}
					}
				}
				succeeded = true;
			}
			finally {
				if (!succeeded && !silent) {
					closeAllContext(true);
				}
			}
		}
	}

	public long getFailoverTimeoutMillis(Context context) {
		if (context.localConfig.failoverTimeoutMillis >= 0) {
			return context.localConfig.failoverTimeoutMillis;
		}

		return config.failoverTimeoutMillis;
	}

	public void setStatementRetryMode(int statementRetryMode) {
		this.statementRetryMode = statementRetryMode;
	}

	public BasicBuffer createRequestBuffer() {
		return createRequestBuffer(INITIAL_BUFFER_SIZE);
	}

	public BasicBuffer createRequestBuffer(int minCapacity) {
		final BasicBuffer buffer =
				new BasicBuffer(Math.max(requestHeadLength, minCapacity));
		NodeConnection.fillRequestHead(key.isIPV6Enabled(), buffer);
		return buffer;
	}

	public BasicBuffer createResponseBuffer() {
		return createResponseBuffer(INITIAL_BUFFER_SIZE);
	}

	public BasicBuffer createResponseBuffer(int minCapacity) {
		return new BasicBuffer(Math.max(requestHeadLength, minCapacity));
	}

	public void setupRequestBuffer(BasicBuffer buffer) {
		buffer.base().position(requestHeadLength);
	}

	public void cleanRemoteResources(
			Context context, Set<? extends Class<?>> targetClasses)
			throws GSException {
		synchronized (context) {
			for (Reference<?> ref;
					(ref = context.remoteRefQueue.poll()) != null;) {
				closeRemoteResource(context, (RemoteReference<?>) ref, false);
			}

			for (Class<?> clazz : targetClasses == null ?
					context.getReferenceTargetClasses() : targetClasses) {

				final Set<RemoteReference<?>> set =
						context.getRemoteReferences(clazz);
				if (set.isEmpty()) {
					continue;
				}

				final List<RemoteReference<?>> pendingList =
						new ArrayList<RemoteReference<?>>(
								MAX_REF_SCAN_COUNT);
				try {
					for (Iterator<RemoteReference<?>> it = set.iterator();
							it.hasNext();) {
						final RemoteReference<?> ref = it.next();
						if (ref.get() == null) {
							closeRemoteResource(context, ref, true);
						}
						else {
							pendingList.add(ref);
						}
						it.remove();

						if (pendingList.size() >= MAX_REF_SCAN_COUNT) {
							break;
						}
					}
				}
				finally {
					set.addAll(pendingList);
				}
			}
		}
	}

	public void closeAllRemoteResources(
			Context context, Set<Class<?>> targetClasses,
			int partitionId, long containerId, boolean silent)
			throws GSException {

		synchronized (context) {
			boolean succeeded = false;
			try {
				for (Class<?> clazz : targetClasses == null ?
						context.getReferenceTargetClasses() : targetClasses) {

					for (Iterator<RemoteReference<?>> it =
							context.getRemoteReferences(clazz).iterator();
							it.hasNext();) {
						final RemoteReference<?> ref = it.next();

						if (targetClasses == null ||
								ref.partitionId == partitionId &&
								ref.containerId == containerId) {
							if (silent) {
								try {
									closeRemoteResource(context, ref, true);
								}
								catch (Throwable t) {
								}
							}
							else {
								closeRemoteResource(context, ref, true);
							}
						}

						it.remove();
					}
				}
				succeeded = true;
			}
			finally {
				if (!succeeded) {
					closeAllRemoteResources(
							context, targetClasses, partitionId, containerId,
							true);
				}
			}
		}
	}

	public void closeRemoteResource(
			Context context, RemoteReference<?> ref,
			boolean refMapRetained) throws GSException {
		synchronized (context) {
			try {
				ref.close(this, context);
			}
			finally {
				try {
					if (!refMapRetained) {
						context.removeRemoteReference(ref);
					}
				}
				finally {
					ref.clear();
				}
			}
		}
	}

	public void closeContext(Context context) throws GSException {
		synchronized (context) {
			try {
				closeAllRemoteResources(context, null, 0, 0, false);
			}
			finally {
				context.partitionId = -1;
				context.lastConnection = null;
				context.closed = true;

				for (Iterator<NodeConnection> it =
						context.activeConnections.values().iterator();
						it.hasNext();) {
					NodeConnection connection = it.next();
					try {
						if (connection != null) {
							pool.add(connection);
						}
						connection = null;
					}
					finally {
						if (connection != null) {
							connection.close();
						}
					}
					it.remove();
				}
			}
		}
	}

	public Object getLastConnection(Context context) throws GSException {
		synchronized (context) {
			return context.lastConnection;
		}
	}

	public void checkActiveConnection(
			Context context, int partitionId, Object connection) throws GSException {
		final SocketAddress address =
				((NodeConnection) connection).getRemoteSocketAddress();
		synchronized (context) {
			final NodeConnection activeConnection =
					context.activeConnections.get(address);
			if (activeConnection == null || activeConnection != connection) {
				throw GSErrorCode.newGSRecoverableException(
						GSErrorCode.RECOVERABLE_CONNECTION_PROBLEM, "", null);
			}
		}
	}

	private static boolean isConnectionDependentStatement(
			GeneralStatement statement) {
		return (statement == Statement.CLOSE_ROW_SET.generalize() ||
				statement == Statement.FETCH_ROW_SET.generalize());
	}

	public void executeStatement(
			Context context, Statement statement,
			int partitionId, long statementId,
			BasicBuffer req, BasicBuffer resp)
			throws GSException {
		executeStatement(
				context, statement.generalize(),
				partitionId, statementId, req, resp, null);
	}

	public void executeStatement(
			Context context, GeneralStatement statement,
			int partitionId, long statementId,
			BasicBuffer req, BasicBuffer resp,
			ContextMonitor contextMonitor)
			throws GSException {

		
		
		synchronized (context) {
			if (context.closed) {
				throw new GSException(
						GSErrorCode.RESOURCE_CLOSED, "Already closed");
			}

			final ContextMonitor localMonitor = (contextMonitor == null ?
					context.contextMonitor : contextMonitor);
			if (localMonitor != null && contextMonitor == null) {
				localMonitor.startStatement(
						statement, statementId, partitionId, null);
			}

			final int reqLength = req.base().position();

			SocketAddress orgAddress = null;
			long failoverStartTime = 0;
			long failoverTrialCount = 0;
			long failoverReconnectedTrial = -1;
			FailureHistory history = null;
			for (boolean firstTrial = true;; firstTrial = false) {
				try {
					if (statement == null) {
						if (localMonitor != null) {
							localMonitor.startStatementIO();
						}
						context.activeResolverExecutor.execute();
					}
					else {
						updateConnection(context, partitionId, firstTrial);

						if (localMonitor != null) {
							localMonitor.startStatementIO();
						}

						context.lastConnection.executeStatement(
								statement, context.partitionId,
								statementId, req, resp);
					}

					if (!firstTrial) {
						if (failoverTrialCount > 0) {
							context.failoverCount++;
						}
						if (LOGGER.isInfoEnabled()) {
							LOGGER.info(
									failoverTrialCount > 0 ?
											"connection.failoverSucceeded" :
											"connection.retrySucceeded",
									ContextMonitor.getObjectId(context),
									statement,
									orgAddress,
									context.getLastRemoteAddress(),
									partitionId,
									statementId,
									failoverTrialCount);
						}
					}

					if (localMonitor != null) {
						localMonitor.endStatementIO();

						if (contextMonitor == null) {
							localMonitor.endStatement();
						}
					}

					break;
				}
				catch (GSConnectionException e) {
					try {
						
						if (context.lastConnection != null) {
							if (!firstTrial && context.lastHeartbeatCount !=
									context.lastConnection.
									getHeartbeatReceiveCount()) {
								failoverStartTime = 0;
								failoverReconnectedTrial = failoverTrialCount;
							}
							final SocketAddress address =
									context.lastConnection.
									getRemoteSocketAddress();
							context.activeConnections.remove(address);
							try {
								context.lastConnection.close();
							}
							catch (GSException e2) {
							}

							if (orgAddress != null) {
								orgAddress = address;
							}
						}
						context.lastConnection = null;
					}
					finally {
						
						
						if (statementRetryMode == 0) {
							invalidateMaster(context);
						}
					}
					req.base().position(reqLength);

					if (localMonitor != null) {
						localMonitor.endStatementIO();
					}

					if (statement != null &&
							isConnectionDependentStatement(statement)) {
						throw GSErrorCode.newGSRecoverableException(
								GSErrorCode.RECOVERABLE_CONNECTION_PROBLEM,
								null, e);
					}

					final long curTime = System.currentTimeMillis();

					
					if (statementRetryMode == 0 || statementRetryMode == 2) {
						if (firstTrial && !(e instanceof GSWrongNodeException)) {
							if (LOGGER.isInfoEnabled()) {
								LOGGER.info(
										"connection.retryStarted",
										ContextMonitor.getObjectId(context),
										statement,
										orgAddress,
										partitionId,
										statementId,
										failoverTrialCount,
										e);
							}
							history = FailureHistory.prepare(history);
							history.add(e, curTime);
							continue;
						}
					}

					if (failoverStartTime == 0) {
						failoverStartTime = curTime;
					}

					final long failureMillis = curTime - failoverStartTime;
					final long failoverTimeout = getFailoverTimeoutMillis(context);
					if (failureMillis >= failoverTimeout) {
						
						final String errorMessage = formatTimeoutError(
								failoverStartTime, failoverTrialCount,
								failoverReconnectedTrial, history, firstTrial,
								curTime, failoverTimeout, e);

						throw new GSTimeoutException(
								e.getErrorCode(), errorMessage, e);
					}

					

					if (LOGGER.isInfoEnabled()) {
						LOGGER.info(
								"connection.failoverWorking",
								ContextMonitor.getObjectId(context),
								statement,
								orgAddress,
								partitionId,
								statementId,
								failoverTrialCount,
								failureMillis,
								failoverTimeout,
								e);
					}

					
					
					if (statementRetryMode == 2 && failoverTrialCount > 0 ||
							statementRetryMode == 1 &&
							(e instanceof GSWrongNodeException)) {
						invalidateMaster(context);
					}

					try {
						Thread.sleep(config.getFailoverRetryIntervalMillis());
					}
					catch (InterruptedException e2) {
					}
					failoverTrialCount++;
					history = FailureHistory.prepare(history);
					history.add(e, curTime);
				}
				catch (GSStatementException e) {
					if (!firstTrial && failoverTrialCount > 0) {
						context.failoverCount++;
					}
					if (!firstTrial && LOGGER.isInfoEnabled()) {
						LOGGER.info(
								"connection.statementErrorAfter" +
										(failoverTrialCount > 0 ?
												"Failover" : "Retry"),
								ContextMonitor.getObjectId(context),
								statement,
								orgAddress,
								context.getLastRemoteAddress(),
								partitionId,
								statementId,
								failoverTrialCount,
								e);
					}

					throw e;
				}
			}
		}
	}

	private static String formatTimeoutError(
			long failoverStartTime, long failoverTrialCount,
			long failoverReconnectedTrial, FailureHistory history,
			boolean firstTrial, long curTime, long failoverTimeout,
			GSException reason) {
		final long failureMillis = curTime - failoverStartTime;
		final String reasonStr =
				(reason.getMessage() == null ? "" : reason.getMessage());
		if (failoverTimeout > 0) {
			final StringBuilder builder = new StringBuilder();
			builder.append("Failover timed out (trialCount=");
			builder.append(failoverTrialCount);
			if (failoverReconnectedTrial >= 0) {
				builder.append(", reconnectedTrial=");
				builder.append(failoverReconnectedTrial);
			}
			builder.append(", failureMillis=");
			builder.append(failureMillis);
			builder.append(", timeoutMillis=");
			builder.append(failoverTimeout);
			builder.append(", reason=");
			builder.append(reasonStr);
			if (history != null) {
				history.format(
						builder, failoverTrialCount, curTime);
			}
			builder.append(")");
			return builder.toString();
		}
		else if (!firstTrial) {
			final StringBuilder builder = new StringBuilder();
			builder.append("Retry failed (reason=");
			builder.append(reasonStr);
			builder.append(")");
			return builder.toString();
		}
		else {
			return null;
		}
	}

	private <T> T executeResolver(
			Context context, ResolverExecutor<T> executor)
			throws GSException {
		
		context.activeResolverExecutor = executor;
		try {
			executeStatement(
					context, null, 0, 0, context.getRequestBuffer(), null, null);
			return executor.result;
		}
		finally {
			context.activeResolverExecutor = null;
		}
	}

	private void updateConnection(
			Context context, int partitionId, boolean preferConnectionPool)
			throws GSException {
		final NodeConnection lastConnection = context.lastConnection;
		if (lastConnection != null && context.partitionId == partitionId) {
			return;
		}

		final InetSocketAddress address;
		do {
			if (partitionId >= 0 && partitionId < context.addressCache.size()) {
				final InetSocketAddress cachedAddress =
						context.addressCache.get(partitionId);
				if (cachedAddress != null) {
					address = cachedAddress;
					break;
				}
			}

			address = nodeResolver.getNodeAddress(
					context.clusterInfo, partitionId,
					!context.loginInfo.isOwnerMode(),
					context.preferableHosts.get(partitionId));

			final int partitionCount =
					nodeResolver.getPartitionCount(context.clusterInfo);

			if (partitionId >= 0 && partitionId < partitionCount) {
				while (partitionId >= context.addressCache.size()) {
					context.addressCache.add(null);
				}
				context.addressCache.set(partitionId, address);
			}
		}
		while (false);

		if (lastConnection != null &&
				address.equals(lastConnection.getRemoteSocketAddress())) {
			context.partitionId = partitionId;
			return;
		}

		context.lastConnection = context.activeConnections.get(address);
		if (context.lastConnection != null) {
			context.partitionId = partitionId;
			return;
		}

		final BasicBuffer req = createRequestBuffer(64);
		final BasicBuffer resp = createResponseBuffer(64);

		final long[] databaseId = new long[1];
		final NodeConnection newConnection = pool.resolve(
				address, req, resp,
				config.getConnectionConfig(), context.loginInfo, databaseId,
				preferConnectionPool);
		nodeResolver.acceptDatabaseId(
				context.clusterInfo, databaseId[0],
				(InetSocketAddress) newConnection.getRemoteSocketAddress());

		boolean updated = false;
		try {
			context.activeConnections.put(address, newConnection);
			context.lastConnection = newConnection;
			context.lastHeartbeatCount =
					newConnection.getHeartbeatReceiveCount();
			context.partitionId = partitionId;
			updated = true;
		}
		finally {
			if (!updated) {
				context.lastConnection = null;
				context.partitionId = -1;
				try {
					context.activeConnections.remove(address);
				}
				finally {
					pool.add(newConnection);
				}
			}
		}
	}

	public void invalidateMaster(Context context) {
		synchronized (context) {
			context.addressCache.clear();
			nodeResolver.invalidateMaster(context.clusterInfo);
		}
	}

	public void checkContextAvailable(Context context) throws GSException {
		synchronized (context) {
			if (context.closed) {
				throw new GSException(GSErrorCode.RESOURCE_CLOSED, "");
			}
		}
	}

	public long getDatabaseId(Context context) throws GSException {
		synchronized (context) {
			final ClusterInfo clusterInfo = context.clusterInfo;

			if (clusterInfo.getDatabaseId() == null) {
				executeResolver(context, new ResolverExecutor<Void>() {
					@Override
					public void execute() throws GSException {
						nodeResolver.getDatabaseId(clusterInfo);
					}
				});
			}

			return clusterInfo.getDatabaseId();
		}
	}

	public InetSocketAddress[] getNodeAddressList(
			Context context, final int partitionId) throws GSException {
		synchronized (context) {
			final ClusterInfo clusterInfo = context.clusterInfo;
			return executeResolver(
					context, new ResolverExecutor<InetSocketAddress[]>() {
				@Override
				public void execute() throws GSException {
					result = nodeResolver.getNodeAddressList(
							clusterInfo, partitionId);
				}
			});
		}
	}

	public int getPartitionCount(Context context) throws GSException {
		synchronized (context) {
			final ClusterInfo clusterInfo = context.clusterInfo;

			if (clusterInfo.getPartitionCount() == null) {
				executeResolver(
						context, new ResolverExecutor<Void>() {
					@Override
					public void execute() throws GSException {
						nodeResolver.getPartitionCount(clusterInfo);
					}
				});
			}

			return clusterInfo.getPartitionCount();
		}
	}

	public int resolvePartitionId(
			final Context context, ContainerKey containerKey,
			boolean internalMode) throws GSException {
		synchronized (context) {
			final ClusterInfo clusterInfo = context.clusterInfo;

			Integer partitionCount;
			ContainerHashMode hashMode;
			for (;;) {
				partitionCount = clusterInfo.getPartitionCount();
				hashMode = clusterInfo.getHashMode();
				if (partitionCount != null && hashMode != null) {
					break;
				}

				executeResolver(context, new ResolverExecutor<Void>() {
					@Override
					public void execute() throws GSException {
						nodeResolver.getPartitionCount(clusterInfo);
						nodeResolver.getContainerHashMode(clusterInfo);
					}
				});
			}

			final ContainerKeyConverter converter =
					context.getKeyConverter(true, false);

			final ContainerKeyConverter.Components components =
					new ContainerKeyConverter.Components();
			converter.decompose(containerKey, components);

			final String affinity = (components.affinityStr == null ?
					null : RowMapper.normalizeSymbolUnchecked(
							components.affinityStr));
			final boolean withSubCount = (components.subCount >= 0);

			if (components.affinityNum >= 0) {
				return (int) (components.affinityNum % partitionCount);
			}
			else if (components.affinityStr != null && !withSubCount) {
				return calculatePartitionId(
						affinity, hashMode, partitionCount);
			}

			final String base = (affinity == null ?
					RowMapper.normalizeSymbolUnchecked(
							components.base) : affinity);
			if (withSubCount) {
				final int subCount = components.subCount;
				final int subId = (int) components.largeId;
				if (partitionCount <= subCount) {
					return subId % partitionCount;
				}
				final int pbase = (partitionCount / subCount);
				final int pmod = (partitionCount % subCount);
				final int baseHash =
						calculatePartitionId(base, hashMode, pbase);
				return (pbase * subId + Math.min(pmod, subId) + baseHash);
			}
			else {
				int partitionId =
						calculatePartitionId(base, hashMode, partitionCount);
				if (components.largeId >= 0) {
					partitionId = (int) (partitionId + components.largeId) %
							partitionCount;
				}
				return partitionId;
			}
		}
	}

	public static int calculatePartitionId(
			String normalizedString, ContainerHashMode hashMode,
			int partitionCount) throws GSException {
		final byte[] bytes =
				normalizedString.getBytes(BasicBuffer.DEFAULT_CHARSET);
		switch (hashMode) {
		case COMPATIBLE1: {
			final CRC32 crc = new CRC32();
			crc.update(bytes);

			return (int) ((crc.getValue() & 0xffffffff) % partitionCount);
		}
		case MD5: {
			final MessageDigest md = MessageDigestFactory.MD5.create();
			md.update(bytes);
			final byte[] digest = md.digest();
			long hash = 0;
			for (int i = 0; i < 4; i++) {
				hash = (hash << 8) | (digest[i] & 0xff);
			}
			return (int) (hash % partitionCount);
		}
		default:
			throw new GSException(
					GSErrorCode.INTERNAL_ERROR, "Unsupported hash mode");
		}
	}

	public static boolean isMasterResolvableByConnection() {
		return NodeConnection.getProtocolVersion() >= 8;
	}

	public static ContainerHashMode getDefaultContainerHashMode() {
		if (NodeConnection.getProtocolVersion() < 14 ||
				v40ContainerHashCompatible) {
			return ContainerHashMode.COMPATIBLE1;
		}

		return ContainerHashMode.MD5;
	}

	public static int statementToNumber(Statement statement) {
		return NodeConnection.statementToNumber(statement.generalize());
	}

	public void close() throws GSException {
		try {
			closeAllContext(false);
		}
		finally {
			try {
				nodeResolver.close();
			}
			finally {
				pool.close();
			}
		}
	}

	static ContextMonitor createContextMonitorIfAvailable() {
		if (STATEMENT_LOGGER.isDebugEnabled()) {
			return new ContextMonitor();
		}
		else {
			return null;
		}
	}

	static class FailureHistory {

		private static final int MAX_HISTORY_SIZE = 10;

		private final List<FailureEntry> entryList =
				new ArrayList<FailureEntry>(MAX_HISTORY_SIZE);

		private long totalTrial;

		private long initialTimeMillis;

		private long lastTimeMillis;

		private boolean reduced;

		static FailureHistory prepare(FailureHistory instance) {
			if (instance == null) {
				return new FailureHistory();
			}
			return instance;
		}

		void add(GSException reason, long curTimeMillis) {
			if (totalTrial == 0) {
				initialTimeMillis = curTimeMillis;
				lastTimeMillis = curTimeMillis;
			}
			totalTrial++;

			while (entryList.size() >= MAX_HISTORY_SIZE) {
				long minMillis = Long.MAX_VALUE;
				int minIndex = -1;
				for (int i = 0; i < entryList.size(); i++) {
					long millis = entryList.get(i).millisFromPrev;
					if (millis < minMillis) {
						minMillis = millis;
						minIndex = i;
					}
				}
				if (minIndex >= 0) {
					entryList.remove(minIndex);
					reduced = true;
				}
				else {
					break;
				}
			}

			final long millisFromInitial = curTimeMillis - initialTimeMillis;
			final long millisFromPrev = curTimeMillis - lastTimeMillis;
			entryList.add(new FailureEntry(
					totalTrial, millisFromInitial, millisFromPrev, reason));

			lastTimeMillis = curTimeMillis;
		}

		void format(
				StringBuilder builder, long failureTrial, long curTimeMillis) {
			if (totalTrial != entryList.size()) {
				if (builder.length() > 0) {
					builder.append(", ");
				}
				builder.append("historySize=");
				builder.append(totalTrial);
			}
			if (!entryList.isEmpty()) {
				if (builder.length() > 0) {
					builder.append(", ");
				}
				builder.append("history");
				if (reduced) {
					builder.append("OfTop");
					builder.append(entryList.size());
				}
				builder.append(
						"(trial, millisFromInitial, millisFromPrev, reason)=");
				if (entryList.size() > 1) {
					builder.append("[");
				}
				boolean first = true;
				for (final FailureEntry entry : entryList) {
					if (first) {
						first = false;
					}
					else {
						builder.append(", ");
					}
					builder.append("(");
					builder.append(entry.trialNumber);
					builder.append(", ");
					builder.append(entry.millisFromInitial);
					builder.append(", ");
					builder.append(entry.millisFromPrev);
					builder.append(", ");
					builder.append(entry.reason.getMessage());
					builder.append(")");
				}
				if (entryList.size() > 1) {
					builder.append("]");
				}
			}
		}

		static class FailureEntry {

			final long trialNumber;

			final long millisFromInitial;

			final long millisFromPrev;

			final GSException reason;

			private FailureEntry(
					long trialNumber, long millisFromInitial,
					long millisFromPrev, GSException reason) {
				this.trialNumber = trialNumber;
				this.millisFromInitial = millisFromInitial;
				this.millisFromPrev = millisFromPrev;
				this.reason = reason;
			}


		}

	}

	static class ContextMonitor {

		ContainerKey containerKey;

		String query;

		long statementStartTime;

		long statementIOStartTime;

		long statementIOTotalTime;

		long sessionStartTime;

		long transactionStartTime;

		boolean containerIdSpecified;

		boolean sessionSpecified;

		boolean transactionStarted;

		GeneralStatement lastStatement;

		long lastStatementId;

		int lastPartitionId;

		long lastContainerId;

		long lastSessionId;

		void setContainerKey(ContainerKey containerKey) {
			this.containerKey = containerKey;
		}

		void setQuery(String query) {
			this.query = query;
		}

		void startStatement(
				GeneralStatement statement, long statementId,
				int partitionId, Long containerId) {
			statementStartTime = System.nanoTime();
			statementIOTotalTime = 0;
			lastStatement = statement;
			lastStatementId = statementId;
			lastPartitionId = partitionId;

			if (containerId == null) {
				lastContainerId = 0;
				containerIdSpecified = false;
			}
			else {
				lastContainerId = containerId;
				containerIdSpecified = true;
			}

			logStatement(true);
		}

		void endStatement() {
			logStatement(false);
		}

		private void logStatement(boolean start) {
			if (!STATEMENT_LOGGER.isDebugEnabled()) {
				return;
			}

			String key = "statementExec.";
			key += (start ? "started" : "ended");

			final List<Object> args = new ArrayList<Object>();
			args.add(getObjectId(this));
			args.add(lastStatement);
			args.add(lastStatementId);
			args.add(lastPartitionId);
			args.add(lastSessionId);

			do {
				if (containerKey != null) {
					args.add(containerKey);
				}
				else if (containerIdSpecified) {
					args.add(lastContainerId);
				}
				else {
					break;
				}

				if (query == null) {
					key += "WithContainer";
				}
				else {
					args.add(query);
					key += "WithQuery";
				}
			}
			while (false);

			if (!start) {
				final long totalTime = System.nanoTime() - statementStartTime;
				args.add(formatNanosAsMillis(statementIOTotalTime));
				args.add(formatNanosAsMillis(totalTime -statementIOTotalTime));
			}

			LOGGER.debug(key, args.toArray());
		}

		void startStatementIO() {
			statementIOStartTime = System.nanoTime();
		}

		void endStatementIO() {
			statementIOTotalTime += (System.nanoTime() - statementIOStartTime);
		}

		void startSession(long sessionId) {
			sessionStartTime = System.nanoTime();
			lastSessionId = sessionId;
			sessionSpecified = true;
		}

		void endSession() {
			sessionStartTime = 0;
			lastSessionId = 0;
			sessionSpecified = false;
		}

		void startTransaction() {
			transactionStartTime = System.nanoTime();
			transactionStarted = true;
		}

		void endTransaction() {
			transactionStartTime = 0;
			transactionStarted = false;
		}

		static String getObjectId(Object obj) {
			return Long.toHexString(System.identityHashCode(obj));
		}

		GSStatementException analyzeStatementException(
				GSStatementException e, Context context, Object resource) {

			final long curTime = System.nanoTime();
			final StringBuilder message = new StringBuilder();
			final Formatter formatter = new Formatter(message);

			message.append("reason=").append(e.getMessage());

			message.append(", localContext=").append(getObjectId(this));
			message.append(", context=").append(getObjectId(context));
			message.append(", resource=").append(getObjectId(resource));

			if (resource != null) {
				message.append(", resourceClass=");
				message.append(resource.getClass().getSimpleName());
			}

			if (lastStatement != null) {
				message.append(", statement=").append(lastStatement);
				message.append(", statementId=").append(lastStatementId);
				message.append(", statementMillis=");
				formatNanosAsMillis(formatter, curTime - statementStartTime);
				message.append(", partitionId=").append(lastPartitionId);
			}

			if (containerIdSpecified) {
				message.append(", containerId=").append(lastContainerId);
			}

			if (sessionSpecified) {
				message.append(", sessionId=").append(lastSessionId);
				message.append(", sessionMillis=");
				formatNanosAsMillis(formatter, curTime - sessionStartTime);
			}

			if (transactionStarted) {
				message.append(", transactionMillis=");
				formatNanosAsMillis(formatter, curTime - transactionStartTime);
			}

			return new GSStatementException(
					e.getErrorCode(), message.toString(), e);
		}

		private static void formatNanosAsMillis(
				Formatter formatter, long nanoTime) {
			formatter.format("%.3f", nanoTime / 1000.0 / 1000.0);
		}

		private static String formatNanosAsMillis(long nanoTime) {
			return String.format("%.3f", nanoTime / 1000.0 / 1000.0);
		}

	}

}
