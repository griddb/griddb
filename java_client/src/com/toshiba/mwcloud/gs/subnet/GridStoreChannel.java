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
import java.util.ArrayList;
import java.util.Collections;
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
import com.toshiba.mwcloud.gs.GSRecoverableException;
import com.toshiba.mwcloud.gs.GSTimeoutException;
import com.toshiba.mwcloud.gs.common.BasicBuffer;
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
import com.toshiba.mwcloud.gs.common.Statement;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.LoginInfo;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.OptionalRequest;
import com.toshiba.mwcloud.gs.subnet.NodeResolver.ContainerHashMode;

@SuppressWarnings("deprecation")
public class GridStoreChannel implements Closeable {

	public static Boolean v1ProtocolCompatible = null;

	public static boolean v1ProtocolCompatible_1_1_103x = false;

	public static boolean v10ResourceCompatible = false;

	public static boolean v15TSPropsCompatible = false;

	public static boolean v15DDLCompatible = true;

	public static boolean v20AffinityCompatible = false;

	public static boolean v21StatementIdCompatible = false;

	private static int FAILOVER_TIMEOUT_DEFAULT = 60 * 1000;

	private static int FAILOVER_RETRY_INTERVAL = 1 * 1000;

	public static int INITIAL_BUFFER_SIZE = 256;

	public static int MAPPER_CACHE_SIZE = 32;

	private static int MAX_REF_SCAN_COUNT = 15;

	private static final int SYSTEM_CONTAINER_PARTITION_ID = 0;

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

	private static String NUMERICAL_REGEX = "\\A(0|[1-9][0-9]{0,8})\\z";
	private static Pattern NUMERICAL_REGEX_PATTERN = Pattern.compile(NUMERICAL_REGEX);

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

	public static class Source {

		private final Key key;

		private final int partitionCount;

		private final long failoverTimeoutMillis;

		private final long transactionTimeoutMillis;

		private final NodeConnection.LoginInfo loginInfo;

		private transient final String password;

		private final int containerCacheSize;

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
			final String database = props.getProperty(
					"database", NodeConnection.LoginInfo.DEFAULT_DATABASE_NAME,
					false);

			this.key = new Key(
					passive[0], address,
					user, clusterName, sarConfig, memberList);
			this.password = password;

			Integer partitionCount =
					props.getIntProperty("partitionCount", true);
			this.partitionCount = (partitionCount == null ? 0 : partitionCount);

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

			final Integer containerCacheSize =
					props.getIntProperty("containerCacheSize", false);
			if (containerCacheSize != null && containerCacheSize < 0) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"Negative container cache size (size=" +
						containerCacheSize + ")");
			}

			this.failoverTimeoutMillis = props.getTimeoutProperty(
					"failoverTimeout", (long) -1, false);
			this.transactionTimeoutMillis = props.getTimeoutProperty(
					"transactionTimeout", (long) -1, false);

			this.loginInfo = new LoginInfo(
					user, password, !backupPreferred, database, clusterName,
					transactionTimeoutMillis);
			this.containerCacheSize =
					(containerCacheSize == null ? 0 : containerCacheSize);
			this.dataAffinityPattern = new DataAffinityPattern(
					props.getProperty("dataAffinityPattern", false));
		}

		public int getPartitionCount() {
			return partitionCount;
		}

		public NodeConnection.LoginInfo getLoginInfo() {
			return loginInfo;
		}

		public Key getKey() {
			return key;
		}

		public Context createContext() {
			final BasicBuffer req = createRequestBuffer();
			final BasicBuffer resp = new BasicBuffer(INITIAL_BUFFER_SIZE);
			final BasicBuffer syncReq = createRequestBuffer();
			final BasicBuffer syncResp = new BasicBuffer(INITIAL_BUFFER_SIZE);

			return new Context(
					failoverTimeoutMillis, transactionTimeoutMillis, loginInfo,
					req, resp, syncReq, syncResp, containerCacheSize,
					dataAffinityPattern);
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

		private final String user;

		private final String clusterName;

		private final ServiceAddressResolver.Config sarConfig;

		private final List<InetSocketAddress> memberList;

		public Key(boolean passive, InetSocketAddress address,
				String user, String clusterName,
				ServiceAddressResolver.Config sarConfig,
				List<InetSocketAddress> memberList) {
			this.passive = passive;
			this.address = address;
			this.user = user;
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
			result = prime * result + ((user == null) ? 0 : user.hashCode());
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
			if (user == null) {
				if (other.user != null)
					return false;
			}
			else if (!user.equals(other.user))
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

		private final long failoverTimeoutMillis;

		private final long transactionTimeoutMillis;

		private int partitionId = -1;

		private int partitionCount = -1;

		private ContainerHashMode containerHashMode;

		private NodeConnection lastConnection;

		private final Map<SocketAddress, NodeConnection> activeConnections =
				new HashMap<SocketAddress, NodeConnection>();

		private final NodeConnection.LoginInfo loginInfo;

		private boolean closed;

		private int failoverCount;

		private final BasicBuffer req;

		private BasicBuffer resp;

		private final BasicBuffer syncReq;

		private final BasicBuffer syncResp;

		private final OptionalRequest optionalRequest = new OptionalRequest();

		private final List<InetSocketAddress> addressCache =
				new ArrayList<InetSocketAddress>();

		private final UUID sessionUUID = UUID.randomUUID();

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

		private Context(
				long failoverTimeoutMillis, long transactionTimeoutMillis,
				NodeConnection.LoginInfo loginInfo,
				BasicBuffer req, BasicBuffer resp,
				BasicBuffer syncReq, BasicBuffer syncResp,
				int containerCacheSize,
				DataAffinityPattern dataAffinityPattern) {
			this.failoverTimeoutMillis = failoverTimeoutMillis;
			this.transactionTimeoutMillis = transactionTimeoutMillis;
			this.loginInfo = loginInfo;
			this.req = req;
			this.resp = resp;
			this.syncReq = syncReq;
			this.syncResp = syncResp;
			this.containerCache = (containerCacheSize > 0 ?
					new ContainerCache(containerCacheSize) : null);
			this.dataAffinityPattern = dataAffinityPattern;
		}

		public boolean isClosedAsync() {
			return closed;
		}

		public long getTransactionTimeoutMillis() {
			return transactionTimeoutMillis;
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

		private final Map<String, LocatedSchema> schemaCache =
				new LinkedHashMap<String, LocatedSchema>();

		private final Map<SessionInfo.Key, SessionInfo> sessionCache =
				new LinkedHashMap<SessionInfo.Key, SessionInfo>();

		public ContainerCache(int cacheSize) {
			this.cacheSize = cacheSize;
		}

		public LocatedSchema findSchema(
				String normalizedContainerName,
				Class<?> rowType, ContainerType containerType) {
			final LocatedSchema schema =
					schemaCache.get(normalizedContainerName);
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
				String normalizedContainerName, LocatedSchema schema) {
			schemaCache.put(normalizedContainerName, schema);
			if (schemaCache.size() > cacheSize) {
				final Iterator<LocatedSchema> it =
						schemaCache.values().iterator();
				it.next();
				it.remove();
			}
		}

		public void removeSchema(String normalizedContainerName) {
			schemaCache.remove(normalizedContainerName);
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
					final SessionInfo eariest = it.next();
					it.remove();
					return eariest;
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

		private final String containerName;

		public LocatedSchema(
				RowMapper mapper, long containerId, int versionId,
				String containerName) {
			this.mapper = mapper;
			this.containerId = containerId;
			this.versionId = versionId;
			this.containerName = containerName;
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

		public String getContainerName() {
			return containerName;
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

		private static class Entry {
			final Pattern pattern;
			final String affinity;

			Entry(Pattern pattern, String affinity) {
				this.pattern = pattern;
				this.affinity = affinity;
			}
		}

		private List<Entry> entryList = new ArrayList<Entry>();

		public DataAffinityPattern(String patternString) throws GSException {
			if (patternString == null || patternString.isEmpty()) {
				return;
			}

			final Pattern basePattern = Pattern.compile("([^%]+)|(%)");

			for (String entryString : patternString.split(",", -1)) {
				final String[] pair = entryString.split("=");
				if (pair.length != 2) {
					throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
							"Illegal data affinity pattern entry (" +
							"entry=\"" + entryString +
							"\", pattern=\"" + patternString + "\")");
				}

				final String subPattern = pair[0];
				try {
					final String nonNodeAffinityPattern = subPattern.replaceFirst("@", "_");
					RowMapper.normalizeSymbol(nonNodeAffinityPattern.replaceAll("%", "_"));
				}
				catch (GSException e) {
					throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
							"Illegal character found or empty sub pattern " +
							"found in data affinity pattern (" +
							"subPattern=\"" + subPattern +
							"\", entry=\"" + entryString +
							"\", pattern=\"" + patternString + "\")");
				}

				if (subPattern.contains("%%")) {
					throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
							"Duplicated wildcard found in data affinity pattern (" +
							"subPattern=\"" + subPattern +
							"\", entry=\"" + entryString +
							"\", pattern=\"" + patternString + "\")");
				}

				final String affinity = pair[1];
				try {
					RowMapper.normalizeSymbol(affinity);
				}
				catch (GSException e) {
					throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
							"Illegal character found or empty affinity string " +
							"found in data affinity pattern (" +
							"affinity=\"" + affinity +
							"\", entry=\"" + entryString +
							"\", pattern=\"" + patternString + "\")");
				}

				final Matcher matcher = basePattern.matcher(subPattern);
				final StringBuilder patternBuilder = new StringBuilder();
				patternBuilder.append("^");
				while (matcher.find()) {
					if (matcher.group(2) == null) {
						final String elem;
						try {
							elem = RowMapper.normalizeExtendedSymbol(
									"_" + matcher.group(1), true).substring(1);
						}
						catch (GSException e) {
							throw new Error(e);
						}
						patternBuilder.append(Pattern.quote(elem));
					}
					else {
						patternBuilder.append(".*");
					}
				}
				patternBuilder.append("$");

				final Pattern compiledSubPattern =
						Pattern.compile(patternBuilder.toString());

				entryList.add(new Entry(compiledSubPattern, affinity));
			}
		}

		public String match(String containerName, String defaultAffinity)
				throws GSException {
			if (!entryList.isEmpty()) {
				final String normalized =
						RowMapper.normalizeExtendedSymbol(containerName, true);

				for (Entry entry : entryList) {
					if (entry.pattern.matcher(normalized).find()) {
						return entry.affinity;
					}
				}
			}

			return defaultAffinity;
		}
	}

	public GridStoreChannel(Config config, Source source)
			throws GSException {
		this.pool = new NodeConnectionPool();
		this.key = source.key;
		this.nodeResolver = new NodeResolver(
				pool, key.passive, key.address,
				config.getConnectionConfig(), source.getLoginInfo(),
				source.partitionCount, key.sarConfig, key.memberList,
				null);
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

	public void apply(Source source) {
		nodeResolver.setUser(source.getLoginInfo().getUser());
		nodeResolver.setPassword(source.password);
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
		if (context.failoverTimeoutMillis >= 0) {
			return context.failoverTimeoutMillis;
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
				throw new GSRecoverableException(
						GSErrorCode.RECOVERABLE_CONNECTION_PROBLEM, "");
			}
		}
	}

	private static boolean isConnectionDependentStatement(
			Statement statement) {
		return (statement == Statement.CLOSE_ROW_SET ||
				statement == Statement.FETCH_ROW_SET);
	}

	public void executeStatement(Context context, Statement statement,
			int partitionId, long statementId,
			BasicBuffer req, BasicBuffer resp)
			throws GSException {
		executeStatement(
				context, statement, partitionId, statementId, req, resp, null);
	}

	public void executeStatement(Context context, Statement statement,
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
			int failoverTrialCount = 0;
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
							final SocketAddress address =
									context.lastConnection.getRemoteSocketAddress();
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
						throw new GSRecoverableException(
								GSErrorCode.RECOVERABLE_CONNECTION_PROBLEM, e);
					}

					
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
							continue;
						}
					}

					final long curTime = System.currentTimeMillis();
					if (failoverStartTime == 0) {
						failoverStartTime = curTime;
					}

					final long trialDuration = curTime - failoverStartTime;
					final long failoverTimeout = getFailoverTimeoutMillis(context);
					if (trialDuration >= failoverTimeout) {
						
						final String errorReason =
								(e.getMessage() == null ? "" : e.getMessage());
						final String errorMessage = (failoverTimeout > 0 ?
								("Failover timed out (trialCount=" +
								failoverTrialCount +
								", trialMillis=" + trialDuration +
								", timeoutMillis=" + failoverTimeout +
								", reason=" + errorReason + ")") :
								(firstTrial ? null : "Retry failed (reason=" +
										errorReason + ")"));

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
								trialDuration,
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

		if (context.partitionCount < 0) {
			context.partitionCount = nodeResolver.getPartitionCount();
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
					partitionId, !context.loginInfo.isOwnerMode(),
					context.partitionCount,
					context.preferableHosts.get(partitionId));
			if (partitionId >= 0 && partitionId < context.partitionCount) {
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

		final NodeConnection newConnection = pool.resolve(
				address, req, resp,
				config.getConnectionConfig(), context.loginInfo,
				preferConnectionPool);
		boolean updated = false;
		try {
			context.activeConnections.put(address, newConnection);
			context.lastConnection = newConnection;
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
			context.containerHashMode = null;
			context.partitionCount = -1;
			context.addressCache.clear();
			nodeResolver.invalidateMaster();
		}
	}

	public void checkContextAvailable(Context context) throws GSException {
		synchronized (context) {
			if (context.closed) {
				throw new GSException(GSErrorCode.RESOURCE_CLOSED, "");
			}
		}
	}

	public InetSocketAddress[] getNodeAddressList(
			Context context, final int partitionId) throws GSException {
		synchronized (context) {
			return executeResolver(
					context, new ResolverExecutor<InetSocketAddress[]>() {
				@Override
				public void execute() throws GSException {
					result = nodeResolver.getNodeAddressList(partitionId);
				}
			});
		}
	}

	public int getPartitionCount(Context context) throws GSException {
		synchronized (context) {
			if (context.partitionCount < 0) {
				context.partitionCount = executeResolver(
						context, new ResolverExecutor<Integer>() {
					@Override
					public void execute() throws GSException {
						result = nodeResolver.getPartitionCount();
					}
				});
			}

			return context.partitionCount;
		}
	}

	public int resolvePartitionId(
			final Context context, String containerName) throws GSException {
		synchronized (context) {
			if (context.partitionCount < 0 ||
					context.containerHashMode == null) {
				executeResolver(context, new ResolverExecutor<Void>() {
					@Override
					public void execute() throws GSException {
						context.partitionCount =
								nodeResolver.getPartitionCount();
						context.containerHashMode =
								nodeResolver.getContainerHashMode();
					}
				});
			}

			if (containerName.equals(SubnetGridStore.SYSTEM_USER_CONTAINER_NAME)) {

				
				return SYSTEM_CONTAINER_PARTITION_ID;

			}

			int subContainerId = -1;
			String base = null;
			String affinity = null;
			int partitionId = -1;
			int partitioningCount = 0;
			boolean newPartitioningRule = false;

			int affinityPos = containerName.indexOf('@');
			int subPartitionIdPos = containerName.indexOf('/');
			if (subPartitionIdPos == -1) {
				if (affinityPos == -1) {
					base = RowMapper.normalizeExtendedSymbol(containerName, true);
					return calculatePartitionId(base,
						context.containerHashMode, context.partitionCount);
				}
				subPartitionIdPos = containerName.length();
			} else if (subPartitionIdPos == 0) {
				throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
						"Illegal container name (" + containerName + ")");
			} else {

				int partitioningPos = containerName.indexOf('_', subPartitionIdPos + 1);
				if (partitioningPos != -1) {
					newPartitioningRule = true;
				}
				else {
					partitioningPos = containerName.length();
				}

				String subPartitionId = containerName.substring(subPartitionIdPos + 1, partitioningPos);

				if (subPartitionId.isEmpty()) {
					throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
							"Illegal container name (" + containerName + ")");
				}

				final Matcher m1 = NUMERICAL_REGEX_PATTERN.matcher(subPartitionId);
				if (m1.find()) {
					subContainerId = Integer.parseInt(subPartitionId);
				} else {
					throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
							"Illegal container name (, " + containerName + ")");
				}


				if (newPartitioningRule) {
					String partitioningCountStr = containerName.substring(partitioningPos + 1,
							containerName.length());

					final Matcher m2 = NUMERICAL_REGEX_PATTERN.matcher(partitioningCountStr);
					if (m2.find()) {
						partitioningCount = Integer.parseInt(partitioningCountStr);
					} else {
						throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
								"Illegal container name (, " + containerName + ")");
					}
				}
			}

			
			if (affinityPos == 0) {
				throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
						"Illegal container name (" + containerName + ")");
			} else if (affinityPos == -1) {
				
				base = containerName.substring(0, subPartitionIdPos);
				base = RowMapper.normalizeExtendedSymbol(base, true);
			} else {

				if (affinityPos > subPartitionIdPos || affinityPos + 1 == subPartitionIdPos) {
					throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
							"Illegal container name (" + containerName + ")");
				}

				affinity = containerName.substring(affinityPos + 1, subPartitionIdPos);
				if (affinity.isEmpty()) {
					throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
							"Illegal container name (" + containerName + ")");
				}

				final Matcher m1 = NUMERICAL_REGEX_PATTERN.matcher(affinity);
				if (m1.find()) {
					partitionId = Integer.parseInt(affinity);
					if (subContainerId != -1) {
						partitionId += subContainerId;
					}
				} else {
					affinity = RowMapper.normalizeSymbol("_" + affinity);
					affinity = affinity.substring(1);
					base = affinity;
				}
			}
			if (partitionId != -1) {
				partitionId %= context.partitionCount;
			} else if (affinity != null && !newPartitioningRule) {

				
				partitionId = calculatePartitionId(affinity,
					context.containerHashMode, context.partitionCount);
			} else {
				if (!newPartitioningRule) {
					partitionId = calculatePartitionId(base,
						context.containerHashMode, context.partitionCount);
					if (subContainerId != -1) {
						partitionId = (int) ((partitionId + subContainerId)) % context.partitionCount;
					}
				}
				else {
					if (context.partitionCount <= partitioningCount) {
						return subContainerId % context.partitionCount;
					}
					int pbase = (context.partitionCount / partitioningCount);
					int pmod = (context.partitionCount % partitioningCount);
					final CRC32 crc = new CRC32();
					crc.update(base.getBytes(BasicBuffer.DEFAULT_CHARSET));
					partitionId = (pbase * subContainerId
							+ (int)Math.min(pmod, subContainerId) + ((int)((crc.getValue() & 0xffffffff) % pbase)));
				}
			}

			return partitionId;
		}
	}

	public static int calculatePartitionId(
			String normalizedString, ContainerHashMode hashMode, int partitionCount)
			throws GSException {
		switch (hashMode) {
		case CRC32: {
			final CRC32 crc = new CRC32();
			crc.update(normalizedString.getBytes(BasicBuffer.DEFAULT_CHARSET));

			return (int) ((crc.getValue() & 0xffffffff) % partitionCount);
		}
		default:
			throw new GSException(
					GSErrorCode.INTERNAL_ERROR, "Unsupported hash mode");
		}
	}

	public static int statementToNumber(Statement statement) {
		return NodeConnection.statementToNumber(statement);
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

	static class ContextMonitor {

		String containerName;

		String query;

		long statementStartTime;

		long statementIOStartTime;

		long statementIOTotalTime;

		long sessionStartTime;

		long transactionStartTime;

		boolean containerIdSpecified;

		boolean sessionSpecified;

		boolean transactionStarted;

		Statement lastStatement;

		long lastStatementId;

		int lastPartitionId;

		long lastContainerId;

		long lastSessionId;

		void setContainerName(String containerName) {
			this.containerName = containerName;
		}

		void setQuery(String query) {
			this.query = query;
		}

		void startStatement(Statement statement, long statementId,
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
				if (containerName != null) {
					args.add(containerName);
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
