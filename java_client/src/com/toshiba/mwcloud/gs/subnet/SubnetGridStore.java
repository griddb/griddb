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

import java.lang.ref.WeakReference;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Blob;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.ColumnInfo;
import com.toshiba.mwcloud.gs.CompressionMethod;
import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.ContainerType;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GSType;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.IndexType;
import com.toshiba.mwcloud.gs.PartitionController;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowKeyPredicate;
import com.toshiba.mwcloud.gs.TimeSeries;
import com.toshiba.mwcloud.gs.TimeSeriesProperties;
import com.toshiba.mwcloud.gs.TimeUnit;
import com.toshiba.mwcloud.gs.TriggerInfo;
import com.toshiba.mwcloud.gs.common.BasicBuffer;
import com.toshiba.mwcloud.gs.common.BlobImpl;
import com.toshiba.mwcloud.gs.common.GSConnectionException;
import com.toshiba.mwcloud.gs.common.GSErrorCode;
import com.toshiba.mwcloud.gs.common.GSStatementException;
import com.toshiba.mwcloud.gs.common.LoggingUtils;
import com.toshiba.mwcloud.gs.common.LoggingUtils.BaseGridStoreLogger;
import com.toshiba.mwcloud.gs.common.RowMapper;
import com.toshiba.mwcloud.gs.common.RowMapper.BlobFactory;
import com.toshiba.mwcloud.gs.common.RowMapper.Cursor;
import com.toshiba.mwcloud.gs.common.Statement;
import com.toshiba.mwcloud.gs.experimental.ContainerAttribute;
import com.toshiba.mwcloud.gs.experimental.ContainerCondition;
import com.toshiba.mwcloud.gs.experimental.DatabaseInfo;
import com.toshiba.mwcloud.gs.experimental.ExtendedContainerInfo;
import com.toshiba.mwcloud.gs.experimental.PrivilegeInfo;
import com.toshiba.mwcloud.gs.experimental.UserInfo;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.ContainerCache;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.Context;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.ContextMonitor;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.ContextReference;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.LocatedSchema;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.RemoteReference;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.OptionalRequest;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.OptionalRequestType;
import com.toshiba.mwcloud.gs.subnet.SubnetContainer.SessionMode;
import com.toshiba.mwcloud.gs.subnet.SubnetContainer.TransactionMode;

public class SubnetGridStore implements GridStore {

	private static final boolean ENABLE_COMPRESSION_WINDOW_SIZE = true;

	private static boolean OLD_TIME_SERIES_PRORERTIES = false;

	private static final BlobFactory BLOB_FACTORY = new BlobFactory() {

		@Override
		public Blob createBlob(byte[] data) throws GSException {
			final BlobImpl blob = new BlobImpl(null);
			blob.setDataDirect(data);
			return blob;
		}

	};

	private static final BaseGridStoreLogger LOGGER =
			LoggingUtils.getLogger("Transaction");

	private static boolean pathKeyOperationEnabled = false;

	static final String SYSTEM_USER_CONTAINER_NAME = "gs#users@0";

	private static final byte DEFAULT_DBUSER_PROPERTY = (byte)0;

	private static final String DEFAULT_PRIVILEGE = "ALL";

	private static final int USER_INFO_COUNT_LIMIT = 128;	

	private static final int DATABASE_INFO_COUNT_LIMIT = 128;	

	private static final int USER_PASSWORD_LENGTH_LIMIT = 64;

	private final GridStoreChannel channel;

	private final Context context;

	private final ContextReference contextRef;

	private final NamedContainerMap<SubnetContainer<?, ?>> containerMap;

	private final ContextMonitor contextMonitor =
			GridStoreChannel.createContextMonitorIfAvailable();

	public SubnetGridStore(
			GridStoreChannel channel, Context context) throws GSException {
		this.channel = channel;
		this.context = context;

		contextRef = channel.registerContext(context, this);

		if (context.getContainerCache() != null) {
			context.addRemoteReference(new CacheReference(this, context));
		}

		containerMap = (pathKeyOperationEnabled ? makeContainerMap() : null);
	}

	private static NamedContainerMap<
	SubnetContainer<?, ?>> makeContainerMap() {
		return new NamedContainerMap<SubnetContainer<?, ?>>() {
			@Override
			protected Class<?> getRowType(SubnetContainer<?, ?> container) {
				return container.getRowMapper().getRowType();
			}
		};
	}

	static void setPathKeyOperationEnabled(boolean pathKeyOperationEnabled) {
		SubnetGridStore.pathKeyOperationEnabled = pathKeyOperationEnabled;
	}

	public GridStoreChannel getChannel() {
		return channel;
	}

	public Context getContext() {
		return context;
	}

	private void executeStatement(
			Statement statement, int partitionId,
			BasicBuffer req, BasicBuffer resp, String containerName)
			throws GSException {

		beginComplexedStatement(statement, partitionId, containerName);
		channel.executeStatement(
				context, statement, partitionId, 0, req, resp,
				contextMonitor);
		endComplexedStatement();
	}

	private void beginComplexedStatement(
			Statement statement, int partitionId, String containerName) {
		if (contextMonitor != null) {
			contextMonitor.setContainerName(containerName);
			contextMonitor.startStatement(statement, 0, partitionId, null);
		}
	}

	private void endComplexedStatement() {
		if (contextMonitor != null) {
			contextMonitor.endStatement();
		}
	}

	public static abstract class NamedContainerMap<C extends Container<?, ?>> {

		private final Map<String, List<WeakReference<C>>> base =
				new HashMap<String, List<WeakReference<C>>>();

		public synchronized C get(
				String normalizedName, Class<?> rowType) throws GSException {
			final List<WeakReference<C>> list =
					base.get(RowMapper.normalizeExtendedSymbol(normalizedName, true));
			if (list != null) {
				do {
					for (WeakReference<C> containerRef : list) {
						final C container = containerRef.get();
						if (container == null) {
							continue;
						}
						if (rowType == null ||
								rowType == getRowType(container)) {
							return container;
						}
					}
					rowType = rowType.getSuperclass();
				}
				while (rowType != null);
			}
			return null;
		}

		public synchronized void add(String normalizedName, C container) {
			List<WeakReference<C>> list = base.get(normalizedName);
			if (list == null) {
				list = new LinkedList<WeakReference<C>>();
				base.put(normalizedName, list);
			}
			list.add(new WeakReference<C>(container));
		}

		public synchronized void remove(String normalizedName, C container) {
			final List<WeakReference<C>> list = base.get(normalizedName);
			if (list == null) {
				return;
			}

			for (final Iterator<WeakReference<C>> it = list.iterator();
					it.hasNext();) {
				final C candidate = it.next().get();
				if (candidate == null || candidate == container) {
					it.remove();
				}
			}

			if (list.isEmpty()) {
				base.remove(normalizedName);
			}
		}

		protected abstract Class<?> getRowType(C container);
	}

	private SubnetContainer<?, ?> resolveContainer(
			String containerName, Class<?> rowType) throws GSException {
		if (containerMap == null) {
			throw new GSException(
					GSErrorCode.UNSUPPORTED_OPERATION,
					"Path key operation is restricted");
		}

		final SubnetContainer<?, ?> container =
				containerMap.get(containerName, rowType);
		if (container == null) {
			throw new GSException(
					GSErrorCode.CONTAINER_NOT_OPENED,
					"Related container not opened " +
					"(name=" + containerName + ", rowType=" +
							(rowType == null ? "" : rowType.getName()) + ")");
		}

		return duplicateContainer(container);
	}

	private <R> SubnetContainer<?, R> duplicateContainer(
			SubnetContainer<?, R> base) throws GSException {
		final RowMapper mapper = base.getRowMapper();
		if (mapper.isForTimeSeries()) {
			return new SubnetTimeSeries<R>(
					this, channel, context,
					base.getRowType(), mapper,
					base.getSchemaVersionId(),
					base.getPartitionId(),
					base.getContainerId(),
					null, null);
		}
		else {
			return new SubnetCollection<Object, R>(
					this, channel, context,
					base.getRowType(), mapper,
					base.getSchemaVersionId(),
					base.getPartitionId(),
					base.getContainerId(),
					null, null);
		}
	}

	void createReference(SubnetContainer<?, ?> container) throws GSException {
		if (containerMap == null) {
			return;
		}

		containerMap.add(container.getNormalizedContainerName(), container);
	}

	void removeReference(SubnetContainer<?, ?> container) {
		if (containerMap == null) {
			return;
		}

		containerMap.remove(container.getNormalizedContainerName(), container);
	}

	@Override
	public boolean put(String pathKey, Object rowObject) throws GSException {
		final String[] keyElements = splitPathKey(pathKey);
		final SubnetContainer<?, ?> container =
				resolveContainer(keyElements[0], rowObject.getClass());
		try {
			final RowMapper mapper = container.getRowMapper();
			final Object key = mapper.resolveKey(keyElements[1]);
			return putChecked(container, key, rowObject);
		}
		finally {
			container.close();
		}
	}

	private static <R> boolean putChecked(
			SubnetContainer<?, R> container, Object key, Object row)
					throws GSException {
		final SubnetContainer<Object, R> typedContainer =
				disguiseTypedContainer(container);
		final Class<R> rowType = typedContainer.getRowType();
		return typedContainer.put(key, rowType.cast(row));
	}

	@Override
	public Object get(String pathKey) throws GSException {
		return getByPathKey(pathKey, null);
	}

	@Override
	public <R> R get(String pathKey, Class<R> rowType) throws GSException {
		return rowType.cast(getByPathKey(pathKey, rowType));
	}

	private Object getByPathKey(
			String pathKey, Class<?> rowType) throws GSException {
		final String[] keyElements = splitPathKey(pathKey);
		final SubnetContainer<Object, ?> container =
				disguiseTypedContainer(
						resolveContainer(keyElements[0], rowType));
		try {
			final RowMapper mapper = container.getRowMapper();
			final Object key = mapper.resolveKey(keyElements[1]);
			return container.get(key);
		}
		finally {
			container.close();
		}
	}

	@Override
	public boolean remove(String pathKey) throws GSException {
		final String[] keyElements = splitPathKey(pathKey);
		final SubnetContainer<Object, ?> container =
				disguiseTypedContainer(
						resolveContainer(keyElements[0], null));
		try {
			final RowMapper mapper = container.getRowMapper();
			final Object key = mapper.resolveKey(keyElements[1]);
			return container.remove(key);
		}
		finally {
			container.close();
		}
	}

	private static String[] splitPathKey(String pathKey) throws GSException {
		final String[] components = pathKey.split("/", 2);
		if (components.length != 2) {
			throw new GSException(
					GSErrorCode.ILLEGAL_PARAMETER,
					"Invalid path key (value=" + pathKey + ")");
		}

		return components;
	}

	private static void tryPutDatabaseOptionalRequest(
			BasicBuffer req, Context context) {
		NodeConnection.tryPutEmptyOptionalRequest(req);
	}

	public static void tryPutSystemOptionalRequest(
			BasicBuffer req, Context context, boolean systemMode) {
		if (!systemMode) {
			NodeConnection.tryPutEmptyOptionalRequest(req);
			return;
		}

		final OptionalRequest optionalRequest = context.getOptionalRequest();
		optionalRequest.put(OptionalRequestType.SYSTEM_MODE, systemMode);
		optionalRequest.format(req);
	}

	
	
	
	
	
	public static int getContainerAttributeFlag(ContainerAttribute attribute) throws GSException {
		switch(attribute) {
		case BASE:
			return 0x00000000;
		case BASE_SYSTEM:
			return 0x00000001;
		case SINGLE:
			return 0x00000010;
		case SINGLE_SYSTEM:
			return 0x00000011;
		case SINGLE_SEMI_PERMANENT_SYSTEM:
			return 0x00000015;
		case LARGE:
			return 0x00000020;
		case SUB:
			return 0x00000030;
		default:
			throw new InternalError();
		}
	}

	public static ContainerAttribute getContainerAttribute(int flag) throws GSException {
		for (ContainerAttribute elem : ContainerAttribute.values()) {
			if (getContainerAttributeFlag(elem) == flag) {
				return elem;
			}
		}
		return null;
	}

	private static void tryPutAttributeOptionalRequest(
			BasicBuffer req, Context context, boolean systemMode,
			ContainerInfo containerInfo) throws GSException {
		final ContainerAttribute attribute;
		if (containerInfo instanceof ExtendedContainerInfo) {
			attribute = ((ExtendedContainerInfo) containerInfo).getAttribute();
		}
		else {
			attribute = null;
		}

		if (!systemMode && attribute == null) {
			NodeConnection.tryPutEmptyOptionalRequest(req);
			return;
		}

		final OptionalRequest optionalRequest = context.getOptionalRequest();
		if (systemMode) {
			optionalRequest.put(OptionalRequestType.SYSTEM_MODE, systemMode);
		}
		if (attribute != null) {
			optionalRequest.put(
					OptionalRequestType.CONTAINER_ATTRIBUTE, getContainerAttributeFlag(attribute));
		}
		optionalRequest.format(req);
	}

	@Override
	public ContainerInfo getContainerInfo(String name) throws GSException {
		return getExtendedContainerInfo(name, false);
	}

	public ExtendedContainerInfo getExtendedContainerInfo(
			String name, boolean systemMode) throws GSException {
		return getExtendedContainerInfo(
				name, EnumSet.allOf(ContainerPropertyType.class), null,
				systemMode);
	}

	private ExtendedContainerInfo getExtendedContainerInfo(
			String name, EnumSet<ContainerPropertyType> typeSet,
			ContainerIdInfo[] containerIdInfoResult, boolean systemMode)
			throws GSException {
		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);

		tryPutSystemOptionalRequest(req, context, systemMode);

		final int partitionId = channel.resolvePartitionId(context, name);
		req.putString(name);

		req.putInt(typeSet.size());

		if (NodeConnection.getProtocolVersion() < 2 ||
				GridStoreChannel.v1ProtocolCompatible_1_1_103x) {
			typeSet = typeSet.clone();
			typeSet.remove(ContainerPropertyType.TRIGGER);
		}

		for (ContainerPropertyType type : typeSet) {
			req.putByteEnum(type);
		}

		executeStatement(
				Statement.GET_CONTAINER_PROPERTIES,
				partitionId, req, resp, name);

		final boolean found = resp.getBoolean();
		if (!found) {
			return null;
		}

		final int propertyCount = resp.base().getInt();
		if (propertyCount != typeSet.size()) {
			throw new GSConnectionException(GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by illegal property count (" +
					"expected=" + typeSet.size() +
					", actual=" + propertyCount + ")");
		}

		ContainerIdInfo containerIdInfo = null;

		final Boolean[] keyAssigned = new Boolean[1];
		final String[] dataAffinity = new String[1];
		final ContainerType[] containerType = new ContainerType[1];
		final List<String> columnNameList = new ArrayList<String>();
		final List<GSType> columnTypeList = new ArrayList<GSType>();
		TimeSeriesProperties tsProps = null;
		ContainerAttribute containerAttribute = null;

		final List<Set<IndexType>> indexTypesList =
				new ArrayList<Set<IndexType>>();
		final List<URL> eventNotificationList =
				new ArrayList<URL>();
		final List<TriggerInfo> triggerList =
				new ArrayList<TriggerInfo>();

		for (int i = 0; i < propertyCount; i++) {
			final ContainerPropertyType propertyType =
					resp.getByteEnum(ContainerPropertyType.class);

			final int propertySize = resp.base().getInt();
			if (propertySize < 0 ||
					propertySize > resp.base().remaining()) {
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by illegal property size");
			}

			final int curEnd = resp.base().position() + propertySize;

			switch (propertyType) {
			case ID:
				containerIdInfo = importIdProperty(resp);
				if (containerIdInfoResult != null) {
					containerIdInfoResult[0] = containerIdInfo;
				}
				break;
			case SCHEMA:
				importSchemaProperty(resp, columnNameList, columnTypeList,
						containerType, keyAssigned);
				tsProps = importContainerProperties(
						resp, columnNameList, dataAffinity, containerType[0]);
				break;
			case INDEX:
				importIndexProperty(resp, indexTypesList, columnNameList.size());
				break;
			case EVENT_NOTIFICATION:
				
				importEventNotificationProperty(resp, eventNotificationList);
				break;
			case TRIGGER:
				importTriggerProperty(resp, columnNameList, triggerList);
				break;
			case ATTRIBUTE:
				containerAttribute = getContainerAttribute(resp.base().getInt());
				break;
			default:
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by illegal property type");
			}

			if (resp.base().position() != curEnd) {
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by illegal property format");
			}
		}

		final List<ColumnInfo> columnInfoList = new ArrayList<ColumnInfo>();
		for (int i = 0; i < columnNameList.size(); i++) {
			columnInfoList.add(new ColumnInfo(
					columnNameList.get(i),
					columnTypeList.get(i),
					indexTypesList.get(i)));
		}

		final ExtendedContainerInfo containerInfo = new ExtendedContainerInfo();
		containerInfo.setName(containerIdInfo.remoteName);
		containerInfo.setType(containerType[0]);
		containerInfo.setColumnInfoList(columnInfoList);
		containerInfo.setRowKeyAssigned(keyAssigned[0]);
		containerInfo.setAttribute(containerAttribute);
		containerInfo.setTimeSeriesProperties(tsProps);
		containerInfo.setTriggerInfoList(triggerList);
		containerInfo.setDataAffinity(dataAffinity[0]);

		return containerInfo;
	}

	@Override
	public <K, R> SubnetCollection<K, R> putCollection(
			String name, Class<R> rowType)
			throws GSException {
		return putCollection(name, rowType, false);
	}

	public static String normalizeFullContainerName(String name) throws GSException {
		return RowMapper.normalizeExtendedSymbol(name, true);
	}

	private <K, R> SubnetContainer<K, R> findContainerByCache(
			ContainerCache cache, String name, Class<R> rowType,
			ContainerType containerType) throws GSException {
		final String normalizedName = normalizeFullContainerName(name);
		final LocatedSchema schema =
				cache.findSchema(normalizedName, rowType, containerType);

		if (schema == null) {
			return null;
		}

		final int partitionId =
				channel.resolvePartitionId(context, normalizedName);

		if (containerType == ContainerType.COLLECTION) {
			return new SubnetCollection<K, R>(
					this, channel, context, rowType,
					schema.getMapper(), schema.getVersionId(),
					partitionId, schema.getContainerId(), normalizedName,
					schema.getContainerName());
		}
		else {
			return disguiseTypedContainer(new SubnetTimeSeries<R>(
					this, channel, context, rowType,
					schema.getMapper(), schema.getVersionId(),
					partitionId, schema.getContainerId(), normalizedName,
					schema.getContainerName()));
		}
	}

	public static boolean isContainerStatementUnified() {
		return NodeConnection.getProtocolVersion() >= 3;
	}

	public static boolean isSessionUUIDSummarized() {
		return NodeConnection.getProtocolVersion() >= 3;
	}

	public static boolean isTSDivisionAndAffinityEnabled() {
		return NodeConnection.getProtocolVersion() >= 3 &&
				!GridStoreChannel.v15TSPropsCompatible;
	}

	private static Statement getContainerStatement(
			Statement statement, ContainerType containerType) {
		if (isContainerStatementUnified()) {
			return statement;
		}
		else if (containerType == ContainerType.TIME_SERIES) {
			switch (statement) {
			case PUT_CONTAINER:
				return Statement.PUT_TIME_SERIES;
			case GET_CONTAINER:
				return Statement.GET_TIME_SERIES;
			case DROP_CONTAINER:
				return Statement.DROP_TIME_SERIES;
			default:
				throw new Error(
						"Internal error by invalid container statement (" +
						"statement=" + statement + ")");
			}
		}
		else if (containerType == ContainerType.COLLECTION) {
			if (statement == Statement.DROP_CONTAINER) {
				return Statement.DROP_COLLECTION;
			}
			else {
				return statement;
			}
		}
		else {
			return statement;
		}
	}

	private static void tryPutContainerType(
			BasicBuffer req, ContainerType containerType) {
		if (!isContainerStatementUnified()) {
			return;
		}

		if (containerType == null) {
			req.put((byte) 0xff);
		}
		else {
			req.putByteEnum(containerType);
		}
	}

	private <K, R> SubnetContainer<K, R> putContainer(
			String name, ContainerType containerType, Class<R> rowType,
			ContainerInfo info, boolean modifiable)
			throws GSException {

		final ContainerCache cache = context.getContainerCache();
		if (cache != null && !modifiable && info == null) {
			final Container<K, R> cachedContainer = findContainerByCache(
					cache, name, rowType, containerType);
			if (cachedContainer != null) {
				return (SubnetContainer<K, R>) cachedContainer;
			}
		}

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		tryPutSystemOptionalRequest(req, context, false);

		final boolean forTimeSeries =
				(containerType == ContainerType.TIME_SERIES);

		final RowMapper orgMapper =
				RowMapper.getInstance(rowType, forTimeSeries);
		final int partitionId = channel.resolvePartitionId(context, name);
		req.putString(name);
		tryPutContainerType(req, containerType);
		req.putBoolean(modifiable);
		orgMapper.exportSchema(req);

		if (info != null && (info.getColumnCount() > 0 ||
				info.isRowKeyAssigned() ||
				info.isColumnOrderIgnorable())) {
			throw new GSException(GSErrorCode.ILLEGAL_SCHEMA,
					"Schema can not be specified on ContainerInfo (" +
					"columnCount=" + info.getColumnCount() + ", " +
					"rowKeyAssigned=" + info.isRowKeyAssigned() + ", " +
					"columnOrderIgnorable=" +
							info.isColumnOrderIgnorable() + ")");
		}
		exportContainerProperties(req, containerType, info, orgMapper, name);

		final Statement statement = getContainerStatement(
				Statement.PUT_CONTAINER, containerType);

		executeStatement(statement, partitionId, req, resp, name);

		final int schemaVerId = resp.base().getInt();
		final long containerId = resp.base().getLong();

		final String[] acceptedNames = acceptRemoteContainerName(resp, name);
		final String normalizedName = acceptedNames[0];
		final String remoteName = acceptedNames[1];

		final RowMapper mapper = orgMapper.reorderBySchema(resp, true);

		if (cache != null) {
			cache.cacheSchema(normalizedName, new LocatedSchema(
					mapper, containerId, schemaVerId, remoteName));
		}

		if (forTimeSeries) {
			return disguiseTypedContainer(new SubnetTimeSeries<R>(
					this, channel, context, rowType, mapper, schemaVerId,
					partitionId, containerId, normalizedName, remoteName));
		}
		else {
			return new SubnetCollection<K, R>(
					this, channel, context, rowType, mapper, schemaVerId,
					partitionId, containerId, normalizedName, remoteName);
		}
	}

	@Override
	public <K, R> SubnetCollection<K, R> putCollection(
			String name, Class<R> rowType,
			boolean modifiable) throws GSException {
		final SubnetContainer<K, R> container = putContainer(
				name, ContainerType.COLLECTION, rowType, null, modifiable);
		return (SubnetCollection<K, R>) container;
	}

	@Override
	public <R> SubnetTimeSeries<R> putTimeSeries(
			String name, Class<R> rowType)
			throws GSException {
		final SubnetContainer<Date, R> container = putContainer(
				name, ContainerType.TIME_SERIES, rowType, null, false);
		return (SubnetTimeSeries<R>) container;
	}

	@Override
	public <R> SubnetTimeSeries<R> putTimeSeries(
			String name, Class<R> rowType,
			TimeSeriesProperties props, boolean modifiable) throws GSException {
		final ContainerInfo info = new ContainerInfo();
		info.setTimeSeriesProperties(props);

		final SubnetContainer<Date, R> container = putContainer(
				name, ContainerType.TIME_SERIES, rowType, info, modifiable);
		return (SubnetTimeSeries<R>) container;
	}

	private String[] acceptRemoteContainerName(
			BasicBuffer in, String localName) throws GSException {
		final String remoteName = in.getString();
		return filterRemoteContainerName(remoteName, localName);
	}

	private String[] filterRemoteContainerName(
			String remoteName, String localName) throws GSException {

		final String normalizedLocalName =
				RowMapper.normalizeExtendedSymbol(localName, true);
		if (!remoteName.equals(localName) &&
				!RowMapper.normalizeExtendedSymbol(
						remoteName, true).equals(normalizedLocalName)) {
			throw new GSConnectionException(
					GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by inconsistent container name (" +
					"localName=" + localName +
					", remoteName=" + remoteName + ")");
		}

		return new String[] {
				(!pathKeyOperationEnabled &&
						context.getContainerCache() == null ?
						null : normalizedLocalName),
				(contextMonitor == null ? null : remoteName)
		};
	}

	private void exportContainerProperties(
			BasicBuffer out, ContainerType type, ContainerInfo info,
			RowMapper mapper, String containerName) throws GSException {

		if (isTSDivisionAndAffinityEnabled() &&
				!GridStoreChannel.v20AffinityCompatible) {
			final String dataAffinity =
					(info == null ? null : info.getDataAffinity());
			out.putString(dataAffinity == null ?
					context.getDataAffinityPattern().match(containerName, "") :
					dataAffinity);
		}

		final TimeSeriesProperties tsProps =
				(info == null ? null : info.getTimeSeriesProperties());

		if (type != ContainerType.TIME_SERIES) {
			if (tsProps != null) {
				throw new GSException(GSErrorCode.ILLEGAL_SCHEMA,
						"TimeSeriesProperties used except for TimeSeries (" +
						"containerType=" + type + ")");
			}
			return;
		}

		if (OLD_TIME_SERIES_PRORERTIES) {
			out.putInt(-1);		
			out.putByteEnum(TimeUnit.DAY);		
			out.putInt(-1);		
			out.putByteEnum(TimeUnit.DAY);		
			out.putInt(0);		
			return;
		}

		if (tsProps == null) {
			out.putBoolean(false);
		}
		else {
			out.putBoolean(true);

			{
				out.putInt(tsProps.getRowExpirationTime());
				final TimeUnit timeUnit = tsProps.getRowExpirationTimeUnit();
				if (timeUnit == null) {
					out.putByteEnum(TimeUnit.DAY);
				}
				else {
					out.putByteEnum(timeUnit);
				}
			}

			if (isTSDivisionAndAffinityEnabled()) {
				out.putInt(tsProps.getExpirationDivisionCount());

				if (GridStoreChannel.v20AffinityCompatible) {
					out.putString("");
				}
			}

			if (ENABLE_COMPRESSION_WINDOW_SIZE) {
				out.putInt(tsProps.getCompressionWindowSize());
				final TimeUnit timeUnit = tsProps.getCompressionWindowSizeUnit();
				if (timeUnit == null) {
					out.putByteEnum(TimeUnit.DAY);
				}
				else {
					out.putByteEnum(timeUnit);
				}
			}

			out.putByteEnum(tsProps.getCompressionMethod());

			final Set<String> columnNameSet = tsProps.getSpecifiedColumns();
			out.putInt(columnNameSet.size());
			for (String columnName : columnNameSet) {
				
				out.putInt(mapper.resolveColumnId(columnName));

				final boolean relative = tsProps.isCompressionRelative(columnName);
				out.putBoolean(relative);

				if (relative) {
					out.putDouble(tsProps.getCompressionRate(columnName));
					out.putDouble(tsProps.getCompressionSpan(columnName));
				}
				else {
					out.putDouble(tsProps.getCompressionWidth(columnName));
				}
			}
		}
	}

	private static TimeSeriesProperties importContainerProperties(
			BasicBuffer in, List<String> columnNameList,
			String[] dataAffinity, ContainerType containerType)
			throws GSException {

		if (isTSDivisionAndAffinityEnabled() &&
				!GridStoreChannel.v20AffinityCompatible) {
			dataAffinity[0] = in.getString();
			if (dataAffinity[0].isEmpty()) {
				dataAffinity[0] = null;
			}
		}

		if (containerType != ContainerType.TIME_SERIES) {
			return null;
		}

		final boolean exists = in.getBoolean();
		if (!exists) {
			return null;
		}

		final TimeSeriesProperties props = new TimeSeriesProperties();
		{
			final int rowExpirationTime = in.base().getInt();
			final TimeUnit timeUnit = in.getByteEnum(TimeUnit.class);

			if (rowExpirationTime >= 0) {
				props.setRowExpiration(rowExpirationTime, timeUnit);
			}
		}

		if (isTSDivisionAndAffinityEnabled()) {
			final int count = in.base().getInt();
			if (count > 0) {
				props.setExpirationDivisionCount(count);
			}

			if (GridStoreChannel.v20AffinityCompatible) {
				in.getString();
			}
		}

		if (ENABLE_COMPRESSION_WINDOW_SIZE) {
			final int compressionWindowSize = in.base().getInt();
			final TimeUnit timeUnit = in.getByteEnum(TimeUnit.class);


			if (compressionWindowSize >= 0) {
				props.setCompressionWindowSize(compressionWindowSize, timeUnit);
			}
		}

		final CompressionMethod compressionMethod =
				in.getByteEnum(CompressionMethod.class);
		props.setCompressionMethod(compressionMethod);

		final int entryCount = in.base().getInt();

		if (entryCount > 0 && compressionMethod != CompressionMethod.HI) {
			throw new GSException(GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by unexpected compression entry (" +
					"entryCount=" + entryCount + ", " +
					"compressionMethod=" + compressionMethod + ")");
		}

		if (entryCount < 0) {
			throw new GSException(GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by negative entry count (" +
					"entryCount=" + entryCount + ")");
		}

		for (int i = 0; i < entryCount; i++) {
			final int columnId = in.base().getInt();
			if (columnId < 0 || columnId >= columnNameList.size()) {
				throw new GSException(GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by illegal column ID (" +
						"columnId=" + columnId + ", " +
						"columnCount=" + columnNameList.size() + ")");
			}
			final String columnName = columnNameList.get(columnId);

			final boolean relative = in.getBoolean();
			if (relative) {
				final double rate = in.base().getDouble();
				final double span = in.base().getDouble();
				props.setRelativeHiCompression(columnName, rate, span);
			}
			else {
				final double width = in.base().getDouble();
				props.setAbsoluteHiCompression(columnName, width);
			}
		}

		return props;
	}

	private <K, R> SubnetContainer<K, R> getContainer(
			String name, ContainerType containerType, Class<R> rowType,
			boolean systemMode) throws GSException {
		final ContainerCache cache = context.getContainerCache();
		if (cache != null && !systemMode) {
			final Container<K, R> cachedContainer = findContainerByCache(
					cache, name, rowType, containerType);
			if (cachedContainer != null) {
				return (SubnetContainer<K, R>) cachedContainer;
			}
		}

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		tryPutSystemOptionalRequest(req, context, systemMode);

		final boolean forTimeSeries =
				(containerType == ContainerType.TIME_SERIES);

		final RowMapper orgMapper =
				RowMapper.getInstance(rowType, forTimeSeries);
		final int partitionId = channel.resolvePartitionId(context, name);
		req.putString(name);
		tryPutContainerType(req, containerType);

		final Statement statement = getContainerStatement(
				Statement.GET_CONTAINER, containerType);

		executeStatement(statement, partitionId, req, resp, name);

		final boolean found = resp.getBoolean();
		if (!found) {
			return null;
		}

		final int schemaVerId = resp.base().getInt();
		final long containerId = resp.base().getLong();

		final String[] acceptedNames = acceptRemoteContainerName(resp, name);
		final String normalizedName = acceptedNames[0];
		final String remoteName = acceptedNames[1];

		final RowMapper mapper = orgMapper.reorderBySchema(resp, true);

		if (cache != null && !systemMode) {
			cache.cacheSchema(normalizedName, new LocatedSchema(
					mapper, containerId, schemaVerId, remoteName));
		}

		if (forTimeSeries) {
			return disguiseTypedContainer(new SubnetTimeSeries<R>(
					this, channel, context, rowType, mapper, schemaVerId,
					partitionId, containerId, normalizedName, remoteName));
		}
		else {
			return new SubnetCollection<K, R>(
					this, channel, context, rowType, mapper, schemaVerId,
					partitionId, containerId, normalizedName, remoteName);
		}
	}

	@Override
	public <K, R> SubnetCollection<K, R> getCollection(
			String name, Class<R> rowType) throws GSException {
		final SubnetContainer<K, R> container =
				getContainer(name, ContainerType.COLLECTION, rowType, false);
		return (SubnetCollection<K, R>) container;
	}

	@Override
	public <R> SubnetTimeSeries<R> getTimeSeries(
			String name, Class<R> rowType) throws GSException {
		final SubnetContainer<Date, R> container =
				getContainer(name, ContainerType.TIME_SERIES, rowType, false);
		return (SubnetTimeSeries<R>) container;
	}

	@Override
	public void dropCollection(String name) throws GSException {
		dropContainer(name, ContainerType.COLLECTION, false);
	}

	@Override
	public void dropTimeSeries(String name) throws GSException {
		dropContainer(name, ContainerType.TIME_SERIES, false);
	}

	@Override
	public void close() throws GSException {
		try {
			channel.closeContext(context);
		}
		finally {
			channel.unregisterContext(contextRef);
		}
	}

	private static class CacheReference
	extends RemoteReference<SubnetGridStore> {

		public CacheReference(SubnetGridStore target, Context context) {
			super(target, SubnetGridStore.class, context, 0, 0);
		}

		@Override
		public void close(GridStoreChannel channel, Context context)
				throws GSException {
			synchronized (context) {
				final ContainerCache cache = context.getContainerCache();
				if (cache != null) {
					final BasicBuffer req =
							context.getSynchronizedRequestBuffer();
					final BasicBuffer resp =
							context.getSynchronizedResponseBuffer();

					SubnetContainer.closeAllSessions(
							channel, context, req, resp,
							cache.takeAllSessions(context));
				}
			}
		}

	}

	

	private enum ContainerPropertyType {

		ID,

		SCHEMA,

		INDEX,

		EVENT_NOTIFICATION,

		TRIGGER,

		ATTRIBUTE,

	}

	private static class ContainerIdInfo {

		final int versionId;

		final long containerId;

		final String remoteName;

		public ContainerIdInfo(
				int versionId, long containerId, String remoteName) {
			this.versionId = versionId;
			this.containerId = containerId;
			this.remoteName = remoteName;
		}

	}

	private static ContainerIdInfo importIdProperty(BasicBuffer in) throws GSException {
		final int versionId = in.base().getInt();
		final long containerId = in.base().getLong();
		final String remoteName = in.getString();

		return new ContainerIdInfo(versionId, containerId, remoteName);
	}

	private static void importSchemaProperty(
			BasicBuffer in, List<String> columnNameList,
			List<GSType> columnTypeList,
			ContainerType[] containerType, Boolean[] keyAssigned)
			throws GSException {
		columnNameList.clear();
		columnTypeList.clear();

		containerType[0] = in.getByteEnum(ContainerType.class);

		final int columnCount = in.base().getInt();
		if (columnCount <= 0) {
			throw new GSConnectionException(GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by non positive column count (" +
					"columnCount=" + columnCount + ")");
		}

		final int keyIndex = in.base().getInt();
		if (!(keyIndex == 0 || keyIndex == -1)) {
			throw new GSConnectionException(GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by illegal row key index (" +
					"keyIndex=" + keyIndex + ")");
		}
		keyAssigned[0] = (keyIndex >= 0);

		for (int i = 0; i < columnCount; i++) {
			final String columnName = in.getString();
			final GSType elementType = in.getByteEnum(GSType.class);
			final boolean arrayUsed = in.getBoolean();

			columnNameList.add(columnName);
			columnTypeList.add(RowMapper.toFullType(elementType, arrayUsed));
		}
	}

	private static void importIndexProperty(
			BasicBuffer in, List<Set<IndexType>> indexTypesList,
			int columnCount) throws GSException {
		indexTypesList.clear();
		for (int i = 0; i < columnCount; i++) {
			indexTypesList.add(Collections.<IndexType>emptySet());
		}

		final int entryCount = in.base().getInt();
		if (entryCount < 0) {
			throw new GSConnectionException(GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by negative entry count (" +
					"entryCount=" + entryCount + ")");
		}

		for (int i = 0; i < entryCount; i++) {
			final int columnId = in.base().getInt();
			if (columnId < 0 || columnId >= columnCount) {
				throw new GSException(GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by illegal column ID (" +
						"entryCount=" + columnId + ", " +
						"columnCount=" + columnCount + ")");
			}

			Set<IndexType> indexTypeSet = indexTypesList.get(columnId);
			if (indexTypeSet.isEmpty()) {
				indexTypeSet = EnumSet.noneOf(IndexType.class);
				indexTypesList.set(columnId, indexTypeSet);
			}

			final IndexType indexType = in.getByteEnum(IndexType.class);
			indexTypeSet.add(indexType);
		}
	}

	private static void importEventNotificationProperty(
			BasicBuffer in, List<URL> eventNotificationInfoList)
			throws GSException {
		eventNotificationInfoList.clear();

		final int entryCount = in.base().getInt();
		if (entryCount < 0) {
			return;
		}

		for (int i = 0; i < entryCount; i++) {
			final String urlString = in.getString();
			try {
				eventNotificationInfoList.add(new URL(urlString));
			}
			catch (MalformedURLException e) {
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by malformed URL (" +
						"urlString=\"" + urlString + "\", " +
						"reason=" + e.getMessage() + ")", e);
			}
		}
	}

	private static void importTriggerProperty(
			BasicBuffer in, List<String> columnNameList,
			List<TriggerInfo> triggerInfoList)
			throws GSException {
		triggerInfoList.clear();

		final int entryCount = in.base().getInt();
		if (entryCount < 0) {
			return;
		}

		for (int i = 0; i < entryCount; i++) {
			final String name = in.getString();
			final TriggerInfo.Type type = in.getByteEnum(TriggerInfo.Type.class);
			final String uriString = in.getString();
			final int eventTypeFlag = in.base().getInt();
			final Set<TriggerInfo.EventType> eventSet = EnumSet.noneOf(TriggerInfo.EventType.class);
			final Set<TriggerInfo.EventType> allEvantSet = EnumSet.allOf(TriggerInfo.EventType.class);
			for (TriggerInfo.EventType e : allEvantSet) {
				if ((eventTypeFlag & (1 << e.ordinal())) != 0) {
					eventSet.add(e);
				}
			}
			final Set<String> columnSet = new HashSet<String>();
			final int columnCount = in.base().getInt();
			for (int j=0; j<columnCount; j++) {
				int columnId = in.base().getInt();
				if (columnId < 0 || columnId >= columnNameList.size()) {
					throw new GSException(GSErrorCode.MESSAGE_CORRUPTED,
							"Protocol error by illegal column ID (" +
							"entryCount=" + columnId + ", " +
							"columnCount=" + columnNameList.size() + ")");
				}
				final String columnName = columnNameList.get(columnId);
				columnSet.add(columnName);
			}
			in.getString();
			final String jmsDestinationType = in.getString();
			final String jmsDestinationName = in.getString();
			final String jmsUser = in.getString();
			final String jmsPassword = in.getString();

			final TriggerInfo info = new TriggerInfo();
			info.setName(name);
			info.setType(type);
			try {
				info.setURI(new URI(uriString));
			}
			catch (URISyntaxException e) {
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by malformed URI (" +
						"uriString=\"" + uriString + "\", " +
						"reason=" + e.getMessage() + ")", e);
			}
			info.setTargetEvents(eventSet);
			info.setTargetColumns(columnSet);
			info.setJMSDestinationType(jmsDestinationType);
			info.setJMSDestinationName(jmsDestinationName);
			info.setUser(jmsUser);
			info.setPassword(jmsPassword);

			triggerInfoList.add(info);
		}
	}

	@SuppressWarnings("unchecked")
	private static <K, R> SubnetContainer<K, R> disguiseTypedContainer(
			SubnetContainer<?, R> container) {
		return (SubnetContainer<K, R>) container;
	}

	private static String resolveContainerName(
			String name, ContainerInfo info) throws GSException {
		return validateName(name, info.getName());
	}

	private static String validateName(
			String name, String anotherName) throws GSException {
		final String another = (anotherName == null ? null : anotherName);

		if (name == null) {
			if (another == null) {
				throw new GSException(GSErrorCode.EMPTY_PARAMETER,
						"Name not specified");
			}
			return another;
		}
		else if (another != null) {
			if (!name.equals(another)) {
				throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
						"Inconsistent name (" +
						"specifiedName=" + name +
						", nameOnInfo=" + another + ")");
			}
		}

		return name;
	}

	private static ContainerType resolveContainerType(
			ContainerType type, ContainerInfo info) throws GSException {
		final ContainerType another = (info == null ? null : info.getType());

		if (type == null) {
			if (another == null) {
				throw new GSException(GSErrorCode.EMPTY_PARAMETER,
						"Container type not specified");
			}
			return another;
		}
		else if (another != null && another != type) {
			throw new GSException(GSErrorCode.ILLEGAL_SCHEMA,
					"Inconsistent container type (" +
					"specifiedType=" + type +
					", typeOnInfo=" + another + ")");
		}

		return type;
	}

	private <K> Container<K, Row> getContainer(
			String name, ContainerType expectedType, boolean systemMode)
			throws GSException {
		if (CONTEXT_CONTROLLER_NAME.equals(name)) {
			final Collection<K, Row> container =
					getContextControllerCollection(expectedType);
			if (container != null) {
				return container;
			}
		}

		final ContainerCache cache = context.getContainerCache();
		if (cache != null && !systemMode) {
			final Container<K, Row> cachedContainer = findContainerByCache(
					cache, name, Row.class, expectedType);
			if (cachedContainer != null) {
				return cachedContainer;
			}
		}

		final ContainerIdInfo[] idInfo = new ContainerIdInfo[1];
		final ContainerInfo containerInfo = getExtendedContainerInfo(
				name, EnumSet.allOf(ContainerPropertyType.class), idInfo,
				systemMode);

		if (containerInfo == null) {
			return null;
		}

		final ContainerType resolvedType =
				resolveContainerType(expectedType, containerInfo);

		final int partitionId = channel.resolvePartitionId(context, name);
		final RowMapper mapper =
				RowMapper.getInstance(resolvedType, containerInfo);

		final String[] acceptedNames =
				filterRemoteContainerName(containerInfo.getName(), name);
		final String normalizedName = acceptedNames[0];
		final String remoteName = acceptedNames[1];

		if (cache != null && !systemMode) {
			cache.cacheSchema(normalizedName, new LocatedSchema(
					mapper, idInfo[0].containerId, idInfo[0].versionId,
					remoteName));
		}

		if (containerInfo.getType() == ContainerType.TIME_SERIES) {
			return disguiseTypedContainer(new SubnetTimeSeries<Row>(
					this, channel, context, Row.class, mapper,
					idInfo[0].versionId, partitionId, idInfo[0].containerId,
					normalizedName, remoteName));
		}
		else {
			return disguiseTypedContainer(new SubnetCollection<Object, Row>(
					this, channel, context, Row.class, mapper,
					idInfo[0].versionId, partitionId, idInfo[0].containerId,
					normalizedName, remoteName));
		}
	}

	public <K> Container<K, Row> getContainer(String name) throws GSException {
		return getContainer(name, null, false);
	}

	public <K> Container<K, Row> getContainer(
			String name, boolean systemMode) throws GSException {
		return getContainer(name, null, systemMode);
	}

	@Override
	public <K> Collection<K, Row> getCollection(
			String name) throws GSException {
		final Container<K, Row> container =
				getContainer(name, ContainerType.COLLECTION, false);
		return (Collection<K, Row>) container;
	}

	@Override
	public TimeSeries<Row> getTimeSeries(String name) throws GSException {
		final Container<Date, Row> container =
				getContainer(name, ContainerType.TIME_SERIES, false);
		return (TimeSeries<Row>) container;
	}

	private <K> Container<K, Row> putContainer(
			String name, ContainerType containerType,
			ContainerInfo containerInfo,
			boolean modifiable, boolean systemMode) throws GSException {
		name = resolveContainerName(name, containerInfo);
		containerType = resolveContainerType(containerType, containerInfo);

		final ContainerCache cache = context.getContainerCache();
		if (cache != null && !modifiable && !systemMode &&
				containerInfo.getTimeSeriesProperties() != null) {
			final Container<K, Row> container = findContainerByCache(
					cache, name, Row.class, containerType);
			if (container != null) {
				return container;
			}
		}

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		tryPutAttributeOptionalRequest(req, context, systemMode, containerInfo);

		final RowMapper orgMapper =
				RowMapper.getInstance(containerType, containerInfo);
		final int partitionId = channel.resolvePartitionId(context, name);
		req.putString(name);
		tryPutContainerType(req, containerType);
		req.putBoolean(modifiable);
		orgMapper.exportSchema(req);

		exportContainerProperties(
				req, containerType, containerInfo, orgMapper, name);

		final Statement statement = getContainerStatement(
				Statement.PUT_CONTAINER, containerType);

		executeStatement(statement, partitionId, req, resp, name);

		final int schemaVerId = resp.base().getInt();
		final long containerId = resp.base().getLong();

		final String[] acceptedNames =
				acceptRemoteContainerName(resp, name);
		final String normalizedName = acceptedNames[0];
		final String remoteName = acceptedNames[1];

		final RowMapper mapper = orgMapper.reorderBySchema(
				resp, containerInfo.isColumnOrderIgnorable());

		if (cache != null && !systemMode) {
			cache.cacheSchema(normalizedName, new LocatedSchema(
					mapper, containerId, schemaVerId, remoteName));
		}

		if (containerType == ContainerType.TIME_SERIES) {
			return disguiseTypedContainer(new SubnetTimeSeries<Row>(
					this, channel, context, Row.class, mapper, schemaVerId,
					partitionId, containerId, normalizedName, remoteName));
		}
		else {
			return disguiseTypedContainer(new SubnetCollection<Object, Row>(
					this, channel, context, Row.class, mapper, schemaVerId,
					partitionId, containerId, normalizedName, remoteName));
		}
	}

	public <K> Container<K, Row> putContainer(
			String name, ContainerInfo info, boolean modifiable,
			boolean systemMode) throws GSException {
		return putContainer(
				name, (ContainerType) null, info, modifiable, systemMode);
	}

	@Override
	public <K> Container<K, Row> putContainer(
			String name, ContainerInfo info, boolean modifiable)
			throws GSException {
		return putContainer(
				name, (ContainerType) null, info, modifiable, false);
	}

	@Override
	public <K> Collection<K, Row> putCollection(
			String name, ContainerInfo info, boolean modifiable)
			throws GSException {

		final Container<K, Row> container = putContainer(
				name, ContainerType.COLLECTION, info, modifiable, false);
		return (Collection<K, Row>) container;
	}

	@Override
	public TimeSeries<Row> putTimeSeries(String name,
			ContainerInfo info, boolean modifiable) throws GSException {

		final Container<Date, Row> container = putContainer(
				name, ContainerType.TIME_SERIES, info, modifiable, false);
		return (TimeSeries<Row>) container;
	}

	private void dropContainer(
			String name, ContainerType containerType, boolean systemMode)
			throws GSException {

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		tryPutSystemOptionalRequest(req, context, systemMode);

		req.putString(name);
		tryPutContainerType(req, containerType);

		final int partitionId = channel.resolvePartitionId(context, name);
		final Statement statement = getContainerStatement(
				Statement.DROP_CONTAINER, containerType);

		executeStatement(statement, partitionId, req, resp, name);

		final ContainerCache cache = context.getContainerCache();
		if (cache != null && !systemMode) {
			cache.removeSchema(RowMapper.normalizeExtendedSymbol(name, true));
		}
	}

	public void dropContainer(String name, boolean systemMode) throws GSException {
		dropContainer(name, null, systemMode);
	}

	@Override
	public void dropContainer(String name) throws GSException {
		dropContainer(name, null, false);
	}

	@Override
	public Row createRow(ContainerInfo info) throws GSException {
		channel.checkContextAvailable(context);
		final RowMapper mapper = RowMapper.getInstance(info.getType(), info);
		return mapper.createGeneralRow();
	}

	private interface MultiTransactionalStatement {

		boolean makeCreateSessionRequest(BasicBuffer req,
				GridStoreChannel channel, Context context)
				throws GSException;

		void acceptCreateSessionResponse(BasicBuffer resp)
				throws GSException;

		boolean makeMainRequest(BasicBuffer req, Context context) throws GSException;

		void acceptMainResponse(
				BasicBuffer resp, Object targetConnection) throws GSException;

		boolean makeCloseSessionRequest(BasicBuffer req, Context context) throws GSException;

		void acceptSessionClosed() throws GSException;

		boolean acceptStatementErrorForSession(
				GSStatementException cause) throws GSException;

	}

	private static class MultiQueryStatement
	implements MultiTransactionalStatement {

		private final List<SubnetQuery<?>> queryList =
				new ArrayList<SubnetQuery<?>>();

		private List<SubnetQuery<?>> optionalQueryList;

		boolean updateQueryFound;

		UUID sessionUUID;

		static SubnetQuery<?> check(
				Query<?> query, SubnetGridStore store) throws GSException {
			if (!(query instanceof SubnetQuery)) {
				if (query == null) {
					throw new GSException(
							GSErrorCode.EMPTY_PARAMETER,
							"Empty query object");
				}

				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"Query type not matched (class=" +
						query.getClass() + ")");
			}

			final SubnetQuery<?> subnetQuery = (SubnetQuery<?>) query;
			if (subnetQuery.getContainer().getStore() != store) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"Derived GridStore instance not matched");
			}

			return subnetQuery;
		}

		void add(SubnetQuery<?> query) throws GSException {
			query.getContainer().clearBlob(false);
			updateQueryFound |= query.isForUpdate();
			queryList.add(query);
		}

		@Override
		public boolean makeCreateSessionRequest(
				BasicBuffer req, GridStoreChannel channel, Context context)
				throws GSException {
			sessionUUID = context.getSessionUUID();
			if (!updateQueryFound || queryList.isEmpty()) {
				return false;
			}

			if (optionalQueryList == null) {
				optionalQueryList = new ArrayList<SubnetQuery<?>>();
			}
			optionalQueryList.clear();

			for (SubnetQuery<?> query : queryList) {
				if (query.isForUpdate() &&
						query.getContainer().getSessionIdDirect() == 0) {
					optionalQueryList.add(query);
				}
			}

			final boolean summarized = isSessionUUIDSummarized();
			if (summarized) {
				req.putUUID(sessionUUID);
			}

			tryPutDatabaseOptionalRequest(req, context);

			boolean withId = true;
			req.putBoolean(withId);
			req.putInt(optionalQueryList.size());
			for (SubnetQuery<?> query : optionalQueryList) {
				final SubnetContainer<?, ?> container = query.getContainer();
				req.putLong(container.getContainerId());

				if (!summarized) {
					req.putUUID(sessionUUID);
				}

				if (SubnetContainer.isSessionIdGeneratorEnabled()) {
					final long sessionId = context.generateSessionId();
					container.setSessionIdDirect(sessionId, false);
					req.putLong(sessionId);
				}
				else {
					SubnetContainer.putNewSessionProperties(req, channel, context);
				}
			}

			return true;
		}

		@Override
		public void acceptCreateSessionResponse(BasicBuffer resp)
				throws GSException {
			final int sessionCount = resp.base().getInt();
			if (optionalQueryList.size() != sessionCount) {
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by inconsistent session count");
			}

			if (!SubnetContainer.isSessionIdGeneratorEnabled()) {
				for (int i = 0; i < sessionCount; i++) {
					final SubnetContainer<?, ?> container =
							optionalQueryList.get(i).getContainer();

					final long containerId = resp.base().getLong();
					if (containerId != container.getContainerId()) {
						throw new GSConnectionException(
								GSErrorCode.MESSAGE_CORRUPTED,
								"Protocol error by inconsistent session ID");
					}

					final long sessionId = resp.base().getLong();
					if (sessionId == 0) {
						throw new GSConnectionException(
								GSErrorCode.MESSAGE_CORRUPTED,
								"Protocol error by empty session ID");
					}

					container.setSessionIdDirect(sessionId, true);
				}
			}

			optionalQueryList.clear();
		}

		@Override
		public boolean makeMainRequest(BasicBuffer req, Context context) throws GSException {
			if (queryList.isEmpty()) {
				return false;
			}

			final boolean summarized = isSessionUUIDSummarized();
			if (summarized) {
				req.putUUID(sessionUUID);
			}

			tryPutDatabaseOptionalRequest(req, context);

			req.putInt(queryList.size());
			for (SubnetQuery<?> query : queryList) {
				final SubnetContainer<?, ?> container = query.getContainer();
				final long statementId = (container.isAutoCommit() ?
						0 : container.updateStatementIdDirect());

				req.putInt(GridStoreChannel.statementToNumber(
						query.getStatement()));
				NodeConnection.putStatementId(req, statementId);
				query.makeRequest(req, summarized);
			}

			return true;
		}

		@Override
		public void acceptMainResponse(
				BasicBuffer resp, Object targetConnection) throws GSException {
			final int count = resp.base().getInt();
			if (queryList.size() != count) {
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by inconsistent query count");
			}

			final int totalEndPos = resp.base().limit();
			for (SubnetQuery<?> query : queryList) {
				final long size = resp.base().getLong();
				if (size < 0 || size > resp.base().remaining()) {
					throw new GSConnectionException(
							GSErrorCode.MESSAGE_CORRUPTED,
							"Protocol error by invalid query message size");
				}

				resp.base().limit(resp.base().position() + (int) size);

				query.acceptResponse(resp, targetConnection, false);

				resp.base().limit(totalEndPos);
			}
		}

		@Override
		public boolean makeCloseSessionRequest(BasicBuffer req, Context context)
				throws GSException {
			return false;
		}

		@Override
		public void acceptSessionClosed() throws GSException {
		}

		@Override
		public boolean acceptStatementErrorForSession(
				GSStatementException cause) throws GSException {
			if (!updateQueryFound) {
				return false;
			}

			boolean initialSessionLost = false;
			for (SubnetQuery<?> query : queryList) {
				final SubnetContainer<?, ?> container = query.getContainer();

				if (SubnetCollection.isInitialSessionLost(
						Statement.EXECUTE_MULTIPLE_QUERIES,
						container.getStatementIdDirect(),
						container.isTransactionStarted(), cause)) {
					initialSessionLost = true;
					container.setSessionIdDirect(0, true);
				}
				else {
					container.disableCache();
				}
			}

			return initialSessionLost;
		}

	}

	@Override
	public void fetchAll(List<? extends Query<?>> queryList) throws GSException {
		final Map<Integer, MultiQueryStatement> requestMap =
				new HashMap<Integer, MultiQueryStatement>();

		for (Query<?> query : queryList) {
			final SubnetQuery<?> subnetQuery =
					MultiQueryStatement.check(query, this);

			final int partitionId =
					subnetQuery.getContainer().getPartitionId();

			MultiQueryStatement multiStatement = requestMap.get(partitionId);
			if (multiStatement == null) {
				multiStatement = new MultiQueryStatement();
				requestMap.put(partitionId, multiStatement);
			}

			multiStatement.add(subnetQuery);
		}

		final Statement statement = Statement.EXECUTE_MULTIPLE_QUERIES;
		beginComplexedStatement(statement, 0, null);
		for (Map.Entry<Integer, MultiQueryStatement> entry :
				requestMap.entrySet()) {
			final int partitionId = entry.getKey();
			final MultiQueryStatement multiStatement = entry.getValue();
			executeStatement(partitionId, statement, multiStatement);
		}
		endComplexedStatement();
	}

	private static class MultiPutStatement
	implements MultiTransactionalStatement {

		private static class SubEntry {

			List<Row> rowList;

			int mapperIndex;

			long containerId;

			long sessionId;

			boolean listMerged;

		}

		final List<RowMapper> mapperList = new ArrayList<RowMapper>();

		final List<String> containerNameList = new ArrayList<String>();

		final Map<String, SubEntry> subEntryMap =
				new HashMap<String, SubEntry>();

		UUID sessionUUID;

		void add(String containerName, List<Row> rowList,
				RowMapper mapper) throws GSException {
			if (rowList.isEmpty()) {
				return;
			}

			final String normalizedName =
					RowMapper.normalizeExtendedSymbol(containerName, true);
			SubEntry entry = subEntryMap.get(normalizedName);

			for (Row row : rowList) {
				if (mapper == null) {
					if (entry == null) {
						mapper = RowMapper.getInstance(row);
					}
					else {
						mapper = RowMapper.getInstance(
								entry.rowList.get(0));
					}
				}
				else {
					mapper.checkSchemaMatched(RowMapper.getInstance(row));
				}
			}

			if (entry == null) {
				entry = new SubEntry();
				entry.rowList = rowList;
				subEntryMap.put(normalizedName, entry);
				containerNameList.add(normalizedName);

				entry.mapperIndex = indexOf(mapper);
				if (entry.mapperIndex < 0) {
					entry.mapperIndex = mapperList.size();
					mapperList.add(mapper);
				}
			}
			else {
				if (!entry.listMerged) {
					entry.rowList = new ArrayList<Row>(entry.rowList);
					entry.listMerged = true;
				}
				entry.rowList.addAll(rowList);
			}
		}

		public boolean makeCreateSessionRequest(
				BasicBuffer req, GridStoreChannel channel, Context context)
				throws GSException {
			sessionUUID = context.getSessionUUID();
			if (containerNameList.isEmpty()) {
				return false;
			}

			final boolean summarized = isSessionUUIDSummarized();
			if (summarized) {
				req.putUUID(sessionUUID);
			}

			tryPutDatabaseOptionalRequest(req, context);

			boolean withId = false;
			req.putBoolean(withId);
			req.putInt(containerNameList.size());
			for (String containerName : containerNameList) {
				req.putString(containerName);

				if (!summarized) {
					req.putUUID(sessionUUID);
				}

				if (SubnetContainer.isSessionIdGeneratorEnabled()) {
					final SubEntry entry = subEntryMap.get(containerName);
					entry.sessionId = context.generateSessionId();
					req.putLong(entry.sessionId);
				}
				else {
					SubnetContainer.putNewSessionProperties(req, channel, context);
				}
			}

			return true;
		}

		public void acceptCreateSessionResponse(BasicBuffer resp)
				throws GSException {
			final int sessionCount = resp.base().getInt();
			if (subEntryMap.size() != sessionCount) {
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by inconsistent session count");
			}

			for (String containerName : containerNameList) {
				final SubEntry entry = subEntryMap.get(containerName);
				entry.containerId = resp.base().getLong();

				if (!SubnetContainer.isSessionIdGeneratorEnabled()) {
					entry.sessionId = resp.base().getLong();
					if (entry.sessionId == 0) {
						throw new GSConnectionException(
								GSErrorCode.MESSAGE_CORRUPTED,
								"Protocol error by empty session ID");
					}
				}
			}
		}

		public boolean makeMainRequest(BasicBuffer req, Context context) throws GSException {
			if (containerNameList.isEmpty()) {
				return false;
			}

			final boolean summarized = isSessionUUIDSummarized();
			if (summarized) {
				req.putUUID(sessionUUID);
			}

			tryPutDatabaseOptionalRequest(req, context);

			req.putInt(mapperList.size());
			for (RowMapper mapper : mapperList) {
				req.putByteEnum(mapper.isForTimeSeries() ?
						ContainerType.TIME_SERIES : ContainerType.COLLECTION);
				mapper.exportSchema(req);
			}

			req.putInt(containerNameList.size());
			for (String containerName : containerNameList) {
				final SubEntry entry = subEntryMap.get(containerName);

				NodeConnection.putStatementId(req, 1);
				req.putLong(entry.containerId);
				req.putLong(entry.sessionId);
				if (!summarized) {
					req.putUUID(sessionUUID);
				}

				if (SubnetContainer.isSessionIdGeneratorEnabled()) {
					req.putByteEnum(SessionMode.GET);
					req.putByteEnum(TransactionMode.AUTO);
				}
				NodeConnection.tryPutEmptyOptionalRequest(req);

				req.putInt(entry.mapperIndex);

				int listBytesSize = 0;
				req.putInt(listBytesSize);
				final int listStartPos = req.base().position();

				req.putLong(entry.rowList.size());
				final RowMapper mapper = mapperList.get(entry.mapperIndex);
				final RowMapper.Cursor cursor = mapper.createCursor(
						req, SubnetContainer.getRowMappingMode(),
						entry.rowList.size(), false, null);
				for (Row row : entry.rowList) {
					mapper.encode(cursor, null, row);
				}

				final int listEndPos = req.base().position();
				req.base().position(listStartPos - Integer.SIZE / Byte.SIZE);
				req.putInt(listEndPos - listStartPos);
				req.base().position(listEndPos);
			}

			return true;
		}

		public void acceptMainResponse(
				BasicBuffer resp, Object targetConnection) {
		}

		public boolean makeCloseSessionRequest(BasicBuffer req, Context context) {
			if (containerNameList.isEmpty()) {
				return false;
			}

			final boolean summarized = isSessionUUIDSummarized();
			if (summarized) {
				req.putUUID(sessionUUID);
			}

			tryPutDatabaseOptionalRequest(req, context);

			req.putInt(containerNameList.size());
			for (String containerName : containerNameList) {
				final SubEntry entry = subEntryMap.get(containerName);

				NodeConnection.putStatementId(req, 2);
				req.putLong(entry.containerId);
				req.putLong(entry.sessionId);
				if (!summarized) {
					req.putUUID(sessionUUID);
				}
			}

			return true;
		}

		public void acceptSessionClosed() {
		}

		public boolean acceptStatementErrorForSession(
				GSStatementException cause) throws GSException {
			return SubnetContainer.isInitialSessionLost(
					Statement.CREATE_MULTIPLE_SESSIONS, 1, false, cause);
		}

		int indexOf(RowMapper mapper) {
			
			for (int i = 0; i < mapperList.size(); i++) {
				if (mapperList.get(i) == mapper) {
					return i;
				}
			}
			return -1;
		}
	}

	@Override
	public void multiPut(
			Map<String, List<Row>> containerRowsMap) throws GSException {
		final Map<Integer, MultiPutStatement> requestMap =
				new HashMap<Integer, MultiPutStatement>();

		for (Map.Entry<String, List<Row>> entry :
				containerRowsMap.entrySet()) {
			final String containerName = entry.getKey();
			final int partitionId =
					channel.resolvePartitionId(context, containerName);

			MultiPutStatement multiStatement = requestMap.get(partitionId);
			if (multiStatement == null) {
				multiStatement = new MultiPutStatement();
				requestMap.put(partitionId, multiStatement);
			}

			multiStatement.add(containerName, entry.getValue(), null);
		}

		final Statement statement = Statement.PUT_MULTIPLE_CONTAINER_ROWS;
		beginComplexedStatement(statement, 0, null);
		for (Map.Entry<Integer, MultiPutStatement> entry :
				requestMap.entrySet()) {
			final int partitionId = entry.getKey();
			final MultiPutStatement multiStatement = entry.getValue();
			executeStatement(partitionId, statement, multiStatement);
		}
		endComplexedStatement();
	}

	private void executeStatement(
			int partitionId, Statement statement,
			MultiTransactionalStatement multiStatement)
			throws GSException {
		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		boolean succeeded = false;
		try {
			for (int trialCount = 0;; trialCount++) {

				for (int sessionTrial = 0;; sessionTrial++) {
					channel.setupRequestBuffer(req);
					if (multiStatement.makeCreateSessionRequest(
							req, channel, context)) {

						try {
							channel.executeStatement(
									context,
									Statement.CREATE_MULTIPLE_SESSIONS,
									partitionId, 0, req, resp, contextMonitor);
						}
						catch (GSStatementException e) {
							if (!SubnetContainer.isNewSessionConflicted(e)) {
								throw e;
							}
							else if (sessionTrial >=
									SubnetContainer.MAX_SESSION_REPAIR_COUNT) {
								throw new GSStatementException(
										e.getErrorCode(),
										"Failed to create session (" +
										"totalTrialCount=" + trialCount +
										", sessionTrialCount=" + sessionTrial +
										", reason=" + e.getMessage() + ")",
										e);
							}

							if (LOGGER.isInfoEnabled()) {
								LOGGER.info(
										"transaction.regeneratingMultiSession",
										ContextMonitor.getObjectId(context),
										statement,
										trialCount,
										sessionTrial,
										e);
							}
							continue;
						}

						multiStatement.acceptCreateSessionResponse(resp);

						if ((trialCount > 0 || sessionTrial > 0) &&
								LOGGER.isInfoEnabled()) {
							LOGGER.info("transaction.sessionRepaired",
									ContextMonitor.getObjectId(context),
									statement,
									trialCount,
									sessionTrial);
						}
					}
					break;
				}

				try {
					channel.setupRequestBuffer(req);
					if (!multiStatement.makeMainRequest(req, context)) {
						break;
					}

					final Object targetConnection;
					synchronized (context) {
						channel.executeStatement(
								context, statement, partitionId, 0, req, resp,
								contextMonitor);
						targetConnection = channel.getLastConnection(context);
					}
					multiStatement.acceptMainResponse(resp, targetConnection);
					succeeded = true;
					break;
				}
				catch (GSStatementException e) {
					if (!multiStatement.acceptStatementErrorForSession(e)) {
						throw e;
					}

					if (LOGGER.isInfoEnabled()) {
						LOGGER.info("transaction.repairingMultiSession",
								ContextMonitor.getObjectId(context),
								statement,
								trialCount,
								e);
					}

					if (trialCount >= SubnetContainer.MAX_SESSION_REPAIR_COUNT) {
						throw new GSStatementException(
								e.getErrorCode(),
								"Failed to repair session (trialCount=" + trialCount +
								", reason=" + e.getMessage() + ")");
					}
				}
			}
		}
		finally {
			
			try {
				channel.setupRequestBuffer(req);
				if (multiStatement.makeCloseSessionRequest(req, context)) {

					channel.executeStatement(
							context, Statement.CLOSE_MULTIPLE_SESSIONS,
							partitionId, 0, req, resp, contextMonitor);

					multiStatement.acceptSessionClosed();
				}
			}
			catch (GSException e) {
				if (succeeded) {
					throw e;
				}
				
			}
		}
	}

	private enum PredicateType {

		RANGE,

		DISTINCT

	}

	private static class MultiGetRequest {

		final List<RowKeyPredicate<?>> predicateList =
				new ArrayList<RowKeyPredicate<?>>();

		final List<String> containerNameList = new ArrayList<String>();

		void add(String containerName, RowKeyPredicate<?> predicate) {
			if (!predicateList.contains(predicate)) {
				predicateList.add(predicate);
			}
			containerNameList.add(containerName);
		}

		boolean makeRequest(
				BasicBuffer req,
				Map<String, ? extends RowKeyPredicate<?>>
				containerPredicateMap,
				Context context) throws GSException {
			if (containerNameList.isEmpty()) {
				return false;
			}

			if (isSessionUUIDSummarized()) {
				req.putUUID(context.getSessionUUID());
			}

			tryPutDatabaseOptionalRequest(req, context);

			req.putInt(predicateList.size());
			for (RowKeyPredicate<?> predicate : predicateList) {
				final GSType keyType = predicate.getKeyType();
				req.putByteEnum(keyType);

				final java.util.Collection<?> distinctKeys = predicate.getDistinctKeys();
				final Object startKey = predicate.getStart();
				final Object finishKey = predicate.getFinish();

				if (distinctKeys == null) {
					req.putByteEnum(PredicateType.RANGE);

					req.putBoolean(startKey != null);
					if (startKey != null) {
						RowMapper.encodeKey(req, startKey, keyType, SubnetContainer.getRowMappingMode());
					}

					req.putBoolean(finishKey != null);
					if (finishKey != null) {
						RowMapper.encodeKey(req, finishKey, keyType, SubnetContainer.getRowMappingMode());
					}
				}
				else {
					req.putByteEnum(PredicateType.DISTINCT);
					req.putInt(distinctKeys.size());
					for (Object key : distinctKeys) {
						RowMapper.encodeKey(req, key, keyType, SubnetContainer.getRowMappingMode());
					}
				}
			}

			req.putInt(containerNameList.size());
			for (String containerName : containerNameList) {
				final RowKeyPredicate<?> predicate =
						containerPredicateMap.get(containerName);
				final int predicateIndex = predicateList.indexOf(predicate);
				if (predicateIndex < 0) {
					throw new Error("Internal error by unregistered predicate");
				}

				req.putString(containerName);
				req.putInt(predicateIndex);
			}

			return true;
		}

	}

	@Override
	public Map<String, List<Row>> multiGet(
			Map<String, ? extends RowKeyPredicate<?>> containerPredicateMap)
			throws GSException {
		final Map<Integer, MultiGetRequest> requestMap =
				new HashMap<Integer, MultiGetRequest>();

		for (Map.Entry<String, ? extends RowKeyPredicate<?>> entry :
				containerPredicateMap.entrySet()) {
			final String containerName = entry.getKey();
			final int partitionId =
					channel.resolvePartitionId(context, containerName);

			MultiGetRequest request = requestMap.get(partitionId);
			if (request == null) {
				request = new MultiGetRequest();
				requestMap.put(partitionId, request);
			}

			final RowKeyPredicate<?> predicate = entry.getValue();
			request.add(containerName, predicate);
		}

		final Statement statement = Statement.GET_MULTIPLE_CONTAINER_ROWS;
		beginComplexedStatement(statement, 0, null);

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		final Map<String, List<Row>> resultMap =
				new HashMap<String, List<Row>>();
		for (Map.Entry<Integer, MultiGetRequest> entry :
				requestMap.entrySet()) {
			channel.setupRequestBuffer(req);

			final int partitionId = entry.getKey();
			final MultiGetRequest request = entry.getValue();
			if (!request.makeRequest(req, containerPredicateMap, context)) {
				continue;
			}

			channel.executeStatement(
					context, statement, partitionId, 0, req, resp, contextMonitor);

			final int headCount = resp.base().getInt();
			final List<RowMapper> mapperList = new ArrayList<RowMapper>();
			for (int i = 0; i < headCount; i++) {
				final ContainerType containerType =
						resp.getByteEnum(ContainerType.class);
				mapperList.add(RowMapper.getInstance(
						resp, containerType == ContainerType.TIME_SERIES));
			}

			final int bodyCount = resp.base().getInt();
			for (int i = 0; i < bodyCount; i++) {
				final String containerName = resp.getString();

				List<Row> rowList = resultMap.get(containerName);
				if (rowList == null) {
					rowList = new ArrayList<Row>();
					resultMap.put(containerName, rowList);
				}

				final int mapperIndex = resp.base().getInt();
				final RowMapper mapper = mapperList.get(mapperIndex);

				final boolean rowIdIncluded = !mapper.isForTimeSeries();
				final int rowCount = (int) resp.base().getLong();
				final Cursor cursor = mapper.createCursor(
						resp, SubnetContainer.getRowMappingMode(), rowCount,
						rowIdIncluded, BLOB_FACTORY);

				for (int j = 0; j < rowCount; j++) {
					rowList.add((Row) mapper.decode(cursor));
				}
			}
		}
		endComplexedStatement();

		return resultMap;
	}

	@Override
	public PartitionController getPartitionController() throws GSException {
		channel.checkContextAvailable(context);
		return new SubnetPartitionController(channel, context);
	}

	private static final String CONTEXT_CONTROLLER_NAME =
			"##internal.contextController";

	private <K> SubnetCollection<K, Row> getContextControllerCollection(
			ContainerType expectedType) throws GSException {
		if (expectedType != null && expectedType != ContainerType.COLLECTION) {
			return null;
		}

		final RowMapper mapper = RowMapper.getInstance(null, new ContainerInfo(
				null, null, Arrays.asList(
						new ColumnInfo("name", GSType.STRING),
						new ColumnInfo("value", GSType.STRING)),
						true));

		return new SubnetCollection<K, Row>(
				this, channel, context, Row.class, mapper, -1, -1, -1,
				CONTEXT_CONTROLLER_NAME, null) {

					@Override
					public Row get(K key, boolean forUpdate) throws GSException {
						if (!(key instanceof String)) {
							throw new GSException(
									GSErrorCode.ILLEGAL_PARAMETER,
									"Type of key for context controller must be string");
						}

						final Row row = mapper.createGeneralRow();
						do {
							if ("failoverCount".equals(key)) {
								row.setString(1, Integer.toString(
										context.getFailoverCount()));
							}
							else {
								break;
							}

							row.setValue(0, key);

							return row;
						}
						while (false);

						throw new GSException(
								GSErrorCode.ILLEGAL_PARAMETER, "Unknown key");
					}

					@Override
					public boolean put(K key, Row value) throws GSException {
						do {
							if ("invalidateMaster".equals(key)) {
								if (value.getString(1).equals(
										Boolean.TRUE.toString())) {
									channel.invalidateMaster(context);
								}
								else {
									break;
								}
							}
							else {
								break;
							}

							return true;
						}
						while (false);

						throw new GSException(
								GSErrorCode.ILLEGAL_PARAMETER, "Unknown key");
					}

		};
	}

	@Override
	public <K, R> Container<K, R> putContainer(
			String name, Class<R> rowType,
			ContainerInfo info, boolean modifiable) throws GSException {
		return putContainer(
				resolveContainerName(name, info),
				resolveContainerType(null, info),
				rowType, info, modifiable);
	}

	private static void setUserInfoRequest(BasicBuffer req,
			String userName)
			throws GSException {

		
		RowMapper.normalizeSymbol(userName);
		req.putString(userName);		
	}

	private void setUserInfoRequest(BasicBuffer req,
			String userName, byte prop, String hashPassword)
			throws GSException {

		

		
		setUserInfoRequest(req, userName);	
		req.put(prop);		

		if (hashPassword != null) {
			if (hashPassword.length() == 0) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER, "invalid parameter string length.");
			}
			req.putBoolean(true);		
			req.putString(hashPassword);
		} else {
			req.putBoolean(false);		
			
		}
	}

	private Map<String, UserInfo> getUserInfoMap(BasicBuffer resp) throws GSException {

		final Map<String, UserInfo> map = new HashMap<String, UserInfo>();

		
		final int userInfoCount = resp.base().getInt();	

		
		if (userInfoCount == 0) {
			return map;
		}

		
		if (USER_INFO_COUNT_LIMIT < userInfoCount) {
			throw new GSConnectionException(GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by too many result num (" +
					"expected less than " + USER_INFO_COUNT_LIMIT +
					": but " + userInfoCount + ")");
		}

		
		for (int i = 0; i < userInfoCount; i++) {

			String userName = resp.getString();		
			Byte property = resp.base().get();		
			boolean passwordExist = resp.getBoolean();	
			String hashPassword = "";

			if (passwordExist) {
				hashPassword = resp.getString();
			}

			map.put(userName, new UserInfo(userName, hashPassword, (property != 0)));
		}

		return map;
	}

	public void putUser(String name, UserInfo userInfo, boolean modifiable)
			throws GSException {

		
		name = validateName(name, userInfo.getName());
		
		RowMapper.normalizeSymbol(name);

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);

		NodeConnection.tryPutEmptyOptionalRequest(req);

		final int partitionId = channel.resolvePartitionId(context, SYSTEM_USER_CONTAINER_NAME);

		if (userInfo.getPassword() != null && userInfo.getHashPassword() == null) {

			if (userInfo.getPassword().length() == 0 || USER_PASSWORD_LENGTH_LIMIT < userInfo.getPassword().length()) {
				throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
						"Illegal password length");
			}

			String hashPassword = NodeConnection.getDigest(userInfo.getPassword());
			setUserInfoRequest(req, name, DEFAULT_DBUSER_PROPERTY, hashPassword);

		} else if (userInfo.getPassword() == null && userInfo.getHashPassword() != null) {
			setUserInfoRequest(req, name, DEFAULT_DBUSER_PROPERTY, userInfo.getHashPassword());

		} else if (userInfo.getPassword() == null) {
			throw new GSException(GSErrorCode.EMPTY_PARAMETER,
					"Password or hash password not specified");
		} else {
			throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
					"Password and hash password not specified");
		}

		req.putBoolean(modifiable);

		executeStatement(
				Statement.PUT_USER,
				partitionId, req, resp, null);
	}

	public void dropUser(String name) throws GSException {

		if (name == null) {
			throw new GSException(GSErrorCode.EMPTY_PARAMETER,
					"Name not specified");
		}

		
		RowMapper.normalizeSymbol(name);

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);

		NodeConnection.tryPutEmptyOptionalRequest(req);

		final int partitionId = channel.resolvePartitionId(context, SYSTEM_USER_CONTAINER_NAME);

		setUserInfoRequest(req, name);

		executeStatement(
				Statement.DROP_USER,
				partitionId, req, resp, null);
	}

	public Map<String, UserInfo> getUsers() throws GSException {

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);

		NodeConnection.tryPutEmptyOptionalRequest(req);

		final int partitionId = channel.resolvePartitionId(context, SYSTEM_USER_CONTAINER_NAME);

		req.putBoolean(false);	
		req.put(DEFAULT_DBUSER_PROPERTY);		

		executeStatement(
				Statement.GET_USERS,
				partitionId, req, resp, null);

		final Map<String, UserInfo> map = getUserInfoMap(resp);

		return map;
	}

	public UserInfo getCurrentUser() throws GSException {

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);

		NodeConnection.tryPutEmptyOptionalRequest(req);

		final int partitionId = channel.resolvePartitionId(context, SYSTEM_USER_CONTAINER_NAME);

		
		RowMapper.normalizeExtendedSymbol(context.getUser(), true);

		req.putBoolean(true);	
		req.putString(context.getUser());	
		req.put(DEFAULT_DBUSER_PROPERTY);		

		executeStatement(
				Statement.GET_USERS,
				partitionId, req, resp, null);

		final Map<String, UserInfo> map = getUserInfoMap(resp);

		
		if (map.size() != 1) {
			throw new GSConnectionException(GSErrorCode.MESSAGE_CORRUPTED,
				"Protocol error by illegal user info count (" +
				"expected=1" +
				", actual=" + map.size() + ")");
		}

		
		UserInfo userInfo = map.get(context.getUser());
		if (userInfo == null) {
			throw new GSConnectionException(GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by illegal user name (" +
					"expected=" + context.getUser() + ")");
		}

		return userInfo;
	}

	private void setDatabaseInfoRequest(BasicBuffer req,
			String dbName)
			throws GSException {

		RowMapper.normalizeSymbol(dbName);
		req.putString(dbName);		
	}

	private void setDatabaseInfoRequest(BasicBuffer req,
			String dbName, byte prop, Map<String, PrivilegeInfo> privileges)
			throws GSException {

		

		
		setDatabaseInfoRequest(req, dbName);	
		req.put(prop);		

		if (privileges != null) {

			req.base().putInt(privileges.size());	

			
			for (Entry<String, PrivilegeInfo> privilegeEntry : privileges.entrySet()) {

				RowMapper.normalizeSymbol(privilegeEntry.getKey());
				req.putString(privilegeEntry.getKey());	
				PrivilegeInfo privilegeInfo = privilegeEntry.getValue();	
				if (privilegeInfo == null) {
					throw new GSException(GSErrorCode.EMPTY_PARAMETER,
							"PrivilegeInfo not specified");
				}
				req.putString(DEFAULT_PRIVILEGE);		
			}
		} else {

			req.base().putInt(0);	
		}
	}

	private void setDatabaseInfoRequest(BasicBuffer req,
			String dbName, byte prop, String userName, PrivilegeInfo info)
			throws GSException {

		final Map<String, PrivilegeInfo> map = new HashMap<String, PrivilegeInfo>();
		map.put(userName, info);
		setDatabaseInfoRequest(req, dbName, DEFAULT_DBUSER_PROPERTY, map);
	}

	private Map<String, DatabaseInfo> getDatabaseInfoMap(BasicBuffer resp) throws GSException {

		final Map<String, DatabaseInfo> map = new HashMap<String, DatabaseInfo>();

		
		final int databaseInfoCount = resp.base().getInt();	

		
		if (databaseInfoCount == 0) {
			return map;
		}

		
		if (DATABASE_INFO_COUNT_LIMIT < databaseInfoCount) {
			throw new GSConnectionException(GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by too many result num (" +
					"expected less than " + DATABASE_INFO_COUNT_LIMIT +
					": but " + databaseInfoCount + ")");
		}

		
		for (int i = 0; i < databaseInfoCount; i++) {

			String databaseName = resp.getString();	
			@SuppressWarnings("unused")
			Byte property = resp.base().get();		

			final int privilegeInfoCount = resp.base().getInt();	

			Map<String, PrivilegeInfo> privilegeInfoMap = new HashMap<String, PrivilegeInfo>();
			for (int j = 0; j < privilegeInfoCount; j++) {
				String userName = resp.getString();		
				String privilegeData = resp.getString();	
				if (!privilegeData.equals(DEFAULT_PRIVILEGE)) {
					throw new GSConnectionException(GSErrorCode.MESSAGE_CORRUPTED,
							"Protocol error by illegal privilegeDataSize (" +
							"expected=" + DEFAULT_PRIVILEGE +
							", actual=" + privilegeData + ")");
				}
				privilegeInfoMap.put(userName, new PrivilegeInfo());
			}

			
			map.put(databaseName, new DatabaseInfo(databaseName, privilegeInfoMap));
		}

		return map;
	}

	public void putDatabase(String name, DatabaseInfo info, boolean modifiable)
			throws GSException {

		if (name == null) {
			throw new GSException(GSErrorCode.EMPTY_PARAMETER,
					"Name not specified");
		}

		
		RowMapper.normalizeSymbol(name);

		
		modifiable = false;

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);

		NodeConnection.tryPutEmptyOptionalRequest(req);

		final int partitionId = channel.resolvePartitionId(context, SYSTEM_USER_CONTAINER_NAME);

		
		setDatabaseInfoRequest(req, name, DEFAULT_DBUSER_PROPERTY, null);
		req.putBoolean(modifiable);

		executeStatement(
				Statement.PUT_DATABASE,
				partitionId, req, resp, null);
	}

	public void dropDatabase(String name) throws GSException {

		DatabaseInfo info = getCurrentDatabase();
		if (!info.getName().equals(name)) {
			throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
					"Illegal database is specified; different from connected database");
		}

		PartitionController partitionController = getPartitionController();
		int partitionCount = partitionController.getPartitionCount();
		ContainerCondition cond = new ContainerCondition();
		cond.setAttributes(EnumSet.of(ContainerAttribute.BASE,
				ContainerAttribute.SINGLE_SYSTEM,
				ContainerAttribute.SINGLE,
				ContainerAttribute.LARGE,
				ContainerAttribute.SUB
				));

		for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
			long containerCount = ((SubnetPartitionController)partitionController).getContainerCount(partitionIndex, cond, true);
			if (0 < containerCount) {

				throw new GSException(GSErrorCode.DATABASE_NOT_EMPTY,
						"Illegal target error by non-empty database");
			}
		}

		if (name == null) {
			throw new GSException(GSErrorCode.EMPTY_PARAMETER, "Name not specified");
		}

		
		RowMapper.normalizeSymbol(name);

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);

		NodeConnection.tryPutEmptyOptionalRequest(req);

		final int partitionId = channel.resolvePartitionId(context, SYSTEM_USER_CONTAINER_NAME);

		setDatabaseInfoRequest(req, name);

		executeStatement(
				Statement.DROP_DATABASE,
				partitionId, req, resp, null);
	}

	public Map<String, DatabaseInfo> getDatabases() throws GSException {

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);

		NodeConnection.tryPutEmptyOptionalRequest(req);

		final int partitionId = channel.resolvePartitionId(context, SYSTEM_USER_CONTAINER_NAME);

		req.putBoolean(false);	
		req.put(DEFAULT_DBUSER_PROPERTY);		

		executeStatement(
				Statement.GET_DATABASES,
				partitionId, req, resp, null);

		final Map<String, DatabaseInfo> map = getDatabaseInfoMap(resp);

		return map;
	}

	public DatabaseInfo getCurrentDatabase() throws GSException {

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);

		NodeConnection.tryPutEmptyOptionalRequest(req);

		final int partitionId = channel.resolvePartitionId(context, SYSTEM_USER_CONTAINER_NAME);

		final String dbName = context.getDatabaseName();

		req.putBoolean(true);	
		req.putString(dbName == null ? "" : dbName);	
		req.put(DEFAULT_DBUSER_PROPERTY);		

		executeStatement(
				Statement.GET_DATABASES, partitionId, req, resp, null);

		final Map<String, DatabaseInfo> map = getDatabaseInfoMap(resp);

		final DatabaseInfo info;

		if (map.size() == 1) {

			if (dbName == null) {
				info = map.values().iterator().next();
			}
			else {
				info = map.get(dbName);
			}

			
			if (info == null) {
				throw new GSConnectionException(GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by illegal database name (" +
						"expected=" + context.getDatabaseName() + ")");
			}

		} else {

			
			throw new GSConnectionException(GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by illegal database info count (" +
					"expected=1" + ", actual=" + map.size() + ")");
		}

		return info;
	}

	public void putPrivilege(String dbName, String userName, PrivilegeInfo info)
			throws GSException {

		
		RowMapper.normalizeSymbol(dbName);
		RowMapper.normalizeSymbol(userName);

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);

		NodeConnection.tryPutEmptyOptionalRequest(req);

		final int partitionId = channel.resolvePartitionId(context, SYSTEM_USER_CONTAINER_NAME);

		setDatabaseInfoRequest(req, dbName, DEFAULT_DBUSER_PROPERTY, userName, info);

		executeStatement(
				Statement.PUT_PRIVILEGE,
				partitionId, req, resp, null);
	}

	public void dropPrivilege(String dbName, String userName, PrivilegeInfo info) throws GSException {

		
		RowMapper.normalizeSymbol(dbName);
		RowMapper.normalizeSymbol(userName);

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);

		NodeConnection.tryPutEmptyOptionalRequest(req);

		final int partitionId = channel.resolvePartitionId(context, SYSTEM_USER_CONTAINER_NAME);

		setDatabaseInfoRequest(req, dbName, DEFAULT_DBUSER_PROPERTY, userName, info);

		executeStatement(
				Statement.DROP_PRIVILEGE,
				partitionId, req, resp, null);
	}

}

