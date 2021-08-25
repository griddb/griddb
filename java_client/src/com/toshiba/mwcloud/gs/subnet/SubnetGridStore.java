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
import com.toshiba.mwcloud.gs.IndexInfo;
import com.toshiba.mwcloud.gs.IndexType;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowKeyPredicate;
import com.toshiba.mwcloud.gs.TimeSeries;
import com.toshiba.mwcloud.gs.TimeSeriesProperties;
import com.toshiba.mwcloud.gs.TimeUnit;
import com.toshiba.mwcloud.gs.TriggerInfo;
import com.toshiba.mwcloud.gs.common.BasicBuffer;
import com.toshiba.mwcloud.gs.common.BasicBuffer.BufferUtils;
import com.toshiba.mwcloud.gs.common.ContainerKeyConverter;
import com.toshiba.mwcloud.gs.common.ContainerKeyConverter.ContainerKey;
import com.toshiba.mwcloud.gs.common.ContainerKeyPredicate;
import com.toshiba.mwcloud.gs.common.ContainerProperties;
import com.toshiba.mwcloud.gs.common.ContainerProperties.ContainerIdInfo;
import com.toshiba.mwcloud.gs.common.ContainerProperties.ContainerVisibility;
import com.toshiba.mwcloud.gs.common.ContainerProperties.MetaDistributionType;
import com.toshiba.mwcloud.gs.common.ContainerProperties.MetaNamingType;
import com.toshiba.mwcloud.gs.common.Extensibles;
import com.toshiba.mwcloud.gs.common.Extensibles.MultiOperationContext;
import com.toshiba.mwcloud.gs.common.Extensibles.MultiTargetConsumer;
import com.toshiba.mwcloud.gs.common.Extensibles.StatementHandler;
import com.toshiba.mwcloud.gs.common.GSConnectionException;
import com.toshiba.mwcloud.gs.common.GSErrorCode;
import com.toshiba.mwcloud.gs.common.GSStatementException;
import com.toshiba.mwcloud.gs.common.LoggingUtils;
import com.toshiba.mwcloud.gs.common.LoggingUtils.BaseGridStoreLogger;
import com.toshiba.mwcloud.gs.common.RowMapper;
import com.toshiba.mwcloud.gs.common.RowMapper.Cursor;
import com.toshiba.mwcloud.gs.common.RowMapper.MutableColumnInfo;
import com.toshiba.mwcloud.gs.common.Statement;
import com.toshiba.mwcloud.gs.common.Statement.GeneralStatement;
import com.toshiba.mwcloud.gs.experimental.ContainerAttribute;
import com.toshiba.mwcloud.gs.experimental.DatabaseInfo;
import com.toshiba.mwcloud.gs.experimental.Experimentals;
import com.toshiba.mwcloud.gs.experimental.PrivilegeInfo;
import com.toshiba.mwcloud.gs.experimental.PrivilegeInfo.RoleType;
import com.toshiba.mwcloud.gs.experimental.UserInfo;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.ContainerCache;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.Context;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.ContextMonitor;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.ContextReference;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.LocatedSchema;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.RemoteReference;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.FeatureVersion;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.OptionalRequest;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.OptionalRequestSource;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.OptionalRequestType;
import com.toshiba.mwcloud.gs.subnet.SubnetCollection.MetaCollection;
import com.toshiba.mwcloud.gs.subnet.SubnetContainer.SessionMode;
import com.toshiba.mwcloud.gs.subnet.SubnetContainer.TransactionMode;

public class SubnetGridStore implements GridStore, Extensibles.AsStore,
Experimentals.AsStore, Experimentals.StoreProvider {

	private static final boolean ENABLE_COMPRESSION_WINDOW_SIZE = true;

	private static boolean OLD_TIME_SERIES_PROPERTIES = false;

	
	static final boolean WORKAROUND_WRONG_API_TEST = false;

	private static final BaseGridStoreLogger LOGGER =
			LoggingUtils.getLogger("Transaction");

	private static final String ERROR_PARAM_CONTAINER = "container";

	private static boolean pathKeyOperationEnabled = false;

	private static boolean compositeIndexTypeUnified = false;

	private static final int SYSTEM_USER_PARTITION_ID = 0;

	private static final String ALL_PRIVILEGE = "ALL";
	private static final String READ_PRIVILEGE = "READ";

	private static final int MAX_PASSWORD_BYTES_LENGTH = 64;

	private static final IndexType[] INDEX_TYPE_CONSTANTS = IndexType.values();

	private static final byte INDEX_TYPE_DEFAULT_VALUE = -1;

	private static final RowMapper.Config DEFAULT_MAPPER_CONFIG =
			new RowMapper.Config(false, true, true, true);

	private static final RowMapper.Config COMPATIBLE_MAPPER_CONFIG_14 =
			new RowMapper.Config(false, true, false, false);

	private static final RowMapper.Config COMPATIBLE_MAPPER_CONFIG_13 =
			new RowMapper.Config(false, false, false, false);

	private static final RowMapper.Config ANY_MAPPER_CONFIG =
			new RowMapper.Config(true, true, true, true);

	private final GridStoreChannel channel;

	private final Context context;

	private final ContextReference contextRef;

	private final NamedContainerMap<SubnetContainer<?, ?>> containerMap;

	private final ContextMonitor contextMonitor =
			GridStoreChannel.tryCreateGeneralContextMonitor();

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

	@Override
	public void executeStatement(
			GeneralStatement statement, int partitionIndex,
			StatementHandler handler) throws GSException {
		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		handler.makeRequest(req);
		channel.executeStatement(
				context, statement, partitionIndex, 0, req, resp,
				contextMonitor);
		handler.acceptResponse(resp);
	}

	private void executeStatement(
			Statement statement, int partitionId,
			BasicBuffer req, BasicBuffer resp, ContainerKey key)
			throws GSException {

		beginComplexedStatement(statement, partitionId, key);
		channel.executeStatement(
				context, statement.generalize(), partitionId, 0, req, resp,
				contextMonitor);
		endComplexedStatement();
	}

	private void beginComplexedStatement(
			Statement statement, int partitionId, ContainerKey key) {
		if (contextMonitor != null) {
			contextMonitor.setContainerKey(key);
			contextMonitor.startStatement(
					statement.generalize(), 0, partitionId, null);
		}
	}

	private void endComplexedStatement() {
		if (contextMonitor != null) {
			contextMonitor.endStatement();
		}
	}

	public static abstract class NamedContainerMap<C extends Container<?, ?>> {

		private final Map<ContainerKey, List<WeakReference<C>>> base =
				new HashMap<ContainerKey, List<WeakReference<C>>>();

		public synchronized C get(
				ContainerKey key, Class<?> rowType) throws GSException {
			final List<WeakReference<C>> list =
					base.get(key.toCaseSensitive(false));
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

		public synchronized void add(
				ContainerKey normalizedKey, C container) {
			List<WeakReference<C>> list = base.get(normalizedKey);
			if (list == null) {
				list = new LinkedList<WeakReference<C>>();
				base.put(normalizedKey, list);
			}
			list.add(new WeakReference<C>(container));
		}

		public synchronized void remove(
				ContainerKey normalizedKey, C container) {
			final List<WeakReference<C>> list = base.get(normalizedKey);
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
				base.remove(normalizedKey);
			}
		}

		protected abstract Class<?> getRowType(C container);
	}

	private SubnetContainer<?, ?> resolveContainer(
			ContainerKey containerKey, Class<?> rowType) throws GSException {
		if (containerMap == null) {
			throw new GSException(
					GSErrorCode.UNSUPPORTED_OPERATION,
					"Path key operation is restricted");
		}

		final SubnetContainer<?, ?> container =
				containerMap.get(containerKey, rowType);
		if (container == null) {
			throw new GSException(
					GSErrorCode.CONTAINER_NOT_OPENED,
					"Related container not opened " +
					"(name=" + containerKey + ", rowType=" +
							(rowType == null ? "" : rowType.getName()) + ")");
		}

		return duplicateContainer(container);
	}

	private <K, R> SubnetContainer<?, R> duplicateContainer(
			SubnetContainer<K, R> base) throws GSException {
		final RowMapper mapper = base.getRowMapper();
		if (mapper.isForTimeSeries()) {
			return new SubnetTimeSeries<R>(
					this, channel, context,
					rebindTimeSeriesType(base.getBindType()),
					mapper,
					base.getSchemaVersionId(),
					base.getPartitionId(),
					base.getContainerId(),
					null, null);
		}
		else {
			return new SubnetCollection<K, R>(
					this, channel, context,
					base.getBindType(), mapper,
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

		containerMap.add(container.getNormalizedContainerKey(), container);
	}

	void removeReference(SubnetContainer<?, ?> container) {
		if (containerMap == null) {
			return;
		}

		containerMap.remove(container.getNormalizedContainerKey(), container);
	}

	@Override
	@Deprecated
	public boolean put(String pathKey, Object rowObject) throws GSException {
		final String[] keyElements = splitPathKey(pathKey);
		final SubnetContainer<?, ?> container = resolveContainer(
				parseContainerKey(keyElements[0]), rowObject.getClass());
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
	@Deprecated
	public Object get(String pathKey) throws GSException {
		return getByPathKey(pathKey, null);
	}

	@Override
	@Deprecated
	public <R> R get(String pathKey, Class<R> rowType) throws GSException {
		return rowType.cast(getByPathKey(pathKey, rowType));
	}

	private Object getByPathKey(
			String pathKey, Class<?> rowType) throws GSException {
		final String[] keyElements = splitPathKey(pathKey);
		final SubnetContainer<Object, ?> container =
				disguiseTypedContainer(resolveContainer(
						parseContainerKey(keyElements[0]), rowType));
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
	@Deprecated
	public boolean remove(String pathKey) throws GSException {
		final String[] keyElements = splitPathKey(pathKey);
		final SubnetContainer<Object, ?> container =
				disguiseTypedContainer(resolveContainer(
						parseContainerKey(keyElements[0]), null));
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

	@Override
	public long getDatabaseId() throws GSException {
		return channel.getDatabaseId(context);
	}

	@Override
	public ContainerKeyConverter getContainerKeyConverter(
			boolean internalMode, boolean forModification) throws GSException {
		return context.getKeyConverter(internalMode, forModification);
	}

	private ContainerKey parseContainerKey(String name) throws GSException {
		return parseContainerKey(name, false, false);
	}

	private ContainerKey parseContainerKey(
			String name, boolean internalMode, boolean forModification)
			throws GSException {
		final boolean caseSensitive = false;
		return getContainerKeyConverter(
				internalMode, forModification).parse(name, caseSensitive);
	}

	private void putContainerKey(
			BasicBuffer req, ContainerKey key,
			ContainerKeyConverter keyConverter) throws GSException {
		final long databaseId = channel.getDatabaseId(context);
		keyConverter.put(key, databaseId, req);
	}

	static void tryPutSystemOptionalRequest(
			BasicBuffer req, Context context, boolean internalMode,
			boolean forCreationDDL, Integer containerAttribute) {
		tryPutSystemOptionalRequest(
				req, context, internalMode, forCreationDDL, containerAttribute,
				null);
	}

	static void tryPutSystemOptionalRequest(
			BasicBuffer req, Context context, boolean internalMode,
			boolean forCreationDDL, Integer containerAttribute,
			OptionalRequestSource source) {
		final boolean clientIdRequired =
				forCreationDDL && SubnetGridStore.isClientIdEnabled();

		if (!internalMode && !clientIdRequired && containerAttribute == null &&
				(source == null || !source.hasOptions())) {
			NodeConnection.tryPutEmptyOptionalRequest(req);
			return;
		}

		final OptionalRequest optionalRequest = context.getOptionalRequest();
		if (internalMode) {
			optionalRequest.put(OptionalRequestType.SYSTEM_MODE, internalMode);
		}
		if (containerAttribute != null) {
			optionalRequest.put(
					OptionalRequestType.CONTAINER_ATTRIBUTE,
					containerAttribute);
		}
		if (clientIdRequired) {
			optionalRequest.put(
					OptionalRequestType.CLIENT_ID, context.generateClientId());
		}
		if (source != null) {
			source.putOptions(optionalRequest);
		}
		optionalRequest.format(req);
	}

	private static void tryPutDatabaseOptionalRequest(
			BasicBuffer req, Context context, OptionalRequestSource source) {
		if (source == null || !source.hasOptions()) {
			NodeConnection.tryPutEmptyOptionalRequest(req);
			return;
		}

		final OptionalRequest optionalRequest = context.getOptionalRequest();
		source.putOptions(optionalRequest);
		optionalRequest.format(req);
	}

	@Override
	public ContainerInfo getContainerInfo(String name) throws GSException {
		return ContainerProperties.findInfo(getContainerProperties(
				parseContainerKey(name), null, null, false));
	}

	@Override
	public ContainerProperties getContainerProperties(
			ContainerKey key, ContainerProperties.KeySet propKeySet,
			Integer attribute, boolean internalMode) throws GSException {
		return getAllContainerProperties(
				Collections.singletonList(key),
				propKeySet, attribute, internalMode).get(0);
	}

	@Override
	public List<ContainerProperties> getAllContainerProperties(
			List<ContainerKey> keyList, ContainerProperties.KeySet propKeySet,
			Integer attribute, boolean internalMode) throws GSException {
		final int keyCount = keyList.size();

		final Map<Integer, List<Integer>> indexMap =
				new HashMap<Integer, List<Integer>>();
		for (int i = 0; i < keyCount; i++) {
			final int partitionId = channel.resolvePartitionId(
					context, keyList.get(i),
					(WORKAROUND_WRONG_API_TEST ? true : internalMode));
			List<Integer> list = indexMap.get(partitionId);
			if (list == null) {
				list = new ArrayList<Integer>();
				indexMap.put(partitionId, list);
			}
			list.add(i);
		}

		final List<ContainerProperties> propsList =
				new ArrayList<ContainerProperties>();
		for (int i = 0; i < keyCount; i++) {
			propsList.add(null);
		}

		for (Map.Entry<Integer, List<Integer>> entry : indexMap.entrySet()) {
			final int partitionId = entry.getKey();
			final List<Integer> indexList = entry.getValue();
			final int indexCount = indexList.size();

			final List<ContainerKey> subKeyList =
					new ArrayList<ContainerKey>();
			for (int i = 0; i < indexCount; i++) {
				final int index = indexList.get(i);
				subKeyList.add(keyList.get(index));
			}

			final List<ContainerProperties> subPropsList =
					getAllContainerProperties(
							partitionId, subKeyList, propKeySet, attribute,
							internalMode);

			for (int i = 0; i < indexCount; i++) {
				final int index = indexList.get(i);
				propsList.set(index, subPropsList.get(i));
			}
		}

		return propsList;
	}

	private List<ContainerProperties> getAllContainerProperties(
			int partitionId, List<ContainerKey> keyList,
			ContainerProperties.KeySet propKeySet,
			Integer attribute, boolean internalMode) throws GSException {
		if (keyList.isEmpty()) {
			throw new IllegalArgumentException();
		}

		if (propKeySet == null) {
			propKeySet = ContainerPropertyKeysConstants.resolveDefault();
		}

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);

		tryPutSystemOptionalRequest(
				req, context, internalMode, false, attribute,
				getContainerPropertyResuestSource());

		final ContainerKeyConverter keyConverter =
				getContainerKeyConverter(internalMode, false);

		final Integer[] rawPropKeys = propKeySet.getRawKeys();

		final int keyCount = keyList.size();
		for (int i = 0; i < keyCount; i++) {
			putContainerKey(req, keyList.get(i), keyConverter);

			if (i == 0) {
				req.putInt(rawPropKeys.length);
				for (int propKey : rawPropKeys) {
					req.put((byte) propKey);
				}

				if (keyCount > 1) {
					req.putInt(keyCount);
				}
			}
		}

		final ContainerKey singleKey = (keyCount > 1 ? null : keyList.get(0));
		executeStatement(
				Statement.GET_CONTAINER_PROPERTIES,
				partitionId, req, resp, singleKey);

		final List<ContainerProperties> propsList =
				new ArrayList<ContainerProperties>();
		for (int i = 0; i < keyCount; i++) {
			propsList.add(importContainerPropertiesResult(
					resp, attribute, internalMode, keyConverter, rawPropKeys));

			if (i == 0 && keyCount > 1) {
				final int remaining = resp.base().getInt();
				if (remaining != keyCount - 1) {
					throw new GSConnectionException(
							GSErrorCode.MESSAGE_CORRUPTED,
							"Protocol error by illegal result count " +
							"of properties");
				}
			}
		}

		return propsList;
	}

	private OptionalRequestSource getContainerPropertyResuestSource() {
		final EnumSet<ContainerVisibility> visibilities =
				context.getConfig().containerVisibility;
		final MetaNamingType metaNamingType =
				context.getConfig().metaNamingType;

		return new OptionalRequestSource() {
			@Override
			public void putOptions(OptionalRequest optionalRequest) {
				if (!visibilities.isEmpty()) {
					int flags = 0;
					for (ContainerVisibility elem : visibilities) {
						flags |= 1 << elem.ordinal();
					}
					optionalRequest.put(
							OptionalRequestType.CONTAINER_VISIBILITY, flags);
				}
				if (metaNamingType != null) {
					optionalRequest.put(
							OptionalRequestType.META_NAMING_TYPE,
							(byte) metaNamingType.ordinal());
				}
				optionalRequest.putAcceptableFeatureVersion(
						FeatureVersion.V4_3);
			}

			@Override
			public boolean hasOptions() {
				return true;
			}
		};
	}

	private static ContainerProperties importContainerPropertiesResult(
			BasicBuffer resp, Integer attribute, boolean internalMode,
			ContainerKeyConverter keyConverter, Integer[] rawPropKeys)
			throws GSException {

		final boolean found = resp.getBoolean();
		if (!found) {
			return null;
		}

		final int propertyCount = resp.base().getInt();
		if (propertyCount != rawPropKeys.length) {
			throw new GSConnectionException(GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by illegal property count (" +
					"expected=" + rawPropKeys.length +
					", actual=" + propertyCount + ")");
		}

		final ContainerInfo containerInfo = new ContainerInfo();

		ContainerIdInfo containerIdInfo = null;
		List<MutableColumnInfo> columnInfoList = null;
		List<IndexInfo> indexInfoList = null;
		Integer respAttribute = null;
		Map<Integer, byte[]> rawEntries = null;

		for (int i = 0; i < propertyCount; i++) {
			final byte rawPropertyKey = resp.base().get();

			final int propertySize = BufferUtils.getIntSize(resp.base());

			final ContainerProperties.Key propertyKey =
					ContainerProperties.tryResolveKey(rawPropertyKey);
			if (propertyKey == null) {
				final byte[] rawProperty = new byte[propertySize];
				resp.base().get(rawProperty);

				if (rawEntries == null) {
					rawEntries = new HashMap<Integer, byte[]>();
				}
				rawEntries.put((int) rawPropertyKey, rawProperty);
				continue;
			}

			final int curEnd = resp.base().position() + propertySize;

			switch (propertyKey) {
			case ID:
				containerIdInfo = importIdProperty(resp, keyConverter, curEnd);
				break;
			case SCHEMA:
				columnInfoList = new ArrayList<MutableColumnInfo>();
				importSchemaProperty(resp, containerInfo, columnInfoList);
				importContainerProperties(resp, containerInfo, columnInfoList);
				break;
			case INDEX:
				importIndexProperty(resp, columnInfoList);
				break;
			case EVENT_NOTIFICATION:
				
				importEventNotificationProperty(resp, new ArrayList<URL>());
				break;
			case TRIGGER:
				importTriggerProperty(resp, containerInfo, columnInfoList);
				break;
			case ATTRIBUTE:
				respAttribute = importAttributeProperty(resp, attribute);
				break;
			case INDEX_DETAIL:
				indexInfoList = new ArrayList<IndexInfo>();
				importIndexDetailProperty(resp, indexInfoList);
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

		if (containerIdInfo != null) {
			containerInfo.setName(containerIdInfo.remoteKey.toString());
		}

		if (indexInfoList != null) {
			if (columnInfoList != null) {
				assignIndexColumnNames(
						columnInfoList, indexInfoList, respAttribute,
						internalMode);
			}
			containerInfo.setIndexInfoList(indexInfoList);
		}

		if (columnInfoList != null) {
			containerInfo.setColumnInfoList(
					new ArrayList<ColumnInfo>(columnInfoList));
		}

		final ContainerProperties containerProps = new ContainerProperties();
		containerProps.setInfo(containerInfo);
		containerProps.setIdInfo(containerIdInfo);
		containerProps.setAttribute(respAttribute);
		containerProps.setRawEntries(rawEntries);

		return containerProps;
	}

	@Override
	public <K, R> SubnetCollection<K, R> putCollection(
			String name, Class<R> rowType) throws GSException {
		return putCollection(name, rowType, false);
	}

	private static ContainerKey normalizeFullContainerKey(
			ContainerKey key, boolean internalMode) throws GSException {
		return key.toCaseSensitive(false);
	}

	private <K, R> SubnetContainer<K, R> findContainerByCache(
			ContainerCache cache, ContainerKey key,
			Container.BindType<K, R, ?> bindType,
			ContainerType containerType, boolean internalMode)
			throws GSException {
		final ContainerKey normalizedKey =
				normalizeFullContainerKey(key, internalMode);
		final LocatedSchema schema = cache.findSchema(
				normalizedKey, bindType.getRowClass(), containerType);

		if (schema == null) {
			return null;
		}

		final int partitionId = channel.resolvePartitionId(
				context, normalizedKey, internalMode);

		if (schema.getMapper().getContainerType() ==
				ContainerType.COLLECTION) {
			return new SubnetCollection<K, R>(
					this, channel, context, bindType,
					schema.getMapper(), schema.getVersionId(),
					partitionId, schema.getContainerId(), normalizedKey,
					schema.getContainerKey());
		}
		else {
			return disguiseTypedContainer(new SubnetTimeSeries<R>(
					this, channel, context,
					rebindTimeSeriesType(bindType),
					schema.getMapper(), schema.getVersionId(),
					partitionId, schema.getContainerId(), normalizedKey,
					schema.getContainerKey()));
		}
	}

	@Override
	public void removeContainerCache(
			ContainerKey key, boolean internalMode) throws GSException {
		final ContainerCache cache = context.getContainerCache();
		if (cache != null) {
			cache.removeSchema(key.toCaseSensitive(false));
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

	public static boolean isClientIdEnabled() {
		return NodeConnection.getProtocolVersion() >= 13;
	}

	public static boolean isIndexDetailEnabled() {
		return NodeConnection.getProtocolVersion() >= 13;
	}

	public static boolean isContainerAttributeUnified() {
		return NodeConnection.getProtocolVersion() >= 13;
	}

	public static boolean isAttributeVerifiable() {
		return NodeConnection.getProtocolVersion() >= 13;
	}

	public static boolean isNullableColumnAllowed(int protocolVersion) {
		return protocolVersion >= 13;
	}

	public static boolean isQueryOptionsExtensible() {
		return NodeConnection.getProtocolVersion() >= 14 &&
				!GridStoreChannel.v40QueryCompatible;
	}

	public static RowMapper.Config getRowMapperConfig() {
		return getRowMapperConfig(NodeConnection.getProtocolVersion());
	}

	public static RowMapper.Config getRowMapperConfig(int protocolVersion) {
		if (protocolVersion >= 14 &&
				!GridStoreChannel.v40SchemaCompatible) {
			return DEFAULT_MAPPER_CONFIG;
		}
		else if (isNullableColumnAllowed(protocolVersion)) {
			return COMPATIBLE_MAPPER_CONFIG_14;
		}
		else {
			return COMPATIBLE_MAPPER_CONFIG_13;
		}
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

	@Override
	public <K, R> SubnetContainer<K, R> putContainer(
			ContainerKey key, Container.BindType<K, R, ?> bindType,
			ContainerProperties props, boolean modifiable,
			boolean internalMode) throws GSException {
		if (bindType.getRowClass() == Row.class) {
			final Container<K, Row> container = putContainerGeneral(
					key, RowMapper.BindingTool.rebindRow(Row.class, bindType),
					props, modifiable, internalMode);
			return (SubnetContainer<K, R>) (Container<K, R>)
					bindType.castContainer(container);
		}

		final ContainerType containerType =
				RowMapper.BindingTool.findContainerType(bindType);
		final ContainerInfo info = findContainerInfo(props);

		final ContainerKeyConverter keyConverter =
				getContainerKeyConverter(internalMode, true);

		final ContainerCache cache = context.getContainerCache();
		if (cache != null && !modifiable && info == null) {
			final Container<K, R> cachedContainer = findContainerByCache(
					cache, key, bindType, containerType, internalMode);
			if (cachedContainer != null) {
				return (SubnetContainer<K, R>) cachedContainer;
			}
		}

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);

		final RowMapper orgMapper = RowMapper.getInstance(
				bindType, containerType, getRowMapperConfig());
		final OptionalRequestSource propsOption =
				containerPropertiesToOption(props, orgMapper);
		tryPutSystemOptionalRequest(
				req, context, internalMode, true, null, propsOption);

		final int partitionId =
				channel.resolvePartitionId(context, key, internalMode);
		putContainerKey(req, key, keyConverter);
		tryPutContainerType(req, containerType);
		req.putBoolean(modifiable);
		orgMapper.exportSchema(req, getRowMapperConfig());

		if (info != null && (info.getColumnCount() > 0 ||
				!info.getRowKeyColumnList().isEmpty() ||
				info.isColumnOrderIgnorable())) {
			throw new GSException(GSErrorCode.ILLEGAL_SCHEMA,
					"Schema can not be specified on ContainerInfo (" +
					"columnCount=" + info.getColumnCount() + ", " +
					"rowKeyColumnList=" + info.getRowKeyColumnList() + ", " +
					"columnOrderIgnorable=" +
							info.isColumnOrderIgnorable() + ")");
		}
		exportContainerProperties(
				req, containerType, props, orgMapper, key, propsOption);

		final Statement statement = getContainerStatement(
				Statement.PUT_CONTAINER, containerType);

		executeStatement(statement, partitionId, req, resp, key);

		final int schemaVerId = resp.base().getInt();
		final long containerId = resp.base().getLong();

		final ContainerKey[] acceptedKeys =
				acceptRemoteContainerKey(resp, keyConverter, key);
		final ContainerKey normalizedKey = acceptedKeys[0];
		final ContainerKey remoteKey = acceptedKeys[1];

		final RowMapper mapper =
				orgMapper.reorderBySchema(resp, getRowMapperConfig(), true);

		if (cache != null) {
			cache.cacheSchema(normalizedKey, new LocatedSchema(
					mapper, containerId, schemaVerId, remoteKey));
		}

		if (mapper.isForTimeSeries()) {
			return disguiseTypedContainer(new SubnetTimeSeries<R>(
					this, channel, context,
					rebindTimeSeriesType(bindType), mapper, schemaVerId,
					partitionId, containerId, normalizedKey, remoteKey));
		}
		else {
			return new SubnetCollection<K, R>(
					this, channel, context, bindType, mapper, schemaVerId,
					partitionId, containerId, normalizedKey, remoteKey);
		}
	}

	@Override
	public <K, R> SubnetCollection<K, R> putCollection(
			String name, Class<R> rowType,
			boolean modifiable) throws GSException {
		final SubnetContainer<K, R> container = putContainer(
				parseContainerKey(name, false, true),
				RowMapper.BindingTool.createCollectionBindType(
						(Class<K>) null, rowType),
				null, modifiable, false);
		return (SubnetCollection<K, R>) container;
	}

	@Override
	public <R> SubnetTimeSeries<R> putTimeSeries(
			String name, Class<R> rowType)
			throws GSException {
		final SubnetContainer<Date, R> container = putContainer(
				parseContainerKey(name, false, true),
				RowMapper.BindingTool.createTimeSeriesBindType(rowType),
				null, false, false);
		return (SubnetTimeSeries<R>) container;
	}

	@Override
	public <R> SubnetTimeSeries<R> putTimeSeries(
			String name, Class<R> rowType,
			TimeSeriesProperties props, boolean modifiable) throws GSException {
		final ContainerInfo info = new ContainerInfo();
		info.setTimeSeriesProperties(props);

		final SubnetContainer<Date, R> container = putContainer(
				parseContainerKey(name, false, true),
				RowMapper.BindingTool.createTimeSeriesBindType(rowType),
				new ContainerProperties(info), modifiable, false);
		return (SubnetTimeSeries<R>) container;
	}

	private ContainerKey[] acceptRemoteContainerKey(
			BasicBuffer in, ContainerKeyConverter keyConverter,
			ContainerKey localKey) throws GSException {
		final ContainerKey remoteKey = keyConverter.get(in, false, true);
		return filterRemoteContainerKey(remoteKey, localKey);
	}

	private ContainerKey[] filterRemoteContainerKey(
			ContainerKey remoteKey, ContainerKey localKey) throws GSException {

		final ContainerKey normalizedLocalKey =
				localKey.toCaseSensitive(false);
		if (!remoteKey.equals(localKey) &&
				!remoteKey.toCaseSensitive(false).equals(normalizedLocalKey)) {
			throw new GSConnectionException(
					GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by inconsistent container name (" +
					"localName=" + localKey +
					", remoteName=" + remoteKey + ")");
		}

		return new ContainerKey[] {
				(!pathKeyOperationEnabled &&
						context.getContainerCache() == null ?
						null : normalizedLocalKey),
				(contextMonitor == null && !isContainerMonitoring() ?
						null : remoteKey)
		};
	}

	private static OptionalRequestSource containerPropertiesToOption(
				ContainerProperties props, RowMapper mapper)
				throws GSException {
		if (mapper.isDefaultValueSpecified()) {
			throw new GSException(
					GSErrorCode.UNSUPPORTED_OPERATION,
					"Default value can not specified for container " +
					"definition in the current version");
		}

		if (props == null ||
				(props.getReservedValue() == null &&
				props.getSchemaOptions() == null)) {
			return null;
		}

		final OptionalRequest request = new OptionalRequest();
		request.putFeatureVersion(FeatureVersion.V4_1);
		return request;
	}

	private void exportContainerProperties(
			BasicBuffer out, ContainerType type, ContainerProperties props,
			RowMapper mapper, ContainerKey containerKey,
			OptionalRequestSource source) throws GSException {
		exportBasicContainerProperties(
				out, type, findContainerInfo(props), mapper, containerKey);
		if (source == null) {
			return;
		}

		final Integer attribute = findContainerAttribute(props);
		out.putInt((attribute == null ?
				ContainerAttribute.SINGLE.flag() : attribute));

		final Long reservedValue = props.getReservedValue();
		out.putLong((reservedValue == null ? 0 : reservedValue));

		final Map<Integer, byte[]> schemaOptions = props.getSchemaOptions();
		if (schemaOptions != null) {
			for (Map.Entry<Integer, byte[]> entry : schemaOptions.entrySet()) {
				final byte[] value = entry.getValue();
				out.putInt(entry.getKey());
				out.putInt(value.length);

				out.prepare(value.length);
				out.base().put(value);
			}
		}
		out.putInt(-1);
	}

	private void exportBasicContainerProperties(
			BasicBuffer out, ContainerType type, ContainerInfo info,
			RowMapper mapper, ContainerKey containerKey) throws GSException {

		if (isTSDivisionAndAffinityEnabled() &&
				!GridStoreChannel.v20AffinityCompatible) {
			final String dataAffinity =
					(info == null ? null : info.getDataAffinity());
			out.putString(dataAffinity == null ?
					context.getDataAffinityPattern().match(
							containerKey, "",
							getContainerKeyConverter(true, false)) :
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

		if (OLD_TIME_SERIES_PROPERTIES) {
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

	private static void importContainerProperties(
			BasicBuffer in, ContainerInfo containerInfo,
			List<? extends ColumnInfo> columnInfoList) throws GSException {

		if (isTSDivisionAndAffinityEnabled() &&
				!GridStoreChannel.v20AffinityCompatible) {
			final String dataAffinity = in.getString();
			containerInfo.setDataAffinity(
					dataAffinity.isEmpty() ? null : dataAffinity);
		}

		if (containerInfo.getType() != ContainerType.TIME_SERIES) {
			return;
		}

		final boolean exists = in.getBoolean();
		if (!exists) {
			return;
		}

		final TimeSeriesProperties props = new TimeSeriesProperties();
		{
			final int rowExpirationTime = in.base().getInt();
			final TimeUnit timeUnit = in.getByteEnum(TimeUnit.class);

			if (rowExpirationTime > 0) {
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

			if (compressionWindowSize > 0) {
				props.setCompressionWindowSize(compressionWindowSize, timeUnit);
			}
		}

		final CompressionMethod compressionMethod =
				in.getByteEnum(CompressionMethod.class);
		props.setCompressionMethod(compressionMethod);

		final int entryCount = BufferUtils.getNonNegativeInt(in.base());

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
			if (columnId < 0 || columnId >= columnInfoList.size()) {
				throw new GSException(GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by illegal column ID (" +
						"columnId=" + columnId + ", " +
						"columnCount=" + columnInfoList.size() + ")");
			}
			final String columnName = columnInfoList.get(columnId).getName();

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

		containerInfo.setTimeSeriesProperties(props);
	}

	@Override
	public <K, R> SubnetContainer<K, R> getContainer(
			ContainerKey key, Container.BindType<K, R, ?> bindType,
			Integer attribute, boolean internalMode) throws GSException {
		if (bindType.getRowClass() == Row.class) {
			final Container<K, Row> container = getContainerGeneral(
					key, RowMapper.BindingTool.rebindRow(Row.class, bindType),
					attribute, internalMode);
			return (SubnetContainer<K, R>) (Container<K, R>)
					bindType.castContainer(container);
		}

		final ContainerType containerType =
				RowMapper.BindingTool.findContainerType(bindType);
		final ContainerCache cache = context.getContainerCache();
		if (cache != null && !internalMode) {
			final Container<K, R> cachedContainer = findContainerByCache(
					cache, key, bindType, containerType, internalMode);
			if (cachedContainer != null) {
				return (SubnetContainer<K, R>) cachedContainer;
			}
		}

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		tryPutSystemOptionalRequest(
				req, context, internalMode, false, attribute);

		final RowMapper orgMapper = RowMapper.getInstance(
				bindType, containerType, getRowMapperConfig());
		final int partitionId =
				channel.resolvePartitionId(context, key, internalMode);
		final ContainerKeyConverter keyConverter =
				getContainerKeyConverter(internalMode, false);
		putContainerKey(req, key, keyConverter);
		tryPutContainerType(req, containerType);

		final Statement statement = getContainerStatement(
				Statement.GET_CONTAINER, containerType);

		executeStatement(statement, partitionId, req, resp, key);

		final boolean found = resp.getBoolean();
		if (!found) {
			return null;
		}

		final int schemaVerId = resp.base().getInt();
		final long containerId = resp.base().getLong();

		final ContainerKey[] acceptedKeys =
				acceptRemoteContainerKey(resp, keyConverter, key);
		final ContainerKey normalizedKey = acceptedKeys[0];
		final ContainerKey remoteKey = acceptedKeys[1];

		final RowMapper mapper =
				orgMapper.reorderBySchema(resp, getRowMapperConfig(), true);

		if (cache != null && !internalMode) {
			cache.cacheSchema(normalizedKey, new LocatedSchema(
					mapper, containerId, schemaVerId, remoteKey));
		}

		if (mapper.isForTimeSeries()) {
			return disguiseTypedContainer(new SubnetTimeSeries<R>(
					this, channel, context, rebindTimeSeriesType(bindType),
					mapper, schemaVerId,
					partitionId, containerId, normalizedKey, remoteKey));
		}
		else {
			return new SubnetCollection<K, R>(
					this, channel, context, bindType, mapper, schemaVerId,
					partitionId, containerId, normalizedKey, remoteKey);
		}
	}

	@Override
	public <K, R> SubnetCollection<K, R> getCollection(
			String name, Class<R> rowType) throws GSException {
		final SubnetContainer<K, R> container = getContainer(
				parseContainerKey(name),
				RowMapper.BindingTool.createCollectionBindType(
						(Class<K>) null, rowType), null, false);
		return (SubnetCollection<K, R>) container;
	}

	@Override
	public <R> SubnetTimeSeries<R> getTimeSeries(
			String name, Class<R> rowType) throws GSException {
		final SubnetContainer<Date, R> container = getContainer(
				parseContainerKey(name),
				RowMapper.BindingTool.createTimeSeriesBindType(
						rowType), null, false);
		return (SubnetTimeSeries<R>) container;
	}

	@Override
	public void dropCollection(String name) throws GSException {
		dropContainer(
				parseContainerKey(name, false, true),
				ContainerType.COLLECTION, false);
	}

	@Override
	public void dropTimeSeries(String name) throws GSException {
		dropContainer(
				parseContainerKey(name, false, true),
				ContainerType.TIME_SERIES, false);
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

	private static class ContainerPropertyKeysConstants {

		static final ContainerProperties.KeySet DEFAULT = create();

		static final ContainerProperties.KeySet COMPATIBLE_TRIGGER = create(
				ContainerProperties.Key.TRIGGER,
				ContainerProperties.Key.INDEX_DETAIL);

		static final ContainerProperties.KeySet COMPATIBLE_INDEX = create(
				ContainerProperties.Key.INDEX_DETAIL);

		static final ContainerProperties.KeySet FOR_OBJECT =
				new ContainerProperties.KeySet(EnumSet.of(
						ContainerProperties.Key.ID,
						ContainerProperties.Key.SCHEMA,
						ContainerProperties.Key.ATTRIBUTE));

		private static ContainerProperties.KeySet create(
				ContainerProperties.Key ...exclusiveKeys) {
			final EnumSet<ContainerProperties.Key> keySet =
					EnumSet.allOf(ContainerProperties.Key.class);
			for (ContainerProperties.Key key : exclusiveKeys) {
				keySet.remove(key);
			}
			return new ContainerProperties.KeySet(keySet);
		}

		private static ContainerProperties.KeySet resolveDefault() {
			if (isIndexDetailEnabled()) {
				return DEFAULT;
			}
			else if (NodeConnection.getProtocolVersion() < 2 ||
					GridStoreChannel.v1ProtocolCompatible_1_1_103x) {
				return COMPATIBLE_TRIGGER;
			}
			else {
				return COMPATIBLE_INDEX;
			}
		}

	}

	private static ContainerIdInfo importIdProperty(
			BasicBuffer in, ContainerKeyConverter keyConverter, int endPos)
			throws GSException {
		final int versionId = in.base().getInt();
		final long containerId = in.base().getLong();
		final ContainerKey remoteKey = keyConverter.get(in, false, true);

		MetaDistributionType metaDistType = MetaDistributionType.NONE;
		long metaContainerId = -1;
		MetaNamingType metaNamingType = null;
		if (in.base().position() < endPos) {
			metaDistType = in.getByteEnum(MetaDistributionType.class);

			final int size =
					BufferUtils.checkSize(in.base(), in.base().getInt());
			final int orgLimit = BufferUtils.limitForward(in.base(), size);
			metaContainerId = in.base().getLong();
			metaNamingType = in.getByteEnum(MetaNamingType.class);
			BufferUtils.restoreLimitExact(in.base(), orgLimit);
		}

		return new ContainerIdInfo(
				versionId, containerId, remoteKey,
				metaDistType, metaContainerId, metaNamingType);
	}

	private static void importSchemaProperty(
			BasicBuffer in, ContainerInfo containerInfo,
			List<MutableColumnInfo> columnInfoList) throws GSException {
		columnInfoList.clear();
		final RowMapper.Config config = getRowMapperConfig();

		containerInfo.setType(in.getByteEnum(ContainerType.class));

		final int columnCount = RowMapper.importColumnCount(in);
		List<Integer> keyList =
				RowMapper.importKeyListBegin(in, config, columnCount);

		for (int i = 0; i < columnCount; i++) {
			columnInfoList.add(RowMapper.importColumnSchema(in, config));
		}

		keyList = RowMapper.importKeyListEnd(in, config, columnCount, keyList);
		containerInfo.setRowKeyColumnList(keyList);
	}

	private static void importIndexProperty(
			BasicBuffer in, List<MutableColumnInfo> columnInfoList)
			throws GSException {
		final List<Set<IndexType>> indexTypesList =
				new ArrayList<Set<IndexType>>();

		final int columnCount =
				(columnInfoList == null ? -1 : columnInfoList.size());
		for (int i = 0; i < columnCount; i++) {
			indexTypesList.add(Collections.<IndexType>emptySet());
		}

		final int entryCount = BufferUtils.getNonNegativeInt(in.base());
		for (int i = 0; i < entryCount; i++) {
			final int columnId = in.base().getInt();
			if (columnId < 0 ||
					(columnId >= columnCount && columnCount >= 0)) {
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
			if (indexType == IndexType.DEFAULT) {
				throw new GSException(GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by illegal index type");
			}
			indexTypeSet.add(indexType);
		}

		for (int i = 0; i < columnCount; i++) {
			columnInfoList.get(i).setIndexTypes(indexTypesList.get(i));
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
			BasicBuffer in, ContainerInfo containerInfo,
			List<? extends ColumnInfo> columnInfoList) throws GSException {
		final List<TriggerInfo> triggerInfoList = new ArrayList<TriggerInfo>();

		final int entryCount = in.base().getInt();
		if (entryCount < 0) {
			containerInfo.setTriggerInfoList(triggerInfoList);
			return;
		}

		final int schemaColumnCount =
				(columnInfoList == null ? -1 : columnInfoList.size());

		for (int i = 0; i < entryCount; i++) {
			final String name = in.getString();
			final TriggerInfo.Type type = in.getByteEnum(TriggerInfo.Type.class);
			final String uriString = in.getString();
			final int eventTypeFlag = in.base().getInt();
			final Set<TriggerInfo.EventType> eventSet = EnumSet.noneOf(TriggerInfo.EventType.class);
			final Set<TriggerInfo.EventType> allEventSet = EnumSet.allOf(TriggerInfo.EventType.class);
			for (TriggerInfo.EventType e : allEventSet) {
				if ((eventTypeFlag & (1 << e.ordinal())) != 0) {
					eventSet.add(e);
				}
			}
			final Set<String> columnSet = new HashSet<String>();
			final int columnCount = in.base().getInt();
			for (int j = 0; j < columnCount; j++) {
				int columnId = in.base().getInt();
				if (schemaColumnCount < 0) {
					continue;
				}
				if (columnId < 0 || (columnId >= schemaColumnCount &&
						schemaColumnCount >= 0)) {
					throw new GSException(GSErrorCode.MESSAGE_CORRUPTED,
							"Protocol error by illegal column ID (" +
							"entryCount=" + columnId + ", " +
							"columnCount=" + schemaColumnCount + ")");
				}
				final String columnName =
						columnInfoList.get(columnId).getName();
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
		containerInfo.setTriggerInfoList(triggerInfoList);
	}

	private static int importAttributeProperty(
			BasicBuffer in, Integer expectedAttribute) throws GSException {
		final int attribute = in.base().getInt();
		if (expectedAttribute != null && attribute != expectedAttribute) {
			if (!isAttributeVerifiable()) {
				throw new Error();
			}
			throw new GSConnectionException(
					GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by unexpected container attribute (" +
					"expected=" + expectedAttribute +
					", actual=" + attribute + ")");
		}

		return attribute;
	}

	private static void importIndexDetailProperty(
			BasicBuffer in, List<IndexInfo> indexInfoList) throws GSException {
		indexInfoList.clear();

		final int entryCount = BufferUtils.getIntSize(in.base());
		for (int i = 0; i < entryCount; i++) {
			final IndexInfo indexInfo = new IndexInfo();
			importIndexInfo(in, indexInfo, false, false);
			indexInfoList.add(indexInfo);
		}
	}

	private static void assignIndexColumnNames(
			List<MutableColumnInfo> columnInfoList,
			List<IndexInfo> indexInfoList, Integer attribute,
			boolean internalMode) throws GSException {
		if (internalMode && (attribute == null ||
				(attribute != ContainerKeyPredicate.ATTRIBUTE_BASE &&
				attribute != ContainerKeyPredicate.ATTRIBUTE_SINGLE))) {
			return;
		}

		for (IndexInfo indexInfo : indexInfoList) {
			final IndexType type = indexInfo.getType();
			final List<Integer> columnList = indexInfo.getColumnList();

			final List<String> columnNameList =
					new ArrayList<String>(columnList.size());
			for (Integer column : columnList) {
				final MutableColumnInfo columnInfo;
				try {
					columnInfo = columnInfoList.get(column);
				}
				catch (IndexOutOfBoundsException e) {
					throw new GSConnectionException(
							GSErrorCode.MESSAGE_CORRUPTED,
							"Protocol error by illegal index column", e);
				}
				columnNameList.add(columnInfo.getName());

				final Set<IndexType> typeSet = columnInfo.getIndexTypes();
				if (typeSet != null && !typeSet.contains(type)) {
					if (columnList.size() == 1) {
						throw new GSConnectionException(
								GSErrorCode.MESSAGE_CORRUPTED,
								"Protocol error by inconsistent index type (" +
								"type=" + type + ")");
					}
					if (compositeIndexTypeUnified) {
						final Set<IndexType> modTypeSet =
								EnumSet.noneOf(IndexType.class);
						modTypeSet.addAll(typeSet);
						modTypeSet.add(type);
						columnInfo.setIndexTypes(modTypeSet);
					}
				}
			}
			indexInfo.setColumnNameList(columnNameList);
		}
	}

	public static void importIndexInfo(
			BasicBuffer in, IndexInfo indexInfo, boolean withMatchOptions,
			boolean defaultTypeAllowed) throws GSException {
		final int infoSize = in.base().getInt();
		final int orgLimit = BufferUtils.limitForward(in.base(), infoSize);

		final String name = in.getString();
		if (!name.isEmpty()) {
			indexInfo.setName(name);
		}

		final int columnCount = BufferUtils.getNonNegativeInt(in.base());
		final List<Integer> columnList = new ArrayList<Integer>(columnCount);
		for (int i = 0; i < columnCount; i++) {
			final int column = in.base().getInt();
			columnList.add(column);
		}
		indexInfo.setColumnList(columnList);

		final byte rawType = in.base().get();
		final IndexType type;
		if (rawType == INDEX_TYPE_DEFAULT_VALUE) {
			type = IndexType.DEFAULT;
		}
		else {
			try {
				type = INDEX_TYPE_CONSTANTS[rawType & 0xff];
			}
			catch (IndexOutOfBoundsException e) {
				throw new GSException(GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by illegal index type (reason=" +
						e.getMessage() + ")", e);
			}
		}
		if (type == IndexType.DEFAULT && !defaultTypeAllowed) {
			throw new GSException(GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by illegal index type");
		}
		indexInfo.setType(type);

		BufferUtils.restoreLimit(in.base(), orgLimit);

		if (withMatchOptions) {
			final boolean anyNameMatches = in.getBoolean();
			final boolean anyTypeMatches = in.getBoolean();
			if (anyNameMatches) {
				indexInfo.setName(null);
			}
			if (anyTypeMatches) {
				indexInfo.setType(null);
			}
		}
	}

	public static void exportIndexInfo(
			BasicBuffer out, IndexInfo indexInfo, boolean withMatchOptions) {
		final int headPos = out.base().position();
		out.putInt(0);

		final int startPos = out.base().position();

		final String name = indexInfo.getName();
		out.putString((name == null ? "" : name));

		final List<Integer> columnList = indexInfo.getColumnList();
		final int columnCount = columnList.size();
		out.putInt(columnCount);
		for (Integer column : columnList) {
			out.putInt(column);
		}

		final IndexType type = indexInfo.getType();
		if (type == null || type == IndexType.DEFAULT) {
			out.put(INDEX_TYPE_DEFAULT_VALUE);
		}
		else {
			out.putByteEnum(type);
		}

		final int endPos = out.base().position();

		out.base().position(headPos);
		out.putInt(endPos - startPos);
		out.base().position(endPos);

		if (withMatchOptions) {
			final boolean anyNameMatches = (indexInfo.getName() == null);
			final boolean anyTypeMatches = (indexInfo.getType() == null);
			out.putBoolean(anyNameMatches);
			out.putBoolean(anyTypeMatches);
		}
	}

	@SuppressWarnings("unchecked")
	private static <K, R> SubnetContainer<K, R> disguiseTypedContainer(
			SubnetContainer<?, R> container) {
		return (SubnetContainer<K, R>) container;
	}

	private static <R> Container.BindType<Date, R, ? extends Container<?, ?>>
	rebindTimeSeriesType(
			Container.BindType<?, R, ? extends Container<?, ?>> src)
			throws GSException {
		return RowMapper.BindingTool.rebindKey(Date.class, src);
	}

	private ContainerKey resolveContainerKey(
			ContainerKey key, ContainerInfo info,
			ContainerKeyConverter keyConverter) throws GSException {
		final boolean caseSensitive = true;
		final String anotherName = info.getName();

		final ContainerKey specifiedKey = (key == null ?
				null : key.toCaseSensitive(caseSensitive));
		final ContainerKey anotherKey = (anotherName == null ?
				null : keyConverter.parse(anotherName, caseSensitive));

		return resolveName(
				specifiedKey, anotherKey, "container name");
	}

	private static <T> T resolveName(
			T name, T anotherName, String objectName) throws GSException {
		final T another = (anotherName == null ? null : anotherName);

		if (name == null) {
			if (another == null) {
				throw new GSException(GSErrorCode.EMPTY_PARAMETER,
						"Non-specified" + objectName);
			}
			return another;
		}
		else if (another != null) {
			if (!name.equals(another)) {
				throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
						"Inconsistent " + objectName + " (" +
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

	private static ContainerInfo findContainerInfo(ContainerProperties props) {
		if (props == null) {
			return null;
		}
		return props.getInfo();
	}

	private static Integer findContainerAttribute(ContainerProperties props) {
		if (props == null) {
			return null;
		}
		return props.getAttribute();
	}

	private <K> SubnetContainer<K, Row> getContainerGeneral(
			ContainerKey key, Container.BindType<K, Row, ?> bindType,
			Integer attribute, boolean internalMode) throws GSException {
		ContainerType expectedType =
				RowMapper.BindingTool.findContainerType(bindType);
		if (CONTEXT_CONTROLLER_KEY.equals(key)) {
			final SubnetContainer<K, Row> container =
					getContextControllerCollection(expectedType);
			if (container != null) {
				return container;
			}
		}

		final ContainerCache cache = context.getContainerCache();
		if (cache != null) {
			final SubnetContainer<K, Row> cachedContainer =
					findContainerByCache(
							cache, key, bindType, expectedType, internalMode);
			if (cachedContainer != null) {
				return cachedContainer;
			}
		}

		final ContainerProperties containerProps = getContainerProperties(
				key, ContainerPropertyKeysConstants.FOR_OBJECT,
				attribute, internalMode);
		if (containerProps == null) {
			return null;
		}

		final ContainerInfo containerInfo = containerProps.getInfo();
		final ContainerIdInfo idInfo = containerProps.getIdInfo();

		final ContainerType resolvedType =
				resolveContainerType(expectedType, containerInfo);

		final int partitionId =
				channel.resolvePartitionId(context, key, internalMode);
		final RowMapper mapper = RowMapper.getInstance(
				bindType, resolvedType, containerInfo, getRowMapperConfig());

		final ContainerKey[] acceptedKeys =
				filterRemoteContainerKey(idInfo.remoteKey, key);
		final ContainerKey normalizedKey = acceptedKeys[0];
		final ContainerKey remoteKey = acceptedKeys[1];

		if (idInfo.metaDistType != MetaDistributionType.NONE) {
			return disguiseTypedContainer(new MetaCollection<K, Row>(
					this, channel, context, bindType, mapper,
					idInfo.versionId, partitionId,
					idInfo.metaDistType,
					idInfo.metaContainerId, idInfo.metaNamingType,
					normalizedKey, idInfo.remoteKey));
		}

		if (cache != null) {
			cache.cacheSchema(normalizedKey, new LocatedSchema(
					mapper, idInfo.containerId, idInfo.versionId,
					remoteKey));
		}

		if (mapper.isForTimeSeries()) {
			return disguiseTypedContainer(new SubnetTimeSeries<Row>(
					this, channel, context,
					rebindTimeSeriesType(bindType), mapper,
					idInfo.versionId, partitionId, idInfo.containerId,
					normalizedKey, remoteKey));
		}
		else {
			return disguiseTypedContainer(new SubnetCollection<K, Row>(
					this, channel, context, bindType, mapper,
					idInfo.versionId, partitionId, idInfo.containerId,
					normalizedKey, remoteKey));
		}
	}

	public <K> Container<K, Row> getContainer(String name) throws GSException {
		return getContainer(
				parseContainerKey(name),
				RowMapper.BindingTool.createBindType(
						(Class<K>) null, Row.class, Container.class), null, false);
	}

	@Override
	public <K> Collection<K, Row> getCollection(String name)
			throws GSException {
		final Container<K, Row> container = getContainer(
				parseContainerKey(name),
				RowMapper.BindingTool.createCollectionBindType(
						(Class<K>) null, Row.class),
				null, false);
		return (Collection<K, Row>) container;
	}

	@Override
	public TimeSeries<Row> getTimeSeries(String name) throws GSException {
		final Container<Date, Row> container = getContainer(
				parseContainerKey(name),
				RowMapper.BindingTool.createTimeSeriesBindType(Row.class),
				null, false);
		return (TimeSeries<Row>) container;
	}

	private <K> Container<K, Row> putContainerGeneral(
			ContainerKey key, Container.BindType<K, Row, ?> bindType,
			ContainerProperties props, boolean modifiable,
			boolean internalMode) throws GSException {
		ContainerType containerType =
				RowMapper.BindingTool.findContainerType(bindType);

		final ContainerInfo containerInfo = findContainerInfo(props);
		final Integer containerAttribute = findContainerAttribute(props);

		final ContainerKeyConverter keyConverter =
				getContainerKeyConverter(internalMode, true);
		key = resolveContainerKey(key, containerInfo, keyConverter);
		containerType = resolveContainerType(containerType, containerInfo);

		final ContainerCache cache = context.getContainerCache();
		if (cache != null && !modifiable && !internalMode &&
				containerInfo.getTimeSeriesProperties() != null) {
			final Container<K, Row> container = findContainerByCache(
					cache, key, bindType, containerType, internalMode);
			if (container != null) {
				return container;
			}
		}

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);

		final RowMapper orgMapper = RowMapper.getInstance(
				bindType, containerType, containerInfo, getRowMapperConfig());
		final OptionalRequestSource propsOption =
				containerPropertiesToOption(props, orgMapper);
		tryPutSystemOptionalRequest(
				req, context, internalMode, true, containerAttribute,
				propsOption);

		final int partitionId =
				channel.resolvePartitionId(context, key, internalMode);
		putContainerKey(req, key, keyConverter);
		tryPutContainerType(req, containerType);
		req.putBoolean(modifiable);
		orgMapper.exportSchema(req, getRowMapperConfig());

		exportContainerProperties(
				req, containerType, props, orgMapper, key, propsOption);

		final Statement statement = getContainerStatement(
				Statement.PUT_CONTAINER, containerType);

		executeStatement(statement, partitionId, req, resp, key);

		final int schemaVerId = resp.base().getInt();
		final long containerId = resp.base().getLong();

		final ContainerKey[] acceptedKeys =
				acceptRemoteContainerKey(resp, keyConverter, key);
		final ContainerKey normalizedKey = acceptedKeys[0];
		final ContainerKey remoteKey = acceptedKeys[1];

		final RowMapper.Config config = getRowMapperConfig();
		final RowMapper mapper = orgMapper.reorderBySchema(
				resp, config, containerInfo.isColumnOrderIgnorable());

		if (cache != null && !internalMode) {
			cache.cacheSchema(normalizedKey, new LocatedSchema(
					mapper, containerId, schemaVerId, remoteKey));
		}

		if (mapper.isForTimeSeries()) {
			return disguiseTypedContainer(new SubnetTimeSeries<Row>(
					this, channel, context,
					rebindTimeSeriesType(bindType), mapper, schemaVerId,
					partitionId, containerId, normalizedKey, remoteKey));
		}
		else {
			return disguiseTypedContainer(new SubnetCollection<K, Row>(
					this, channel, context, bindType, mapper, schemaVerId,
					partitionId, containerId, normalizedKey, remoteKey));
		}
	}

	@Override
	public <K> Container<K, Row> putContainer(
			String name, ContainerInfo info, boolean modifiable)
			throws GSException {
		final ContainerKey key =
				(name == null ? null : parseContainerKey(name, false, true));
		return putContainer(
				key, RowMapper.BindingTool.createBindType(
						(Class<K>) null, Row.class, Container.class),
				new ContainerProperties(info), modifiable, false);
	}

	@Override
	public <K> Collection<K, Row> putCollection(
			String name, ContainerInfo info, boolean modifiable)
			throws GSException {
		final ContainerKey key =
				(name == null ? null : parseContainerKey(name, false, true));

		final Container<K, Row> container = putContainer(
				key, RowMapper.BindingTool.createCollectionBindType(
						(Class<K>) null, Row.class),
				new ContainerProperties(info), modifiable, false);
		return (Collection<K, Row>) container;
	}

	@Override
	public TimeSeries<Row> putTimeSeries(String name,
			ContainerInfo info, boolean modifiable) throws GSException {
		final ContainerKey key =
				(name == null ? null : parseContainerKey(name, false, true));

		final Container<Date, Row> container = putContainer(
				key, RowMapper.BindingTool.createTimeSeriesBindType(Row.class),
				new ContainerProperties(info), modifiable, false);
		return (TimeSeries<Row>) container;
	}

	@Override
	public void dropContainer(
			ContainerKey key, ContainerType containerType, boolean internalMode)
			throws GSException {
		final ContainerKeyConverter keyConverter =
				getContainerKeyConverter(internalMode, true);

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		tryPutSystemOptionalRequest(req, context, internalMode, false, null);

		putContainerKey(req, key, keyConverter);
		tryPutContainerType(req, containerType);

		final int partitionId =
				channel.resolvePartitionId(context, key, internalMode);
		final Statement statement = getContainerStatement(
				Statement.DROP_CONTAINER, containerType);

		executeStatement(statement, partitionId, req, resp, key);

		final ContainerCache cache = context.getContainerCache();
		if (cache != null) {
			cache.removeSchema(key.toCaseSensitive(false));
		}
	}

	@Override
	public void dropContainer(String name) throws GSException {
		dropContainer(parseContainerKey(name, false, true), null, false);
	}

	@Override
	public <K, R, C extends Container<K, R>> C getContainer(
			String name, Container.BindType<K, R, C> bindType)
			throws GSException {
		final SubnetContainer<K, R> container = getContainer(
				parseContainerKey(name), bindType, null, false);
		return bindType.castContainer(container);
	}

	@Override
	public <K, R, C extends Container<K, R>> C putContainer(
			String name, Container.BindType<K, R, C> bindType)
			throws GSException {
		return putContainer(name, bindType, null, false);
	}

	@Override
	public <K, R, C extends Container<K, R>> C putContainer(
			String name, Container.BindType<K, R, C> bindType,
			ContainerInfo info, boolean modifiable) throws GSException {
		final Container<K, ?> container = putContainer(
				parseContainerKey(name, false, true), bindType,
				new ContainerProperties(info), modifiable, false);
		return bindType.castContainer(container);
	}

	@Override
	public Row createRow(ContainerInfo info) throws GSException {
		channel.checkContextAvailable(context);
		final RowMapper mapper = RowMapper.getInstance(
				info.getType(), info, getRowMapperConfig());
		return mapper.createGeneralRow();
	}

	@Override
	public Row.Key createRowKey(ContainerInfo info) throws GSException {
		channel.checkContextAvailable(context);
		final RowMapper mapper = RowMapper.getInstance(
				info.getType(), info, getRowMapperConfig());
		return mapper.createGeneralRowKey();
	}

	private static class UnnamedMultiOperationContext<K, V, R>
	implements MultiOperationContext<K, V, R> {

		private final Iterable<? extends V> iterable;

		private UnnamedMultiOperationContext(Iterable<? extends V> iterable) {
			this.iterable = iterable;
		}

		@Override
		public void listTarget(MultiTargetConsumer<K, V> consumer)
				throws GSException {
			for (V value : iterable) {
				consumer.consume(null, value, null, null);
			}
		}

		@Override
		public boolean acceptException(GSException exception)
				throws GSException {
			return false;
		}

		@Override
		public void acceptCompletion(K key, R result) throws GSException {
		}

		@Override
		public boolean isRemaining() throws GSException {
			return false;
		}

	}

	private static class NamedMultiOperationContext<V, R>
	implements MultiOperationContext<ContainerKey, V, R> {

		private final Map<String, V> targetMap;

		private final ContainerKeyConverter keyConverter;

		public NamedMultiOperationContext(
				Map<String, V> targetMap,
				ContainerKeyConverter keyConverter) {
			this.targetMap = targetMap;
			this.keyConverter = keyConverter;
		}

		@Override
		public void listTarget(MultiTargetConsumer<ContainerKey, V> consumer)
				throws GSException {
			for (Map.Entry<String, V> entry : targetMap.entrySet()) {
				consumer.consume(
						keyConverter.parse(entry.getKey(), false),
						entry.getValue(), null, null);
			}
		}

		@Override
		public boolean acceptException(GSException exception) {
			return false;
		}

		@Override
		public void acceptCompletion(ContainerKey key, R result) {
		}

		@Override
		public boolean isRemaining() throws GSException {
			return false;
		}

	}

	private static class ListingMultiOperationContext<V, E>
	extends NamedMultiOperationContext<V, List<E>> {

		private final Map<String, List<E>> resultMap =
				new HashMap<String, List<E>>();

		public ListingMultiOperationContext(
				Map<String, V> targetMap, ContainerKeyConverter keyConverter) {
			super(targetMap, keyConverter);
		}

		@Override
		public void acceptCompletion(
				ContainerKey containerKey, List<E> result) {
			final String name = containerKey.toString();
			List<E> list = resultMap.get(name);
			if (list == null) {
				list = new ArrayList<E>();
				resultMap.put(name, list);
			}
			list.addAll(result);
		}

		public Map<String, List<E>> getAllResult() {
			return resultMap;
		}

		public static <V, E> ListingMultiOperationContext<V, E> create(
				Map<String, V> targetMap, ContainerKeyConverter keyConverter) {
			return new ListingMultiOperationContext<V, E>(
					targetMap, keyConverter);
		}

	}

	private abstract static class MultiOperationStatement<K, T> {

		abstract static class Factory<K, T, V, R> {

			private final MultiOperationContext<K, V, R> multiContext;

			private final ContainerKeyConverter keyConverter;

			Factory(
					MultiOperationContext<K, V, R> multiContext,
					ContainerKeyConverter keyConverter) {
				this.multiContext = multiContext;
				this.keyConverter = keyConverter;
			}

			abstract MultiOperationStatement<K, T> create();

			MultiOperationStatement<K, T> resolveMultiStatement(
					Map<Integer, MultiOperationStatement<K, T>> requestMap,
					int partitionId) {
				MultiOperationStatement<K, T> multiStatement =
						requestMap.get(partitionId);
				if (multiStatement == null) {
					multiStatement = create();
					requestMap.put(partitionId, multiStatement);
				}
				return multiStatement;
			}

			MultiOperationContext<K, V, R> getMultiContext() {
				return multiContext;
			}

			ContainerKeyConverter getKeyConverter() {
				return keyConverter;
			}

			abstract MultiTargetConsumer<K, V> createConsumer(
					Map<Integer, MultiOperationStatement<K, T>> requestMap,
					GridStoreChannel channel, Context context,
					boolean internalMode);

		}

		abstract static class BasicFactory<V, R>
		extends Factory<ContainerKey, V, V, R> {

			private BasicFactory(
					MultiOperationContext<ContainerKey, V, R> multiContext,
					ContainerKeyConverter keyConverter) {
				super(multiContext, keyConverter);
			}

			@Override
			MultiTargetConsumer<ContainerKey, V> createConsumer(
					Map<Integer,
					MultiOperationStatement<ContainerKey, V>> requestMap,
					GridStoreChannel channel, Context context,
					boolean internalMode) {
				return new BasicConsumer<V, R>(
						this, requestMap, channel, context, internalMode);
			}

		}

		static class BasicConsumer<V, R>
		implements MultiTargetConsumer<ContainerKey, V> {

			private final BasicFactory<V, R> factory;

			private final Map<
			Integer, MultiOperationStatement<ContainerKey, V>> requestMap;

			private final GridStoreChannel channel;

			private final Context context;

			private final boolean internalMode;

			private BasicConsumer(
					BasicFactory<V, R> factory,
					Map<Integer,
					MultiOperationStatement<ContainerKey, V>> requestMap,
					GridStoreChannel channel, Context context,
					boolean internalMode) {
				this.factory = factory;
				this.requestMap = requestMap;
				this.channel = channel;
				this.context = context;
				this.internalMode = internalMode;
			}

			@Override
			public void consume(
					ContainerKey key, V value, Integer attribute,
					OptionalRequestSource source) throws GSException {
				final int partitionId =
						channel.resolvePartitionId(context, key, internalMode);
				final MultiOperationStatement<ContainerKey, V> multiStatement =
						factory.resolveMultiStatement(requestMap, partitionId);
				multiStatement.addRequestValue(key, value, attribute, source);
			}

		};

		abstract void addRequestValue(
				K key, T value, Integer attribute,
				OptionalRequestSource source) throws GSException;

		boolean makeCreateSessionRequest(
				BasicBuffer req, GridStoreChannel channel, Context context,
				boolean internalMode) throws GSException {
			return false;
		}

		void acceptCreateSessionResponse(BasicBuffer resp)
				throws GSException {
		}

		abstract boolean makeMainRequest(
				BasicBuffer req, GridStoreChannel channel, Context context,
				boolean internalMode) throws GSException;

		abstract void acceptMainResponse(
				BasicBuffer resp, Object targetConnection) throws GSException;

		boolean makeCloseSessionRequest(
				BasicBuffer req, Context context) throws GSException {
			return false;
		}

		void acceptSessionClosed() throws GSException {
		}

		boolean acceptStatementErrorForSession(
				GSStatementException cause) throws GSException {
			return false;
		}

		<R> void repairOperation(MultiOperationContext<K, ?, R> multiContext)
				throws GSException {
		}

	}

	private static class MultiQueryStatement
	extends MultiOperationStatement<Integer, SubnetQuery<?>> {

		private final MultiOperationContext<Integer, ?, Query<?>> multiContext;

		private final List<Integer> keyList = new ArrayList<Integer>();

		private final List<SubnetQuery<?>> queryList =
				new ArrayList<SubnetQuery<?>>();

		private List<SubnetQuery<?>> optionalQueryList;

		boolean updateQueryFound;

		UUID sessionUUID;

		private MultiQueryStatement(
				MultiOperationContext<Integer, ?, Query<?>> multiContext) {
			this.multiContext = multiContext;
		}

		static <V extends Query<?>> MultiQueryFactory<V> newFactory(
				MultiOperationContext<Integer, V, Query<?>> multiContext,
				SubnetGridStore store) {
			return new MultiQueryFactory<V>(multiContext, store);
		}

		static class MultiQueryFactory<V extends Query<?>>
		extends Factory<Integer, SubnetQuery<?>, V, Query<?>> {

			private final SubnetGridStore store;

			MultiQueryFactory(
					MultiOperationContext<Integer, V, Query<?>> multiContext,
					SubnetGridStore store) {
				super(multiContext, null);
				this.store = store;
			}

			@Override
			MultiQueryStatement create() {
				return new MultiQueryStatement(getMultiContext());
			}

			@Override
			MultiTargetConsumer<Integer, V> createConsumer(
					final Map<Integer, MultiOperationStatement<
					Integer, SubnetQuery<?>>> requestMap,
					GridStoreChannel channel, Context context,
					boolean internalMode) {
				return new MultiTargetConsumer<Integer, V>() {
					@Override
					public void consume(
							Integer key, V value, Integer attribute,
							OptionalRequestSource source) throws GSException {
						final SubnetQuery<?> query = check(value, store);
						final int partitionId =
								query.getContainer().getPartitionId();
						resolveMultiStatement(
								requestMap, partitionId).addRequestValue(
										key, query, attribute, source);
					}
				};
			}

		}

		static SubnetQuery<?> check(
				Query<?> query, SubnetGridStore store) throws GSException {
			final Query<?> baseQuery;
			if (!(query instanceof SubnetQuery)) {
				if (query == null) {
					throw new GSException(
							GSErrorCode.EMPTY_PARAMETER,
							"Empty query object");
				}

				if (!(query instanceof Extensibles.AsQuery) ||
						!((baseQuery = ((Extensibles.AsQuery<?>)
								query).getBaseQuery()) instanceof
								SubnetQuery)) {
					throw new GSException(
							GSErrorCode.ILLEGAL_PARAMETER,
							"Query type not matched (class=" +
							query.getClass() + ")");
				}
			}
			else {
				baseQuery = query;
			}

			final SubnetQuery<?> subnetQuery = (SubnetQuery<?>) baseQuery;
			if (subnetQuery.getContainer().getStore() != store) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"Derived GridStore instance not matched");
			}

			return subnetQuery;
		}

		@Override
		void addRequestValue(
				Integer key, SubnetQuery<?> value, Integer attribute,
				OptionalRequestSource source) throws GSException {
			value.getContainer().clearBlob(false);
			updateQueryFound |= value.isForUpdate();
			keyList.add(key);
			queryList.add(value);
		}

		@Override
		boolean makeCreateSessionRequest(
				BasicBuffer req, GridStoreChannel channel, Context context,
				boolean internalMode) throws GSException {
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

			tryPutDatabaseOptionalRequest(req, context, null);

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
		void acceptCreateSessionResponse(BasicBuffer resp)
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
								"Protocol error by inconsistent container ID");
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
		boolean makeMainRequest(
				BasicBuffer req, GridStoreChannel channel, Context context,
				boolean internalMode) throws GSException {
			if (queryList.isEmpty()) {
				return false;
			}

			final boolean summarized = isSessionUUIDSummarized();
			if (summarized) {
				req.putUUID(sessionUUID);
			}

			tryPutDatabaseOptionalRequest(
					req, context, context.bindQueryOptions(null));

			req.putInt(queryList.size());
			for (SubnetQuery<?> query : queryList) {
				final SubnetContainer<?, ?> container = query.getContainer();
				final long statementId;
				if (container.isAutoCommit() ||
						container.getSessionIdDirect() == 0) {
					statementId = 0;
				}
				else {
					statementId = container.updateStatementIdDirect();
				}

				req.putInt(GridStoreChannel.statementToNumber(
						query.getStatement()));
				NodeConnection.putStatementId(req, statementId);
				query.makeRequest(req, summarized);
			}

			return true;
		}

		@Override
		void acceptMainResponse(
				BasicBuffer resp, Object targetConnection) throws GSException {
			final int count = resp.base().getInt();
			if (queryList.size() != count) {
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by inconsistent query count");
			}

			final int totalEndPos = resp.base().limit();
			for (int i = 0; i < count; i++) {
				final int size = (int) BufferUtils.getLongSize(resp.base());

				BufferUtils.limitForward(resp.base(), size);

				final SubnetQuery<?> query = queryList.get(i);
				query.acceptResponse(resp, targetConnection, false);
				multiContext.acceptCompletion(keyList.get(i), query);

				BufferUtils.restoreLimit(resp.base(), totalEndPos);
			}
		}

		@Override
		boolean makeCloseSessionRequest(BasicBuffer req, Context context)
				throws GSException {
			return false;
		}

		@Override
		void acceptSessionClosed() throws GSException {
		}

		@Override
		boolean acceptStatementErrorForSession(
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
		fetchAll(new UnnamedMultiOperationContext<Integer, Query<?>, Query<?>>(
				queryList));
	}

	@Override
	public void fetchAll(
			MultiOperationContext<Integer, Query<?>, Query<?>> multiContext)
			throws GSException {
		final boolean internalMode = false;
		executeAllMulti(
				Statement.EXECUTE_MULTIPLE_QUERIES,
				MultiQueryStatement.newFactory(multiContext, this),
				internalMode);
	}

	private static class MultiPutStatement
	extends MultiOperationStatement<ContainerKey, List<Row>> {

		private static class SubEntry {

			List<Row> rowList;

			int mapperIndex;

			long containerId;

			long sessionId;

			boolean listMerged;

			Integer attribute;

		}

		final List<RowMapper> mapperList = new ArrayList<RowMapper>();

		final List<ContainerKey> containerKeyList =
				new ArrayList<ContainerKey>();

		final Map<ContainerKey, SubEntry> subEntryMap =
				new HashMap<ContainerKey, SubEntry>();

		UUID sessionUUID;

		static BasicFactory<List<Row>, Void> newFactory(
				MultiOperationContext<
				ContainerKey, List<Row>, Void> multiContext) {
			return new BasicFactory<List<Row>, Void>(multiContext, null) {
				@Override
				public MultiPutStatement create() {
					return new MultiPutStatement();
				}
			};
		}

		@Override
		void addRequestValue(
				ContainerKey key, List<Row> value, Integer attribute,
				OptionalRequestSource source) throws GSException {
			if (value.isEmpty()) {
				return;
			}

			final ContainerKey normalizedKey = key.toCaseSensitive(false);
			SubEntry entry = subEntryMap.get(normalizedKey);

			RowMapper mapper = null;
			for (Row row : value) {
				if (mapper == null) {
					final Row lastRow =
							(entry == null ? row : entry.rowList.get(0));
					mapper = RowMapper.getInstance(
							lastRow, getRowMapperConfig());
				}
				else {
					mapper.checkSchemaMatched(RowMapper.getInstance(
							row, getRowMapperConfig()));
				}
			}

			if (entry == null) {
				entry = new SubEntry();
				entry.rowList = value;
				entry.attribute = attribute;
				subEntryMap.put(normalizedKey, entry);
				containerKeyList.add(normalizedKey);

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
				entry.rowList.addAll(value);
			}
		}

		boolean makeCreateSessionRequest(
				BasicBuffer req, GridStoreChannel channel, Context context,
				boolean internalMode) throws GSException {
			sessionUUID = context.getSessionUUID();
			if (containerKeyList.isEmpty()) {
				return false;
			}

			final boolean summarized = isSessionUUIDSummarized();
			if (summarized) {
				req.putUUID(sessionUUID);
			}

			tryPutSystemOptionalRequest(req, context, internalMode, false, null);

			final ContainerKeyConverter keyConverter =
					context.getKeyConverter(internalMode, true);
			final long databaseId = channel.getDatabaseId(context);

			boolean withId = false;
			req.putBoolean(withId);
			req.putInt(containerKeyList.size());
			for (ContainerKey containerKey : containerKeyList) {
				if (isAttributeVerifiable()) {
					final SubEntry entry = subEntryMap.get(containerKey);
					tryPutSystemOptionalRequest(
							req, context, false, false, entry.attribute);
				}

				keyConverter.put(containerKey, databaseId, req);

				if (!summarized) {
					req.putUUID(sessionUUID);
				}

				if (SubnetContainer.isSessionIdGeneratorEnabled()) {
					final SubEntry entry = subEntryMap.get(containerKey);
					entry.sessionId = context.generateSessionId();
					req.putLong(entry.sessionId);
				}
				else {
					SubnetContainer.putNewSessionProperties(req, channel, context);
				}
			}

			return true;
		}

		void acceptCreateSessionResponse(BasicBuffer resp)
				throws GSException {
			final int sessionCount = resp.base().getInt();
			if (subEntryMap.size() != sessionCount) {
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by inconsistent session count");
			}

			for (ContainerKey containerKey : containerKeyList) {
				final SubEntry entry = subEntryMap.get(containerKey);
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

		boolean makeMainRequest(
				BasicBuffer req, GridStoreChannel channel, Context context,
				boolean internalMode) throws GSException {
			if (containerKeyList.isEmpty()) {
				return false;
			}

			final boolean summarized = isSessionUUIDSummarized();
			if (summarized) {
				req.putUUID(sessionUUID);
			}

			tryPutDatabaseOptionalRequest(req, context, null);

			req.putInt(mapperList.size());
			for (RowMapper mapper : mapperList) {
				req.putByteEnum(mapper.isForTimeSeries() ?
						ContainerType.TIME_SERIES : ContainerType.COLLECTION);
				mapper.exportSchema(req, getRowMapperConfig());
			}

			req.putInt(containerKeyList.size());
			for (ContainerKey containerKey : containerKeyList) {
				final SubEntry entry = subEntryMap.get(containerKey);

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
				tryPutSystemOptionalRequest(
						req, context, false, false, entry.attribute);

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

		void acceptMainResponse(
				BasicBuffer resp, Object targetConnection) {
		}

		boolean makeCloseSessionRequest(BasicBuffer req, Context context) {
			if (containerKeyList.isEmpty()) {
				return false;
			}

			final boolean summarized = isSessionUUIDSummarized();
			if (summarized) {
				req.putUUID(sessionUUID);
			}

			tryPutDatabaseOptionalRequest(req, context, null);

			req.putInt(containerKeyList.size());
			for (ContainerKey containerKey : containerKeyList) {
				final SubEntry entry = subEntryMap.get(containerKey);

				NodeConnection.putStatementId(req, 2);
				req.putLong(entry.containerId);
				req.putLong(entry.sessionId);
				if (!summarized) {
					req.putUUID(sessionUUID);
				}
			}

			return true;
		}

		void acceptSessionClosed() {
		}

		boolean acceptStatementErrorForSession(
				GSStatementException cause) throws GSException {
			return SubnetContainer.isInitialSessionLost(
					Statement.CREATE_MULTIPLE_SESSIONS, 1, false, cause);
		}

		@Override
		<R> void repairOperation(
				MultiOperationContext<ContainerKey, ?, R> multiContext)
				throws GSException {
			for (ContainerKey key : subEntryMap.keySet()) {
				multiContext.acceptCompletion(key, null);
			}
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
		multiPut(new NamedMultiOperationContext<List<Row>, Void>(
				containerRowsMap,
				getContainerKeyConverter(false, true)), false);
	}

	@Override
	public void multiPut(
			MultiOperationContext<ContainerKey, List<Row>, Void> multiContext,
			final boolean internalMode) throws GSException {
		executeAllMulti(
				Statement.PUT_MULTIPLE_CONTAINER_ROWS,
				MultiPutStatement.newFactory(multiContext), internalMode);
	}

	private <K, T, V, R> void executeAllMulti(
			final Statement statement,
			final MultiOperationStatement.Factory<K, T, V, R> factory,
			final boolean internalMode) throws GSException {
		final MultiOperationContext<K, V, R> multiContext =
				factory.getMultiContext();

		final Map<Integer, MultiOperationStatement<K, T>> requestMap =
				new HashMap<Integer, MultiOperationStatement<K, T>>();
		final MultiTargetConsumer<K, V> consumer = factory.createConsumer(
				requestMap, channel, context, internalMode);

		beginComplexedStatement(statement, 0, null);

		int lastIncompleteCount = -1;
		int noProgressCount = 0;
		for (int trialCount = 0;; trialCount++) {
			multiContext.listTarget(consumer);

			Set<Integer> incompleteSet = null;
			final Iterator<
			Map.Entry<Integer, MultiOperationStatement<K, T>>> it =
					requestMap.entrySet().iterator();
			while (it.hasNext()) {
				final Map.Entry<
				Integer, MultiOperationStatement<K, T>> entry = it.next();
				final int partitionId = entry.getKey();
				final MultiOperationStatement<K, T> multiStatement =
						entry.getValue();
				try {
					executeMulti(
							partitionId, statement, multiStatement,
							internalMode);
				}
				catch (GSStatementException e) {
					if (multiContext.acceptException(e)) {
						incompleteSet = new HashSet<Integer>();
						incompleteSet.add(partitionId);
						while (it.hasNext()) {
							incompleteSet.add(it.next().getKey());
						}

						if (lastIncompleteCount != incompleteSet.size()) {
							noProgressCount = -1;
						}

						final int maxNoProgressCount = 1;
						if (++noProgressCount <= maxNoProgressCount) {
							lastIncompleteCount = incompleteSet.size();
							LOGGER.info(
									"transaction.repairingMultiOperation",
									ContextMonitor.getObjectId(context),
									statement,
									trialCount,
									lastIncompleteCount,
									e);
							break;
						}
					}
					final Map<String, String> filteredParams =
							filterMultiOperationParamters(e.getParameters());
					throw new GSStatementException(
							0, null,
							formatMultiOperationDescription(e, filteredParams),
							filteredParams, e);
				}
				noProgressCount = 0;
			}

			if (incompleteSet == null) {
				if (multiContext.isRemaining()) {
					requestMap.clear();
					lastIncompleteCount = -1;
					noProgressCount = 0;
					continue;
				}
				break;
			}

			for (Map.Entry<Integer, MultiOperationStatement<K, T>> entry :
					requestMap.entrySet()) {
				if (incompleteSet.contains(entry.getKey())) {
					continue;
				}
				entry.getValue().repairOperation(multiContext);
			}
			requestMap.clear();
		}

		endComplexedStatement();
	}

	private void executeMulti(
			int partitionId, Statement statement,
			MultiOperationStatement<?, ?> multiStatement,
			boolean internalMode) throws GSException {
		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		boolean succeeded = false;
		try {
			for (int trialCount = 0;; trialCount++) {

				for (int sessionTrial = 0;; sessionTrial++) {
					channel.setupRequestBuffer(req);
					if (multiStatement.makeCreateSessionRequest(
							req, channel, context, internalMode)) {

						try {
							channel.executeStatement(
									context,
									Statement.CREATE_MULTIPLE_SESSIONS.
									generalize(),
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
					if (!multiStatement.makeMainRequest(
							req, channel, context, internalMode)) {
						break;
					}

					final Object targetConnection;
					synchronized (context) {
						channel.executeStatement(
								context, statement.generalize(),
								partitionId, 0, req, resp, contextMonitor);
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
							context,
							Statement.CLOSE_MULTIPLE_SESSIONS.generalize(),
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

	private String formatMultiOperationDescription(
			GSException e, Map<String, String> filteredParams) {
		final String description = GSErrorCode.getDescription(e);
		final String containerName =
				filteredParams.get(ERROR_PARAM_CONTAINER);
		final StringBuilder sb = new StringBuilder();
		if (description != null) {
			sb.append(description);
			sb.append(" ");
		}
		sb.append("(containerName=");
		if (containerName == null) {
			sb.append("(unidentified)");
		}
		else {
			sb.append(containerName);
		}
		sb.append(")");
		return sb.toString();
	}

	private Map<String, String> filterMultiOperationParamters(
			Map<String, String> src) {
		final String orgContainerName = src.get(ERROR_PARAM_CONTAINER);
		if (orgContainerName == null) {
			return src;
		}

		try {
			final ContainerKeyConverter keyConverter =
					getContainerKeyConverter(true, false);
			final ContainerKeyConverter.Components components =
					new ContainerKeyConverter.Components();

			keyConverter.decompose(
					keyConverter.parse(orgContainerName, true), components);
			if (components.largeId == -1 && components.subCount == -1) {
				return src;
			}

			components.largeId = -1;
			components.subCount = -1;
			components.affinityNum = -1;
			components.affinityStr = null;
			final String filteredContainerName =
					keyConverter.compose(components, true).toString();

			final Map<String, String> dest = GSErrorCode.newParameters(src);
			dest.put(ERROR_PARAM_CONTAINER, filteredContainerName);
			return dest;
		}
		catch (GSException e) {
		}

		return src;
	}

	private enum PredicateType {

		RANGE,

		DISTINCT

	}

	private static class MultiGetRequest<V extends RowKeyPredicate<?>>
	extends MultiOperationStatement<ContainerKey, V> {

		private static class SubEntry {

			ContainerKey containerKey;

			Integer attribute;

			OptionalRequestSource source;

			int predicateIndex;

		}

		final MultiOperationContext<ContainerKey, V, List<Row>> multiContext;

		final List<RowKeyPredicate<?>> predicateList =
				new ArrayList<RowKeyPredicate<?>>();

		final List<SubEntry> entryList = new ArrayList<SubEntry>();

		final ContainerKeyConverter keyConverter;

		MultiGetRequest(
				MultiOperationContext<ContainerKey, V, List<Row>> multiContext,
				ContainerKeyConverter keyConverter) {
			this.multiContext = multiContext;
			this.keyConverter = keyConverter;
		}

		static <V extends RowKeyPredicate<?>>
		BasicFactory<V, List<Row>> newFactory(
				MultiOperationContext<ContainerKey, V, List<Row>> multiContext,
				ContainerKeyConverter keyConverter) {
			return new BasicFactory<V, List<Row>>(multiContext, keyConverter) {
				@Override
				public MultiGetRequest<V> create() {
					return new MultiGetRequest<V>(
							getMultiContext(), getKeyConverter());
				}
			};
		}

		@Override
		void addRequestValue(
				ContainerKey key, V value, Integer attribute,
				OptionalRequestSource source) throws GSException {
			final SubEntry entry = new SubEntry();

			entry.containerKey = key;
			entry.predicateIndex = predicateList.indexOf(value);
			if (entry.predicateIndex < 0) {
				entry.predicateIndex = predicateList.size();
				predicateList.add(value);
			}
			entry.attribute = attribute;
			entry.source = source;

			entryList.add(entry);
		}

		@Override
		boolean makeMainRequest(
				BasicBuffer req, GridStoreChannel channel, Context context,
				boolean internalMode) throws GSException {
			if (entryList.isEmpty()) {
				return false;
			}

			if (isSessionUUIDSummarized()) {
				req.putUUID(context.getSessionUUID());
			}

			tryPutSystemOptionalRequest(req, context, internalMode, false, null);

			final RowMapper.MappingMode mappingMode =
					SubnetContainer.getRowMappingMode();

			req.putInt(predicateList.size());
			for (RowKeyPredicate<?> predicate : predicateList) {
				final RowMapper mapper =
						RowMapper.getInstance(predicate, ANY_MAPPER_CONFIG);
				final boolean composite = (mapper.getKeyCategory() ==
						RowMapper.KeyCategory.COMPOSITE);

				mapper.exportKeySchemaSingle(req);

				final java.util.Collection<?> distinctKeys =
						predicate.getDistinctKeys();
				final Object startKey = predicate.getStart();
				final Object finishKey = predicate.getFinish();

				java.util.Collection<?> keys = null;

				if (distinctKeys == null) {
					req.putByteEnum(PredicateType.RANGE);
					mapper.exportKeySchemaComposite(req);

					java.util.Collection<Object> rangeKeys = null;
					if (composite) {
						rangeKeys = new ArrayList<Object>();
					}

					req.putBoolean(startKey != null);
					if (startKey != null) {
						if (composite) {
							rangeKeys.add(startKey);
						}
						else {
							mapper.encodeKey(req, startKey, mappingMode);
						}
					}

					req.putBoolean(finishKey != null);
					if (finishKey != null) {
						if (composite) {
							rangeKeys.add(finishKey);
						}
						else {
							mapper.encodeKey(req, finishKey, mappingMode);
						}
					}

					keys = rangeKeys;
				}
				else {
					req.putByteEnum(PredicateType.DISTINCT);
					mapper.exportKeySchemaComposite(req);

					if (composite) {
						keys = distinctKeys;
					}
					else {
						req.putInt(distinctKeys.size());
						for (Object key : distinctKeys) {
							mapper.encodeKey(req, key, mappingMode);
						}
					}
				}

				if (keys != null) {
					mapper.encodeKey(req, keys, mappingMode, true, true);
				}
			}

			final long databaseId = channel.getDatabaseId(context);

			req.putInt(entryList.size());
			for (SubEntry entry : entryList) {
				if (isAttributeVerifiable()) {
					tryPutSystemOptionalRequest(
							req, context, false, false, entry.attribute,
							entry.source);
				}

				keyConverter.put(entry.containerKey, databaseId, req);
				req.putInt(entry.predicateIndex);
			}

			return true;
		}

		@Override
		void acceptMainResponse(BasicBuffer resp, Object targetConnection)
				throws GSException {
			final int headCount = resp.base().getInt();
			final List<RowMapper> mapperList = new ArrayList<RowMapper>();
			for (int i = 0; i < headCount; i++) {
				final ContainerType containerType =
						resp.getByteEnum(ContainerType.class);
				mapperList.add(RowMapper.getInstance(
						resp, containerType, getRowMapperConfig()));
			}

			final List<Row> rowList = new ArrayList<Row>();
			final int bodyCount = resp.base().getInt();
			for (int i = 0; i < bodyCount; i++) {
				final ContainerKey containerKey =
						keyConverter.get(resp, false, true);

				final int mapperIndex = resp.base().getInt();
				final RowMapper mapper = mapperList.get(mapperIndex);

				final boolean rowIdIncluded = !mapper.isForTimeSeries();
				final int rowCount = (int) resp.base().getLong();
				final Cursor cursor = mapper.createCursor(
						resp, SubnetContainer.getRowMappingMode(), rowCount,
						rowIdIncluded, RowMapper.getDirectBlobFactory());

				for (int j = 0; j < rowCount; j++) {
					rowList.add((Row) mapper.decode(cursor));
				}

				multiContext.acceptCompletion(containerKey, rowList);
				rowList.clear();
			}
		}

	}

	@Override
	public Map<String, List<Row>> multiGet(
			Map<String, ? extends RowKeyPredicate<?>> containerPredicateMap)
			throws GSException {
		final ContainerKeyConverter keyConverter =
				getContainerKeyConverter(false, false);
		final ListingMultiOperationContext<
		? extends RowKeyPredicate<?>, Row> multiContext =
				ListingMultiOperationContext.create(
						containerPredicateMap, keyConverter);

		multiGet(multiContext, false);
		return multiContext.getAllResult();
	}

	@Override
	public void multiGet(
			MultiOperationContext<ContainerKey,
			? extends RowKeyPredicate<?>, List<Row>> multiContext,
			boolean internalMode) throws GSException {
		final ContainerKeyConverter keyConverter =
				getContainerKeyConverter(internalMode, false);
		executeAllMulti(
				Statement.GET_MULTIPLE_CONTAINER_ROWS,
				MultiGetRequest.newFactory(multiContext, keyConverter),
				internalMode);
	}

	@Override
	public void setContainerMonitoring(boolean monitoring) {
		context.getConfig().containerMonitoring = monitoring;
	}

	@Override
	public boolean isContainerMonitoring() {
		return context.getConfig().containerMonitoring;
	}

	@Override
	public SubnetPartitionController getPartitionController()
			throws GSException {
		channel.checkContextAvailable(context);
		return new SubnetPartitionController(channel, context);
	}

	private static final ContainerKey CONTEXT_CONTROLLER_KEY =
			createSystemContainerKey("#_internal_context_controller");

	private static ContainerKey createSystemContainerKey(String name) {
		final ContainerKeyConverter keyConverter =
				ContainerKeyConverter.getInstance(0, true, true);
		try {
			return keyConverter.parse(name, false);
		}
		catch (GSException e) {
			throw new Error(e);
		}
	}

	private <K> SubnetCollection<K, Row> getContextControllerCollection(
			ContainerType expectedType) throws GSException {
		if (expectedType != null && expectedType != ContainerType.COLLECTION) {
			return null;
		}

		final RowMapper mapper = RowMapper.getInstance(null, new ContainerInfo(
				null, null, Arrays.asList(
						new ColumnInfo("name", GSType.STRING),
						new ColumnInfo("value", GSType.STRING)),
						true),
				getRowMapperConfig());

		final Container.BindType<K, Row, ?> bindType =
				RowMapper.BindingTool.createCollectionBindType(
						(Class<K>) null, Row.class);
		return new SubnetCollection<K, Row>(
				this, channel, context, bindType, mapper, -1, -1, -1,
				CONTEXT_CONTROLLER_KEY, null) {

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
		final ContainerKeyConverter keyConverter =
				getContainerKeyConverter(false, true);
		return putContainer(
				resolveContainerKey(
						keyConverter.parse(name, false), info, keyConverter),
				RowMapper.BindingTool.createBindType(
						(Class<K>) null, rowType, Container.class),
				new ContainerProperties(info), modifiable, false);
	}

	private void setUserInfoRequest(BasicBuffer req, String userName)
			throws GSException {
		
		RowMapper.checkSymbol(userName, "user name");
		req.putString(userName);		
	}

	private void setUserInfoRequest(
			BasicBuffer req, String userName, byte prop, String hashPassword)
			throws GSException {

		

		
		setUserInfoRequest(req, userName);	
		req.put(prop);		

		if (hashPassword != null) {
			if (hashPassword.isEmpty()) {
				throw new GSException(
						GSErrorCode.EMPTY_PARAMETER, "Empty hash password");
			}
			req.putBoolean(true);		
			req.putString(hashPassword);
		}
		else {
			req.putBoolean(false);		
			
		}
	}

	private Map<String, UserInfo> getUserInfoMap(BasicBuffer resp) throws GSException {

		Map<String, UserInfo> map = new HashMap<String, UserInfo>();

		
		final int userInfoCount = resp.base().getInt();	

		
		if (userInfoCount == 0) {
			return map;
		}

		List<UserInfo> list = new LinkedList<UserInfo>();
		for (int i = 0; i < userInfoCount; i++) {

			String userName = resp.getString();		
			Byte property = resp.base().get();		
			Boolean passwordExist = resp.getBoolean();	
			String tmp = resp.getString();
			String hashPassword = "";

			if (passwordExist) {
				hashPassword = tmp;
			}
			list.add(new UserInfo(userName, hashPassword, (Boolean)(property != 0), false, ""));
		}
		for (int i = 0; i < userInfoCount; i++) {
			Boolean isGroupMapping = resp.getBoolean();
			String roleName = resp.getString();
			
			if (list.get(i).getName().length() > 0) {
				map.put(list.get(i).getName(), new UserInfo(list.get(i).getName(), list.get(i).getHashPassword(), list.get(i).isSuperUser(), isGroupMapping, roleName));
			} else {
				map.put(roleName, new UserInfo(list.get(i).getName(), list.get(i).getHashPassword(), list.get(i).isSuperUser(), isGroupMapping, roleName));
			}
		}
		return map;
	}

	public void putUser(String name, UserInfo userInfo, boolean modifiable)
			throws GSException {

		GSErrorCode.checkNullParameter(userInfo, "userInfo", null);

		
		name = resolveName(name, userInfo.getName(), "user name");

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);

		NodeConnection.tryPutEmptyOptionalRequest(req);

		final int partitionId = SYSTEM_USER_PARTITION_ID;

		final String password = userInfo.getPassword();
		final String hashPassword = userInfo.getHashPassword();

		if (password != null && hashPassword == null) {
			RowMapper.checkString(password, "password");

			final int bytesLength =
					password.getBytes(BasicBuffer.DEFAULT_CHARSET).length;
			if (bytesLength > MAX_PASSWORD_BYTES_LENGTH) {
				throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
						"The length of password string bytes exceeded (max=" +
						MAX_PASSWORD_BYTES_LENGTH + ")");
			}

			setUserInfoRequest(
					req, name, (byte) 0, NodeConnection.getDigest(password));
		}
		else if (password == null && hashPassword != null) {
			setUserInfoRequest(req, name, (byte) 0, hashPassword);
		}
		else if (password == null) {
			throw new GSException(GSErrorCode.EMPTY_PARAMETER,
					"Password and hash password not specified");
		}
		else {
			throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
					"Both password and hash password specified");
		}

		req.putBoolean(modifiable);

		executeStatement(Statement.PUT_USER, partitionId, req, resp, null);
	}

	public void dropUser(String name) throws GSException {
		GSErrorCode.checkNullParameter(name, "name", null);

		
		name = resolveName(name, null, "user name");

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);

		NodeConnection.tryPutEmptyOptionalRequest(req);

		final int partitionId = SYSTEM_USER_PARTITION_ID;

		setUserInfoRequest(req, name);

		executeStatement(Statement.DROP_USER, partitionId, req, resp, null);
	}

	public Map<String, UserInfo> getUsers() throws GSException {

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);

		OptionalRequest opt = new OptionalRequest();
		opt.putAcceptableFeatureVersion(FeatureVersion.V4_5);
		opt.format(req);

		final int partitionId = SYSTEM_USER_PARTITION_ID;

		req.putBoolean(false);	
		
		req.put((byte) 0);		

		executeStatement(Statement.GET_USERS, partitionId, req, resp, null);

		Map<String, UserInfo> map = getUserInfoMap(resp);

		return map;
	}

	public UserInfo getCurrentUser() throws GSException {

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);

		OptionalRequest opt = new OptionalRequest();
		opt.putAcceptableFeatureVersion(FeatureVersion.V4_5);
		opt.format(req);

		final int partitionId = SYSTEM_USER_PARTITION_ID;

		req.putBoolean(true);	
		req.putString(context.getUser());	
		req.put((byte) 0);		

		executeStatement(Statement.GET_USERS, partitionId, req, resp, null);

		Map<String, UserInfo> map = getUserInfoMap(resp);

		
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

	private void setDatabaseInfoRequest(BasicBuffer req, String dbName)
			throws GSException {
		RowMapper.checkSymbol(dbName, "database name");
		req.putString(dbName);		
	}

	private void setDatabaseInfoRequest(
			BasicBuffer req, String dbName, byte prop,
			Map<String, PrivilegeInfo> privileges) throws GSException {

		

		
		setDatabaseInfoRequest(req, dbName);	
		req.put(prop);		

		if (privileges != null) {
			req.putInt(privileges.size());	

			
			for (Entry<String, PrivilegeInfo> privilegeEntry : privileges.entrySet()) {

				RowMapper.checkSymbol(privilegeEntry.getKey(), "privilege name");
				req.putString(privilegeEntry.getKey());	
				PrivilegeInfo privilegeInfo = privilegeEntry.getValue();
				if (privilegeInfo == null) {
					throw new GSException(
							GSErrorCode.EMPTY_PARAMETER,
							"Privilege info not specified");
				}
				PrivilegeInfo.RoleType role = privilegeInfo.getRole();
				if (role == RoleType.ALL) {
					req.putString(ALL_PRIVILEGE);
				} else {
					req.putString(READ_PRIVILEGE);
				}
			}
		}
		else {
			req.putInt(0);	
		}
	}

	private void setDatabaseInfoRequest(
			BasicBuffer req, String dbName, byte prop, String userName,
			PrivilegeInfo info) throws GSException {
		RowMapper.checkSymbol(userName, "user name");

		Map<String, PrivilegeInfo> map = new HashMap<String, PrivilegeInfo>();
		map.put(userName, info);
		setDatabaseInfoRequest(req, dbName, (byte) 0, map);
	}

	private Map<String, DatabaseInfo> getDatabaseInfoMap(BasicBuffer resp)
			throws GSException {

		Map<String, DatabaseInfo> map = new HashMap<String, DatabaseInfo>();

		
		final int databaseInfoCount = resp.base().getInt();	

		
		if (databaseInfoCount == 0) {
			return map;
		}

		
		for (int i = 0; i < databaseInfoCount; i++) {

			final String databaseName = resp.getString();	
			resp.base().get();		

			final int privilegeInfoCount = resp.base().getInt();	

			final Map<String, PrivilegeInfo> privilegeInfoMap =
					new HashMap<String, PrivilegeInfo>();
			for (int j = 0; j < privilegeInfoCount; j++) {
				final String userName = resp.getString();		
				final String privilegeData = resp.getString();	

				PrivilegeInfo privilege = new PrivilegeInfo();
				if (privilegeData.equals(ALL_PRIVILEGE)) {
					privilege.setRole(PrivilegeInfo.RoleType.ALL);
				}
				privilegeInfoMap.put(userName, privilege);
			}

			
			map.put(databaseName, new DatabaseInfo(databaseName, privilegeInfoMap));
		}

		return map;
	}

	public void putDatabase(String name, DatabaseInfo info, boolean modifiable)
			throws GSException {
		GSErrorCode.checkNullParameter(info, "info", null);

		
		name = resolveName(name, info.getName(), "database name");

		
		modifiable = false;

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);

		NodeConnection.tryPutEmptyOptionalRequest(req);

		final int partitionId = SYSTEM_USER_PARTITION_ID;

		
		setDatabaseInfoRequest(req, name, (byte) 0, null);
		req.putBoolean(modifiable);

		executeStatement(Statement.PUT_DATABASE, partitionId, req, resp, null);
	}

	public void dropDatabase(String name) throws GSException {
		GSErrorCode.checkNullParameter(name, "name", null);

		final DatabaseInfo info = getCurrentDatabase();
		if (!RowMapper.normalizeSymbolUnchecked(info.getName()).equals(
				RowMapper.normalizeSymbolUnchecked(name))) {
			throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
					"Only connected database can be dropped (" +
					"connected=" + info.getName() +
					", specified=" + name + ")");
		}

		final SubnetPartitionController partitionController =
				getPartitionController();
		final int partitionCount = partitionController.getPartitionCount();

		final EnumSet<ContainerAttribute> attributeSet = EnumSet.of(
				ContainerAttribute.BASE,
				ContainerAttribute.SINGLE_SYSTEM,
				ContainerAttribute.SINGLE,
				ContainerAttribute.LARGE,
				ContainerAttribute.SUB);
		if (isContainerAttributeUnified()) {
			attributeSet.remove(ContainerAttribute.BASE);
		}

		final ContainerKeyPredicate pred =
				new ContainerKeyPredicate(attributeSet);

		for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
			final long containerCount =
					partitionController.getContainerCount(partitionIndex, pred, true);
			if (0 < containerCount) {
				throw new GSException(GSErrorCode.DATABASE_NOT_EMPTY,
						"Non-empty database cannot be dropped");
			}
		}

		
		name = resolveName(name, null, "database name");

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);

		NodeConnection.tryPutEmptyOptionalRequest(req);

		final int partitionId = SYSTEM_USER_PARTITION_ID;

		setDatabaseInfoRequest(req, name);

		executeStatement(
				Statement.DROP_DATABASE, partitionId, req, resp, null);
	}

	public Map<String, DatabaseInfo> getDatabases() throws GSException {

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);

		OptionalRequest opt = new OptionalRequest();
		opt.putAcceptableFeatureVersion(FeatureVersion.V4_5);
		opt.format(req);

		final int partitionId = SYSTEM_USER_PARTITION_ID;

		req.putBoolean(false);	
		
		req.put((byte) 0);		

		executeStatement(
				Statement.GET_DATABASES, partitionId, req, resp, null);

		Map<String, DatabaseInfo> map = getDatabaseInfoMap(resp);

		return map;
	}

	public DatabaseInfo getCurrentDatabase() throws GSException {

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);

		OptionalRequest opt = new OptionalRequest();
		opt.putAcceptableFeatureVersion(FeatureVersion.V4_5);
		opt.format(req);

		final int partitionId = SYSTEM_USER_PARTITION_ID;

		final String dbName = context.getDatabaseName();

		req.putBoolean(true);	
		req.putString(dbName == null ? "" : dbName);	
		req.put((byte) 0);		

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
						"expected=" + dbName + ")");
			}
		}
		else {
			
			throw new GSConnectionException(GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by illegal database info count (" +
					"expected=1" + ", actual=" + map.size() + ")");
		}

		return info;
	}

	public void putPrivilege(String dbName, String userName, PrivilegeInfo info)
			throws GSException {
		GSErrorCode.checkNullParameter(dbName, "dbName", null);
		GSErrorCode.checkNullParameter(userName, "userName", null);
		GSErrorCode.checkNullParameter(info, "info", null);

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);

		OptionalRequest opt = new OptionalRequest();
		if (info.getRole() == RoleType.READ) {
			opt.putFeatureVersion(FeatureVersion.V4_3);
		}
		opt.format(req);

		final int partitionId = SYSTEM_USER_PARTITION_ID;

		setDatabaseInfoRequest(req, dbName, (byte) 0, userName, info);

		executeStatement(
				Statement.PUT_PRIVILEGE, partitionId, req, resp, null);
	}

	public void dropPrivilege(String dbName, String userName, PrivilegeInfo info)
			throws GSException {
		GSErrorCode.checkNullParameter(dbName, "dbName", null);
		GSErrorCode.checkNullParameter(userName, "userName", null);
		GSErrorCode.checkNullParameter(info, "info", null);

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);

		NodeConnection.tryPutEmptyOptionalRequest(req);

		final int partitionId = SYSTEM_USER_PARTITION_ID;

		setDatabaseInfoRequest(req, dbName, (byte) 0, userName, info);

		executeStatement(
				Statement.DROP_PRIVILEGE, partitionId, req, resp, null);
	}

	@Override
	public Experimentals.AsStore getExperimentalStore() {
		return this;
	}

}
