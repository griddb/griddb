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

import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;

import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.ColumnInfo;
import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.Container.BindType;
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
import com.toshiba.mwcloud.gs.common.BasicBuffer;
import com.toshiba.mwcloud.gs.common.BasicBuffer.BufferUtils;
import com.toshiba.mwcloud.gs.common.ContainerKeyConverter;
import com.toshiba.mwcloud.gs.common.ContainerKeyConverter.ContainerKey;
import com.toshiba.mwcloud.gs.common.ContainerKeyPredicate;
import com.toshiba.mwcloud.gs.common.ContainerProperties;
import com.toshiba.mwcloud.gs.common.ContainerProperties.ContainerIdInfo;
import com.toshiba.mwcloud.gs.common.ContainerProperties.KeySet;
import com.toshiba.mwcloud.gs.common.Extensibles;
import com.toshiba.mwcloud.gs.common.Extensibles.MultiOperationContext;
import com.toshiba.mwcloud.gs.common.Extensibles.MultiTargetConsumer;
import com.toshiba.mwcloud.gs.common.Extensibles.StatementHandler;
import com.toshiba.mwcloud.gs.common.GSErrorCode;
import com.toshiba.mwcloud.gs.common.PropertyUtils.WrappedProperties;
import com.toshiba.mwcloud.gs.common.RowMapper;
import com.toshiba.mwcloud.gs.common.Statement.GeneralStatement;
import com.toshiba.mwcloud.gs.experimental.Experimentals;
import com.toshiba.mwcloud.gs.partitioned.PartQuery.PartitionedMultiQueryContext;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel;
import com.toshiba.mwcloud.gs.subnet.NodeConnection;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.BytesRequestFormatter;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.OptionalRequest;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.OptionalRequestSource;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.RequestFormatter;
import com.toshiba.mwcloud.gs.subnet.NodeResolver.ContainerHashMode;
import com.toshiba.mwcloud.gs.subnet.SubnetGridStore;

class PartStore implements GridStore,
Extensibles.AsStore, Experimentals.StoreProvider {

	private static final int CONTAINER_PROP_KEY_LARGE_INFO = 16;

	private static final int CONTAINER_NOT_FOUND_ERROR_CODE = 10016;

	private static final int ATTRIBUTE_UNMATCH_ERROR_CODE = 10093;

	private static final int SUB_CONTAINER_EXPIRATION_ERROR_CODE = 60161;

	private static final int SUB_CONTAINER_VERSION_ERROR_CODE = 270004;

	static final boolean LARGE_INFO_EXIPIRABLE_DEFAULT = true;

	static final boolean WORKAROUND_WRONG_API_TEST = false;

	static final boolean WORKAROUND_NORMALIZE = true;

	private static final ContainerProperties.KeySet CONTAINER_PROP_KEYS_MIN =
			new ContainerProperties.KeySet(
					Collections.<ContainerProperties.Key>emptySet());

	private static final ContainerProperties.KeySet CONTAINER_PROP_KEYS_FULL =
			new ContainerProperties.KeySet(
					EnumSet.allOf(ContainerProperties.Key.class),
					CONTAINER_PROP_KEY_LARGE_INFO);

	private static final ContainerProperties.KeySet CONTAINER_PROP_KEYS_LARGE =
			new ContainerProperties.KeySet(
					EnumSet.of(
							ContainerProperties.Key.ID,
							ContainerProperties.Key.SCHEMA,
							ContainerProperties.Key.ATTRIBUTE),
					CONTAINER_PROP_KEY_LARGE_INFO);

	private final Extensibles.AsStore base;

	private final Extensibles.AsPartitionController partitionController;

	private final Config config;

	private final LargeInfoCache largeInfoCache;

	PartStore(
			Extensibles.AsStore base, Properties props) throws GSException {
		this.base = base;
		partitionController = base.getPartitionController();

		config = new Config();
		config.set(new WrappedProperties(props));

		largeInfoCache = new LargeInfoCache(
				config.largeInfoCacheSize, config.largeInfoExpirationMillis);
	}

	@SuppressWarnings("deprecation")
	@Override
	public boolean put(String pathKey, Object rowObject) throws GSException {
		return base.put(pathKey, rowObject);
	}

	@SuppressWarnings("deprecation")
	@Override
	public Object get(String pathKey) throws GSException {
		return base.get(pathKey);
	}

	@SuppressWarnings("deprecation")
	@Override
	public <R> R get(String pathKey, Class<R> rowType) throws GSException {
		return base.get(pathKey, rowType);
	}

	@SuppressWarnings("deprecation")
	@Override
	public boolean remove(String pathKey) throws GSException {
		return base.remove(pathKey);
	}

	@Override
	public ContainerInfo getContainerInfo(String name) throws GSException {
		GSErrorCode.checkNullParameter(name, "name", null);
		final ContainerKeyConverter keyConverter =
				getContainerKeyConverter(WORKAROUND_NORMALIZE, false);
		final ContainerKey key = keyConverter.parse(name, false);

		return ContainerProperties.findInfo(
				getContainerProperties(key, keyConverter, null));
	}

	@Override
	public <K, R> Collection<K, R> putCollection(String name, Class<R> rowType)
			throws GSException {
		return base.putCollection(name, rowType);
	}

	@Override
	public <K, R> Collection<K, R> putCollection(
			String name, Class<R> rowType,
			boolean modifiable) throws GSException {
		return base.putCollection(name, rowType, modifiable);
	}

	@Override
	public <R> TimeSeries<R> putTimeSeries(String name, Class<R> rowType)
			throws GSException {
		return base.putTimeSeries(name, rowType);
	}

	@Override
	public <R> TimeSeries<R> putTimeSeries(
			String name, Class<R> rowType, TimeSeriesProperties props,
			boolean modifiable) throws GSException {
		return base.putTimeSeries(name, rowType, props, modifiable);
	}

	@Override
	public <K, R> Collection<K, R> getCollection(String name, Class<R> rowType)
			throws GSException {
		return getPartitionedContainer(
				name, RowMapper.BindingTool.createCollectionBindType(
						(Class<K>) null, rowType), false);
	}

	@Override
	public <R> TimeSeries<R> getTimeSeries(String name, Class<R> rowType)
			throws GSException {
		return getPartitionedContainer(
				name, RowMapper.BindingTool.createTimeSeriesBindType(
						rowType), false);
	}

	@Override
	public void dropCollection(String name) throws GSException {
		base.dropCollection(name);
	}

	@Override
	public void dropTimeSeries(String name) throws GSException {
		base.dropTimeSeries(name);
	}

	@Override
	public void close() throws GSException {
		base.close();
	}

	@Override
	public <K> Container<K, Row> putContainer(
			String name, ContainerInfo info, boolean modifiable)
			throws GSException {
		return base.putContainer(name, info, modifiable);
	}

	@Override
	public <K> Container<K, Row> getContainer(String name) throws GSException {
		return getPartitionedContainer(
				name, RowMapper.BindingTool.createBindType(
						(Class<K>) null, Row.class, Container.class), true);
	}

	@Override
	public <K> Collection<K, Row> putCollection(
			String name, ContainerInfo info, boolean modifiable)
			throws GSException {
		return base.putCollection(name, info, modifiable);
	}

	@Override
	public <K> Collection<K, Row> getCollection(String name)
			throws GSException {
		return getPartitionedContainer(
				name, RowMapper.BindingTool.createCollectionBindType(
						(Class<K>) null, Row.class), true);
	}

	@Override
	public TimeSeries<Row> putTimeSeries(
			String name, ContainerInfo info, boolean modifiable)
			throws GSException {
		return base.putTimeSeries(name, info, modifiable);
	}

	@Override
	public TimeSeries<Row> getTimeSeries(String name) throws GSException {
		return getPartitionedContainer(
				name, RowMapper.BindingTool.createTimeSeriesBindType(
						Row.class), true);
	}

	@Override
	public void dropContainer(String name) throws GSException {
		base.dropContainer(name);
	}

	@Override
	public <K, R, C extends Container<K, R>> C getContainer(
			String name, Container.BindType<K, R, C> bindType)
			throws GSException {
		final boolean general = (bindType.getRowClass() == Row.class);
		return getPartitionedContainer(name, bindType, general);
	}

	@Override
	public <K, R, C extends Container<K, R>> C putContainer(
			String name, BindType<K, R, C> bindType) throws GSException {
		return base.putContainer(name, bindType);
	}

	@Override
	public <K, R, C extends Container<K, R>> C putContainer(
			String name, Container.BindType<K, R, C> bindType,
			ContainerInfo info, boolean modifiable) throws GSException {
		return base.putContainer(name, bindType, info, modifiable);
	}

	@Override
	public Row createRow(ContainerInfo info) throws GSException {
		return base.createRow(info);
	}

	@Override
	public Row.Key createRowKey(ContainerInfo info) throws GSException {
		return base.createRowKey(info);
	}

	@Override
	public void fetchAll(
			List<? extends Query<?>> queryList) throws GSException {
		base.fetchAll(new PartitionedMultiQueryContext(this, queryList));
	}

	@Override
	public void multiPut(Map<String, List<Row>> containerRowsMap)
			throws GSException {
		boolean succeeded = false;
		try {
			base.multiPut(new PartitionedMultiOperationContext<
					List<Row>, Void>(new RowPartitioner(), containerRowsMap),
					true);
			succeeded = true;
		}
		finally {
			if (!succeeded) {
				disableCache(
						containerRowsMap,
						new GeneralKeyConverter.ForString(), true);
			}
		}
	}

	@Override
	public Map<String, List<Row>> multiGet(
			Map<String, ? extends RowKeyPredicate<?>> containerPredicateMap)
			throws GSException {
		boolean succeeded = false;
		try {
			final ListingMultiOperationContext<
			RowKeyPredicate<?>, Row> multiContext =
					new ListingMultiOperationContext<RowKeyPredicate<?>, Row>(
							new PredicatePartitioner(), containerPredicateMap);

			base.multiGet(multiContext, true);
			succeeded = true;

			return multiContext.getResultMap();
		}
		finally {
			if (!succeeded) {
				disableCache(
						containerPredicateMap,
						new GeneralKeyConverter.ForString(), true);
			}
		}
	}

	@Override
	public PartPartitionController getPartitionController()
			throws GSException {
		return new PartPartitionController(base.getPartitionController());
	}

	@Override
	public void executeStatement(
			GeneralStatement statement, int partitionIndex,
			StatementHandler handler) throws GSException {
		base.executeStatement(statement, partitionIndex, handler);
	}

	@Override
	public long getDatabaseId() throws GSException {
		return base.getDatabaseId();
	}

	@Override
	public ContainerKeyConverter getContainerKeyConverter(
			boolean internalMode, boolean forModification) throws GSException {
		return base.getContainerKeyConverter(internalMode, forModification);
	}

	@Override
	public <K, R> Container<K, R> putContainer(
			String name, Class<R> rowType,
			ContainerInfo info, boolean modifiable) throws GSException {
		return base.putContainer(name, rowType, info, modifiable);
	}

	@Override
	public ContainerProperties getContainerProperties(
			ContainerKey key, KeySet propKeySet,
			Integer attribute, boolean internalMode) throws GSException {
		return base.getContainerProperties(
				key, propKeySet, attribute, internalMode);
	}

	@Override
	public List<ContainerProperties> getAllContainerProperties(
			List<ContainerKey> keyList, KeySet propKeySet, Integer attribute,
			boolean internalMode) throws GSException {
		return base.getAllContainerProperties(
				keyList, propKeySet, attribute, internalMode);
	}

	@Override
	public <K, R> Extensibles.AsContainer<K, R> getContainer(
			ContainerKey key, Container.BindType<K, R, ?> bindType,
			Integer attribute, boolean internalMode) throws GSException {
		return base.getContainer(key, bindType, attribute, internalMode);
	}

	@Override
	public <K, R> Container<K, R> putContainer(
			ContainerKey key, Container.BindType<K, R, ?> bindType,
			ContainerProperties props, boolean modifiable,
			boolean internalMode) throws GSException {
		return base.putContainer(key, bindType, props,  modifiable, internalMode);
	}

	@Override
	public void dropContainer(
			ContainerKey key, ContainerType containerType, boolean internalMode)
			throws GSException {
		base.dropContainer(key, containerType, internalMode);
	}

	@Override
	public void removeContainerCache(ContainerKey key, boolean internalMode)
			throws GSException {
		base.removeContainerCache(key, internalMode);
	}

	@Override
	public void fetchAll(
			MultiOperationContext<Integer, Query<?>, Query<?>> multiContext)
			throws GSException {
		base.fetchAll(multiContext);
	}

	@Override
	public void multiPut(
			MultiOperationContext<ContainerKey, List<Row>, Void> multiContext,
			boolean internalMode) throws GSException {
		base.multiPut(multiContext, internalMode);
	}

	@Override
	public void multiGet(
			MultiOperationContext<ContainerKey,
			? extends RowKeyPredicate<?>, List<Row>> multiContext,
			boolean internalMode) throws GSException {
		base.multiGet(multiContext, internalMode);
	}

	@Override
	public void setContainerMonitoring(boolean monitoring) {
		base.setContainerMonitoring(monitoring);
	}

	@Override
	public boolean isContainerMonitoring() {
		return base.isContainerMonitoring();
	}

	@Override
	public Experimentals.AsStore getExperimentalStore() throws GSException {
		return Experimentals.get(base);
	}

	private <K> void disableCache(
			Map<K, ?> map, GeneralKeyConverter<K> converter, boolean silent)
			throws GSException {
		final ContainerKeyConverter keyConverter =
				getContainerKeyConverter(true, false);
		for (K key : map.keySet()) {
			try {
				removeLargeInfo(converter.convert(key, keyConverter), null);
			}
			catch (GSException e) {
				if (!silent) {
					throw e;
				}
			}
		}
	}

	private <K, R, C extends Container<K, R>> C getPartitionedContainer(
			String name, final Container.BindType<K, R, C> bindType,
			boolean general) throws GSException {
		GSErrorCode.checkNullParameter(name, "name", null);

		final ContainerKeyConverter keyConverter =
				getContainerKeyConverter(WORKAROUND_WRONG_API_TEST, false);
		final ContainerKey normalizedKey = keyConverter.parse(name, false);

		for (boolean firstTrial = true;; firstTrial = false) {
			final SchemaPropertiesResolver<
			Extensibles.AsContainer<K, R>> resolver =
					new SchemaPropertiesResolver<
					Extensibles.AsContainer<K, R>>() {
				@Override
				public ContainerProperties resolve(ContainerKey key)
						throws GSException {
					final int attribute = PartContainer.Attributes.SUB;
					final boolean internalMode = true;
					final Extensibles.AsContainer<K, R> container =
							base.getContainer(
									key, bindType, attribute, internalMode);
					setSource(container);
					if (container == null) {
						return null;
					}
					return container.getSchemaProperties();
				}
			};

			boolean cacheConsistent = false;
			LargeInfo largeInfo = null;
			try {
				largeInfo = getLargeInfo(normalizedKey, true, keyConverter, resolver);
				if (largeInfo == null) {
					return null;
				}

				final C container;
				if (largeInfo.isLarge()) {
					try {
						container = getPartitionedContainerInternal(
								normalizedKey, bindType, general, largeInfo,
								resolver.getSource());
					}
					catch (ContainerPartitioningException e) {
						if (firstTrial) {
							continue;
						}
						throw e.toGSException();
					}
					catch (ContainerExpirationException e) {
						if (firstTrial) {
							continue;
						}
						throw e.toGSException();
					}
				}
				else {
					container = getSingleContainer(normalizedKey, bindType);
				}

				if (container == null) {
					if (firstTrial) {
						continue;
					}
				}
				else {
					cacheConsistent = true;
				}

				return container;
			}
			catch (GSException e) {
				if (!firstTrial ||
						e.getErrorCode() != ATTRIBUTE_UNMATCH_ERROR_CODE) {
					throw e;
				}
			}
			finally {
				if (!cacheConsistent) {
					try {
						if (largeInfo != null) {
							removeLargeInfo(normalizedKey, largeInfo);
						}
					}
					finally {
						final Container<?, ?> subSontainer =
								resolver.getSource();
						if (subSontainer != null) {
							subSontainer.close();
						}
					}
				}
			}
		}
	}

	private <K, R, C extends Container<K, R>> C getPartitionedContainerInternal(
			ContainerKey key, Container.BindType<K, R, C> bindType,
			boolean general, LargeInfo largeInfo,
			Extensibles.AsContainer<K, R> fixedSubContainer)
			throws GSException, ContainerPartitioningException,
			ContainerExpirationException {
		final LargeInfo[] largeInfoRef = { largeInfo };
		final long subId = largeInfo.getFixedSubId();
		Extensibles.AsContainer<K, R> localSubContainer = null;
		try {
			final Extensibles.AsContainer<K, R> subContainer;
			if (fixedSubContainer == null) {
				localSubContainer = getSubContainer(
						key, largeInfoRef, subId, bindType, general, false, false);
				if (localSubContainer == null) {
					return null;
				}
				subContainer = localSubContainer;
			}
			else {
				subContainer = fixedSubContainer;
			}

			final PartContainer<K, R> container;
			if (subContainer.getType() == ContainerType.TIME_SERIES) {
				container = disguiseTypedContainer(
						new PartContainer.PartTimeSeries<R>(
								this, key, largeInfoRef[0],
								RowMapper.BindingTool.rebindKey(
										Date.class, bindType)));
			}
			else {
				container = new PartContainer.PartCollection<K, R>(
						this, key, largeInfoRef[0], bindType);
			}
			container.setSub(subId, subContainer);
			localSubContainer = null;

			return bindType.castContainer(container);
		}
		finally {
			if (localSubContainer != null) {
				localSubContainer.close();
			}
		}
	}

	<K, R, C extends Container<K, R>> C getSingleContainer(
			ContainerKey key, Container.BindType<K, R, C> bindType)
			throws GSException {
		final Integer attribute = (SubnetGridStore.isAttributeVerifiable() ?
				ContainerKeyPredicate.ATTRIBUTE_SINGLE : null);
		final boolean internalMode = SubnetGridStore.isAttributeVerifiable();
		return bindType.castContainer(base.getContainer(
				key, bindType, attribute, internalMode));
	}

	<K, R> Extensibles.AsContainer<K, R> getSubContainer(
			ContainerKey largeKey, LargeInfo[] largeInfoRef, long subId,
			Container.BindType<K, R, ?> bindType,
			boolean general, boolean always, boolean checkOnly)
			throws GSException, ContainerPartitioningException,
			ContainerExpirationException {
		final int attribute = PartContainer.Attributes.SUB;
		final boolean internalMode = true;
		final ContainerKeyConverter keyConverter =
				getContainerKeyConverter(internalMode, false);
		final ContainerKey subKey =
				largeInfoRef[0].generateSubKey(largeKey, keyConverter, subId);

		boolean revovered = false;
		for (int trialCount = 0;; trialCount++) {
			if (general && bindType.getRowClass() != Row.class) {
				throw new IllegalArgumentException();
			}
			final Extensibles.AsContainer<K, R> container = base.getContainer(
					subKey, bindType, attribute, internalMode);

			if (container == null && always) {
				if (trialCount == 0 ||
						(isPartitionedContainerRecoverable() && !revovered)) {
					if (trialCount > 0) {
						if (!checkOnly) {
							recoverSubContainer(
									largeKey, subId,
									largeInfoRef[0].containerId);
						}
						revovered = true;
					}

					final LargeInfo nextInfo = createSubContainer(
							largeKey, subId, largeInfoRef[0], checkOnly);
					if (nextInfo != null) {
						largeInfoRef[0] = nextInfo;
						continue;
					}
				}
				throw new GSException(
						GSErrorCode.PARTITION_NOT_AVAILABLE,
						"Partitioned container is not available (" +
						"name=" + largeKey + ", subName=" + subKey + ")");
			}

			return container;
		}
	}

	private LargeInfo createSubContainer(
			ContainerKey largeKey, long subId, LargeInfo info,
			boolean checkOnly)
			throws GSException, ContainerPartitioningException,
			ContainerExpirationException {
		removeLargeInfo(largeKey.toCaseSensitive(false), info);

		final boolean internalMode = true;
		final ContainerKeyConverter keyConverter =
				base.getContainerKeyConverter(internalMode, true);
		final LargeInfo[] infoRef = new LargeInfo[1];
		final ContainerProperties props =
				getContainerProperties(largeKey, keyConverter, infoRef);

		LargeInfo nextInfo = infoRef[0];
		if (nextInfo == null || !nextInfo.isLarge()) {
			throw new ContainerPartitioningException(
					GSErrorCode.UNSUPPORTED_OPERATION,
					"Partitioned container unexpectedly lost (" +
					"name=" + largeKey + ")", null);
		}

		props.getInfo().setName(null);
		props.setIdInfo(null);
		props.setAttribute(PartContainer.Attributes.SUB);
		props.setSchemaOptions(null);
		setSubContainerExpiration(nextInfo, subId, props);

		final long intervalSubId =
				info.subIdFromInterval(info.intervalFromSubId(subId), 0);
		final SubOperationStatus status = new SubOperationStatus(
				SubOperation.CREATE_END, intervalSubId, info);
		status.accept(nextInfo);

		if (checkOnly) {
			return nextInfo.isAvailable(subId) ? nextInfo : null;
		}
		for (;;) {
			if (nextInfo.isExpired(subId)) {
				throw new ContainerExpirationException(
					GSErrorCode.INTERNAL_ERROR, "", null);
			}

			final Long nextSubId = status.getNextSubId();
			if (nextSubId == null) {
				break;
			}

			final SubOperation nextOp = status.getNextOperation();
			putSubOperationState(
					largeKey, nextSubId, nextOp, nextInfo.containerId);

			switch (nextOp) {
			case CREATE_BEGIN:
				operateSubContainerDefinition(
						largeKey, nextSubId, nextInfo, keyConverter, props,
						true);
				break;
			case DROP_BEGIN:
				operateSubContainerDefinition(
						largeKey, nextSubId, nextInfo, keyConverter, props,
						false);
				break;
			case INDEX_CREATE_BEGIN:
				operateSubContainerIndex(
						largeKey, nextInfo, keyConverter, true);
				break;
			case INDEX_DROP_BEGIN:
				operateSubContainerIndex(
						largeKey, nextInfo, keyConverter, false);
				break;
			default:
				break;
			}

			removeLargeInfo(largeKey.toCaseSensitive(false), null);
			nextInfo = takeLargeInfo(largeKey, false, keyConverter);
			status.accept(nextInfo);
		}

		return nextInfo;
	}

	private void operateSubContainerDefinition(
			ContainerKey largeKey, long subId, LargeInfo info,
			ContainerKeyConverter keyConverter,
			ContainerProperties props, boolean forCreation)
			throws GSException {
		final boolean internalMode = true;
		final long interval = info.intervalFromSubId(subId);
		final int unit = info.getPartitioningUnit();

		for (int i = 0; i < unit; i++) {
			final ContainerKey subKey = info.generateSubKey(
					largeKey, keyConverter,
					info.subIdFromInterval(interval, i));
			if (forCreation) {
				putContainerAndProperties(subKey, props, internalMode);
			}
			else {
				base.dropContainer(subKey, null, internalMode);
			}
		}

		if (forCreation) {
			final long version = info.partitioningVersion + 1;

			for (int i = 0; i < 2; i++) {
				final long neighborId =
						info.findNeighborSub(true, (i == 0), subId);
				if (neighborId < 0) {
					continue;
				}
				final ContainerKey neighborKey = info.generateSubKey(
						largeKey, keyConverter, neighborId);
				putSubContainerVersion(neighborKey, version);
			}
		}
	}

	private void operateSubContainerIndex(
			ContainerKey largeKey, LargeInfo info,
			ContainerKeyConverter keyConverter, boolean forCreation)
			throws GSException {
		final boolean internalMode = true;
		final long version = info.partitioningVersion + 1;

		final OptionalRequest optionalRequest = new OptionalRequest();
		optionalRequest.putExt(
				ExtRequestOptionType.SUB_CONTAINER_VERSION.number(),
				getSubContainerVersionFormatter(version).format());

		for (Long subId : info) {
			final ContainerKey subKey = info.generateSubKey(
					largeKey, keyConverter, subId);
			putSubContainerVersion(subKey, version);

			final Extensibles.AsContainer<?, Row> sub = base.getContainer(
					subKey, BindType.of(null, Row.class),
					PartContainer.Attributes.SUB, internalMode);

			if (forCreation) {
				sub.createIndex(info.operatingIndex, optionalRequest);
			}
			else {
				sub.dropIndex(info.operatingIndex, optionalRequest);
			}
		}
	}

	private void putContainerAndProperties(
			ContainerKey key, ContainerProperties props, boolean internalMode)
			throws GSException {
		final Container<?, ?> container = base.putContainer(
				key, 
				RowMapper.BindingTool.createBindType(
						null, Row.class, Container.class),
				props, false, internalMode);
		try {
			for (IndexInfo indexInfo : props.getInfo().getIndexInfoList()) {
				container.createIndex(indexInfo);
			}
		}
		finally {
			container.close();
		}
	}

	static boolean isSubContainerExpirationError(GSException exception) {
		return exception.getErrorCode() == SUB_CONTAINER_EXPIRATION_ERROR_CODE;
	}

	static boolean isSubContainerVersionError(GSException exception) {
		return exception.getErrorCode() == SUB_CONTAINER_VERSION_ERROR_CODE;
	}

	static BytesRequestFormatter getSubContainerVersionFormatter(
			final long version) {
		return new BytesRequestFormatter() {
			@Override
			public void format(BasicBuffer buf) throws GSException {
				buf.putLong(version);
			}
		};
	}

	static OptionalRequestSource getSubContainerVersionOptionSource(
			long version) throws GSException {
		final byte[] optionValue =
				getSubContainerVersionFormatter(version).format();
		return new OptionalRequestSource() {
			@Override
			public void putOptions(OptionalRequest optionalRequest) {
				optionalRequest.putExt(
						ExtRequestOptionType.SUB_CONTAINER_VERSION.number(),
						optionValue);
			}

			@Override
			public boolean hasOptions() {
				return true;
			}
		};
	}

	private void putSubContainerVersion(ContainerKey key, final long version)
			throws GSException {
		final RequestFormatter formatter =
				getSubContainerVersionFormatter(version);
		alternateContainer(
				key, AlternationType.PUT_SUB_CONTAINER_VERSION, formatter);
	}

	private static void setSubContainerExpiration(
			LargeInfo info, long subId, ContainerProperties props)
			throws GSException {
		if (!info.isExpirable()) {
			return;
		}
		final long[] range = info.intervalValueRangeFromSubId(subId);
		final long start = range[0];
		final long interval = range[1] - range[0] + 1;
		final long duration = info.expirationTimeMillis;
		setContainerSchemaOption(
				props, ContainerSchemaOptionType.PARTITION_EXPIRATION,
				getSubContainerExpirationFormatter(start, interval, duration));
	}

	private static BytesRequestFormatter getSubContainerExpirationFormatter(
			final long start, final long interval, final long duration) {
		return new BytesRequestFormatter() {
			@Override
			public void format(BasicBuffer buf) throws GSException {
				buf.putLong(start);
				buf.putLong(interval);
				buf.putLong(duration);
			}
		};
	}

	private static void setContainerSchemaOption(
			ContainerProperties props, ContainerSchemaOptionType type,
			BytesRequestFormatter formatter) throws GSException {
		Map<Integer, byte[]> options = props.getSchemaOptions();
		if (options == null) {
			options = new HashMap<Integer, byte[]>();
			props.setSchemaOptions(options);
		}
		options.put(type.number(), formatter.format());
	}

	private void putSubOperationState(
			ContainerKey key, final long subId, final SubOperation state,
			final long largeContainerId) throws GSException {
		final RequestFormatter formatter = new RequestFormatter() {
			@Override
			public void format(BasicBuffer buf) {
				if (isPartitionedContainerRecoverable()) {
					buf.putLong(largeContainerId);
				}
				buf.putLong(subId);
				buf.putByteEnum(state);
			}
		};
		alternateContainer(key, AlternationType.PUT_SUB_STATE, formatter);
	}

	private void recoverSubContainer(
			ContainerKey key, final long subId,
			final long largeContainerId) throws GSException {
		final RequestFormatter formatter = new RequestFormatter() {
			@Override
			public void format(BasicBuffer buf) {
				buf.putLong(largeContainerId);
				buf.putLong(subId);
			}
		};
		alternateContainer(
				key, AlternationType.RECOVER_SUB_CONTAINER, formatter);
	}

	private void alternateContainer(
			final ContainerKey key, final AlternationType alternationType,
			final RequestFormatter formatter)
			throws GSException {
		final int partitionIndex =
				partitionController.getPartitionIndexOfContainer(key, true);
		final StatementHandler handler = new StatementHandler() {

			@Override
			public void makeRequest(BasicBuffer buf) throws GSException {
				NodeConnection.tryPutEmptyOptionalRequest(buf);

				final long databaseId = base.getDatabaseId();
				getContainerKeyConverter(true, true).put(key, databaseId, buf);

				buf.putByteEnum(alternationType);

				final int headPos = buf.base().position();
				buf.putInt(0);
				final int bodyPos = buf.base().position();
				formatter.format(buf);
				final int endPos = buf.base().position();

				buf.base().position(headPos);
				buf.putInt(endPos - bodyPos);
				buf.base().position(endPos);
			}

			@Override
			public void acceptResponse(BasicBuffer buf) throws GSException {
			}

		};

		base.executeStatement(
				PartitionedStatement.ALTER_CONTAINER_PROPERTIES.generalize(),
				partitionIndex, handler);
	}

	private ContainerProperties getContainerProperties(
			ContainerKey key, ContainerKeyConverter keyConverter,
			LargeInfo[] largeInfoRef) throws GSException {

		final boolean withLargeInfo = isLargePropKeyEnabled();
		final boolean internalMode = true;
		final ContainerProperties props = base.getContainerProperties(
				key, (withLargeInfo ? CONTAINER_PROP_KEYS_FULL : null),
				null, internalMode);

		final LargeInfoBuilder largeInfoBuilder = createLargeInfoBuilder(key, props);
		if (largeInfoBuilder == null) {
			return null;
		}
		else if (largeInfoBuilder.isLarge()) {
			final ContainerProperties subProps = base.getContainerProperties(
					largeInfoBuilder.getFixedSubKey(keyConverter),
					CONTAINER_PROP_KEYS_FULL, null, internalMode);
			if (!largeInfoBuilder.setFixedSubProperties(subProps)) {
				return null;
			}

			props.setInfo(
					mergeContainerInfo(subProps.getInfo(), props.getInfo()));
		}

		if (largeInfoRef != null) {
			largeInfoRef[0] = largeInfoBuilder.build();
		}

		return props;
	}

	boolean checkLargeInfoExpiration(LargeInfo largeInfo) throws GSException {
		return largeInfoCache.checkExpiration(
				largeInfo, largeInfoCache.currentNanosForExpiration());
	}

	LargeInfo takeLargeInfo(
			ContainerKey key, boolean emptyAllowed,
			ContainerKeyConverter keyConverter)
			throws GSException, ContainerPartitioningException {
		final LargeInfo largeInfo = getLargeInfo(
				key.toCaseSensitive(false), emptyAllowed, keyConverter);
		if (largeInfo == null) {
			throw new ContainerPartitioningException(
					GSErrorCode.UNSUPPORTED_OPERATION,
					"Partitioned container unexpectedly lost (" +
					"name=" + key + ")", null);
		}
		return largeInfo;
	}

	LargeInfo getLargeInfo(
			ContainerKey normalizedKey, boolean emptyAllowed,
			ContainerKeyConverter keyConverter) throws GSException {
		return getLargeInfo(normalizedKey, emptyAllowed, keyConverter, null);
	}

	private LargeInfo getLargeInfo(
			ContainerKey normalizedKey, boolean emptyAllowed,
			ContainerKeyConverter keyConverter,
			SchemaPropertiesResolver<?> resolver) throws GSException {
		LargeInfo largeInfo = largeInfoCache.find(
				normalizedKey, largeInfoCache.currentNanosForExpiration());
		if (largeInfo == null || !largeInfo.isStable(emptyAllowed)) {
			final ContainerProperties props = base.getContainerProperties(
					normalizedKey, getLargeInfoPropKeys(), null, true);
			final LargeInfoBuilder largeInfoBuilder =
					createLargeInfoBuilder(normalizedKey, props);

			if (largeInfoBuilder != null && largeInfoBuilder.isLarge()) {
				final SchemaPropertiesResolver<?> localResolver;
				if (resolver == null) {
					final boolean internalMode = true;
					localResolver = new SchemaPropertiesResolver<Object>() {
						@Override
						public ContainerProperties resolve(ContainerKey key)
								throws GSException {
							return base.getContainerProperties(
									key, CONTAINER_PROP_KEYS_FULL, null,
									internalMode);
						}
					};
				}
				else {
					localResolver = resolver;
				}

				final ContainerKey subKey =
						largeInfoBuilder.getFixedSubKey(keyConverter);
				largeInfoBuilder.setFixedSubProperties(
						localResolver.resolve(subKey));
			}
			if (largeInfoBuilder != null) {
				largeInfo = largeInfoBuilder.build();
			}

			if (largeInfo != null && largeInfo.isStable(true)) {
				largeInfoCache.add(normalizedKey, largeInfo);
			}
		}
		return largeInfo;
	}

	Map<ContainerKey, LargeInfo> getAllLargeInfo(
			java.util.Collection<ContainerKey> normalizedKeyCollection,
			ContainerKeyConverter keyConverter) throws GSException {
		final Map<ContainerKey, LargeInfo> infoMap =
				new HashMap<ContainerKey, LargeInfo>();
		final List<ContainerKey> nonCachedKeys = new ArrayList<ContainerKey>();

		for (ContainerKey key : normalizedKeyCollection) {
			final LargeInfo largeInfo = largeInfoCache.find(
					key, largeInfoCache.currentNanosForExpiration());
			if (largeInfo == null) {
				nonCachedKeys.add(key);
			}
			else {
				infoMap.put(key, largeInfo);
			}
		}

		final boolean internalMode = true;

		final List<LargeInfoBuilder> builderList = new ArrayList<LargeInfoBuilder>();
		final List<ContainerKey> subKeys = new ArrayList<ContainerKey>();
		{
			final List<ContainerProperties> propsList =
					base.getAllContainerProperties(
							nonCachedKeys, getLargeInfoPropKeys(), null,
							internalMode);
			final int propsCount = propsList.size();
			for (int i = 0; i < propsCount; i++) {
				final ContainerKey key = nonCachedKeys.get(i);
				final LargeInfoBuilder builder =
						createLargeInfoBuilder(key, propsList.get(i));
				if (builder == null) {
					continue;
				}
				if (builder.isLarge()) {
					builderList.add(builder);
					subKeys.add(builder.getFixedSubKey(keyConverter));
				}
				else {
					infoMap.put(key, builder.build());
				}
			}
		}
		{
			final List<ContainerProperties> propsList =
					base.getAllContainerProperties(
							subKeys, CONTAINER_PROP_KEYS_FULL, null,
							internalMode);
			final int propsCount = propsList.size();
			for (int i = 0; i < propsCount; i++) {
				final LargeInfoBuilder builder = builderList.get(i);
				if (builder.setFixedSubProperties(propsList.get(i))) {
					infoMap.put(builder.getLargeKey(), builder.build());
				}
			}
		}

		return infoMap;
	}

	void removeLargeInfo(
			ContainerKey normalizedKey, LargeInfo largeInfo) throws GSException {
		if (largeInfo == null) {
			final LargeInfo foundInfo = largeInfoCache.find(
					normalizedKey, largeInfoCache.currentNanosForExpiration());
			if (foundInfo != null) {
				removeLargeInfo(normalizedKey, foundInfo);
				return;
			}
		}

		largeInfoCache.remove(normalizedKey);
		base.removeContainerCache(normalizedKey, WORKAROUND_NORMALIZE);

		if (largeInfo != null && largeInfo.isLarge()) {
			final ContainerKeyConverter keyConverter =
					getContainerKeyConverter(true, false);
			for (long subId : largeInfo) {
				final ContainerKey subKey = largeInfo.generateSubKey(
						normalizedKey, keyConverter, subId);
				base.removeContainerCache(subKey, true);
			}
		}
	}

	private static ContainerProperties.KeySet getLargeInfoPropKeys() {
		if (isLargePropKeyEnabled()) {
			return CONTAINER_PROP_KEYS_LARGE;
		}
		else {
			return CONTAINER_PROP_KEYS_MIN;
		}
	}

	private LargeInfoBuilder createLargeInfoBuilder(
			ContainerKey key, ContainerProperties props) throws GSException {
		return LargeInfoBuilder.getInstance(
				key, props, partitionController, largeInfoCache);
	}

	private static ContainerInfo mergeContainerInfo(
			ContainerInfo sub, ContainerInfo large) throws GSException {
		final List<IndexInfo> largeIndexInfoList = large.getIndexInfoList();
		if (largeIndexInfoList.isEmpty()) {
			final ContainerInfo retInfo = new ContainerInfo(sub);
			retInfo.setName(large.getName());
			return retInfo;
		}

		final int columnCount = sub.getColumnCount();
		final List<ColumnInfo> columnInfoList =
				new ArrayList<ColumnInfo>(columnCount);
		for (int i = 0; i < columnCount; i++) {
			columnInfoList.add(sub.getColumnInfo(i));
		}

		final List<IndexInfo> destIndexInfoList =
				new ArrayList<IndexInfo>(largeIndexInfoList.size());

		for (IndexInfo srcIndexInfo : largeIndexInfoList) {
			final IndexInfo destIndexInfo = new IndexInfo(srcIndexInfo);
			final List<Integer> columnList = destIndexInfo.getColumnList();

			final List<String> columnNameList = new ArrayList<String>();
			for (Integer column : columnList) {
				final ColumnInfo columnInfo = columnInfoList.get(column);

				columnNameList.add(columnInfo.getName());
				if (columnList.size() > 1) {
					continue;
				}

				final Set<IndexType> typeSet =
						EnumSet.of(srcIndexInfo.getType());
				if (columnInfo.getIndexTypes() != null) {
					typeSet.addAll(columnInfo.getIndexTypes());
				}

				final ColumnInfo.Builder builder =
						new ColumnInfo.Builder(columnInfo);
				builder.setIndexTypes(typeSet);
				columnInfoList.set(column, builder.toInfo());
			}

			destIndexInfo.setColumnNameList(columnNameList);
			destIndexInfoList.add(destIndexInfo);
		}

		final ContainerInfo retInfo = new ContainerInfo(sub);
		retInfo.setName(large.getName());
		retInfo.setColumnInfoList(columnInfoList);
		retInfo.setIndexInfoList(destIndexInfoList);

		return retInfo;
	}

	@SuppressWarnings("unchecked")
	private static <K, R> PartContainer<K, R> disguiseTypedContainer(
			PartContainer<?, R> container) {
		return (PartContainer<K, R>) container;
	}

	private static boolean isLargePropKeyEnabled() {
		return SubnetGridStore.isContainerAttributeUnified();
	}

	private static boolean isPartitionedContainerRecoverable() {
		return NodeConnection.getProtocolVersion() >= 15;
	}

	private static boolean isPartitioningIntervalUnified() {
		return false;
	}

	static class ConfigKey {

		static final String PREFIX = "internal.advance.";

		static final String LARGE_INFO_CACHE_SIZE_KEY =
				"largeContainerCacheSize";

		static final String LARGE_INFO_EXPIRATION_KEY =
				"largeContainerCacheExpirationMillis";

		private static final String[] ALL_BASE_KEYS = {
			LARGE_INFO_CACHE_SIZE_KEY,
			LARGE_INFO_EXPIRATION_KEY
		};

		public static String getKey(String baseKey) {
			return PREFIX + baseKey;
		}

		public static void filter(
				Properties src, Properties acceptable, Properties other) {
			other.putAll(src);
			for (String baseKey : ALL_BASE_KEYS) {
				final String key = getKey(baseKey);
				if (other.containsKey(key)) {
					acceptable.put(key, other.remove(key));
				}
			}
		}

	}

	static class Config {

		int largeInfoCacheSize = 10000;

		int largeInfoExpirationMillis =
				LARGE_INFO_EXIPIRABLE_DEFAULT ? (1000 * 60 * 10) : -1;

		public Config() {
		}

		public void set(WrappedProperties props) throws GSException {
			final Integer largeInfoCacheSize = props.getIntProperty(
					ConfigKey.getKey(ConfigKey.LARGE_INFO_CACHE_SIZE_KEY),
					false);
			if (largeInfoCacheSize != null) {
				this.largeInfoCacheSize = largeInfoCacheSize;
			}

			final Integer largeInfoExpirationMillis = props.getIntProperty(
					ConfigKey.getKey(ConfigKey.LARGE_INFO_EXPIRATION_KEY),
					false);
			if (largeInfoExpirationMillis != null) {
				this.largeInfoExpirationMillis = largeInfoExpirationMillis;
			}
		}

	}

	static class LargeInfo implements Iterable<Long> {

		static final LargeInfo NOT_LARGE = new LargeInfo();

		static final int MAX_PARTITIONING_COLUMN_COUNT = 2;

		private static final PartitioningType[] PARTITIONING_TYPE_VALUES =
				PartitioningType.values();

		private static final ContainerHashMode[] PARTITIONING_MODE_VALUES =
				ContainerHashMode.values();

		private static final SubOperation[] SUB_OPERATION_VALUES =
				SubOperation.values();

		private static final Set<GSType> INTERVAL_COLUMN_TYPES = EnumSet.of(
				GSType.BYTE,
				GSType.SHORT,
				GSType.INTEGER,
				GSType.LONG,
				GSType.TIMESTAMP);

		private static final long MAX_NODE_AFFINITY_NUM =
				(1L << (Long.SIZE - 2)) - 1;

		private static final int MAX_AVAILABLE_SUB_COUNT = 10000;

		private static final Map<Integer, Integer> SINGLE_KEY_COLUMN_MAP =
				Collections.unmodifiableMap(Collections.singletonMap(0, 0));

		int locationMode;

		PartitioningType partitioningType;

		ContainerHashMode partitioningMode;

		int partitioningCount;

		ColumnInfo intervalType;

		long intervalValue;

		int intervalUnit;

		long partitioningVersion;

		long[] nodeAffinityList;

		int[] nodeAffinityRevList;

		int[] partitioningColumns1;

		int[] partitioningColumns2;

		long[] availableIntervalList;

		long[] availableCountList;

		long[] disabledIntervalList;

		long[] disabledCountList;

		SubOperation operation;

		Long operatingSubId;

		IndexInfo operatingIndex;

		int clusterPartitionCount;

		long remoteTimeMillis;

		long expirationTimeMillis;

		TimeUnit expirationTimeUnit;

		int partitionId;

		long containerId;

		Map<Integer, Integer> keyColumnMap;

		long cachedNanos;

		LargeInfo() {
		}

		LargeInfo(
				BasicBuffer in, ContainerIdInfo idInfo,
				int clusterPartitionCount, int partitionId,
				long cachedNanos) throws GSException {
			locationMode = getLocationMode(in);
			partitioningType = getPartitioningType(in, locationMode);
			partitioningMode = getPartitioningMode(in, locationMode);
			partitioningCount = getPartitioningCount(in, partitioningType);
			getIntervalInfo(in, partitioningType, this);
			partitioningVersion = getPartitioningVersion(in, locationMode);
			nodeAffinityList = getNodeAffinityList(
					in, locationMode, partitioningType, partitioningCount,
					clusterPartitionCount);

			partitioningColumns1 = getColumnList(
					in, locationMode, partitioningType, true);
			partitioningColumns2 = getColumnList(
					in, locationMode, partitioningType, false);

			long[][] list;
			list = getIntervalList(in, locationMode, true, partitioningType);
			availableIntervalList = list[0];
			availableCountList = list[1];

			list = getIntervalList(in, locationMode, false, partitioningType);
			disabledIntervalList = list[0];
			disabledCountList = list[1];

			getOperationStatus(in, this);
			getExpirationInfo(in, this);

			this.clusterPartitionCount = clusterPartitionCount;
			this.partitionId = partitionId;
			containerId = idInfo.containerId;
			this.cachedNanos = cachedNanos;
		}

		void put(BasicBuffer out) throws GSException {
			out.putInt(locationMode);
			out.putInt(partitioningType.ordinal());
			out.putInt(partitioningMode.ordinal());
			putPartitioningCount(out, partitioningType, partitioningCount);
			putIntervalInfo(out, partitioningType, this);
			putPartitioningVersion(out, locationMode, partitioningVersion);
			putNodeAffinityList(out, locationMode, nodeAffinityList);
			putColumnList(
					out, locationMode, partitioningColumns1, partitioningType,
					true);
			putColumnList(
					out, locationMode, partitioningColumns2, partitioningType,
					false);
			putIntervalList(
					out, locationMode,
					availableIntervalList, availableCountList);
			putIntervalList(
					out, locationMode,
					disabledIntervalList, disabledCountList);
			putOperationStatus(out, this);
			putExpirationInfo(out, this);
		}

		@Override
		public Iterator<Long> iterator() {
			final int finishIndex = availableIntervalList.length - 1;
			if (finishIndex < 0) {
				return Collections.<Long>emptyList().iterator();
			}
			return new SubIdIterator(
					this, availableIntervalList, availableCountList,
					0, 0,
					finishIndex, availableCountList[finishIndex] - 1);
		}

		boolean isLarge() {
			return partitioningType != null;
		}

		boolean isStable(boolean emptyAllowed) {
			return !isLarge() || !isOrdered() || (operation == null &&
					(emptyAllowed || availableIntervalList.length > 0));
		}

		boolean isExpirable() {
			return expirationTimeMillis > 0;
		}

		boolean isKeyPartitioned() {
			if (keyColumnMap.isEmpty()) {
				return false;
			}
			return keyColumnMap.containsKey(partitioningColumns1[0]);
		}

		int getPartitioningColumnCount() {
			return (partitioningType == PartitioningType.INTERVAL_HASH ?
					2 : 1);
		}

		int getPartitioningColumn(int index) {
			return (index == 0 ?
					partitioningColumns1[0] : partitioningColumns2[0]);
		}

		static Map<Integer, Integer> makeKeyColumnMap(ContainerInfo info) {
			final int columnCount = info.getColumnCount();
			if (columnCount <= 0) {
				throw new Error();
			}

			final List<Integer> keyList = info.getRowKeyColumnList();
			if (keyList.isEmpty()) {
				return Collections.emptyMap();
			}
			else if (keyList.size() == 1 && keyList.get(0) == 0) {
				return SINGLE_KEY_COLUMN_MAP;
			}

			final Map<Integer, Integer> map =
					new LinkedHashMap<Integer, Integer>();
			for (Integer column : keyList) {
				final int keyColumn = map.size();
				map.put(column, keyColumn);
			}

			return Collections.unmodifiableMap(map);
		}

		long getFixedSubId() {
			if (locationMode == 0) {
				return 0;
			}

			return partitionId;
		}

		long getFeasibleSubId() {
			if (locationMode == 0) {
				return 0;
			}

			final long interval = getTailInterval(true);
			if (interval < 0) {
				return getFixedSubId();
			}

			return subIdFromInterval(interval, 0);
		}

		boolean isOrdered() {
			return partitioningType != PartitioningType.HASH;
		}

		int getPartitioningUnit() {
			if (partitioningType == PartitioningType.INTERVAL) {
				return 1;
			}
			else {
				return partitioningCount;
			}
		}

		ContainerKey generateSubKey(
				ContainerKey largeKey, ContainerKeyConverter keyConverter,
				long subId) throws GSException {
			return SubContainerLocator.getSubContainerKey(
					largeKey, keyConverter, subId, partitioningCount,
					clusterPartitionCount, containerId, locationMode);
		}

		private Object singleKeyToPartitioningValue(Object key)
				throws ContainerPartitioningException {
			if (keyColumnMap.size() != 1) {
				throw new ContainerPartitioningException(
						GSErrorCode.ILLEGAL_SCHEMA, "Single key expected",
						null);
			}

			if (!isKeyPartitioned()) {
				return null;
			}

			return key;
		}

		private Object[] composedKeyToPartitioningValues(Object[] key)
				throws ContainerPartitioningException {
			final Object[] values = new Object[getPartitioningColumnCount()];
			for (int i = 0; i < values.length; i++) {
				final Integer keyColumn =
						keyColumnMap.get(getPartitioningColumn(i));
				if (keyColumn == null) {
					break;
				}
				final Object srcElem;
				try {
					srcElem = key[keyColumn];
				}
				catch (ArrayIndexOutOfBoundsException e) {
					throw new ContainerPartitioningException(
							GSErrorCode.ILLEGAL_SCHEMA, null, e);
				}
				values[i] = srcElem;
			}
			return values;
		}

		private Object[] composedKeyToPartitioningValues(Row.Key key)
				throws ContainerPartitioningException {
			final Object[] values = new Object[getPartitioningColumnCount()];
			for (int i = 0; i < values.length; i++) {
				final Integer keyColumn =
						keyColumnMap.get(getPartitioningColumn(i));
				if (keyColumn == null) {
					break;
				}
				final Object srcElem;
				try {
					srcElem = key.getValue(keyColumn);
				}
				catch (GSException e) {
					throw new ContainerPartitioningException(
							GSErrorCode.ILLEGAL_SCHEMA, null, e);
				}
				values[i] = srcElem;
			}
			return values;
		}

		Long subIdFromKey(Object key, boolean decomposed, boolean start)
				throws GSException, ContainerPartitioningException {
			final Object value1;
			final Object value2;
			do {
				final Object[] composedValues;
				if (decomposed) {
					if (!(key instanceof Object[])) {
						value1 = singleKeyToPartitioningValue(key);
						value2 = null;
						break;
					}
					composedValues =
							composedKeyToPartitioningValues((Object[]) key);
				}
				else {
					if (!(key instanceof Row.Key)) {
						value1 = singleKeyToPartitioningValue(key);
						value2 = null;
						break;
					}
					composedValues =
							composedKeyToPartitioningValues((Row.Key) key);
				}
				if (composedValues == null || composedValues[0] == null) {
					return null;
				}

				value1 = composedValues[0];
				if (composedValues.length <= 1) {
					if (composedValues.length < 1) {
						throw new Error();
					}
					value2 = null;
					break;
				}

				value2 = composedValues[1];
				if (composedValues.length > 2) {
					throw new Error();
				}
			}
			while (false);

			if (value1 == null) {
				return null;
			}

			if (value2 == null) {
				if (partitioningType == PartitioningType.INTERVAL_HASH) {
					final long interval = intervalFromValue(value1);
					final int hash = (start ? 0 : partitioningCount - 1);
					return subIdFromInterval(interval, hash);
				}
				return subIdFromValues(value1);
			}
			else {
				return subIdFromValues(value1, value2);
			}
		}

		long subIdFromValues(Object ...values)
				throws GSException, ContainerPartitioningException {
			if (locationMode == 0) {
				return ValueHasher.hashAndMod(
						values[0], partitioningCount);
			}

			final int count = clusterPartitionCount;
			switch (partitioningType) {
			case HASH: {
				final int hash = ValueHasher.hashAndMod(
						values[0], partitioningCount);
				return hash / count * count +
						nodeAffinityList[hash % count] + count;
			}
			case INTERVAL: {
				final long interval = intervalFromValue(values[0]);
				return interval / (count * 2) * (count * 2) +
						nodeAffinityList[(int) ((interval >> 1) % count)] +
						(((interval & 0x1) == 0) ? 0 : count) +
						count;
			}
			case INTERVAL_HASH: {
				final long interval = intervalFromValue(values[0]);
				final int unit = partitioningCount;
				final int hash = ValueHasher.hashAndMod(values[1], unit);
				return interval / (count * 2) * (count * 2) * unit +
						nodeAffinityList[(int) ((interval >> 1) % count)] +
						(((interval & 0x1) == 0) ? 0 : count * unit) +
						hash + count;
			}
			default:
				throw new Error();
			}
		}

		long subIdFromInterval(long interval, int hash) {
			if (locationMode == 0) {
				return hash;
			}

			final int count = clusterPartitionCount;
			switch (partitioningType) {
			case HASH:
				return hash / count * count +
						nodeAffinityList[hash % count] + count;
			case INTERVAL:
				return interval / (count * 2) * (count * 2) +
						nodeAffinityList[(int) ((interval >> 1) % count)] +
						(((interval & 0x1) == 0) ? 0 : count) +
						count;
			case INTERVAL_HASH: {
				final int unit = partitioningCount;
				return interval / (count * 2) * (count * 2) * unit +
						nodeAffinityList[(int) ((interval >> 1) % count)] +
						(((interval & 0x1) == 0) ? 0 : count * unit) +
						hash + count;
			}
			default:
				throw new Error();
			}
		}

		long subIdFromRawInterval(long rawInterval, int hash) {
			return subIdFromInterval(intervalFromRaw(rawInterval), hash);
		}

		long[] intervalValueRangeFromSubId(long subId) {
			final long rawInterval = toRawInterval(intervalFromSubId(subId));
			final long unit = intervalValue;

			final long min = Long.MIN_VALUE;
			final long max = Long.MAX_VALUE;

			final long lower;
			final long upper;
			if (rawInterval >= 0) {
				final long cur = rawInterval;
				if (cur > max / unit) {
					throw new IllegalArgumentException();
				}
				final long begin = cur * unit;
				final long offset = unit - 1;

				lower = begin;
				if (begin > max - offset) {
					upper = max;
				}
				else {
					upper = begin + offset;
				}
			}
			else {
				final long next = rawInterval + 1;
				if (next < min / unit) {
					throw new IllegalArgumentException();
				}
				final long end = next * unit;

				if (end < min + unit) {
					lower = min;
				}
				else {
					lower = end - unit;
				}
				upper = end - 1;
			}
			return new long[] { lower, upper };
		}

		long intervalFromSubId(long subId) {
			final int count = clusterPartitionCount;
			if (subId < count) {
				throw new IllegalArgumentException();
			}

			if (partitioningType == PartitioningType.HASH) {
				return 0;
			}

			final long base = (subId - count) / getPartitioningUnit();
			final int[] revList = getNodeAffinityRevList();

			return base / (count * 2) * (count * 2) +
					((long) revList[(int) (base % count)] << 1L |
					((base / count) % 2 == 0 ? 0 : 1));
		}

		private long intervalFromValue(Object value)
				throws ContainerPartitioningException {
			final long longValue;
			try {
				if (intervalType.getType() == GSType.TIMESTAMP) {
					longValue = ((Date) value).getTime();
				}
				else {
					longValue = ((Number) value).longValue();
				}
			}
			catch (NullPointerException e) {
				throw new ContainerPartitioningException(
						GSErrorCode.ILLEGAL_SCHEMA,
						"Null is not allowed for interval partitioning key (" +
						"partitioningKeyType=" + intervalType.getType() +
						")", e);
			}
			catch (ClassCastException e) {
				throw new ContainerPartitioningException(
						GSErrorCode.ILLEGAL_SCHEMA,
						"Unmatched type for interval partitioning key (" +
						"valueClass=" + value.getClass().getName() +
						", partitioningKeyType=" + intervalType.getType() +
						")", e);
			}

			final long rawInterval;
			if (longValue >= 0) {
				rawInterval = longValue / intervalValue;
			}
			else {
				rawInterval = (longValue + 1) / intervalValue - 1;
			}
			return intervalFromRaw(rawInterval);
		}

		private static long intervalFromRaw(long rawInterval) {
			if (rawInterval < 0) {
				return ((-(rawInterval + 1)) << 1) | 1;
			}
			else {
				return rawInterval << 1;
			}
		}

		private static long toRawInterval(long interval) {
			if ((interval & 0x1) == 1) {
				return (-(interval >>> 1) - 1);
			}
			else {
				return (interval >>> 1);
			}
		}

		private int[] getNodeAffinityRevList() {
			if (nodeAffinityRevList != null) {
				return nodeAffinityRevList;
			}

			final long unit;
			switch (partitioningType) {
			case INTERVAL:
				unit = 1;
				break;
			case INTERVAL_HASH:
				unit = partitioningCount;
				break;
			default:
				throw new Error();
			}

			final int[] list = new int[nodeAffinityList.length];
			for (int i = 0; i < list.length; i++) {
				list[(int) (nodeAffinityList[i] / unit)] = i;
			}
			nodeAffinityRevList = list;
			return list;
		}

		boolean isAvailable(long subId) {
			if (subId == getFixedSubId()) {
				return true;
			}
			return containsSub(true, subId);
		}

		boolean isDisabled(long subId) {
			if (subId == getFixedSubId()) {
				return false;
			}
			return containsSub(false, subId);
		}

		boolean isExpired(long subId) {
			if (!isExpirable() || subId == getFixedSubId()) {
				return false;
			}

			final long[] range = intervalValueRangeFromSubId(subId);
			final long duration = expirationTimeMillis;
			final long maxAvailableTime;
			if (range[1] > Long.MAX_VALUE - duration) {
				maxAvailableTime = Long.MAX_VALUE;
			}
			else {
				maxAvailableTime = range[1] + duration;
			}

			return maxAvailableTime < remoteTimeMillis;
		}

		boolean containsSub(boolean forAvailable, long subId) {
			return findNeighbor(
					forAvailable, intervalFromSubId(subId), false, true, true) >= 0;
		}

		long findNeighborSub(
				boolean forAvailable, boolean upper, long subId) {
			final long interval = findNeighbor(
					forAvailable, intervalFromSubId(subId),
					upper, false, false);
			if (interval < 0) {
				return -1;
			}
			return subIdFromInterval(interval, 0);
		}

		private int findIntervalIndex(boolean forAvailable, long interval) {
			final long[] intervalList = getIntervalList(forAvailable);

			final long rawInterval = toRawInterval(interval);
			int index = Arrays.binarySearch(intervalList, rawInterval);
			if (index >= 0) {
				return index;
			}
			else if (index == -1) {
				return -1;
			}
			else {
				return -(index + 1) - 1;
			}
		}

		private long findNeighbor(
				boolean forAvailable, long interval, boolean upper,
				boolean inclusive, boolean exact) {
			final long[] intervalList = getIntervalList(forAvailable);
			final long[] countList = getIntervalCountList(forAvailable);

			final long ret;
			do {
				final long rawInterval = toRawInterval(interval) +
						(inclusive ? 0 : (upper ? 1 : -1));
				int index = Arrays.binarySearch(intervalList, rawInterval);
				if (index >= 0) {
					ret = rawInterval;
					break;
				}

				index = -(index + 1);
				if (index == 0) {
					if (upper && !exact && intervalList.length > 0) {
						ret = intervalList[0];
						break;
					}
					return -1;
				}

				final long end = intervalList[index - 1] + countList[index - 1];
				if (rawInterval < end) {
					ret = rawInterval;
					break;
				}
				else if (!upper && !exact) {
					ret = end - 1;
					break;
				}
				return -1;
			}
			while (false);

			return intervalFromRaw(ret);
		}

		Iterable<Long> subIdRangeByKey(Object key, boolean decomposed)
				throws GSException, ContainerPartitioningException {
			final Long startSubId = subIdFromKey(key, decomposed, true);
			final Long finishSubId = subIdFromKey(key, decomposed, false);
			return subIdRange(startSubId, finishSubId);
		}

		Iterable<Long> subIdRange(Long startSubId, Long finishSubId) {
			final long startInterval = (startSubId == null ?
					getBeginInterval(true) : findNeighbor(
							true, intervalFromSubId(startSubId),
							true, true, false));
			final long finishInterval = (finishSubId == null ?
					getTailInterval(true) : findNeighbor(
							true, intervalFromSubId(finishSubId),
							false, true, false));
			if (startInterval < 0 || finishInterval < 0) {
				return Collections.emptySet();
			}

			final long startRawInterval = toRawInterval(startInterval);
			final long finishRawInterval = toRawInterval(finishInterval);
			if (startRawInterval > finishRawInterval) {
				return Collections.emptySet();
			}

			final int startIndex = findIntervalIndex(true, startInterval);
			final int finishIndex = findIntervalIndex(true, finishInterval);

			final long[] intervalList = getIntervalList(true);
			final long[] countList = getIntervalCountList(true);

			final long startOffset =
					startRawInterval - intervalList[startIndex];
			final long finishOffset =
					finishRawInterval - intervalList[finishIndex];

			return new Iterable<Long>() {
				@Override
				public Iterator<Long> iterator() {
					return new SubIdIterator(
							LargeInfo.this, intervalList, countList,
							startIndex, startOffset,
							finishIndex, finishOffset);
				}
			};
		}

		private long getBeginInterval(boolean forAvailable) {
			final long[] intervalList = getIntervalList(forAvailable);
			if (intervalList.length <= 0) {
				return -1;
			}
			return intervalFromRaw(intervalList[0]);
		}

		private long getTailInterval(boolean forAvailable) {
			final long[] intervalList = getIntervalList(forAvailable);
			final long[] countList = getIntervalCountList(forAvailable);
			final int index = intervalList.length - 1;
			if (index < 0) {
				return -1;
			}
			return intervalFromRaw(intervalList[index] + countList[index] - 1);
		}

		private long[] getIntervalList(boolean forAvailable) {
			return (forAvailable ?
					availableIntervalList : disabledIntervalList);
		}

		private long[] getIntervalCountList(boolean forAvailable) {
			return (forAvailable ? availableCountList : disabledCountList);
		}

		private static int getLocationMode(BasicBuffer in) throws GSException {
			final int locationMode = in.base().getInt();
			switch (locationMode) {
			case 0:
			case 1:
			case 2:
				return locationMode;
			default:
				throw new GSException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by unknown location mode (mode=" +
						locationMode + ")");
			}
		}

		private static PartitioningType getPartitioningType(
				BasicBuffer in, int locationMode) throws GSException {
			final int typeValue = in.base().getInt();
			if (locationMode == 0) {
				if (typeValue == 1) {
					return PartitioningType.HASH;
				}
			}
			else {
				if (typeValue >= 0 &&
						typeValue < PARTITIONING_TYPE_VALUES.length) {
					return PARTITIONING_TYPE_VALUES[typeValue];
				}
			}
			throw new GSException(
					GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by unknown container partitioning type");
		}

		private static ContainerHashMode getPartitioningMode(
				BasicBuffer in, int locationMode) throws GSException {
			final int modeValue = in.base().getInt();
			if (locationMode == 0) {
				if (modeValue == 0) {
					return ContainerHashMode.COMPATIBLE1;
				}
			}
			else {
				if (modeValue >= 0 &&
						modeValue < PARTITIONING_MODE_VALUES.length) {
					return PARTITIONING_MODE_VALUES[modeValue];
				}
			}

			throw new GSException(
					GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by unknown container partitioning mode");
		}

		private static int getPartitioningCount(
				BasicBuffer in, PartitioningType type) throws GSException {
			switch (type) {
			case HASH:
			case INTERVAL_HASH:
				break;
			default:
				return 0;
			}

			final int count = in.base().getInt();
			if (count <= 0) {
				throw new GSException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by too small container " +
						"partitioning count");
			}
			return count;
		}

		private static void putPartitioningCount(
				BasicBuffer out, PartitioningType type,
				int partitioningCount) {
			switch (type) {
			case HASH:
			case INTERVAL_HASH:
				break;
			default:
				return;
			}

			out.putInt(partitioningCount);
		}

		private static void getIntervalInfo(
				BasicBuffer in, PartitioningType partitioningType,
				LargeInfo info) throws GSException {
			switch (partitioningType) {
			case INTERVAL:
			case INTERVAL_HASH:
				break;
			default:
				return;
			}

			final ColumnInfo type = getIntervalColumnType(in);
			final long value = getIntervalValueAsLong(
					type, RowMapper.TypeUtils.getSingleField(in, type));

			info.intervalType = type;
			info.intervalValue = value;
			info.intervalUnit = getIntervalUnitType(in, type);
		}

		private static void putIntervalInfo(
				BasicBuffer out, PartitioningType partitioningType,
				LargeInfo info) throws GSException {
			switch (partitioningType) {
			case INTERVAL:
			case INTERVAL_HASH:
				break;
			default:
				return;
			}

			RowMapper.TypeUtils.putFullColumnType(out, info.intervalType);

			final ColumnInfo type = info.intervalType;
			final long value = info.intervalValue;
			RowMapper.TypeUtils.putSingleField(
					out, type, getIntervalValueAsObject(type, value));

			out.put((byte) info.intervalUnit);
		}

		private static ColumnInfo getIntervalColumnType(BasicBuffer in)
				throws GSException {
			final ColumnInfo type = RowMapper.TypeUtils.getFullColumnType(in);
			if (!INTERVAL_COLUMN_TYPES.contains(type.getType())) {
				throw new GSException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by illegal column type");
			}
			return type;
		}

		private static long getIntervalValueAsLong(
				ColumnInfo type, Object src) {
			try {
				if (type.getType() == GSType.TIMESTAMP) {
					return ((Date) src).getTime();
				}
				else {
					return ((Number) src).longValue();
				}
			}
			catch (ClassCastException e) {
				throw new IllegalArgumentException(e);
			}
		}

		private static Object getIntervalValueAsObject(
				ColumnInfo type, long src) {
			switch (type.getType()) {
			case BYTE:
				return (byte) src;
			case SHORT:
				return (short) src;
			case INTEGER:
				return (int) src;
			case LONG:
				return src;
			case TIMESTAMP:
				if (type.getTimePrecision() != TimeUnit.MILLISECOND) {
					return new Timestamp(src);
				}
				return new Date(src);
			default:
				throw new IllegalStateException();
			}
		}

		private static int getIntervalUnitType(
				BasicBuffer in, ColumnInfo intervalType) throws GSException {
			final int typeValue = in.base().get();
			if (intervalType.getType() == GSType.TIMESTAMP) {
				if ((typeValue == TimeUnit.DAY.ordinal() || typeValue == TimeUnit.HOUR.ordinal())) {
					return typeValue;
				}
			}
			else {
				if (typeValue == -1) {
					return typeValue;
				}
			}
			throw new GSException(
					GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by illegal interval unit type");
		}

		private static long getPartitioningVersion(
				BasicBuffer in, int locationMode) throws GSException {
			if (locationMode <= 0) {
				return 0;
			}

			final long version = in.base().getLong();
			if (version < 0) {
				throw new GSException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by illegal partitioning version");
			}
			return version;
		}

		private static void putPartitioningVersion(
				BasicBuffer out, int locationMode, long version) {
			if (locationMode <= 0) {
				return;
			}

			out.putLong(version);
		}

		private static long[] getNodeAffinityList(
				BasicBuffer in, int locationMode, PartitioningType type,
				int partitioningCount, int clusterPartitionCount)
				throws GSException {
			if (locationMode <= 0) {
				return null;
			}

			final int expectedCount;
			final int unit;
			switch (type) {
			case INTERVAL:
				expectedCount = clusterPartitionCount;
				unit = 1;
				break;
			case INTERVAL_HASH:
				expectedCount = clusterPartitionCount;
				unit = partitioningCount;
				break;
			default:
				expectedCount = (clusterPartitionCount < partitioningCount) ?
						clusterPartitionCount : partitioningCount;
				unit = 1;
				break;
			}

			final int count = BufferUtils.getIntSize(in.base());
			if (count <= 0 || count != expectedCount ||
					count > MAX_NODE_AFFINITY_NUM) {
				throw new GSException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by illegal node affinity count (" +
						"expected=" + expectedCount +
						", actual=" + count + ")");
			}

			final long[] list = new long[count];
			for (int i = 0; i < count; i++) {
				final long affinity = in.base().getLong();
				if (affinity < 0 || affinity >= clusterPartitionCount * unit ||
						affinity % unit != 0) {
					throw new GSException(
							GSErrorCode.MESSAGE_CORRUPTED,
							"Protocol error by illegal node affinity value (" +
							"value=" + affinity + ")");
				}
				list[i] = affinity;
			}

			return list;
		}

		private static void putNodeAffinityList(
				BasicBuffer out, int locationMode, long[] list) {
			if (locationMode <= 0) {
				return;
			}

			out.putInt(list.length);
			for (final long value : list) {
				out.putLong(value);
			}
		}

		private static int[] getColumnList(
				BasicBuffer in, int locationMode, PartitioningType type,
				boolean primary) throws GSException {
			if (locationMode <= 0) {
				if (!primary) {
					return new int[0];
				}
				final int column = in.base().getInt();
				if (column < 0) {
					throw new GSException(
							GSErrorCode.MESSAGE_CORRUPTED,
							"Protocol error by illegal partitioning column");
				}
				return new int[] { column };
			}

			if (!primary && type != PartitioningType.INTERVAL_HASH) {
				return new int[0];
			}

			final int count = BufferUtils.getNonNegativeInt(in.base());
			if (count != 1) {
				throw new GSException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by illegal partitioning column count");
			}

			final int[] list = new int[count];
			for (int i = 0; i < count; i++) {
				final int column = in.base().getInt();
				if (column < 0) {
					throw new GSException(
							GSErrorCode.MESSAGE_CORRUPTED,
							"Protocol error by illegal partitioning column");
				}
				list[i] = column;
			}

			return list;
		}

		private static void putColumnList(
				BasicBuffer out, int locationMode, int[] list,
				PartitioningType type, boolean primary) {
			if (locationMode <= 0) {
				if (!primary) {
					return;
				}
				out.putInt(list[0]);
				return;
			}

			if (!primary && type != PartitioningType.INTERVAL_HASH) {
				return;
			}

			out.putInt(list.length);
			for (final int value : list) {
				out.putInt(value);
			}
		}

		private static long[][] getIntervalList(
				BasicBuffer in, int locationMode, boolean forAvailable,
				PartitioningType partitioningType) throws GSException {
			if (locationMode <= 0) {
				if (!forAvailable) {
					return new long[][] { {}, {} };
				}

				return new long[][] { { 0 }, { 1 } };
			}

			final int count = BufferUtils.getIntSize(in.base());
			if (partitioningType == PartitioningType.HASH) {
				if (count != 0) {
					throw new GSException(
							GSErrorCode.MESSAGE_CORRUPTED,
							"Protocol error by illegal interval list size");
				}
				return getIntervalList(null, 0, forAvailable, null);
			}
			else if (count < 0 ||
					(forAvailable && count > MAX_AVAILABLE_SUB_COUNT)) {
				throw new GSException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by illegal interval list size");
			}

			final long[] intervalList1 = new long[count];
			final long[] intervalList2 = new long[count];
			final long[] countList1 = new long[count];
			final long[] countList2 = new long[count];

			int pos1 = 0;
			int pos2 = 0;

			long prevRawInterval = Long.MIN_VALUE;
			long totalIntervalCount = 0;
			for (int i = 0; i < count; i++) {
				final long interval = in.base().getLong();
				final long intervalCount = in.base().getLong();

				if (intervalCount < 1 ||
						intervalCount > MAX_AVAILABLE_SUB_COUNT ||
						intervalCount > Long.MAX_VALUE - totalIntervalCount) {
					throw new GSException(
							GSErrorCode.MESSAGE_CORRUPTED,
							"Protocol error by illegal interval count");
				}

				totalIntervalCount += intervalCount;
				if (forAvailable &&
						totalIntervalCount > MAX_AVAILABLE_SUB_COUNT) {
					throw new GSException(
							GSErrorCode.MESSAGE_CORRUPTED,
							"Protocol error by illegal total interval count");
				}

				final long rawInterval =
						importRawInterval(toRawInterval(interval));
				if (rawInterval < 0) {
					intervalList1[pos1] = rawInterval;
					countList1[pos1] = intervalCount;
					pos1++;
				}
				else {
					intervalList2[pos2] = rawInterval;
					countList2[pos2] = intervalCount;
					pos2++;
				}

				if (i > 0 && rawInterval <= prevRawInterval) {
					throw new GSException(
							GSErrorCode.MESSAGE_CORRUPTED,
							"Protocol error by illegal interval");
				}
				prevRawInterval = rawInterval + intervalCount;
			}

			System.arraycopy(intervalList2, 0, intervalList1, pos1, pos2);
			System.arraycopy(countList2, 0, countList1, pos1, pos2);

			return new long[][] { intervalList1, countList1 };
		}

		private static void putIntervalList(
				BasicBuffer out, int locationMode, long[] idList,
				long[] countList) {
			if (locationMode <= 0) {
				return;
			}

			out.putInt(idList.length);
			for (int i = 0; i < idList.length; i++) {
				final long rawInterval = idList[i];
				final long intervalCount = countList[i];
				out.putLong(intervalFromRaw(exportRawInterval(rawInterval)));
				out.putLong(intervalCount);
			}
		}

		private static long importRawInterval(long src) throws GSException {
			if (!isPartitioningIntervalUnified()) {
				if (src == -1) {
					throw new GSException(
							GSErrorCode.MESSAGE_CORRUPTED,
							"Protocol error by illegal negative interval");
				}
				return src + (src < 0 ? 1 : 0);
			}
			return src;
		}

		private static long exportRawInterval(long src) {
			if (!isPartitioningIntervalUnified()) {
				return src - (src < 0 ? 1 : 0);
			}
			return src;
		}

		private static void getOperationStatus(
				BasicBuffer in, LargeInfo info) throws GSException {
			if (info.locationMode <= 0) {
				return;
			}

			final int operationValue = in.base().get();
			if (operationValue >= 0 &&
					operationValue < SUB_OPERATION_VALUES.length) {
				info.operation = SUB_OPERATION_VALUES[operationValue];

				if (!info.operation.isBegin()) {
					throw new GSException(
							GSErrorCode.MESSAGE_CORRUPTED,
							"Protocol error by illegal sub operation type");
				}
			}
			else if (operationValue != -1) {
				throw new GSException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by illegal sub operation");
			}
			else {
				return;
			}

			switch (info.operation) {
			case CREATE_BEGIN:
			case DROP_BEGIN: {
				final long subId = in.base().getLong();
				if (subId < 0 || subId > MAX_NODE_AFFINITY_NUM) {
					throw new GSException(
							GSErrorCode.MESSAGE_CORRUPTED,
							"Protocol error by illegal operating sub ID");
				}
				info.operatingSubId = subId;
				break;
			}
			case INDEX_CREATE_BEGIN:
			case INDEX_DROP_BEGIN:
				info.operatingIndex = new IndexInfo();
				SubnetGridStore.importIndexInfo(
						in, info.operatingIndex, true, true);
				break;
			default:
				throw new Error();
			}

			if ((info.operatingSubId == null) != (info.operation == null)) {
				throw new GSException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by illegal sub operation");
			}
		}

		private static void putOperationStatus(
				BasicBuffer out, LargeInfo info) {
			if (info.locationMode <= 0) {
				return;
			}

			if (info.operation == null) {
				out.put((byte) -1);
				return;
			}
			out.putByteEnum(info.operation);
			switch (info.operation) {
			case CREATE_BEGIN:
			case DROP_BEGIN:
				out.putLong(info.operatingSubId);
				break;
			case INDEX_CREATE_BEGIN:
			case INDEX_DROP_BEGIN:
				SubnetGridStore.exportIndexInfo(out, info.operatingIndex, true);
				break;
			default:
				throw new Error();
			}
		}

		private static void getExpirationInfo(
				BasicBuffer in, LargeInfo info) throws GSException {
			if (info.locationMode < 2) {
				return;
			}

			final long remoteTimeMillis = in.base().getLong();
			if (remoteTimeMillis < 0) {
				throw new GSException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by negative remote time");
			}

			final int time = in.base().getInt();
			final TimeUnit timeUnit = in.getByteEnum(TimeUnit.class);
			final long timeMillis = expirationTimeToMillis(time, timeUnit);

			if (time > 0 && (info.intervalType == null ||
					info.intervalType.getType() != GSType.TIMESTAMP ||
					!(info.partitioningType == PartitioningType.INTERVAL ||
					info.partitioningType == PartitioningType.INTERVAL_HASH))) {
				throw new GSException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by unexpected expiration info");
			}

			info.remoteTimeMillis = remoteTimeMillis;
			info.expirationTimeMillis = timeMillis;
			info.expirationTimeUnit = timeUnit;
		}

		private static void putExpirationInfo(
				BasicBuffer out, LargeInfo info) {
			if (info.locationMode < 2) {
				return;
			}

			out.putLong(info.remoteTimeMillis);
			out.putInt(expirationTimeFromMillis(
					info.expirationTimeMillis, info.expirationTimeUnit));
			out.putByteEnum((info.expirationTimeUnit == null ?
					TimeUnit.DAY : info.expirationTimeUnit));
		}

		private static long expirationTimeToMillis(
				int srcTime, TimeUnit timeUnit) throws GSException {
			if (srcTime < 0) {
				if (srcTime < -1) {
					throw new GSException(
							GSErrorCode.MESSAGE_CORRUPTED,
							"Protocol error by illegal expiration time");
				}
				return -1;
			}

			try {
				return srcTime * getTimeUnitBase(timeUnit);
			}
			catch (IllegalArgumentException e) {
				throw new GSException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by unsupported time unit", e);
			}
		}

		private static int expirationTimeFromMillis(
				long srcMillis, TimeUnit timeUnit) {
			if (srcMillis < 0) {
				return -1;
			}
			final long time = srcMillis / getTimeUnitBase(timeUnit);
			if (time > Integer.MAX_VALUE) {
				throw new IllegalArgumentException();
			}
			return (int) time;
		}

		private static long getTimeUnitBase(TimeUnit timeUnit) {
			switch (timeUnit) {
			case MILLISECOND:
				return 1;
			case SECOND:
				return 1000;
			case MINUTE:
				return 1000 * 60;
			case HOUR:
				return 1000 * 60 * 60;
			case DAY:
				return 1000 * 60 * 60 * 24;
			default:
				throw new IllegalArgumentException();
			}
		}

	}

	private static class LargeInfoBuilder {

		private static final LargeInfoBuilder NOT_LARGE =
				new LargeInfoBuilder(LargeInfo.NOT_LARGE);

		private final ContainerKey largeKey;

		private LargeInfo largeInfo;

		private LargeInfoBuilder(
				ContainerKey largeKey, ContainerProperties props,
				Extensibles.AsPartitionController partitionController,
				LargeInfoCache largeInfoCache) throws GSException {
			final byte[] infoBytes =
					props.getRawEntries().get(CONTAINER_PROP_KEY_LARGE_INFO);
			if (infoBytes == null) {
				throw new GSException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by empty large info");
			}

			final BasicBuffer buf = new BasicBuffer(infoBytes.length);
			buf.base().put(infoBytes);
			buf.base().flip();

			final int clusterPartitionCount =
					partitionController.getPartitionCount();
			final int partitionId =
					partitionController.getPartitionIndexOfContainer(
							largeKey, true);

			this.largeKey = largeKey;
			this.largeInfo = new LargeInfo(
					buf, props.getIdInfo(),
					clusterPartitionCount, partitionId,
					largeInfoCache.currentNanosForExpiration());
		}

		private LargeInfoBuilder(LargeInfo largeInfo) {
			this.largeKey = null;
			this.largeInfo = largeInfo;
		}

		public static LargeInfoBuilder getInstance(
				ContainerKey key, ContainerProperties props,
				Extensibles.AsPartitionController partitionController,
				LargeInfoCache largeInfoCache) throws GSException {
			if (props == null) {
				return null;
			}

			final int attribute = props.getAttribute();
			if (attribute == PartContainer.Attributes.LARGE) {
				if (!isLargePropKeyEnabled()) {
					return NOT_LARGE;
				}

				return new LargeInfoBuilder(
						key, props, partitionController, largeInfoCache);
			}
			else if (attribute == ContainerKeyPredicate.ATTRIBUTE_BASE ||
					attribute == ContainerKeyPredicate.ATTRIBUTE_SINGLE) {
				return NOT_LARGE;
			}
			else {
				return null;
			}
		}

		public boolean isLarge() {
			if (largeInfo == null) {
				throw new Error();
			}
			return largeInfo.isLarge();
		}

		public ContainerKey getLargeKey() {
			return largeKey;
		}

		public ContainerKey getFixedSubKey(ContainerKeyConverter keyConverter)
				throws GSException {
			if (largeInfo == null || !largeInfo.isLarge()) {
				throw new Error();
			}
			final long subId = largeInfo.getFixedSubId();
			return largeInfo.generateSubKey(largeKey, keyConverter, subId);
		}

		public boolean setFixedSubProperties(ContainerProperties props) {
			if (largeInfo == null || !largeInfo.isLarge() ||
					largeInfo.keyColumnMap != null) {
				throw new Error();
			}
			final Integer attribute;
			if (props == null || ((attribute = props.getAttribute()) != null &&
					attribute != PartContainer.Attributes.SUB)) {
				largeInfo = null;
				return false;
			}
			largeInfo.keyColumnMap =
					LargeInfo.makeKeyColumnMap(props.getInfo());
			return true;
		}

		public LargeInfo build() {
			if (largeInfo != null && largeInfo.isLarge() &&
					largeInfo.keyColumnMap == null) {
				throw new Error();
			}
			return largeInfo;
		}

	}

	static class ContainerPartitioningException extends Exception {

		private static final long serialVersionUID = 1090617685025081585L;

		private final int errorCode;

		ContainerPartitioningException(
				int errorCode, String message, Throwable cause) {
			super(message, cause);
			this.errorCode = errorCode;
		}

		int getErrorCode() {
			return errorCode;
		}

		GSException toGSException() {
			return new GSException(errorCode, getMessage(), this);
		}

	}

	static class ContainerExpirationException extends Exception {

		private static final long serialVersionUID = -1850729080452420370L;

		private final int errorCode;

		ContainerExpirationException(
				int errorCode, String message, Throwable cause) {
			super(message, cause);
			this.errorCode = errorCode;
		}

		int getErrorCode() {
			return errorCode;
		}

		GSException toGSException() {
			return new GSException(errorCode, getMessage(), this);
		}

	}

	enum PartitioningType {
		NONE,
		HASH,
		INTERVAL,
		INTERVAL_HASH
	}

	private static class SubIdIterator implements Iterator<Long> {

		final LargeInfo largeInfo;

		final long[] intervalList;

		final long[] countList;

		final int finishIndex;

		final long finishOffset;

		final int unit;

		int nextIndex;

		long nextOffset;

		int nextUnitOffset;

		SubIdIterator(
				LargeInfo largeInfo,
				long[] intervalList, long[] countList, int startIndex,
				long startOffset, int finishIndex, long finishOffset) {
			this.largeInfo = largeInfo;
			this.intervalList = intervalList;
			this.countList = countList;
			this.nextIndex = startIndex;
			this.nextOffset = startOffset;
			this.finishIndex = finishIndex;
			this.finishOffset = finishOffset;
			unit = largeInfo.getPartitioningUnit();
		}

		@Override
		public boolean hasNext() {
			return nextIndex <= finishIndex;
		}

		@Override
		public Long next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}

			final long subId = largeInfo.subIdFromRawInterval(
					intervalList[nextIndex] + nextOffset,
					nextUnitOffset);

			if (++nextUnitOffset >= unit) {
				if (nextIndex >= finishIndex) {
					if (++nextOffset > finishOffset) {
						nextIndex++;
					}
				}
				else {
					if (++nextOffset >= countList[nextIndex]) {
						nextIndex++;
						nextOffset = 0;
					}
				}
				nextUnitOffset = 0;
			}

			return subId;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

	static class LargeInfoCache {

		private final int limit;

		private final int expirationMillis;

		private long lastCheckNanos;

		private Map<ContainerKey, LargeInfo> map =
				new LinkedHashMap<ContainerKey, LargeInfo>();

		public LargeInfoCache(int limit, int expirationMillis) {
			this.limit = limit;
			this.expirationMillis = expirationMillis;
			lastCheckNanos = currentNanosForExpiration();
		}

		public long currentNanosForExpiration() {
			return isExpirable() ? System.nanoTime() : 0;
		}

		public boolean isExpirable() {
			return expirationMillis >= 0;
		}

		public boolean checkExpiration(LargeInfo largeInfo, long curNanos) {
			if (!isExpirable() || largeInfo.partitioningType == null) {
				return false;
			}

			if (isTimeRotated(lastCheckNanos, curNanos)) {
				map.clear();
				return true;
			}

			return isTimeExpired(
					largeInfo.cachedNanos, curNanos, expirationMillis);
		}

		public LargeInfo find(ContainerKey normalizedKey, long curNanos) {
			final LargeInfo largeInfo = map.get(normalizedKey);

			if (largeInfo != null && checkExpiration(largeInfo, curNanos)) {
				remove(normalizedKey);
				return null;
			}

			return largeInfo;
		}

		public void add(ContainerKey normalizedKey, LargeInfo largeInfo) {
			if (map.put(normalizedKey, largeInfo) == null) {
				adjust();
			}
		}

		public void remove(ContainerKey normalizedKey) {
			map.remove(normalizedKey);
		}

		private void adjust() {
			int extra = map.size() - limit;
			if (extra <= 0) {
				return;
			}

			for (Iterator<ContainerKey> it = map.keySet().iterator();
					it.hasNext();) {
				remove(it.next());

				if (--extra <= 0) {
					break;
				}
			}
		}

		private static boolean isTimeExpired(
				long nanos1, long nanos2, int expirationMillis) {
			if (isTimeRotated(nanos1, nanos2)) {
				return true;
			}

			return (nanos2 - nanos1) / 1000 / 1000 >= expirationMillis;
		}

		private static boolean isTimeRotated(long nanos1, long nanos2) {
			return (nanos1 < 0) != (nanos2 < 0);
		}

	}

	static class SubContainerLocator {

		public static ContainerKey getSubContainerKey(
				ContainerKey largeKey, ContainerKeyConverter keyConverter,
				long subId, int subCount, int clusterPartitionCount,
				long largeId, int mode) throws GSException {
			final ContainerKeyConverter.Components components =
					new ContainerKeyConverter.Components();
			keyConverter.decompose(largeKey, components);

			final long affinityNum;
			if (mode == 0) {
				if (subId < 0 || subId >= subCount) {
					throw new GSException(GSErrorCode.INTERNAL_ERROR, "");
				}
				affinityNum = getClusterPartitionIndex(
						components.base, components.affinityStr,
						components.affinityNum, (int) subId, subCount,
						clusterPartitionCount);
			}
			else {
				affinityNum = subId;
			}

			components.affinityStr = null;
			components.affinityNum = affinityNum;
			components.largeId = largeId;

			return keyConverter.compose(components, false);
		}

		private static int getClusterPartitionIndex(
				String baseName, String affinityStr, long affinityNum,
				int subIndex, int subCount, int clusterPartitionCount)
				throws GSException {
			if (subIndex == 0) {
				if (affinityStr == null && affinityNum < 0) {
					final String normalizedBase =
							RowMapper.normalizeSymbolUnchecked(baseName);
					return hash(normalizedBase, clusterPartitionCount);
				}
				else {
					return affinityToPartition(
							affinityStr, affinityNum, clusterPartitionCount);
				}
			}
			else {
				final int base = clusterPartitionCount / subCount;
				final int mod = clusterPartitionCount % subCount;

				final int baseIndex;
				{
					final int largePartition = getClusterPartitionIndex(
							baseName, affinityStr, affinityNum,
							0, subCount, clusterPartitionCount);
					baseIndex = groupIndex(largePartition, base, mod);
				}
				final int rawIndex = (baseIndex + subIndex) % subCount;

				if (subCount > clusterPartitionCount) {
					return rawIndex % clusterPartitionCount;
				}
				else {
					final int offset = groupOffset(rawIndex, base, mod);
					final int range = groupRange(rawIndex, base, mod);

					final String hashingStr =
							RowMapper.normalizeSymbolUnchecked(baseName) +
							"/" + rawIndex;
					return offset + hash(hashingStr, range);
				}
			}
		}

		private static int groupOffset(int index, int base, int mod) {
			return base * index + Math.min(mod, index);
		}

		private static int groupRange(int index, int base, int mod) {
			return base + (index < mod ? 1 : 0);
		}

		private static int groupIndex(int offset, int base, int mod) {
			if (offset < (base + 1) * mod) {
				return offset / (base + 1);
			}
			else {
				return (offset - mod) / base;
			}
		}

		private static int affinityToPartition(
				String affinityStr, long affinityNum,
				int clusterPartitionCount) throws GSException {
			if (affinityNum >= 0) {
				return (int) ((affinityNum & 0xffffffffL) %
						clusterPartitionCount);
			}
			else {
				final String normalizedStr =
						RowMapper.normalizeSymbolUnchecked(affinityStr);
				return hash(normalizedStr, clusterPartitionCount);
			}
		}

		private static int hash(
				String normalizedStr, int count) throws GSException {
			return GridStoreChannel.calculatePartitionId(
					normalizedStr, ContainerHashMode.COMPATIBLE1, count);
		}

		public static ContainerKey subKeyToLarge(
				ContainerKey subKey, ContainerKey baseLargeKey,
				ContainerKeyConverter keyConverter) throws GSException {
			final ContainerKeyConverter.Components subComponents =
					new ContainerKeyConverter.Components();
			final ContainerKeyConverter.Components largeComponents =
					new ContainerKeyConverter.Components();

			keyConverter.decompose(subKey, subComponents);
			keyConverter.decompose(baseLargeKey, largeComponents);

			largeComponents.base = subComponents.base;

			return keyConverter.compose(largeComponents, false);
		}

	}

	private static abstract class SchemaPropertiesResolver<S> {

		private S source;

		protected void setSource(S source) {
			this.source = source;
		}

		public S getSource() {
			return source;
		}

		public abstract ContainerProperties resolve(ContainerKey key)
				throws GSException; 

	}

	static class ValueHasher {

		public static int hashAndMod(
				Object value, int count) throws GSException {
			return (int) (hashAsUInt(value) % count);
		}

		public static long hashAsUInt(Object value) throws GSException {
			return hash(value) & 0xffffffffL;
		}

		public static int hash(Object value) throws GSException {
			return hash(FNV1a.init32(), value);
		}

		public static int hash(int base, Object value) throws GSException {
			if (value instanceof Number) {
				return hashNumber(base, (Number) value);
			}
			if (value instanceof Date) {
				if (value instanceof Timestamp) {
					return hashPreciseTimestamp(base, (Timestamp) value);
				}
				return hashLong(base, ((Date) value).getTime());
			}
			else if (value instanceof String) {
				return hashString(base, (String) value);
			}
			else if (value instanceof Blob) {
				return hashBlob(base, (Blob) value);
			}
			else if (value instanceof Boolean) {
				return hashNumber(base, ((Boolean) value) ? 1 : 0);
			}
			else if (value == null) {
				return base;
			}
			else {
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"Illegal type for container partitioning key (class=" +
						value.getClass().getName() + ")");
			}
		}

		public static int hashNumber(int base, Number value) {
			if (value instanceof Double ||
					value instanceof Float) {
				return hashDouble(base, value.doubleValue());
			}
			else {
				return hashLong(base, value.longValue());
			}
		}

		private static int hashLong(int base, long value) {
			return hashDouble(base, (double) value);
		}

		private static int hashDouble(int base, double value) {
			if (Double.isNaN(value)) {
				return base;
			}

			final long bits = Double.doubleToRawLongBits(value);
			int hash = base;
			hash = FNV1a.update(hash, (byte) bits);
			hash = FNV1a.update(hash, (byte) (bits >> 8));
			hash = FNV1a.update(hash, (byte) (bits >> 16));
			hash = FNV1a.update(hash, (byte) (bits >> 24));
			hash = FNV1a.update(hash, (byte) (bits >> 32));
			hash = FNV1a.update(hash, (byte) (bits >> 40));
			hash = FNV1a.update(hash, (byte) (bits >> 48));
			hash = FNV1a.update(hash, (byte) (bits >> 56));
			return hash;
		}

		private static int hashPreciseTimestamp(int base, Timestamp value) {
			int hash = base;
			hash = hashLong(hash, value.getTime());

			int nanos = value.getNanos();
			if (nanos != 0) {
				hash = FNV1a.update(hash, (byte) nanos);
				hash = FNV1a.update(hash, (byte) (nanos >> 8));
				hash = FNV1a.update(hash, (byte) (nanos >> 16));
				hash = FNV1a.update(hash, (byte) (nanos >> 24));
			}

			return hash;
		}

		private static int hashString(int base, String value) {
			final byte[] bytes =
					value.getBytes(BasicBuffer.DEFAULT_CHARSET);
			return FNV1a.update(base, bytes, bytes.length);
		}

		private static int hashBlob(int base, Blob value) throws GSException {
			try {
				final InputStream in = value.getBinaryStream();
				try {
					return hashInputStream(base, in);
				}
				finally {
					in.close();
				}
			}
			catch (SQLException e) {
				throw new GSException(e);
			}
			catch (IOException e) {
				throw new GSException(e);
			}
		}

		private static int hashInputStream(
				int base, InputStream in) throws IOException {
			int hash = base;
			final byte[] buf = new byte[8192];
			for (int len; (len = in.read()) >= 0;) {
				hash = FNV1a.update(hash, buf, len);
			}
			return hash;
		}

	}

	private static enum SubOperation {
		CREATE_BEGIN,
		CREATE_END,
		DROP_BEGIN,
		DROP_END,
		INDEX_CREATE_BEGIN,
		INDEX_CREATE_END,
		INDEX_DROP_BEGIN,
		INDEX_DROP_END;

		boolean isBegin() {
			switch (this) {
			case CREATE_BEGIN:
			case DROP_BEGIN:
			case INDEX_CREATE_BEGIN:
			case INDEX_DROP_BEGIN:
				return true;
			default:
				return false;
			}
		}

		SubOperation toBegin() {
			switch (this) {
			case CREATE_END:
				return CREATE_BEGIN;
			case DROP_END:
				return DROP_BEGIN;
			case INDEX_CREATE_END:
				return INDEX_CREATE_BEGIN;
			case INDEX_DROP_END:
				return INDEX_DROP_BEGIN;
			default:
				return this;
			}
		}

		SubOperation toEnd() {
			switch (this) {
			case CREATE_BEGIN:
				return CREATE_END;
			case DROP_BEGIN:
				return DROP_END;
			case INDEX_CREATE_BEGIN:
				return INDEX_CREATE_END;
			case INDEX_DROP_BEGIN:
				return INDEX_DROP_END;
			default:
				return this;
			}
		}

	}

	private static class SubOperationStatus {

		private final SubOperation goalOp;

		private final long goalSubId;

		private SubOperation nextOp;

		private Long nextSubId;

		private LargeInfo info;

		SubOperationStatus(
				SubOperation goalOp, long goalSubId, LargeInfo info)
				throws ContainerPartitioningException {
			this.goalOp = goalOp;
			this.goalSubId = goalSubId;
			accept(info);
		}

		void accept(LargeInfo info) throws ContainerPartitioningException {
			if (this.info != null && this.info.containerId != info.containerId) {
				throw new ContainerPartitioningException(
						GSErrorCode.UNSUPPORTED_OPERATION,
						"Partitioned container unexpectedly changed", null);
			}

			final boolean reached;
			switch (goalOp) {
			case CREATE_END:
				if (info.isDisabled(goalSubId)) {
					throw errorUnreachableOperation();
				}
				reached = info.isAvailable(goalSubId);
				break;
			case DROP_END:
				reached = info.isDisabled(goalSubId);
				break;
			default:
				throw new IllegalStateException();
			}

			final Long subId = info.operatingSubId;
			final SubOperation op = info.operation;
			if (op != null && !op.isBegin()) {
				throw new IllegalArgumentException();
			}

			final SubOperation nextOp;
			final Long nextSubId;
			if (reached) {
				nextOp = null;
				nextSubId = null;
			}
			else if (op == null) {
				nextOp = goalOp.toBegin();
				nextSubId = goalSubId;
			}
			else if (op == this.nextOp && subId.equals(this.nextSubId)) {
				nextOp = op.toEnd();
				nextSubId = subId;
			}
			else {
				nextOp = op;
				nextSubId = subId;
			}

			this.nextSubId = nextSubId;
			this.nextOp = nextOp;
			this.info = info;
		}

		Long getNextSubId() {
			return nextSubId;
		}

		SubOperation getNextOperation() {
			return nextOp;
		}

		private static ContainerPartitioningException
		errorUnreachableOperation() {
			return new ContainerPartitioningException(
					GSErrorCode.UNSUPPORTED_OPERATION,
					"Dropped container partition cannot create again", null);
		}

	}

	private enum AlternationType {
		PUT_SUB_STATE,
		PUT_SUB_CONTAINER_VERSION,
		RECOVER_SUB_CONTAINER
	}

	static abstract class Partitioner<V> {

		private static final int LIST_THRESHOLD = 128;

		private final List<V> list = new ArrayList<V>();

		private final BitSet bits = new BitSet();

		private Map<Long, V> map;

		private Iterator<Map.Entry<Long, V>> mapIterator;

		private V fullValue;

		private int minIndex;

		private int maxIndex;

		private boolean uncovered;

		private Map.Entry<Long, V> nextMapEntry;

		{
			clearIndexRange();
		}

		private void clearIndexRange() {
			minIndex = Integer.MAX_VALUE;
			maxIndex = -1;
		}

		private long prepareMapEntry() {
			do {
				if (map == null) {
					break;
				}

				if (nextMapEntry == null) {
					if (mapIterator == null) {
						mapIterator = map.entrySet().iterator();
					}
					if (mapIterator.hasNext()) {
						nextMapEntry = mapIterator.next();
					}
					else {
						break;
					}
				}
				return nextMapEntry.getKey();
			}
			while (false);

			return -1;
		}

		protected void put(long subId, V value) {
			if (subId >= LIST_THRESHOLD) {
				if (map == null) {
					map = new HashMap<Long, V>();
				}
				map.put(subId, value);
				return;
			}

			final int subIndex = (int) subId;
			while (subIndex >= list.size()) {
				list.add(null);
			}

			list.set(subIndex, value);
			bits.set(subIndex);

			if (subIndex < minIndex) {
				minIndex = subIndex;
			}

			if (subIndex > maxIndex) {
				maxIndex = subIndex;
			}
		}

		protected void putFull(V value) {
			fullValue = value;
		}

		protected V get(long subId) {
			if (subId >= list.size()) {
				if (map == null) {
					return null;
				}
				return map.get(subId);
			}

			final int subIndex = (int) subId;
			return list.get(subIndex);
		}

		protected void setUncovered(boolean uncovered) {
			this.uncovered = uncovered;
		}

		public void clear() {
			fullValue = null;
			clearIndexRange();
			list.clear();
			bits.clear();

			if (map != null) {
				map.clear();
				mapIterator = null;
				nextMapEntry = null;
			}

			uncovered = false;
		}

		public abstract void add(LargeInfo largeInfo, V value)
				throws GSException, ContainerPartitioningException;

		public long getNextId() {
			if (minIndex > maxIndex) {
				return prepareMapEntry();
			}

			return minIndex;
		}

		public V next() {
			if (minIndex > maxIndex) {
				if (prepareMapEntry() < 0) {
					return null;
				}
				final V value = nextMapEntry.getValue();
				nextMapEntry = null;
				return value;
			}

			final int subIndex = minIndex;
			final V value = list.get(subIndex);

			minIndex = bits.nextSetBit(subIndex + 1);
			if (minIndex < 0) {
				clearIndexRange();
			}

			return value;
		}

		public V popFull() {
			final V value = fullValue;
			fullValue = null;
			return value;
		}

		public boolean isUncovered() {
			return uncovered;
		}

	}

	static abstract class ListPartitioner<E> extends Partitioner<List<E>> {

		protected abstract long getSubId(LargeInfo largeInfo, E elem)
				throws GSException, ContainerPartitioningException;

		public void addElement(LargeInfo largeInfo, E elem)
				throws GSException, ContainerPartitioningException {
			if (elem == null) {
				throw new GSException(
						GSErrorCode.EMPTY_PARAMETER, "Null row found");
			}

			final long subId = getSubId(largeInfo, elem);
			List<E> list = get(subId);
			if (list == null) {
				list = new ArrayList<E>();
				put(subId, list);
			}
			list.add(elem);
		}

		@Override
		public void add(LargeInfo largeInfo, List<E> list)
				throws GSException, ContainerPartitioningException {
			add(largeInfo, (Iterable<E>) list);
		}

		public void add(LargeInfo largeInfo, Iterable<E> iterable)
				throws GSException, ContainerPartitioningException {
			for (E row : iterable) {
				addElement(largeInfo, row);
			}
		}

	}

	static class RowPartitioner extends ListPartitioner<Row> {

		private final Object[] partitioningValues =
				new Object[LargeInfo.MAX_PARTITIONING_COLUMN_COUNT];

		@Override
		protected long getSubId(LargeInfo largeInfo, Row elem)
				throws GSException, ContainerPartitioningException {
			final int count = largeInfo.getPartitioningColumnCount();
			for (int i = 0; i < count; i++) {
				final int column = largeInfo.getPartitioningColumn(i);
				partitioningValues[i] = elem.getValue(column);
			}
			return largeInfo.subIdFromValues(partitioningValues);
		}

	}

	static class PredicatePartitioner extends Partitioner<RowKeyPredicate<?>> {

		@Override
		public void add(LargeInfo largeInfo, RowKeyPredicate<?> pred)
				throws GSException, ContainerPartitioningException {
			if (pred == null) {
				throw new GSException(
						GSErrorCode.EMPTY_PARAMETER, "Null predicate found");
			}

			final boolean decomposed = false;
			final Iterable<?> distinctKeys = pred.getDistinctKeys();
			final boolean keyPartitioned = largeInfo.isKeyPartitioned();
			if (!keyPartitioned || distinctKeys == null) {
				if (largeInfo.isOrdered()) {
					setUncovered(true);
				}

				final Object start = pred.getStart();
				final Object finish = pred.getFinish();
				if (!keyPartitioned || !largeInfo.isOrdered() ||
						(start == null && finish == null)) {
					putFull(pred);
					return;
				}

				final Long startId = (start == null ? null :
						largeInfo.subIdFromKey(start, decomposed, true));
				final Long finishId = (finish == null ? null :
						largeInfo.subIdFromKey(finish, decomposed, false));
				for (Long subId : largeInfo.subIdRange(startId, finishId)) {
					put(subId, pred);
				}
			}
			else {
				for (Object key : distinctKeys) {
					boolean found = false;
					for (Long subId :
							largeInfo.subIdRangeByKey(key, decomposed)) {
						put(subId, pred);
						found = true;
					}
					if (!found) {
						setUncovered(true);
					}
				}
			}
		}

	}

	static abstract class GeneralKeyConverter<S> {
		abstract ContainerKey convert(
				S src, ContainerKeyConverter keyConverter) throws GSException;
		static class ForString extends GeneralKeyConverter<String> {
			@Override
			ContainerKey convert(
					String src, ContainerKeyConverter keyConverter)
					throws GSException {
				return keyConverter.parse(src, false);
			}
		}
		static class ForKey extends GeneralKeyConverter<ContainerKey> {
			@Override
			ContainerKey convert(
					ContainerKey src, ContainerKeyConverter keyConverter)
					throws GSException {
				return src;
			}
		}
	}

	private class PartitionedMultiOperationContext<V, R>
	implements MultiOperationContext<ContainerKey, V, R> {

		private final Partitioner<V> partitioner;

		private final ContainerKeyConverter keyConverter;

		private Map<String, ? extends V> nameTargetMap;

		private Map<ContainerKey, ? extends V> keyTargetMap;

		private Map<ContainerKey, ContainerKey> subToLargeNameMap;

		private Set<ContainerKey> singleCompletedSet;

		private Set<ContainerKey> subCompletedSet;

		private Set<ContainerKey> subVisitedSet;

		private boolean firstListed;

		private boolean singleLost;

		private boolean subOrSingleLost;

		public PartitionedMultiOperationContext(
				Partitioner<V> partitioner,
				Map<String, ? extends V> targetMap) throws GSException {
			this.partitioner = partitioner;
			this.keyConverter =
					getContainerKeyConverter(false, !isResultAcceptable());
			this.nameTargetMap = targetMap;
			this.keyTargetMap = Collections.emptyMap();
		}

		@Override
		public void listTarget(MultiTargetConsumer<ContainerKey, V> consumer)
				throws GSException {
			if (subToLargeNameMap != null || singleCompletedSet != null) {
				updateTarget(keyConverter);
			}

			listTarget(
					consumer, nameTargetMap,
					new GeneralKeyConverter.ForString(), keyConverter);
			listTarget(
					consumer, keyTargetMap,
					new GeneralKeyConverter.ForKey(), keyConverter);
			firstListed = true;
			subOrSingleLost = false;
		}

		private <K> void listTarget(
				MultiTargetConsumer<ContainerKey, V> consumer,
				Map<K, ? extends V> targetMap,
				GeneralKeyConverter<K> converter,
				ContainerKeyConverter keyConverter) throws GSException {
			final int entryCount = targetMap.size();
			final List<ContainerKey> keyList = new ArrayList<ContainerKey>();
			final List<V> valueList = new ArrayList<V>();
			for (Map.Entry<K, ? extends V> entry : targetMap.entrySet()) {
				final ContainerKey key =
						converter.convert(entry.getKey(), keyConverter);
				keyList.add(key);
				valueList.add(entry.getValue());
			}

			final Map<ContainerKey, LargeInfo> largeInfoMap =
					getAllLargeInfo(keyList, keyConverter);

			for (int i = 0; i < entryCount; i++) {
				final ContainerKey key = keyList.get(i);
				final V value = valueList.get(i);

				for (boolean refreshed = false;; refreshed = true) {
					LargeInfo largeInfo = (refreshed ? null : largeInfoMap.get(key));
					if (refreshed) {
						largeInfo = getLargeInfo(
								key.toCaseSensitive(false), false,
								keyConverter);
					}

					if (largeInfo != null && largeInfo.isLarge()) {
						partitioner.clear();
						try {
							partitioner.add(largeInfo, value);
						}
						catch (ContainerPartitioningException e) {
							acceptContainerPartitioningException(
									e, refreshed, key, largeInfo);
							continue;
						}
						if (!consumeLarge(
								consumer, largeInfo, key, keyConverter,
								refreshed)) {
							continue;
						}
					}
					else {
						if (largeInfo == null) {
							singleLost = true;
						}
						consumer.consume(
								key, value,
								ContainerKeyPredicate.ATTRIBUTE_SINGLE, null);
					}
					break;
				}
			}
		}

		private boolean consumeLarge(
				MultiTargetConsumer<ContainerKey, V> consumer,
				LargeInfo largeInfo,
				ContainerKey largeKey, ContainerKeyConverter keyConverter,
				boolean refreshed) throws GSException {
			final V fullValue = partitioner.popFull();
			if (fullValue != null) {
				for (long subId : largeInfo) {
					if (!consumeLarge(
							consumer, largeInfo, largeKey, keyConverter, subId,
							fullValue, refreshed)) {
						return false;
					}
				}
			}

			for (long subId; (subId = partitioner.getNextId()) >= 0;) {
				if (!consumeLarge(
						consumer, largeInfo, largeKey, keyConverter, subId,
						partitioner.next(), refreshed)) {
					return false;
				}
			}

			return true;
		}

		private boolean consumeLarge(
				MultiTargetConsumer<ContainerKey, V> consumer,
				LargeInfo largeInfo, ContainerKey largeKey,
				ContainerKeyConverter keyConverter,
				long subId, V value, boolean refreshed)
				throws GSException {

			final ContainerKey subKey =
					largeInfo.generateSubKey(largeKey, keyConverter, subId);
			final ContainerKey normalizedSubKey =
					subKey.toCaseSensitive(false);

			if (subCompletedSet != null &&
					subCompletedSet.contains(normalizedSubKey)) {
				return true;
			}

			boolean expired = false;
			if (!isResultAcceptable() && largeInfo.isExpired(subId)) {
				expired = true;
			}
			else if (!isResultAcceptable() &&
					(subOrSingleLost || !largeInfo.isAvailable(subId))) {
				if (subVisitedSet == null) {
					subVisitedSet = new HashSet<ContainerKey>();
				}

				final boolean always = true;
				final boolean checkOnly =
						subVisitedSet.contains(normalizedSubKey);

				Container<?, ?> container;
				try {
					container = getSubContainer(
							largeKey, new LargeInfo[] { largeInfo }, subId,
							RowMapper.BindingTool.createBindType(
									null, Row.class, Container.class),
							true, always, checkOnly);
				}
				catch (ContainerPartitioningException e) {
					acceptContainerPartitioningException(
							e, refreshed, largeKey, null);
					return false;
				}
				catch (ContainerExpirationException e) {
					removeLargeInfo(
							largeKey.toCaseSensitive(false), largeInfo);
					container = null;
					expired = true;
				}

				if (container != null) {
					container.close();
				}

				subVisitedSet.add(normalizedSubKey);
			}

			if (expired) {
				if (subCompletedSet == null) {
					subCompletedSet = new HashSet<ContainerKey>();
				}
				subCompletedSet.add(normalizedSubKey);
				return true;
			}

			OptionalRequestSource source = null;
			if (!firstListed && partitioner.isUncovered()) {
				source = getSubContainerVersionOptionSource(
						largeInfo.partitioningVersion);
			}

			consumer.consume(
					subKey, value, PartContainer.Attributes.SUB, source);

			if (subToLargeNameMap == null) {
				subToLargeNameMap = new HashMap<ContainerKey, ContainerKey>();
			}
			subToLargeNameMap.put(normalizedSubKey, largeKey);
			return true;
		}

		private void acceptContainerPartitioningException(
				ContainerPartitioningException exception,
				boolean refreshed, ContainerKey key,
				LargeInfo largeInfo) throws GSException {
			removeLargeInfo(key.toCaseSensitive(false), largeInfo);
			if (!refreshed) {
				return;
			}
			throw new GSException(
					exception.getErrorCode(), null,
					exception.getMessage() + " (containerName=" + key + ")",
					Collections.singletonMap("containerName", key.toString()),
					exception);
		}

		private void updateTarget(
				ContainerKeyConverter keyConverter) throws GSException {
			if (singleCompletedSet == null) {
				singleCompletedSet = Collections.emptySet();
			}
			if (subToLargeNameMap == null) {
				subToLargeNameMap = Collections.emptyMap();
			}
			else if (subCompletedSet != null) {
				subToLargeNameMap.keySet().removeAll(subCompletedSet);
			}

			final Set<ContainerKey> restLargeSet = new HashSet<ContainerKey>();
			for (ContainerKey key : subToLargeNameMap.values()) {
				final ContainerKey normalizedKey = key.toCaseSensitive(false);
				restLargeSet.add(normalizedKey);
			}

			final Map<ContainerKey, V> nextTargetMap =
					new HashMap<ContainerKey, V>();
			makeNextTargetMap(
					new GeneralKeyConverter.ForString(), keyConverter,
					restLargeSet, nameTargetMap, nextTargetMap);
			makeNextTargetMap(
					new GeneralKeyConverter.ForKey(), keyConverter,
					restLargeSet, keyTargetMap, nextTargetMap);
			nameTargetMap = Collections.emptyMap();
			keyTargetMap = nextTargetMap;

			singleCompletedSet = null;
			subToLargeNameMap = null;

			if (isResultAcceptable()) {
				subCompletedSet = null;
			}

			disableCache(keyTargetMap, new GeneralKeyConverter.ForKey(), true);
		}

		private <K> void makeNextTargetMap(
				GeneralKeyConverter<K> converter,
				ContainerKeyConverter keyConverter,
				Set<ContainerKey> restLargeSet,
				Map<K, ? extends V> prevTargetMap,
				Map<ContainerKey, V> nextTargetMap) throws GSException {
			for (Map.Entry<K, ? extends V> entry : prevTargetMap.entrySet()) {
				final ContainerKey key =
						converter.convert(entry.getKey(), keyConverter);
				final ContainerKey normalizedKey = key.toCaseSensitive(false);

				if (!restLargeSet.contains(normalizedKey) ||
						singleCompletedSet.contains(normalizedKey)) {
					continue;
				}
				clearResult(normalizedKey);
				nextTargetMap.put(key, entry.getValue());
			}
		}

		@Override
		public boolean acceptException(
				GSException exception) throws GSException {
			disableCache(
					nameTargetMap, new GeneralKeyConverter.ForString(), true);
			disableCache(keyTargetMap, new GeneralKeyConverter.ForKey(), true);

			if (exception.getErrorCode() == CONTAINER_NOT_FOUND_ERROR_CODE &&
					!isResultAcceptable() && !singleLost &&
					!(subToLargeNameMap == null ||
							subToLargeNameMap.isEmpty())) {
				subOrSingleLost = true;
				return true;
			}

			return exception.getErrorCode() == ATTRIBUTE_UNMATCH_ERROR_CODE ||
					isSubContainerExpirationError(exception) ||
					isSubContainerVersionError(exception);
		}

		@Override
		public void acceptCompletion(
				ContainerKey containerKey, R result) throws GSException {
			final ContainerKey normalizedKey =
					containerKey.toCaseSensitive(false);

			if (subToLargeNameMap == null ||
					!subToLargeNameMap.containsKey(normalizedKey)) {

				if (singleCompletedSet == null) {
					singleCompletedSet = new HashSet<ContainerKey>();
				}
				singleCompletedSet.add(normalizedKey);
			}
			else {
				if (subCompletedSet == null) {
					subCompletedSet = new HashSet<ContainerKey>();
				}
				subCompletedSet.add(normalizedKey);
			}
		}

		@Override
		public boolean isRemaining() throws GSException {
			if (isResultAcceptable()) {
				return keyTargetMap.isEmpty() &&
						subToLargeNameMap != null &&
						!(subCompletedSet == null ?
								Collections.<ContainerKey>emptySet() :
								subCompletedSet).containsAll(
										subToLargeNameMap.keySet());
			}
			else {
				return false;
			}
		}

		protected boolean isResultAcceptable() {
			return false;
		}

		protected void clearResult(ContainerKey containerKey)
				throws GSException {
		}

		protected ContainerKey toNonSubKey(
				ContainerKey key) throws GSException {
			final ContainerKey normalizedKey = key.toCaseSensitive(false);
			final ContainerKey largeKey = (subToLargeNameMap == null ?
					null : subToLargeNameMap.get(normalizedKey));

			if (largeKey == null) {
				return key;
			}
			else {
				return SubContainerLocator.subKeyToLarge(
						key, largeKey, keyConverter);
			}
		}

	}

	private class ListingMultiOperationContext<V, E>
	extends PartitionedMultiOperationContext<V, List<E>> {

		private final Map<String, List<E>> resultMap =
				new HashMap<String, List<E>>();

		public ListingMultiOperationContext(
				Partitioner<V> partitioner,
				Map<String, ? extends V> targetMap) throws GSException {
			super(partitioner, targetMap);
		}

		@Override
		public void acceptCompletion(
				ContainerKey containerKey, List<E> result) throws GSException {
			final String nonSubName = toNonSubKey(containerKey).toString();

			List<E> list = resultMap.get(nonSubName);
			if (list == null) {
				list = new ArrayList<E>();
				resultMap.put(nonSubName, list);
			}
			list.addAll(result);

			super.acceptCompletion(containerKey, result);
		}

		public Map<String, List<E>> getResultMap() {
			return resultMap;
		}

		@Override
		protected boolean isResultAcceptable() {
			return true;
		}

		@Override
		protected void clearResult(ContainerKey containerKey)
				throws GSException {
			final String nonSubName = toNonSubKey(containerKey).toString();
			resultMap.remove(nonSubName);
		}

	}

	enum PartitionedStatement {

		ALTER_CONTAINER_PROPERTIES;

		static final int ORDINAL_OFFSET = 201;

		private final GeneralStatement general;

		private PartitionedStatement() {
			general = GeneralStatement.getInstance(
					name(), ORDINAL_OFFSET + ordinal());
		}

		GeneralStatement generalize() {
			return general;
		}

	}

	enum ExtRequestOptionType {

		SUB_CONTAINER_VERSION(13001),
		SUB_CONTAINER_EXPIRABLE(13002),
		DIST_QUERY(13003),
		LARGE_INFO(13004);

		private final int number;

		ExtRequestOptionType(int number) {
			this.number = number;
		}

		public int number() {
			return number;
		}

	}

	enum ContainerSchemaOptionType {

		PARTITION_EXPIRATION(100);

		private final int number;

		ContainerSchemaOptionType(int number) {
			this.number = number;
		}

		public int number() {
			return number;
		}

	}

}
