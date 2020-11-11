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

import java.net.URL;
import java.sql.Blob;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.toshiba.mwcloud.gs.Aggregation;
import com.toshiba.mwcloud.gs.AggregationResult;
import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.ContainerType;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.Geometry;
import com.toshiba.mwcloud.gs.GeometryOperator;
import com.toshiba.mwcloud.gs.IndexInfo;
import com.toshiba.mwcloud.gs.IndexType;
import com.toshiba.mwcloud.gs.InterpolationMode;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.QueryOrder;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.TimeOperator;
import com.toshiba.mwcloud.gs.TimeSeries;
import com.toshiba.mwcloud.gs.TimeUnit;
import com.toshiba.mwcloud.gs.TriggerInfo;
import com.toshiba.mwcloud.gs.common.ContainerKeyConverter;
import com.toshiba.mwcloud.gs.common.ContainerKeyConverter.ContainerKey;
import com.toshiba.mwcloud.gs.common.ContainerProperties;
import com.toshiba.mwcloud.gs.common.Extensibles;
import com.toshiba.mwcloud.gs.common.Extensibles.AsContainer;
import com.toshiba.mwcloud.gs.common.GSErrorCode;
import com.toshiba.mwcloud.gs.experimental.ContainerAttribute;
import com.toshiba.mwcloud.gs.experimental.Experimentals;
import com.toshiba.mwcloud.gs.partitioned.PartStore.ContainerExpirationException;
import com.toshiba.mwcloud.gs.partitioned.PartStore.ContainerPartitioningException;
import com.toshiba.mwcloud.gs.partitioned.PartStore.LargeInfo;
import com.toshiba.mwcloud.gs.partitioned.PartStore.ListPartitioner;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.OptionalRequestSource;

abstract class PartContainer<K, R> implements Container<K, R>,
Extensibles.AsContainer<K, R>, Experimentals.AsContainer<K, R> {

	private PartStore store;

	private final ContainerKey largeKey;

	private final long largeId;

	private LargeInfo largeInfo;

	private final Class<R> rowType;

	private final Map<Long, Extensibles.AsContainer<K, R>> subMap;

	private List<Extensibles.AsContainer<K, R>> disabledSubList;

	protected PartContainer(
			PartStore store, ContainerKey largeKey, LargeInfo largeInfo,
			Container.BindType<K, R, ?> bindType) {
		this.store = store;
		this.largeKey = largeKey;
		this.largeId = largeInfo.containerId;
		this.largeInfo = largeInfo;
		this.rowType = bindType.getRowClass();
		subMap = new HashMap<Long, Extensibles.AsContainer<K, R>>();
	}

	@Override
	public boolean put(K key, R row) throws GSException {
		GSErrorCode.checkNullParameter(row, "row", null);

		final SingleRowPartitioner<K, R> partitioner = createRowPartitioner();
		boolean succeeded = false;
		try {
			final long subId = (key == null ?
					partitioner.getSubId(row) :
					partitioner.getSubId(key, row));
			final boolean ret;
			try {
				ret = getSub(subId, true).put(key, row);
			}
			catch (ContainerExpirationException e) {
				return false;
			}
			catch (GSException e) {
				if (!PartStore.isSubContainerExpirationError(e)) {
					throw e;
				}
				return false;
			}
			succeeded = true;
			return ret;
		}
		finally {
			disableCacheOnError(succeeded);
		}
	}

	@Override
	public boolean put(R row) throws GSException {
		return put(null, row);
	}

	@Override
	public boolean put(
			java.util.Collection<R> rowCollection) throws GSException {
		GSErrorCode.checkNullParameter(rowCollection, "rowCollection", null);

		boolean succeeded = false;
		try {
			final RowListPartitioner<R> partitioner =
					new RowListPartitioner<R>(createRowPartitioner());
			partitioner.add(null, rowCollection);

			boolean expired = false;
			for (long subId; (subId = partitioner.getNextId()) >= 0;) {
				final List<R> list = partitioner.next();
				try {
					getSub(subId, true).put(list);
				}
				catch (ContainerExpirationException e) {
					expired = true;
					continue;
				}
				catch (GSException e) {
					if (!PartStore.isSubContainerExpirationError(e)) {
						throw e;
					}
					expired = true;
					continue;
				}
			}
			succeeded = !expired;
		}
		catch (ContainerPartitioningException e) {
			throw e.toGSException();
		}
		finally {
			disableCacheOnError(succeeded);
		}

		return false;
	}

	@Override
	public R get(K key) throws GSException {
		return get(key, false);
	}

	@Override
	public R get(K key, boolean forUpdate) throws GSException {
		if (forUpdate) {
			errorNotSupported();
		}

		GSErrorCode.checkNullParameter(key, "key", null);

		boolean succeeded = false;
		try {
			boolean expired = false;
			R row = null;
			for (Long subId : subIdRangeByRowKey(key)) {
				final Container<K, R> sub;
				try {
					sub = getSub(subId, false);
				}
				catch (ContainerExpirationException e) {
					expired = true;
					continue;
				}
				if (sub == null) {
					continue;
				}
				try {
					row = sub.get(key);
				}
				catch (GSException e) {
					if (!PartStore.isSubContainerExpirationError(e)) {
						throw e;
					}
					expired = true;
					continue;
				}
				if (row != null) {
					break;
				}
			}
			succeeded = !expired;
			return row;
		}
		catch (ContainerPartitioningException e) {
			throw e.toGSException();
		}
		finally {
			disableCacheOnError(succeeded);
		}
	}

	@Override
	public boolean remove(K key) throws GSException {
		GSErrorCode.checkNullParameter(key, "key", null);

		boolean succeeded = false;
		try {
			boolean expired = false;
			boolean ret = false;
			for (Long subId : subIdRangeByRowKey(key)) {
				final Container<K, R> sub;
				try {
					sub = getSub(subId, false);
				}
				catch (ContainerExpirationException e) {
					expired = true;
					continue;
				}
				if (sub == null) {
					continue;
				}
				try {
					ret = sub.remove(key);
				}
				catch (GSException e) {
					if (!PartStore.isSubContainerExpirationError(e)) {
						throw e;
					}
					expired = true;
					continue;
				}
				if (ret) {
					break;
				}
			}
			succeeded = !expired;
			return ret;
		}
		catch (ContainerPartitioningException e) {
			throw e.toGSException();
		}
		finally {
			disableCacheOnError(succeeded);
		}
	}

	@Override
	public Query<R> query(String tql) throws GSException {
		return query(tql, rowType);
	}

	@Override
	public <S> Extensibles.AsQuery<S> query(
			final String tql, final Class<S> rowType) throws GSException {
		GSErrorCode.checkNullParameter(tql, "tql", null);

		return new PartQuery<S>(this, new SubQueryGenerator<S>() {
			Extensibles.AsQuery<S> generate(long subId) throws GSException {
				final Extensibles.AsContainer<K, R> sub;
				try {
					sub = getSub(subId, false);
				}
				catch (ContainerExpirationException e) {
					return null;
				}
				return (sub == null ? null : sub.query(tql, rowType));
			}
		});
	}

	@Override
	public Blob createBlob() throws GSException {
		return getSubFixed().createBlob();
	}

	@Override
	public void commit() throws GSException {
		throw errorNotSupported();
	}

	@Override
	public void abort() throws GSException {
		throw errorNotSupported();
	}

	@Override
	public void setAutoCommit(boolean enabled) throws GSException {
		if (!enabled) {
			throw errorNotSupported();
		}
	}

	@Override
	public void createIndex(String columnName) throws GSException {
		throw errorNotSupported();
	}

	@Override
	public void createIndex(String columnName, IndexType type)
			throws GSException {
		throw errorNotSupported();
	}

	@Override
	public void createIndex(IndexInfo info) throws GSException {
		throw errorNotSupported();
	}

	@Override
	public void dropIndex(String columnName) throws GSException {
		throw errorNotSupported();
	}

	@Override
	public void dropIndex(String columnName, IndexType type)
			throws GSException {
		throw errorNotSupported();
	}

	@Override
	public void dropIndex(IndexInfo info) throws GSException {
		throw errorNotSupported();
	}

	@Override
	public void createEventNotification(URL url) throws GSException {
		throw errorNotSupported();
	}

	@Override
	public void dropEventNotification(URL url) throws GSException {
		throw errorNotSupported();
	}

	@Override
	public void createTrigger(TriggerInfo info) throws GSException {
		throw errorNotSupported();
	}

	@Override
	public void dropTrigger(String name) throws GSException {
		throw errorNotSupported();
	}

	@Override
	public void flush() throws GSException {
		boolean succeeded = false;
		try {
			boolean expired = false;
			for (Long subId : getLargeInfo()) {
				final Container<K, R> sub;
				try {
					sub = getSub(subId, false);
				}
				catch (ContainerExpirationException e) {
					expired = true;
					continue;
				}
				if (sub == null) {
					continue;
				}
				try {
					sub.flush();
				}
				catch (GSException e) {
					if (!PartStore.isSubContainerExpirationError(e)) {
						throw e;
					}
					expired = true;
					continue;
				}
			}
			succeeded = !expired;
		}
		finally {
			disableCacheOnError(succeeded);
		}
	}

	@Override
	public void close() throws GSException {
		try {
			closeAllSub(false, false);
		}
		finally {
			store = null;
		}
	}

	@Override
	public ContainerType getType() throws GSException {
		if (this instanceof TimeSeries) {
			return ContainerType.TIME_SERIES;
		}
		else {
			return ContainerType.COLLECTION;
		}
	}

	@Override
	public R createRow() throws GSException {
		return getSubFixed().createRow();
	}

	@Override
	public BindType<K, R, ? extends Container<K, R>> getBindType()
			throws GSException {
		return getSubFixed().getBindType();
	}

	@Override
	public void updateRowById(long transactionId, long baseId, R rowObj)
			throws GSException {
		throw errorNotSupported();
	}

	@Override
	public void removeRowById(long transactionId, long baseId)
			throws GSException {
		throw errorNotSupported();
	}

	@Override
	public RowSet<Row> getRowSet(Object[] position, long fetchLimit)
			throws GSException {
		throw errorNotSupported();
	}

	@Override
	public Class<R> getRowType() {
		return rowType;
	}

	@Override
	public ContainerProperties getSchemaProperties() throws GSException {
		return getSubFixed().getSchemaProperties();
	}

	@Override
	public Object getRowValue(Object rowObj, int column) throws GSException {
		return getSubFixed().getRowValue(rowObj, column);
	}

	@Override
	public Object getRowKeyValue(Object keyObj, int keyColumn)
			throws GSException {
		return getSubFixed().getRowKeyValue(keyObj, keyColumn);
	}

	@Override
	public void createIndex(IndexInfo info, OptionalRequestSource source)
			throws GSException {
		throw errorNotSupported();
	}

	@Override
	public void dropIndex(IndexInfo info, OptionalRequestSource source)
			throws GSException {
		throw errorNotSupported();
	}

	@Override
	public ContainerKey getKey() {
		final PartStore store = this.store;
		if (store == null || !store.isContainerMonitoring()) {
			return null;
		}
		return getLargeKey();
	}

	PartStore getStore() {
		return store;
	}

	ContainerKey getLargeKey() {
		return largeKey;
	}

	LargeInfo getLargeInfo() throws GSException {
		return getLargeInfo(false);
	}

	LargeInfo getLargeInfo(boolean emptyAllowed) throws GSException {
		if (largeInfo == null || !largeInfo.isStable(emptyAllowed) ||
				store.checkLargeInfoExpiration(largeInfo)) {
			final ContainerKeyConverter keyConverter =
					store.getContainerKeyConverter(true, false);
			try {
				acceptLargeInfo(
						store.takeLargeInfo(largeKey, false, keyConverter));
			}
			catch (ContainerPartitioningException e) {
				throw e.toGSException();
			}

			final boolean unavailableOnly = true;
			closeAllSub(false, unavailableOnly);
		}
		return largeInfo;
	}

	private void acceptLargeInfo(LargeInfo largeInfo) throws GSException {
		if (largeInfo.containerId != largeId) {
			throw new GSException(
					GSErrorCode.UNSUPPORTED_OPERATION,
					"Partitioned container unexpectedly lost and " +
					"another container with same name unexpectedly created (" +
					"name=" + largeKey +
					", expectedId=" + largeId +
					", actualId=" + largeInfo.containerId + ")");
		}
		this.largeInfo = largeInfo;
	}

	private void closeAllSub(boolean force, boolean unavailableOnly)
			throws GSException {
		if (unavailableOnly && largeInfo == null) {
			return;
		}

		boolean done = false;
		try {
			for (final Iterator<
					Map.Entry<Long, Extensibles.AsContainer<K, R>>> it =
					subMap.entrySet().iterator(); it.hasNext();) {
				final Map.Entry<Long, Extensibles.AsContainer<K, R>> entry =
						it.next();

				final Long subId = entry.getKey();
				final Extensibles.AsContainer<K, R> sub = entry.getValue();
				if (sub == null ||
						(unavailableOnly && !largeInfo.isDisabled(subId))) {
					continue;
				}

				if (force) {
					try {
						disableOrCloseSub(sub, unavailableOnly);
					}
					catch (Throwable t) {
					}
				}
				else {
					disableOrCloseSub(sub, unavailableOnly);
				}
				it.remove();
			}

			if (!unavailableOnly && disabledSubList != null) {
				for (final Iterator<Extensibles.AsContainer<K, R>>  it =
						disabledSubList.iterator(); it.hasNext();) {
					final Extensibles.AsContainer<K, R> sub = it.next();
					if (force) {
						try {
							sub.close();
						}
						catch (Throwable t) {
						}
					}
					else {
						sub.close();
					}
					it.remove();
				}
			}

			done = true;
		}
		finally {
			if (!done && !force) {
				closeAllSub(true, unavailableOnly);
			}
		}
	}

	private void disableOrCloseSub(
			Extensibles.AsContainer<K, R> sub, boolean disabling)
			throws GSException {
		boolean disabled = false;
		try {
			if (disabling) {
				if (disabledSubList == null) {
					disabledSubList =
							new ArrayList<Extensibles.AsContainer<K,R>>();
				}
				disabledSubList.add(sub);
				disabled = true;
			}
		}
		finally {
			if (!disabled) {
				sub.close();
			}
		}
	}

	private void checkOpened() throws GSException {
		if (store == null) {
			throw new GSException(
					GSErrorCode.CONTAINER_CLOSED, "Already closed");
		}
	}

	void disableCache() throws GSException {
		disableCacheOnError(false);
	}

	private void disableCacheOnError(boolean succeeded) throws GSException {
		if (succeeded) {
			return;
		}

		try {
			final ContainerKey normalizedKey = largeKey.toCaseSensitive(false);
			store.removeLargeInfo(normalizedKey, largeInfo);
		}
		finally {
			largeInfo = null;
		}
	}

	private SingleRowPartitioner<K, R> createRowPartitioner() throws GSException {
		return new SingleRowPartitioner<K, R>(getLargeInfo(), getSubFixed());
	}

	private Extensibles.AsContainer<K, R> getSubFixed() throws GSException {
		try {
			return getSub(getLargeInfo(true).getFixedSubId(), true);
		}
		catch (ContainerExpirationException e) {
			throw e.toGSException();
		}
	}

	private Extensibles.AsContainer<K, R> getSub(Long subId, boolean always)
			throws GSException, ContainerExpirationException {
		Extensibles.AsContainer<K, R> sub = subMap.get(subId);

		if (sub == null) {
			checkOpened();

			final boolean general = (rowType == Row.class);
			final LargeInfo[] largeInfoRef = { getLargeInfo() };
			try {
				sub = store.getSubContainer(
						largeKey, largeInfoRef, subId, getBindType(), general,
						always, false);
			}
			catch (ContainerPartitioningException e) {
				throw e.toGSException();
			}
			if (sub == null && subId == largeInfoRef[0].getFixedSubId()) {
				throw new GSException(
						GSErrorCode.UNSUPPORTED_OPERATION,
						"Partitioned container unexpectedly lost (" +
						"name=" + largeKey + ")");
			}
			acceptLargeInfo(largeInfoRef[0]);
			setSub(subId, sub);
		}

		return sub;
	}

	void setSub(Long subId, Extensibles.AsContainer<K, R> sub) {
		subMap.put(subId, sub);
	}

	private Iterable<Long> subIdRangeByRowKey(K key)
			throws GSException, ContainerPartitioningException {
		final int keyColumnCount = largeInfo.keyColumnMap.size();
		final Object resolvedKey;
		final boolean decomposed;
		if (keyColumnCount > 1) {
			final Object[] decomposedKey = new Object[keyColumnCount];
			for (int i = 0; i < decomposedKey.length; i++) {
				decomposedKey[i] = getRowKeyValue(key, i);
			}
			resolvedKey = decomposedKey;
			decomposed = true;
		}
		else {
			resolvedKey = key;
			decomposed = false;
		}
		return getLargeInfo().subIdRangeByKey(resolvedKey, decomposed);
	}

	static GSException errorNotSupported() throws GSException {
		return new GSException(
				GSErrorCode.UNSUPPORTED_OPERATION,
				"Not supported on partitioned container");
	}

	public static class PartCollection<K, R>
	extends PartContainer<K, R> implements Collection<K, R> {

		public PartCollection(
				PartStore store, ContainerKey largeKey, LargeInfo largeInfo,
				Container.BindType<K, R, ?> bindType) {
			super(store, largeKey, largeInfo, bindType);
		}

		@Override
		public Query<R> query(
				String column, Geometry geometry, GeometryOperator geometryOp)
				throws GSException {
			throw errorNotSupported();
		}

		@Override
		public Query<R> query(
				String column, Geometry geometryIntersection,
				Geometry geometryDisjoint) throws GSException {
			throw errorNotSupported();
		}

	}

	public static class PartTimeSeries<R>
	extends PartContainer<Date, R> implements TimeSeries<R> {

		protected PartTimeSeries(
				PartStore store, ContainerKey largeKey, LargeInfo largeInfo,
				Container.BindType<Date, R, ?> bindType) {
			super(store, largeKey, largeInfo, bindType);
		}

		@Override
		public boolean append(R row) throws GSException {
			throw errorNotSupported();
		}

		@Override
		public R get(Date base, TimeOperator timeOp) throws GSException {
			throw errorNotSupported();
		}

		@Override
		public R interpolate(Date base, String column) throws GSException {
			throw errorNotSupported();
		}

		@Override
		public Query<R> query(Date start, Date end) throws GSException {
			throw errorNotSupported();
		}

		@Override
		public Query<R> query(Date start, Date end, QueryOrder order)
				throws GSException {
			throw errorNotSupported();
		}

		@Override
		public Query<R> query(
				Date start, Date end, Set<String> columnSet,
				InterpolationMode mode, int interval, TimeUnit intervalUnit)
				throws GSException {
			throw errorNotSupported();
		}

		@Override
		public Query<R> query(
				Date start, Date end, Set<String> columnSet,
				int interval, TimeUnit intervalUnit) throws GSException {
			throw errorNotSupported();
		}

		@Override
		public AggregationResult aggregate(
				Date start, Date end, String column,
				Aggregation aggregation) throws GSException {
			throw errorNotSupported();
		}

	}

	public static class Attributes {

		public static final int LARGE = ContainerAttribute.LARGE.flag();

		public static final int SUB = ContainerAttribute.SUB.flag();

		private Attributes() {
		}

	}

	private static class SingleRowPartitioner<K, R> {

		private final LargeInfo largeInfo;

		private final Extensibles.AsContainer<K, R> subContainer;

		private final Object[] partitioningValues;

		SingleRowPartitioner(
				LargeInfo largeInfo, AsContainer<K, R> subContainer) {
			this.largeInfo = largeInfo;
			this.subContainer = subContainer;
			this.partitioningValues =
					new Object[largeInfo.getPartitioningColumnCount()];
		}

		long getSubId(R rowObj) throws GSException {
			for (int i = 0; i < partitioningValues.length; i++) {
				final int column = largeInfo.getPartitioningColumn(i);
				partitioningValues[i] = getRowOrKeyField(null, rowObj, column);
			}
			try {
				return largeInfo.subIdFromValues(partitioningValues);
			}
			catch (ContainerPartitioningException e) {
				throw e.toGSException();
			}
		}

		long getSubId(K key, R rowObj) throws GSException {
			for (int i = 0; i < partitioningValues.length; i++) {
				final int column = largeInfo.getPartitioningColumn(i);
				partitioningValues[i] = getRowOrKeyField(key, rowObj, column);
			}
			try {
				return largeInfo.subIdFromValues(partitioningValues);
			}
			catch (ContainerPartitioningException e) {
				throw e.toGSException();
			}
		}

		private Object getRowOrKeyField(K keyObj, R rowObj, int column)
				throws GSException {
			if (keyObj != null) {
				final Integer keyColumn = largeInfo.keyColumnMap.get(column);
				if (keyColumn != null) {
					return subContainer.getRowKeyValue(keyObj, keyColumn);
				}
			}
			return subContainer.getRowValue(rowObj, column);
		}

	}

	private static class RowListPartitioner<R> extends ListPartitioner<R> {

		private final SingleRowPartitioner<?, R> base;

		RowListPartitioner(SingleRowPartitioner<?, R> base) {
			this.base = base;
		}

		@Override
		protected long getSubId(LargeInfo largeInfo, R elem)
				throws GSException {
			return base.getSubId(elem);
		}

	}

	static abstract class SubQueryGenerator<S> {

		abstract Extensibles.AsQuery<S> generate(long subId)
				throws GSException;

	}

}
