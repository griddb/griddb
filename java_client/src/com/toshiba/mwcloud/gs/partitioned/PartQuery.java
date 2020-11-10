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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.toshiba.mwcloud.gs.FetchOption;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.common.BasicBuffer;
import com.toshiba.mwcloud.gs.common.ContainerKeyConverter.ContainerKey;
import com.toshiba.mwcloud.gs.common.Extensibles;
import com.toshiba.mwcloud.gs.common.Extensibles.AsContainer;
import com.toshiba.mwcloud.gs.common.Extensibles.AsQuery;
import com.toshiba.mwcloud.gs.common.Extensibles.MultiOperationContext;
import com.toshiba.mwcloud.gs.common.Extensibles.MultiTargetConsumer;
import com.toshiba.mwcloud.gs.common.Extensibles.QueryInfo;
import com.toshiba.mwcloud.gs.common.GSErrorCode;
import com.toshiba.mwcloud.gs.experimental.Experimentals;
import com.toshiba.mwcloud.gs.partitioned.PartContainer.SubQueryGenerator;
import com.toshiba.mwcloud.gs.partitioned.PartRowSet.ExtResultOptionType;
import com.toshiba.mwcloud.gs.partitioned.PartStore.ExtRequestOptionType;
import com.toshiba.mwcloud.gs.partitioned.PartStore.LargeInfo;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.BytesRequestFormatter;
import com.toshiba.mwcloud.gs.subnet.SubnetQuery;
import com.toshiba.mwcloud.gs.subnet.SubnetRowSet.DistributedQueryTarget;

class PartQuery<R>
implements Query<R>, Extensibles.AsQuery<R>, Experimentals.AsQuery<R> {

	static boolean distQueryOptionWithContainerKey = true;

	private final PartContainer<?, ?> container;

	private final SubQueryGenerator<R> generator;

	private final Extensibles.AsQuery<R> fixedSubQuery;

	private PartRowSet<R> lastRowSet;

	private boolean lastRowSetVisible;

	PartQuery(
			PartContainer<?, ?> container,
			SubQueryGenerator<R> generator) throws GSException {
		this.container = container;
		this.generator = generator;
		this.fixedSubQuery = generator.generate(
				container.getLargeInfo(true).getFixedSubId());
	}

	@Override
	public void setFetchOption(FetchOption option, Object value)
			throws GSException {
		GSErrorCode.checkNullParameter(option, "option", null);

		switch (option) {
		case LIMIT:
		case PARTIAL_EXECUTION:
			break;
		default:
			throw new GSException(
					GSErrorCode.UNSUPPORTED_OPERATION,
					"Not supported option on partitioned container (" +
					"option=" + option + ")");
		}

		fixedSubQuery.setFetchOption(option, value);
	}

	@Override
	public RowSet<R> fetch() throws GSException {
		return fetch(false);
	}

	@Override
	public RowSet<R> fetch(boolean forUpdate) throws GSException {
		checkOpened();

		if (forUpdate) {
			throw new GSException(
					GSErrorCode.UNSUPPORTED_OPERATION,
					"Not supported option on partitioned container (" +
					"forUpdate=" + forUpdate + ")");
		}

		container.getStore().fetchAll(Collections.singletonList(this));
		lastRowSet.prepareFollowing();

		return lastRowSet;
	}

	@Override
	public void close() throws GSException {
		try {
			clearRowSet();
		}
		finally {
			fixedSubQuery.close();
		}
	}

	@Override
	public PartRowSet<R> getRowSet() throws GSException {
		checkOpened();

		if (!lastRowSetVisible) {
			return null;
		}

		lastRowSet.prepareFollowing();
		lastRowSetVisible = false;

		return lastRowSet;
	}

	@Override
	public long getFetchLimit() throws GSException {
		return Experimentals.get(fixedSubQuery).getFetchLimit();
	}

	@Override
	public void applyOptionsTo(AsQuery<?> dest) throws GSException {
		fixedSubQuery.applyOptionsTo(dest);
	}

	@Override
	public void setExtOption(
			int key, byte[] value, boolean followingInheritable)
			throws GSException {
		throw PartContainer.errorNotSupported();
	}

	@Override
	public void setAcceptableResultKeys(Set<Integer> keys)
			throws GSException {
		throw PartContainer.errorNotSupported();
	}

	@Override
	public void setContainerLostAcceptable(boolean acceptable)
			throws GSException {
		throw PartContainer.errorNotSupported();
	}

	@Override
	public QueryInfo getInfo() {
		return fixedSubQuery.getInfo();
	}

	@Override
	public Query<R> getBaseQuery() {
		return this;
	}

	@Override
	public AsContainer<?, ?> getContainer() {
		return container;
	}

	void clearRowSet() throws GSException {
		final PartRowSet<R> rowSet = lastRowSet;
		lastRowSet = null;
		lastRowSetVisible = false;

		if (rowSet == null) {
			return;
		}

		rowSet.close();
	}

	void replaceRowSet(PartRowSet<R> rowSet) {
		if (lastRowSet != null) {
			throw new IllegalStateException();
		}

		if (rowSet == null) {
			throw new NullPointerException();
		}

		lastRowSet = rowSet;
		lastRowSetVisible = false;
	}

	void setRowSetVisible() {
		lastRowSetVisible = true;
	}

	Extensibles.AsQuery<R> createSubQuery(long subId) throws GSException {
		checkOpened();

		final Extensibles.AsQuery<R> subQuery = generator.generate(subId);
		if (subQuery == null) {
			return null;
		}

		fixedSubQuery.applyOptionsTo(subQuery);
		return subQuery;
	}

	PartRowSet<R> createEmptyRowSet() throws GSException {
		return new PartRowSet<R>(container, getInfo());
	}

	static void setSubContainerVersion(
			Extensibles.AsQuery<?> query, long version)
			throws GSException {
		query.setExtOption(
				ExtRequestOptionType.SUB_CONTAINER_VERSION.number(),
				PartStore.getSubContainerVersionFormatter(
						version).format(),
				false);
	}

	static void setSubContainerExpirable(
			Extensibles.AsQuery<?> query, final boolean expirable)
			throws GSException {
		query.setExtOption(
				ExtRequestOptionType.SUB_CONTAINER_EXPIRABLE.number(),
				new BytesRequestFormatter() {
					@Override
					public void format(BasicBuffer buf) throws GSException {
						buf.putBoolean(expirable);
					}
				}.format(), true);
	}

	static void setDistributed(
			Extensibles.AsQuery<?> query, Extensibles.AsStore store,
			final boolean distributed, final ContainerKey largeContainerKey)
			throws GSException {
		query.setExtOption(
				ExtRequestOptionType.DIST_QUERY.number(),
				SubnetQuery.getContainerKeyFormatter(
						distributed, store, largeContainerKey).format(), true);
	}

	static void setLargeInfo(
			Extensibles.AsQuery<?> query, final LargeInfo largeInfo)
			throws GSException {
		query.setExtOption(
				ExtRequestOptionType.LARGE_INFO.number(),
				new BytesRequestFormatter() {
					@Override
					public void format(BasicBuffer buf) throws GSException {
						largeInfo.put(buf);
					}
				}.format(), false);
	}

	private void checkOpened() throws GSException {
		fixedSubQuery.getRowSet();
	}

	static class PartitionedMultiQueryContext
	implements MultiOperationContext<Integer, Query<?>, Query<?>> {

		final PartStore store;

		final List<? extends Query<?>> queryList;

		Map<Integer, PartQuery<?>> partitionedMap;

		boolean versionErrorOccurred;

		boolean allCacheDisabled;

		PartitionedMultiQueryContext(
				PartStore store, List<? extends Query<?>> queryList) {
			this.store = store;
			this.queryList = queryList;
		}

		@Override
		public void listTarget(MultiTargetConsumer<Integer, Query<?>> consumer)
				throws GSException {
			if (partitionedMap == null) {
				listTargetInitial(consumer);
			}
			else {
				listTargetUnresolved(consumer);
			}
		}

		@Override
		public boolean acceptException(GSException exception)
				throws GSException {
			if (PartStore.isSubContainerVersionError(exception) ||
					PartStore.isSubContainerExpirationError(exception)) {
				versionErrorOccurred = true;
				return true;
			}
			return false;
		}

		@Override
		public void acceptCompletion(Integer key, Query<?> result)
				throws GSException {
		}

		@Override
		public boolean isRemaining() throws GSException {
			return (partitionedMap == null || !partitionedMap.isEmpty());
		}

		private void listTargetInitial(
				MultiTargetConsumer<Integer, Query<?>> consumer)
				throws GSException {
			final int count = queryList.size();
			for (int key = 0; key < count; key++) {
				final Query<?> query = queryList.get(key);

				final Query<?> baseQuery;
				if (!(query instanceof PartQuery)) {
					if (!(query instanceof Extensibles.AsQuery) ||
							!((baseQuery = ((Extensibles.AsQuery<?>)
									query).getBaseQuery()) instanceof
									PartQuery)) {
						consumer.consume(key, query, null, null);
						continue;
					}
				}
				else {
					baseQuery = query;
				}

				final PartQuery<?> advQuery = (PartQuery<?>) baseQuery;
				final PartContainer<?, ?> container = advQuery.container;
				if (container.getStore() != store) {
					throw new GSException(
							GSErrorCode.ILLEGAL_PARAMETER,
							"Derived GridStore instance not matched");
				}

				advQuery.clearRowSet();

				if (partitionedMap == null) {
					partitionedMap = new HashMap<Integer, PartQuery<?>>();
				}
				partitionedMap.put(key, advQuery);

				final long subId = container.getLargeInfo().getFeasibleSubId();
				consumeSub(consumer, key, advQuery, subId, true, true);
			}

			if (partitionedMap == null) {
				partitionedMap = Collections.emptyMap();
			}
		}

		private void listTargetUnresolved(
				MultiTargetConsumer<Integer, Query<?>> consumer)
				throws GSException {
			for (final Iterator<Map.Entry<Integer, PartQuery<?>>> it =
					partitionedMap.entrySet().iterator(); it.hasNext();) {

				final Map.Entry<Integer, PartQuery<?>> entry = it.next();
				final Integer key = entry.getKey();
				final PartQuery<?> query = entry.getValue();

				final PartRowSet<?> rowSet = query.lastRowSet;
				if (versionErrorOccurred) {
					rowSet.removeUncheckedSub();
				}

				final Set<Long> subIdSet =
						new HashSet<Long>(rowSet.getSubIdList());

				final boolean checking;
				if (rowSet.checkSubContainerLost() || versionErrorOccurred) {
					disableCache(query, rowSet);
					rowSet.clearSubContainerLost();
					checking = false;
				}
				else {
					checking = !allCacheDisabled;
				}

				final DistributedQueryTarget target = rowSet.getTarget(true);
				final Iterable<Long> nextIdSet;
				boolean consumed = false;
				boolean found = false;
				if (target == null || target.targetList == null) {
					final LargeInfo largeInfo = query.container.getLargeInfo();
					do {
						if (target != null) {
							break;
						}

						final long subId = largeInfo.getFixedSubId();
						if (subIdSet.contains(subId)) {
							break;
						}

						found = true;
						consumed |= consumeSub(
								consumer, key, query, subId, true, checking);
						break;
					}
					while (false);

					if (consumed) {
						nextIdSet = Collections.emptySet();
					}
					else {
						nextIdSet = largeInfo;
					}
				}
				else {
					nextIdSet = target.targetList;
				}

				for (Long subId : nextIdSet) {
					if (subIdSet.contains(subId) ||
							rowSet.isProblemSubId(subId)) {
						continue;
					}
					found = true;
					consumed |= consumeSub(
							consumer, key, query, subId, false, checking);
				}

				if (!consumed) {
					if (found && checking) {
						disableCache(query, rowSet);
						continue;
					}
					query.lastRowSetVisible = true;
					it.remove();
				}
			}
			allCacheDisabled = versionErrorOccurred;
			versionErrorOccurred = false;
		}

		private static boolean consumeSub(
				MultiTargetConsumer<Integer, Query<?>> consumer, Integer key,
				PartQuery<?> query, long subId, boolean resolving,
				boolean checking) throws GSException {
			final Extensibles.AsQuery<?> subQuery =
					createSub(query, subId, checking);
			if (subQuery == null) {
				return false;
			}

			subQuery.setAcceptableResultKeys(resolving ?
					ExtResultOptionType.TARGETED_OPTIONS :
					ExtResultOptionType.DEFAULT_OPTIONS);

			if (resolving) {
				setLargeInfo(subQuery, query.container.getLargeInfo());
			}

			consumer.consume(key, subQuery, null, null);
			return true;
		}

		private static <R> Extensibles.AsQuery<R> createSub(
				PartQuery<R> query, long subId, boolean checking)
				throws GSException {

			PartRowSet<R> rowSet = query.lastRowSet;
			if (rowSet == null) {
				rowSet = query.createEmptyRowSet();
				query.replaceRowSet(rowSet);
			}

			final Extensibles.AsQuery<R> subQuery =
					query.createSubQuery(subId);
			if (subQuery == null) {
				rowSet.addProblemSubId(subId);
				return null;
			}

			final PartContainer<?, ?> container = query.container;
			setDistributed(
					subQuery, container.getStore(), true,
					container.getLargeKey());

			final LargeInfo largeInfo = container.getLargeInfo();
			if (largeInfo.isOrdered()) {
				final DistributedQueryTarget target = rowSet.getTarget(false);
				if (checking && (target == null || target.uncovered)) {
					setSubContainerVersion(
							subQuery, largeInfo.partitioningVersion);
				}

				setSubContainerExpirable(subQuery, true);
				subQuery.setContainerLostAcceptable(true);
			}

			rowSet.addSub(subId, subQuery);

			return subQuery;
		}

		private static void disableCache(
				PartQuery<?> query, PartRowSet<?> rowSet)
				throws GSException {
			query.container.disableCache();
			rowSet.clearTarget();
		}

	}

}
