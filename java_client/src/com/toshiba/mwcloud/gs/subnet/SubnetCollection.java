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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.Geometry;
import com.toshiba.mwcloud.gs.GeometryOperator;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.common.BasicBuffer;
import com.toshiba.mwcloud.gs.common.ContainerKeyConverter.ContainerKey;
import com.toshiba.mwcloud.gs.common.ContainerProperties.MetaDistributionType;
import com.toshiba.mwcloud.gs.common.ContainerProperties.MetaNamingType;
import com.toshiba.mwcloud.gs.common.GSErrorCode;
import com.toshiba.mwcloud.gs.common.GeometryUtils;
import com.toshiba.mwcloud.gs.common.RowMapper;
import com.toshiba.mwcloud.gs.common.RowMapper.MappingMode;
import com.toshiba.mwcloud.gs.common.Statement;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.Context;
import com.toshiba.mwcloud.gs.subnet.SubnetRowSet.DistributedQueryTarget;
import com.toshiba.mwcloud.gs.subnet.SubnetRowSet.ExtResultOptionType;

public class SubnetCollection<K, R>
extends SubnetContainer<K, R> implements Collection<K, R> {

	public SubnetCollection(
			SubnetGridStore store, GridStoreChannel channel, Context context,
			Container.BindType<K, R, ? extends Container<?, ?>> bindType,
			RowMapper mapper, int schemaVerId,
			int partitionId, long containerId, ContainerKey normalizedContainerKey,
			ContainerKey remoteContainerKey)
			throws GSException {
		super(store, channel, context, bindType, mapper, schemaVerId,
				partitionId, containerId, normalizedContainerKey,
				remoteContainerKey);
	}

	@Override
	public Query<R> query(final String column,
			final Geometry geometry, final GeometryOperator geometryOp)
			throws GSException {
		checkOpened();
		return new SubnetQuery<R>(
				this, rowType, mapper,
				new QueryFormatter(
						Statement.QUERY_COLLECTION_GEOMETRY_RELATED) {
					@Override
					public void format(BasicBuffer inBuf) throws GSException {
						formatQuery(inBuf, column, geometry, geometryOp);
					}

					@Override
					public String getQueryString() {
						return "{column=" + column +
								", geometry=" + geometry +
								", op=" + geometryOp + "}";
					}
				});
	}

	private void formatQuery(BasicBuffer inBuf, String column,
			Geometry geometry, GeometryOperator geometryOp)
			throws GSException {
		GSErrorCode.checkNullParameter(column, "column", null);
		try {
			inBuf.putInt(mapper.resolveColumnId(column));
			RowMapper.putSize(
					inBuf, GeometryUtils.getBytesLength(geometry),
					getRowMappingMode());
			GeometryUtils.putGeometry(inBuf, geometry);
			inBuf.putByteEnum(geometryOp);
		}
		catch (NullPointerException e) {
			GSErrorCode.checkNullParameter(geometry, "geometry", e);
			GSErrorCode.checkNullParameter(geometryOp, "geometryOp", e);
			throw e;
		}
	}

	@Override
	public Query<R> query(final String column,
			final Geometry geometryIntersection,
			final Geometry geometryDisjoint)
			throws GSException {
		checkOpened();
		return new SubnetQuery<R>(
				this, rowType, mapper,
				new QueryFormatter(
						Statement.QUERY_COLLECTION_GEOMETRY_WITH_EXCLUSION) {
					@Override
					public void format(BasicBuffer inBuf) throws GSException {
						formatQuery(inBuf, column,
								geometryIntersection, geometryDisjoint);
					}

					@Override
					public String getQueryString() {
						return "{column=" + column +
								", intersection=" + geometryIntersection +
								", disjoint=" + geometryDisjoint + "}";
					}
				});
	}

	private void formatQuery(BasicBuffer inBuf, String column,
			Geometry geometryIntersection, Geometry geometryDisjoint)
			throws GSException {
		GSErrorCode.checkNullParameter(column, "column", null);
		try {
			inBuf.putInt(mapper.resolveColumnId(column));
			final MappingMode mode =
					SubnetContainer.getRowMappingMode();

			RowMapper.putSize(inBuf, GeometryUtils.getBytesLength(
					geometryIntersection), mode);
			GeometryUtils.putGeometry(inBuf, geometryIntersection);

			RowMapper.putSize(inBuf, GeometryUtils.getBytesLength(
					geometryDisjoint), mode);
			GeometryUtils.putGeometry(inBuf, geometryDisjoint);
		}
		catch (NullPointerException e) {
			GSErrorCode.checkNullParameter(
					geometryIntersection, "geometryIntersection", e);
			GSErrorCode.checkNullParameter(
					geometryDisjoint, "geometryDisjoint", e);
			throw e;
		}
	}

	public static class MetaCollection<K, R> extends SubnetCollection<K, R> {

		private static final Set<Integer> QUERY_RESULT_OPTIONS =
				Collections.singleton(ExtResultOptionType.DIST_TARGET);

		private final GridStoreChannel channel;

		private final Context context;

		private final MetaDistributionType metaDistType;

		private final long metaContainerId;

		private final MetaNamingType metaNamingType;

		private final ContainerKey queryContainerKey;

		public MetaCollection(
				SubnetGridStore store, GridStoreChannel channel,
				Context context,
				Container.BindType<K, R, ? extends Container<?, ?>> bindType,
				RowMapper mapper,
				int schemaVerId, int partitionId,
				MetaDistributionType metaDistType,
				long metaContainerId, MetaNamingType metaNamingType,
				ContainerKey normalizedContainerKey,
				ContainerKey remoteContainerKey)
				throws GSException {
			super(store, channel, context, bindType, mapper, schemaVerId,
					partitionId, -1, normalizedContainerKey,
					remoteContainerKey);
			this.channel = channel;
			this.context = context;
			this.metaDistType = metaDistType;
			this.metaContainerId = metaContainerId;
			this.metaNamingType = metaNamingType;
			this.queryContainerKey = remoteContainerKey;
		}

		@Override
		protected void executeStatement(
				Statement statement, BasicBuffer req, BasicBuffer resp,
				StatementFamily familyForSession)
				throws GSException {
			if (statement != Statement.QUERY_TQL) {
				throw new GSException(
						GSErrorCode.UNSUPPORTED_OPERATION, "");
			}
			super.executeStatement(statement, req, resp, familyForSession);
		}

		@Override
		public <S> SubnetQuery<S> query(final String tql, Class<S> rowType)
				throws GSException {
			checkOpened();
			return new MetaQuery<S>(this, rowType, getRowMapper(), tql);
		}

		@Override
		public void close() throws GSException {
			super.close();
		}

		private List<SubnetCollection<?, ?>> createSubList(
				List<InetSocketAddress> addressList) throws GSException {
			addressList.clear();
			final List<SubnetCollection<?, ?>> subList =
					new ArrayList<SubnetCollection<?, ?>>();

			switch (metaDistType) {
			case FULL: {
				final int count = channel.getPartitionCount(context);
				for (int i = 0; i < count; i++) {
					subList.add(createSub(i));
					addressList.add(null);
				}
				break;
			}
			case NODE:
				for (InetSocketAddress address :
						channel.getActiveNodeAddressSet(context)) {
					subList.add(createSub(0));
					addressList.add(address);
				}
				break;
			default:
				throw new GSException(
						GSErrorCode.UNSUPPORTED_OPERATION, "");
			}

			return subList;
		}

		private SubnetCollection<K, R> createSub(int partitionId)
				throws GSException {
			return new SubnetCollection<K, R>(
					getStore(), channel, context, getBindType(), getRowMapper(),
					getSchemaVersionId(), partitionId, -1,
					getNormalizedContainerKey(), queryContainerKey);
		}

		<S> RowSet<S> fetchInternal(
				SubnetQuery<S> query, InetSocketAddress address,
				boolean forUpdate) throws GSException {
			if (address == null) {
				query.setAcceptableResultKeys(QUERY_RESULT_OPTIONS);
				query.setMetaContainerId(
						metaContainerId, metaNamingType);
				query.setQeuryContainerKey(
						true, queryContainerKey);

				return query.fetch(forUpdate);
			}

			final Context.FixedAddressScope scope =
					new Context.FixedAddressScope(context, address);
			try {
				return fetchInternal(query, null, forUpdate);
			}
			finally {
				scope.close();
			}
		}

		static void closeSub(
				List<SubnetCollection<?, ?>> subList, boolean silent)
				throws GSException {
			if (subList == null) {
				return;
			}

			try {
				for (SubnetContainer<?, ?> sub : subList) {
					if (silent) {
						try {
							sub.close();
						}
						catch (Throwable t) {
						}
					}
					else {
						sub.close();
					}
				}
			}
			finally {
				if (!silent) {
					closeSub(subList, true);
				}
			}
		}

	}

	public static class MetaQuery<R> extends SubnetQuery<R> {

		private final MetaCollection<?, ?> container;

		private final Class<R> resultType;

		private final String tql;

		private MetaQuery(
				MetaCollection<?, ?> container,
				Class<R> resultType, RowMapper mapper, String tql) {
			super(container, resultType, mapper, createFormatter(tql));
			this.container = container;
			this.resultType = resultType;
			this.tql = tql;
		}

		private static <R> SubnetContainer.QueryFormatter createFormatter(
				final String tql) {
			return new SubnetContainer.QueryFormatter(null) {
				@Override
				public String getQueryString() {
					return tql;
				}

				@Override
				void format(BasicBuffer out) throws GSException {
					throw new GSException(
							GSErrorCode.UNSUPPORTED_OPERATION, "");
				}
			};
		}

		@Override
		public RowSet<R> fetch(boolean forUpdate) throws GSException {
			checkOpened();

			List<SubnetCollection<?, ?>> subContainerList = null;
			List<SubnetQuery<R>> subList = null;
			List<RowSet<R>> subRowSetList = null;

			final List<InetSocketAddress> addressList =
					new ArrayList<InetSocketAddress>();

			boolean succeeded = false;
			try {
				subContainerList = container.createSubList(addressList);
				subList = createSubList(subContainerList);
				subRowSetList =
						fetchAllInternal(subList, addressList, forUpdate);

				final MetaRowSet<R> rowSet = new MetaRowSet<R>(
						subContainerList, subList, subRowSetList);

				succeeded = true;
				return rowSet;
			}
			finally {
				if (!succeeded) {
					MetaRowSet.closeAllSub(
							subContainerList, subList, subRowSetList, true);
				}
			}
		}

		@Override
		public void close() throws GSException {
			super.close();
		}

		private void checkOpened() throws GSException {
			super.getFetchLimit();
		}

		private List<SubnetQuery<R>> createSubList(
				List<SubnetCollection<?, ?>> subContainerList)
				throws GSException {
			final List<SubnetQuery<R>> subList =
					new ArrayList<SubnetQuery<R>>();
			boolean succeeded = false;
			try {
				for (SubnetCollection<?, ?> subContainer : subContainerList) {
					final SubnetQuery<R> subQuery =
							subContainer.query(tql, resultType);
					subList.add(subQuery);
				}
				succeeded = true;
				return subList;
			}
			finally {
				if (!succeeded) {
					closeSub(subList, true);
				}
			}
		}

		private List<RowSet<R>> fetchAllInternal(
				List<SubnetQuery<R>> subList,
				List<InetSocketAddress> addressList, boolean forUpdate)
				throws GSException {
			final int subCount = subList.size();
			final List<RowSet<R>> subRowSetList =
					new ArrayList<RowSet<R>>(subCount);

			boolean succeeded = false;
			try {
				List<Long> targetList = null;
				final int firstTarget = 0;
				if (subCount > 0) {
					final SubnetQuery<R> subQuery = subList.get(firstTarget);

					container.fetchInternal(
							subQuery, addressList.get(firstTarget), forUpdate);

					final SubnetRowSet<R> subRowSet = subQuery.getRowSet();
					subRowSetList.add(subRowSet);

					final DistributedQueryTarget distTarget =
							SubnetRowSet.getDistTarget(subRowSet);
					if (distTarget != null && distTarget.targeted) {
						targetList = distTarget.targetList;
					}
				}

				if (targetList == null) {
					for (int i = 1; i < subCount; i++) {
						subRowSetList.add(container.fetchInternal(
								subList.get(i), addressList.get(i), forUpdate));
					}
				}
				else {
					for (long targetId : targetList) {
						if (targetId < 0 || targetId >= subCount) {
							throw new GSException(
									GSErrorCode.MESSAGE_CORRUPTED,
									"Protocol error by invalid distributed " +
									"target");
						}
						else if (targetId == firstTarget) {
							continue;
						}
						final int intTargetId = (int) targetId;
						subRowSetList.add(container.fetchInternal(
								subList.get(intTargetId),
								addressList.get(intTargetId), forUpdate));
					}
				}

				succeeded = true;
				return subRowSetList;
			}
			finally {
				if (!succeeded) {
					MetaRowSet.closeSub(subRowSetList, true);
				}
			}
		}

		static <R> void closeSub(
				List<SubnetQuery<R>> subList, boolean silent)
				throws GSException {
			if (subList == null) {
				return;
			}

			try {
				for (SubnetQuery<?> sub : subList) {
					if (silent) {
						try {
							sub.close();
						}
						catch (Throwable t) {
						}
					}
					else {
						sub.close();
					}
				}
			}
			finally {
				if (!silent) {
					closeSub(subList, true);
				}
			}
		}

	}

	public static class MetaRowSet<R> implements RowSet<R> {

		private final List<SubnetCollection<?, ?>> subContainerList;

		private final List<SubnetQuery<R>> subQueryList;

		private final List<RowSet<R>> subList;

		private RowSet<R> nextSub;

		private int nextIndex;

		private int size = -1;

		MetaRowSet(
				List<SubnetCollection<?, ?>> subContainerList,
				List<SubnetQuery<R>> subQueryList,
				List<RowSet<R>> subList) throws GSException {
			this.subContainerList = subContainerList;
			this.subQueryList = subQueryList;
			this.subList = subList;

			nextSub = this.subList.get(nextIndex);
			prepareNext();
		}

		private void prepareNext() throws GSException {
			if (nextSub == null) {
				return;
			}
			for (;;) {
				if (nextSub.hasNext()) {
					break;
				}
				nextIndex++;
				if (nextIndex >= subList.size()) {
					nextSub = null;
					break;
				}
				nextSub = subList.get(nextIndex);
			}
		}

		@Override
		public boolean hasNext() throws GSException {
			return (nextSub != null);
		}

		@Override
		public R next() throws GSException {
			final R row;
			try {
				row = nextSub.next();
			}
			catch (NullPointerException e) {
				if (nextSub == null) {
					throw new GSException(
							GSErrorCode.NO_SUCH_ELEMENT,
							"No more rows were found");
				}
				throw e;
			}
			if (!nextSub.hasNext()) {
				prepareNext();
			}
			return row;
		}

		@Override
		public void remove() throws GSException {
			throw new GSException(
					GSErrorCode.UNSUPPORTED_OPERATION, "");
		}

		@Override
		public void update(R rowObj) throws GSException {
			throw new GSException(
					GSErrorCode.UNSUPPORTED_OPERATION, "");
		}

		@Override
		public int size() {
			if (size < 0) {
				int size = 0;
				for (RowSet<?> sub : subList) {
					size += sub.size();
				}
				this.size = size;
			}
			return size;
		}

		@Override
		public void close() throws GSException {
			closeAllSub(subContainerList, subQueryList, subList, false);
		}

		static <R> void closeAllSub(
				List<SubnetCollection<?, ?>> subContainerList,
				List<SubnetQuery<R>> subQueryList,
				List<RowSet<R>> subRowSetList, boolean silent)
				throws GSException {
			try {
				try {
					MetaCollection.closeSub(subContainerList, silent);
				}
				finally {
					MetaQuery.closeSub(subQueryList, silent);
				}
			}
			finally {
				closeSub(subRowSetList, silent);
			}
		}

		static <R> void closeSub(List<RowSet<R>> subList, boolean silent)
				throws GSException {
			if (subList == null) {
				return;
			}

			try {
				for (RowSet<?> sub : subList) {
					if (silent) {
						try {
							sub.close();
						}
						catch (Throwable t) {
						}
					}
					else {
						sub.close();
					}
				}
			}
			finally {
				if (!silent) {
					closeSub(subList, true);
				}
			}
		}

	}

}
