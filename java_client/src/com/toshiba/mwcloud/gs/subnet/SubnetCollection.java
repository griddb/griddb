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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.Geometry;
import com.toshiba.mwcloud.gs.GeometryOperator;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.common.BasicBuffer;
import com.toshiba.mwcloud.gs.common.ContainerKeyConverter.ContainerKey;
import com.toshiba.mwcloud.gs.common.ContainerProperties.MetaContainerType;
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
			Class<R> rowType, RowMapper mapper, int schemaVerId,
			int partitionId, long containerId, ContainerKey normalizedContainerKey,
			ContainerKey remoteContainerKey)
			throws GSException {
		super(store, channel, context, rowType, mapper, schemaVerId,
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
		try {
			inBuf.putInt(mapper.resolveColumnId(column));
			RowMapper.putSize(
					inBuf, GeometryUtils.getBytesLength(geometry),
					getRowMappingMode());
			GeometryUtils.putGeometry(inBuf, geometry);
			inBuf.putByteEnum(geometryOp);
		}
		catch (NullPointerException e) {
			GSErrorCode.checkNullParameter(column, "column", e);
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
			GSErrorCode.checkNullParameter(column, "column", e);
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

		private final long metaContainerId;

		private final MetaNamingType metaNamingType;

		private final ContainerKey queryContainerKey;

		private final ArrayList<SubnetCollection<K, R>> subList =
				new ArrayList<SubnetCollection<K,R>>();

		public MetaCollection(
				SubnetGridStore store, GridStoreChannel channel,
				Context context, Class<R> rowType, RowMapper mapper,
				int schemaVerId, int partitionId,
				MetaContainerType metaContainerType,
				long metaContainerId, MetaNamingType metaNamingType,
				ContainerKey normalizedContainerKey,
				ContainerKey remoteContainerKey)
				throws GSException {
			super(store, channel, context, rowType, mapper, schemaVerId,
					partitionId, -1, normalizedContainerKey,
					remoteContainerKey);
			this.metaContainerId = metaContainerId;
			this.metaNamingType = metaNamingType;
			this.queryContainerKey = remoteContainerKey;
			boolean succeeded = false;
			try {
				if (metaContainerType != MetaContainerType.FULL) {
					throw new GSException(
							GSErrorCode.UNSUPPORTED_OPERATION, "");
				}

				final int count = channel.getPartitionCount(context);
				subList.ensureCapacity(count);
				for (int i = 0; i < count; i++) {
					subList.add(new SubnetCollection<K, R>(
							store, channel, context, rowType, mapper,
							schemaVerId, i, -1,
							normalizedContainerKey, remoteContainerKey));
				}
				succeeded = true;
			}
			finally {
				if (!succeeded) {
					close();
				}
			}
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
			List<SubnetQuery<S>> subQueryList =
					new ArrayList<SubnetQuery<S>>(subList.size());
			try {
				for (SubnetCollection<?, R> sub : subList) {
					final SubnetQuery<S> subQuery = sub.query(tql, rowType);
					subQuery.setAcceptableResultKeys(QUERY_RESULT_OPTIONS);
					subQueryList.add(subQuery);
				}
				for (SubnetQuery<S> subQuery : subQueryList) {
					subQuery.setMetaContainerId(
							metaContainerId, metaNamingType);
					subQuery.setQeuryContainerKey(true, queryContainerKey);
				}

				final MetaQuery<S> query = new MetaQuery<S>(
						this, rowType, getRowMapper(), subQueryList);
				subQueryList = null;
				return query;
			}
			finally {
				if (subQueryList != null) {
					MetaQuery.closeSub(subQueryList, true);
				}
			}
		}

		@Override
		public void close() throws GSException {
			try {
				closeSub(subList, false);
			}
			finally {
				super.close();
			}
		}

		static <K, R> void closeSub(
				List<SubnetCollection<K, R>> subList, boolean silent)
				throws GSException {
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

		private final List<SubnetQuery<R>> subList;

		private MetaQuery(
				SubnetContainer<?, ?> container,
				Class<R> resultType, RowMapper mapper,
				List<SubnetQuery<R>> subList) {
			super(container, resultType, mapper, createFormatter());
			this.subList = subList;
		}

		private static <R> SubnetContainer.QueryFormatter createFormatter() {
			return new SubnetContainer.QueryFormatter(null) {
				@Override
				String getQueryString() {
					throw new IllegalStateException();
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
			final int subCount = subList.size();
			List<RowSet<R>> subRowSetList = new ArrayList<RowSet<R>>(subCount);
			try {
				List<Long> targetList = null;
				final int firstTarget = 0;
				if (subCount > 0) {
					final SubnetQuery<R> subQuery = subList.get(firstTarget);
					subQuery.fetch(forUpdate);

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
						subRowSetList.add(subList.get(i).fetch());
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
						subRowSetList.add(subList.get((int) targetId).fetch());
					}
				}

				final RowSet<R> rowSet = new MetaRowSet<R>(subRowSetList);
				subRowSetList = null;
				return rowSet;
			}
			finally {
				if (subRowSetList != null) {
					MetaRowSet.closeSub(subRowSetList, true);
				}
			}
		}

		@Override
		public void close() throws GSException {
			closeSub(subList, false);
		}

		static <R> void closeSub(List<SubnetQuery<R>> subList, boolean silent)
				throws GSException {
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

		private final List<RowSet<R>> subList;

		private RowSet<R> nextSub;

		private int nextIndex;

		private int size = -1;

		MetaRowSet(List<RowSet<R>> subList) throws GSException {
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
			closeSub(subList, false);
		}

		static <R> void closeSub(List<RowSet<R>> subList, boolean silent)
				throws GSException {
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
