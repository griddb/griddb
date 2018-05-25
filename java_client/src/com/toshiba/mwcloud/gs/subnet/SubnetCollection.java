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

import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.Geometry;
import com.toshiba.mwcloud.gs.GeometryOperator;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.common.BasicBuffer;
import com.toshiba.mwcloud.gs.common.ContainerKeyConverter.ContainerKey;
import com.toshiba.mwcloud.gs.common.GSErrorCode;
import com.toshiba.mwcloud.gs.common.GeometryUtils;
import com.toshiba.mwcloud.gs.common.RowMapper;
import com.toshiba.mwcloud.gs.common.RowMapper.MappingMode;
import com.toshiba.mwcloud.gs.common.Statement;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.Context;

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

}
