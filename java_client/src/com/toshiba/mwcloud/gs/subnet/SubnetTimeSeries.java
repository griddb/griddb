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

import java.util.Date;
import java.util.Set;

import com.toshiba.mwcloud.gs.Aggregation;
import com.toshiba.mwcloud.gs.AggregationResult;
import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.InterpolationMode;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.QueryOrder;
import com.toshiba.mwcloud.gs.TimeOperator;
import com.toshiba.mwcloud.gs.TimeSeries;
import com.toshiba.mwcloud.gs.TimeUnit;
import com.toshiba.mwcloud.gs.TimestampUtils;
import com.toshiba.mwcloud.gs.common.BasicBuffer;
import com.toshiba.mwcloud.gs.common.ContainerKeyConverter.ContainerKey;
import com.toshiba.mwcloud.gs.common.GSErrorCode;
import com.toshiba.mwcloud.gs.common.RowMapper;
import com.toshiba.mwcloud.gs.common.RowMapper.MappingMode;
import com.toshiba.mwcloud.gs.common.Statement;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.Context;

public class SubnetTimeSeries<R>
extends SubnetContainer<Date, R> implements TimeSeries<R> {

	private static final Date EMPTY_TIMESTAMP = new Date(0);

	private static final Date UNDEF_TIMESTAMP = new Date(Long.MAX_VALUE);

	public SubnetTimeSeries(
			SubnetGridStore store, GridStoreChannel channel, Context context,
			Container.BindType<Date, R, ? extends Container<?, ?>> bindType,
			RowMapper mapper, int schemaVerId,
			int partitionId, long containerId, ContainerKey normalizedContainerKey,
			ContainerKey remoteContainerKey)
			throws GSException {
		super(store, channel, context, bindType, mapper, schemaVerId,
				partitionId, containerId, normalizedContainerKey,
				remoteContainerKey);
	}

	@Override
	public boolean append(R row) throws GSException {
		final StatementFamily family = prepareSession(StatementFamily.POST);

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		req.putLong(getContainerId());
		putTransactionInfo(req, family, null, null);

		mapper.encode(req, getRowMappingMode(), EMPTY_TIMESTAMP, row);

		executeStatement(Statement.APPEND_TIME_SERIES_ROW, req, resp, family);

		final boolean found = resp.getBoolean();
		clearBlob(false);
		return found;
	}

	@Override
	public R get(Date base, TimeOperator timeOp) throws GSException {
		final StatementFamily family = prepareSession(StatementFamily.QUERY);

		clearBlob(false);
		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		req.putLong(getContainerId());
		putTransactionInfo(
				req, family, TransactionInfoType.SKIP_COMMIT_MODE, null);

		try {
			req.putDate(base);
			req.putByteEnum(timeOp);
		}
		catch (NullPointerException e) {
			GSErrorCode.checkNullParameter(base, "base", e);
			GSErrorCode.checkNullParameter(timeOp, "timeOp", e);
			throw e;
		}

		executeStatement(
				Statement.GET_TIME_SERIES_ROW_RELATED,
				req, resp, family);

		return (resp.getBoolean() ? rowType.cast(mapper.decode(
				resp, getRowMappingMode(), this)) : null);
	}

	@Override
	public R interpolate(Date base, String column) throws GSException {
		final StatementFamily family = prepareSession(StatementFamily.QUERY);

		clearBlob(false);
		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		req.putLong(getContainerId());
		putTransactionInfo(
				req, family, TransactionInfoType.SKIP_COMMIT_MODE, null);

		try {
			req.putDate(base);
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(base, "base", e);
		}

		GSErrorCode.checkNullParameter(column, "column", null);
		req.putInt(mapper.resolveColumnId(column));

		executeStatement(
				Statement.INTERPOLATE_TIME_SERIES_ROW,
				req, resp, family);

		return (resp.getBoolean() ? rowType.cast(mapper.decode(
				resp, getRowMappingMode(), this)) : null);
	}

	@Override
	public Query<R> query(
			final Date start, final Date end) throws GSException {
		return query(start, end, QueryOrder.ASCENDING);
	}

	@Override
	public Query<R> query(
			final Date start, final Date end, final QueryOrder order)
			throws GSException {
		checkOpened();
		return new SubnetQuery<R>(
				this, rowType, mapper,
				new QueryFormatter(Statement.QUERY_TIME_SERIES_RANGE) {
					@Override
					public void format(BasicBuffer inBuf) throws GSException {
						inBuf.putDate(wrapOptionalTimestamp(start));
						inBuf.putDate(wrapOptionalTimestamp(end));
						try {
							inBuf.putByteEnum(order);
						}
						catch (NullPointerException e) {
							throw GSErrorCode.checkNullParameter(
									order, "order", e);
						}
					}

					@Override
					public String getQueryString() {
						return "{start=" + getOptionalTimestampString(start) +
								", end=" + getOptionalTimestampString(end) +
								", order=" + order + "}";
					}
				});
	}

	private static Date wrapOptionalTimestamp(Date timestamp)
			throws GSException {
		if (timestamp == null) {
			return UNDEF_TIMESTAMP;
		}

		if (timestamp.getTime() == UNDEF_TIMESTAMP.getTime()) {
			throw new GSException(
					GSErrorCode.ILLEGAL_PARAMETER,
					"Timestamp out of range (value=" + timestamp + ")");
		}

		return timestamp;
	}

	private static String getOptionalTimestampString(Date timestamp) {
		if (timestamp == null) {
			return "(Not bounded)";
		}

		return TimestampUtils.format(timestamp);
	}

	@Override
	public Query<R> query(
			final Date start, final Date end,
			final Set<String> columnSet, final InterpolationMode mode,
			final int interval, final TimeUnit intervalUnit)
			throws GSException {
		checkOpened();
		final int[] columnIds;
		if (columnSet == null) {
			columnIds = null;
		}
		else {
			columnIds = new int[columnSet.size()];
			int index = 0;
			for (String column : columnSet) {
				columnIds[index++] = mapper.resolveColumnId(column);
			}
		}

		return new SubnetQuery<R>(
				this, rowType, mapper,
				new QueryFormatter(Statement.QUERY_TIME_SERIES_SAMPLING) {
					@Override
					public void format(BasicBuffer inBuf) {
						try {
							inBuf.putDate(start);
							inBuf.putDate(end);
							if (columnIds == null) {
								inBuf.putInt(0);
							}
							else {
								inBuf.putInt(columnIds.length);
								for (int id : columnIds) {
									inBuf.putInt(id);
								}
							}
							inBuf.putInt(interval);
							inBuf.putByteEnum(intervalUnit);
							inBuf.putByteEnum(mode);
						}
						catch (NullPointerException e) {
							GSErrorCode.checkNullParameter(start, "start", e);
							GSErrorCode.checkNullParameter(end, "end", e);
							GSErrorCode.checkNullParameter(
									intervalUnit, "intervalUnit", e);
							GSErrorCode.checkNullParameter(mode, "mode", e);
							throw e;
						}
					}

					@Override
					public String getQueryString() {
						return "{start=" + getOptionalTimestampString(start) +
								", end=" + getOptionalTimestampString(end) +
								", columnSet=" + columnSet +
								", mode=" + mode +
								", interval=" + interval +
								", intervalUnit=" + intervalUnit + "}";
					}
				});
	}

	@Override
	@Deprecated
	public Query<R> query(
			Date start, Date end, Set<String> columnSet,
			int interval, TimeUnit intervalUnit)
			throws GSException {
		return query(start, end, columnSet,
				InterpolationMode.LINEAR_OR_PREVIOUS, interval, intervalUnit);
	}

	@Override
	public AggregationResult aggregate(Date start, Date end, String column,
			Aggregation aggregation) throws GSException {
		final StatementFamily family = prepareSession(StatementFamily.QUERY);

		clearBlob(false);
		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		req.putLong(getContainerId());
		putTransactionInfo(
				req, family, TransactionInfoType.SKIP_COMMIT_MODE, null);

		try {
			req.putDate(start);
			req.putDate(end);
			req.putInt(column == null ? -1 : mapper.resolveColumnId(column));
			req.putByteEnum(aggregation);
		}
		catch (NullPointerException e) {
			GSErrorCode.checkNullParameter(start, "start", e);
			GSErrorCode.checkNullParameter(end, "end", e);
			GSErrorCode.checkNullParameter(aggregation, "aggregation", e);
			throw e;
		}

		executeStatement(
				Statement.AGGREGATE_TIME_SERIES, req, resp, family);

		return (resp.getBoolean() ? (AggregationResult)
				RowMapper.getAggregationResultMapper().decode(
						resp, MappingMode.AGGREGATED, this) : null);
	}

}
