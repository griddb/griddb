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

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.TimeSeries;
import com.toshiba.mwcloud.gs.common.Extensibles;
import com.toshiba.mwcloud.gs.common.GSErrorCode;
import com.toshiba.mwcloud.gs.common.RowMapper;
import com.toshiba.mwcloud.gs.experimental.Experimentals;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.Context;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.RemoteReference;
import com.toshiba.mwcloud.gs.subnet.SubnetContainer.ContainerSubResource;
import com.toshiba.mwcloud.gs.subnet.SubnetContainer.PartialExecutionStatus;
import com.toshiba.mwcloud.gs.subnet.SubnetContainer.PartialFetchStatus;
import com.toshiba.mwcloud.gs.subnet.SubnetContainer.QueryFormatter;
import com.toshiba.mwcloud.gs.subnet.SubnetContainer.TransactionalResource;
import com.toshiba.mwcloud.gs.subnet.SubnetQuery.QueryParameters;

public class SubnetRowSet<R>
implements RowSet<R>, Extensibles.AsRowSet<R>, Experimentals.AsRowSet<R> {

	private final SubnetContainer<?, ?> container;

	private final Class<R> rowType;

	private final RowMapper mapper;

	private RowMapper.Cursor resultCursor;

	private Object lastKey;

	private final long totalRowCount;

	private long remainingRowCount;

	private Map<Integer, byte[]> extResultMap;

	private final QueryFormatter queryFormatter;

	private QueryParameters queryParameters;

	private final PartialFetchStatus fetchStatus;

	private Object targetConnection;

	private Throwable lastProblem;

	private boolean previousFound;

	private boolean followingLost;

	private boolean closed;

	private final PartialRowSetReference remoteRef;

	public SubnetRowSet(
			SubnetContainer<?, ?> container, Class<R> rowType,
			RowMapper mapper, RowMapper.Cursor resultCursor,
			Map<Integer, byte[]> extResultMap,
			QueryFormatter queryFormatter, QueryParameters queryParameters,
			PartialFetchStatus fetchStatus, Object targetConnection)
			throws GSException {
		this.container = container;
		this.rowType = rowType;
		this.mapper = mapper;
		this.resultCursor = resultCursor;

		PartialRowSetReference remoteRef = null;
		if (fetchStatus == null) {
			if (QueryParameters.get(
					queryParameters).isPartialExecutionConfigured()) {
				totalRowCount = -1;
			}
			else {
				totalRowCount = resultCursor.getRowCount();
			}
			remainingRowCount = 0;
		}
		else {
			totalRowCount = fetchStatus.totalRowCount;
			remainingRowCount = totalRowCount - resultCursor.getRowCount();
		}

		this.extResultMap = extResultMap;
		this.queryFormatter = queryFormatter;
		this.queryParameters = queryParameters;
		this.fetchStatus = fetchStatus;

		if (fetchStatus != null) {
			this.targetConnection = targetConnection;

			synchronized (container.context) {
				container.channel.cleanRemoteResources(
						container.context, new HashSet<Class<?>>(
								Arrays.<Class<?>>asList(
										NormalTarget.class,
										TransactionalTarget.class)));

				remoteRef = new PartialRowSetReference(
						this, fetchStatus, targetConnection);
				container.context.addRemoteReference(remoteRef);
			}
		}
		this.remoteRef = remoteRef;
	}

	private void checkOpened() throws GSException {
		if (GridStoreChannel.v10ResourceCompatible) {
			return;
		}

		if (closed || container.isClosed() ||
				(remoteRef != null && remoteRef.closed)) {
			final Throwable lastProblem;
			synchronized (container.context) {
				lastProblem = this.lastProblem;
			}
			if (lastProblem != null) {
				throw new GSException(
						"Row set already closed by other problem (" +
						"reason=" + lastProblem.getMessage() + ")",
						lastProblem);
			}

			throw new GSException(GSErrorCode.RESOURCE_CLOSED,
					"This row set or related container has been closed");
		}
	}

	private GSException closeByProblem(Throwable cause) {
		try {
			close();
		}
		catch (Throwable t) {
		}
		finally {
			synchronized (container.context) {
				lastProblem = cause;
			}
		}

		if (cause instanceof GSException) {
			return (GSException) cause;
		}
		return new GSException(cause);
	}

	@Override
	public void close() throws GSException {
		closed = true;

		if (targetConnection != null) {
			
			synchronized (container.context) {
				if (targetConnection != null) {
					try {
						container.channel.closeRemoteResource(
								container.context, remoteRef, false);
					}
					finally {
						targetConnection = null;
					}
				}
			}
		}

		resultCursor.resetBuffer();
	}

	@Override
	public boolean hasNext() throws GSException {
		return resultCursor.hasNext();
	}

	void prepareFollowing() throws GSException {
		checkOpened();

		try {
			prepareFollowingDirect();
		}
		catch (Throwable t) {
			throw closeByProblem(t);
		}
	}

	private void prepareFollowingDirect() throws GSException {
		if (!resultCursor.hasNext()) {
			if (fetchStatus != null) {
				if (remainingRowCount > 0) {
					fetchFollowing();
				}
			}
			else if (QueryParameters.get(
					queryParameters).executionStatus.isEnabled()) {
				executeFollowing();
			}
		}
	}

	private void fetchFollowing() throws GSException {
		
		synchronized (container.context) {
			do {
				if (followingLost) {
					break;
				}

				if (targetConnection == null) {
					throw new GSException(GSErrorCode.RESOURCE_CLOSED,
							"Row set lost by transactional problem");
				}

				final Object[] targetConnectionResult = { targetConnection };
				boolean succeeded = false;
				try {
					resultCursor = container.fetchRowSet(
							remainingRowCount, fetchStatus, queryParameters,
							targetConnectionResult, mapper);
					succeeded = true;
				}
				finally {
					targetConnection = targetConnectionResult[0];

					try {
						if (targetConnection == null) {
							container.context.removeRemoteReference(remoteRef);
						}
					}
					finally {
						if (!succeeded) {
							try {
								close();
							}
							catch (Throwable t) {
							}
						}
					}
				}

				remainingRowCount -= resultCursor.getRowCount();

				if (remainingRowCount > 0 && targetConnection == null) {
					followingLost = true;
				}

				if (resultCursor.hasNext()) {
					return;
				}
			}
			while (false);

			throw GSErrorCode.newGSRecoverableException(
					GSErrorCode.RECOVERABLE_ROW_SET_LOST,
					"Row set lost by modifications (" +
					"remainingRowCount=" + remainingRowCount + ")", null);
		}
	}

	private void executeFollowing() throws GSException {
		do {
			final SubnetRowSet<R> rowSet = container.queryAndFetch(
					rowType, mapper, queryFormatter, queryParameters, false);
			if (rowSet == null) {
				queryParameters.executionStatus =
						PartialExecutionStatus.DISABLED;
			}
			else {
				resultCursor = rowSet.resultCursor;
				extResultMap = rowSet.extResultMap;
				queryParameters = rowSet.queryParameters;
			}
		}
		while (!resultCursor.hasNext() &&
				queryParameters.executionStatus.isEnabled());
	}

	@Override
	public R next() throws GSException {
		checkOpened();

		if (!resultCursor.hasNext()) {
			throw new GSException(
					GSErrorCode.NO_SUCH_ELEMENT, "No more rows were found");
		}

		final R rowObj;
		try {
			if (rowType == null) {
				@SuppressWarnings("unchecked")
				final R uncheckedRowObj = (R) mapper.decode(resultCursor);

				rowObj = uncheckedRowObj;
			}
			else {
				rowObj = rowType.cast(mapper.decode(resultCursor));
			}

			if (mapper.hasKey()) {
				lastKey = mapper.resolveKey(null, rowObj);
			}
			else {
				lastKey = null;
			}

			if (!resultCursor.hasNext()) {
				prepareFollowingDirect();
				previousFound = true;
			}
		}
		catch (Throwable t) {
			throw closeByProblem(t);
		}

		return rowObj;
	}

	@Override
	public void remove() throws GSException {
		checkOpened();
		checkInRange();

		container.remove(
				mapper, getTransactionId(), isUpdatable(),
				resultCursor.getLastRowId(), lastKey);
	}

	@Override
	public void update(R rowObj) throws GSException {
		checkOpened();
		checkInRange();

		container.update(
				mapper, getTransactionId(), isUpdatable(),
				resultCursor.getLastRowId(), lastKey, rowObj);
	}

	private void checkInRange() throws GSException {
		if (!resultCursor.isInRange() && !previousFound) {
			throw new GSException(
					GSErrorCode.NO_SUCH_ELEMENT, "Cursor out of range");
		}
	}

	private long getTransactionId() {
		return QueryParameters.resolveInitialTransactionId(queryParameters);
	}

	private boolean isUpdatable() {
		return QueryParameters.isForUpdate(queryParameters, false);
	}

	@Override
	public int size() {
		if (totalRowCount < 0) {
			raiseSizeError();
		}

		return (int) totalRowCount;
	}

	private static void raiseSizeError() {
		try {
			throw new GSException(
					GSErrorCode.UNSUPPORTED_OPERATION,
					"Size of row set with partial execution is not supported");
		}
		catch (GSException e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public Class<R> getRowType() throws GSException {
		return rowType;
	}

	@Override
	public Set<Integer> getExtOptionKeys() throws GSException {
		if (extResultMap == null) {
			return Collections.emptySet();
		}
		return extResultMap.keySet();
	}

	@Override
	public byte[] getExtOption(int key) throws GSException {
		if (extResultMap == null) {
			return null;
		}
		return extResultMap.get(key);
	}

	private static interface NormalTarget extends ContainerSubResource {
	}

	private static interface TransactionalTarget
	extends TransactionalResource {
	}

	private static class PartialRowSetReference
	extends RemoteReference<RowSet<?>> {

		private final PartialFetchStatus fetchStatus;
		private final Object targetConnection;
		private boolean closed;

		PartialRowSetReference(
				SubnetRowSet<?> rowSet,
				PartialFetchStatus fetchStatus, Object targetConnection) {
			super(rowSet, selectTargetClass(rowSet),
					rowSet.container.context,
					rowSet.container.getPartitionId(),
					rowSet.container.getContainerId());
			this.fetchStatus = fetchStatus;
			this.targetConnection = targetConnection;
		}

		private static Class<?> selectTargetClass(SubnetRowSet<?> rowSet) {
			if (rowSet.getTransactionId() == 0) {
				return NormalTarget.class;
			}
			else {
				return TransactionalTarget.class;
			}
		}

		@Override
		public void close(
				GridStoreChannel channel, Context context) throws GSException {
			if (closed) {
				return;
			}

			closed = true;
			SubnetContainer.closeRowSet(
					channel, context, partitionId, containerId,
					fetchStatus, targetConnection);
		}

	}

	private static class Tool {

		private Tool() {
		}

		public static Container<?, ?> getContainer(SubnetRowSet<?> rowSet) {
			return rowSet.container;
		}

		public static long getTransactionId(SubnetRowSet<?> rowSet) {
			return rowSet.getTransactionId();
		}

		public static long getRowId(
				SubnetRowSet<?> rowSet) throws GSException {
			rowSet.checkOpened();

			if (!rowSet.resultCursor.isInRange()) {
				throw new GSException(
						GSErrorCode.NO_SUCH_ELEMENT, "Cursor out of range");
			}

			if (!rowSet.resultCursor.isRowIdIncluded()) {
				throw new GSException(
						GSErrorCode.UNSUPPORTED_OPERATION,
						"Elements of this RowSet are not collection row");
			}

			return rowSet.resultCursor.getLastRowId();
		}

		public static Object getRowKey(
				SubnetRowSet<?> rowSet) throws GSException {
			rowSet.checkOpened();

			if (!rowSet.resultCursor.isInRange()) {
				throw new GSException(
						GSErrorCode.NO_SUCH_ELEMENT, "Cursor out of range");
			}

			if (!rowSet.container.getRowMapper().hasKey()) {
				return null;
			}

			return rowSet.lastKey;
		}

	}

	

	Row nextGeneralRow() throws GSException {
		checkOpened();

		if (!hasNext()) {
			throw new GSException(
					GSErrorCode.NO_SUCH_ELEMENT, "Cursor out of range");
		}

		final Row rowObj = (Row) mapper.decode(resultCursor, true);

		if (mapper.hasKey()) {
			lastKey = mapper.resolveKey(null, rowObj, true);
		}
		else {
			lastKey = null;
		}

		return rowObj;
	}

	@Override
	public Experimentals.RowId getRowIdForUpdate() throws GSException {
		final Container<?, ?> container = Tool.getContainer(this);

		final long transactionId = Tool.getTransactionId(this);
		if (transactionId == 0) {
			throw new GSException("Not locked");
		}

		final long baseId;
		if (container instanceof Collection) {
			baseId = Tool.getRowId(this);
		}
		else if (container instanceof TimeSeries) {
			baseId = ((Date) Tool.getRowKey(this)).getTime();
		}
		else {
			throw new Error("Unsupported container type");
		}

		return new Experimentals.RowId(container, transactionId, baseId);
	}

}
