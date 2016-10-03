/*
   Copyright (c) 2012 TOSHIBA CORPORATION.

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
import java.util.HashSet;

import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GSRecoverableException;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.common.GSErrorCode;
import com.toshiba.mwcloud.gs.common.RowMapper;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.Context;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.RemoteReference;
import com.toshiba.mwcloud.gs.subnet.SubnetContainer.ContainerSubResource;
import com.toshiba.mwcloud.gs.subnet.SubnetContainer.TransactionalResource;

@SuppressWarnings("deprecation")
public class SubnetRowSet<R> implements RowSet<R> {

	private final SubnetContainer<?, ?> container;

	private final Class<R> rowType;

	private final RowMapper mapper;

	private RowMapper.Cursor resultCursor;

	private final long transactionId;

	private Object lastKey;

	private final long totalRowCount;

	private long remainingRowCount;

	private final long rowSetId;

	private final long rowSetIdHint;

	private final long fetchSize;

	private Object targetConnection;

	private final boolean partial;

	private boolean followingLost;

	private boolean closed;

	private final PartialRowSetReference remoteRef;

	public SubnetRowSet(SubnetContainer<?, ?> container, Class<R> rowType,
			RowMapper mapper, RowMapper.Cursor resultCursor,
			long transactionId) {
		this.container = container;
		this.rowType = rowType;
		this.mapper = mapper;
		this.resultCursor = resultCursor;
		this.transactionId = transactionId;
		this.totalRowCount = resultCursor.getRowCount();
		this.rowSetId = 0;
		this.rowSetIdHint = 0;
		this.fetchSize = 0;
		this.partial = false;
		this.remoteRef = null;
	}

	public SubnetRowSet(SubnetContainer<?, ?> container, Class<R> rowType,
			RowMapper mapper, RowMapper.Cursor resultCursor,
			long transactionId, long totalRowCount,
			long rowSetId, long rowSetIdHint, long fetchSize,
			Object targetConnection) throws GSException {
		this.container = container;
		this.rowType = rowType;
		this.mapper = mapper;
		this.resultCursor = resultCursor;
		this.transactionId = transactionId;
		this.totalRowCount = totalRowCount;
		this.remainingRowCount = totalRowCount - resultCursor.getRowCount();
		this.rowSetId = rowSetId;
		this.rowSetIdHint = rowSetIdHint;
		this.fetchSize = fetchSize;
		this.targetConnection = targetConnection;
		this.partial = true;

		synchronized (container.context) {
			container.channel.cleanRemoteResources(
					container.context, new HashSet<Class<?>>(
							Arrays.<Class<?>>asList(
									NormalTarget.class,
									TransactionalTarget.class)));

			remoteRef = new PartialRowSetReference(
					this, rowSetId, rowSetIdHint, targetConnection);
			container.context.addRemoteReference(remoteRef);
		}
	}

	public boolean isPartial() {
		return partial;
	}

	private void checkOpened() throws GSException {
		if (GridStoreChannel.v10ResourceCompatible) {
			return;
		}

		if (closed || container.isClosed() ||
				(remoteRef != null && remoteRef.closed)) {
			throw new GSException(GSErrorCode.RESOURCE_CLOSED,
					"This row set or related container has been closed");
		}
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
		return (resultCursor.hasNext() || remainingRowCount > 0);
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
							rowSetId, rowSetIdHint,
							totalRowCount, remainingRowCount,
							fetchSize, targetConnectionResult, mapper);
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

			throw new GSRecoverableException(
					GSErrorCode.RECOVERABLE_ROW_SET_LOST,
					"Row set lost by modifications (" +
					"remainingRowCount=" + remainingRowCount + ")");
		}
	}

	@Override
	public R next() throws GSException {
		checkOpened();

		if (!resultCursor.hasNext()) {
			if (remainingRowCount <= 0) {
				throw new GSException(
						GSErrorCode.NO_SUCH_ELEMENT,
						"No more rows were found");
			}
			fetchFollowing();
		}

		final R rowObj;
		if (rowType == null) {
			@SuppressWarnings("unchecked")
			final R unchekedRowObj = (R) mapper.decode(resultCursor);

			rowObj = unchekedRowObj;
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

		return rowObj;
	}

	@Override
	public void remove() throws GSException {
		checkOpened();

		if (!resultCursor.isInRange()) {
			throw new GSException(
					GSErrorCode.NO_SUCH_ELEMENT, "Cursor out of range");
		}

		container.remove(mapper, transactionId,
				resultCursor.getLastRowId(), lastKey);
	}

	@Override
	public void update(R rowObj) throws GSException {
		checkOpened();

		if (!resultCursor.isInRange()) {
			throw new GSException(
					GSErrorCode.NO_SUCH_ELEMENT, "Cursor out of range");
		}

		container.update(mapper, transactionId,
				resultCursor.getLastRowId(), lastKey, rowObj);
	}

	@Override
	public int size() {
		return (int) totalRowCount;
	}

	private static interface NormalTarget extends ContainerSubResource {
	}

	private static interface TransactionalTarget
	extends TransactionalResource {
	}

	private static class PartialRowSetReference
	extends RemoteReference<RowSet<?>> {

		private final long rowSetId;
		private final long rowSetIdHint;
		private final Object targetConnection;
		private boolean closed;

		PartialRowSetReference(
				SubnetRowSet<?> rowSet,
				long rowSetId, long rowSetIdHint, Object targetConnection) {
			super(rowSet, selectTargetClass(rowSet), rowSet.container.context,
					rowSet.container.getPartitionId(),
					rowSet.container.getContainerId());
			this.rowSetId = rowSetId;
			this.rowSetIdHint = rowSetIdHint;
			this.targetConnection = targetConnection;
		}

		private static Class<?> selectTargetClass(SubnetRowSet<?> rowSet) {
			if (rowSet.transactionId == 0) {
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
					rowSetId, rowSetIdHint, targetConnection);
		}

	}

	public static class Tool {

		private Tool() {
		}

		public static Container<?, ?> getContainer(SubnetRowSet<?> rowSet) {
			return rowSet.container;
		}

		public static long getTransactionId(SubnetRowSet<?> rowSet) {
			return rowSet.transactionId;
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

	

	public Row nextGeneralRow() throws GSException {
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

}
