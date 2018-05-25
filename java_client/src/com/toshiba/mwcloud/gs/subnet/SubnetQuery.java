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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.toshiba.mwcloud.gs.FetchOption;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.common.BasicBuffer;
import com.toshiba.mwcloud.gs.common.Extensibles;
import com.toshiba.mwcloud.gs.common.GSErrorCode;
import com.toshiba.mwcloud.gs.common.RowMapper;
import com.toshiba.mwcloud.gs.common.Statement;
import com.toshiba.mwcloud.gs.experimental.Experimentals;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.OptionalRequest;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.OptionalRequestSource;
import com.toshiba.mwcloud.gs.subnet.SubnetContainer.PartialExecutionStatus;
import com.toshiba.mwcloud.gs.subnet.SubnetContainer.QueryFormatter;

public class SubnetQuery<R>
implements Query<R>, Extensibles.AsQuery<R>, Experimentals.AsQuery<R> {

	private final SubnetContainer<?, ?> container;

	private final Class<R> resultType;

	private final RowMapper mapper;

	private final QueryFormatter formatter;

	private QueryParameters parameters;

	private SubnetRowSet<R> lastRowSet;

	private boolean lastRowSetVisible;

	private boolean closed;

	public SubnetQuery(
			SubnetContainer<?, ?> container, Class<R> resultType,
			RowMapper mapper, QueryFormatter formatter) {
		this.container = container;
		this.resultType = resultType;
		this.mapper = mapper;
		this.formatter = formatter;
	}

	SubnetContainer<?, ?> getContainer() {
		return container;
	}

	Statement getStatement() {
		return formatter.getStatement();
	}

	private void checkOpened() throws GSException {
		if (GridStoreChannel.v10ResourceCompatible) {
			return;
		}

		if (closed || container.isClosed()) {
			throw new GSException(GSErrorCode.RESOURCE_CLOSED, "");
		}
	}

	@SuppressWarnings("deprecation")
	@Override
	public void setFetchOption(FetchOption option, Object value)
			throws GSException {
		checkOpened();

		if (option == null) {
			throw GSErrorCode.checkNullParameter(value, "option", null);
		}

		if (value == null) {
			throw GSErrorCode.checkNullParameter(value, "value", null);
		}

		parameters = QueryParameters.resolve(parameters);
		switch (option) {
		case LIMIT:
			parameters.setFetchLimit(filterSizedFetchOption(option, value));
			break;
		case SIZE:
			parameters.setFetchSize(filterSizedFetchOption(option, value));
			break;
		case PARTIAL_EXECUTION:
			parameters.setPartialExecutionEnabled(
					filterFetchOption(option, value, Boolean.class, true));
			break;
		default:
			throw new Error();
		}
	}

	private static long filterSizedFetchOption(
			FetchOption option, Object value) throws GSException {
		final Integer intValue =
				filterFetchOption(option, value, Integer.class, false);

		final long longValue;
		if (intValue == null) {
			longValue = filterFetchOption(option, value, Long.class, true);
		}
		else {
			longValue = (int) intValue;
		}

		if (longValue < 0) {
			throw new GSException(
					GSErrorCode.ILLEGAL_PARAMETER,
					"Negative fetch option value specified " +
					"(option=" + option +
					", value=" + longValue + ")");
		}

		return longValue;
	}

	private static <T> T filterFetchOption(
			FetchOption option, Object value, Class<T> type,
			boolean force) throws GSException {
		if (!type.isInstance(value)) {
			if (!force) {
				return null;
			}
			throw new GSException(
					GSErrorCode.UNSUPPORTED_FIELD_TYPE,
					"Unsupported fetch option value type " +
					"(option=" + option +
					", value=" + value +
					", valueClass=" + value.getClass() + ")");
		}

		return type.cast(value);
	}

	@Override
	public void close() throws GSException {
		closed = true;

		
		
		
		
		clearLastRowSet();
	}

	@Override
	public RowSet<R> fetch() throws GSException {
		return fetch(false);
	}

	@Override
	public RowSet<R> fetch(boolean forUpdate) throws GSException {
		checkOpened();
		clearLastRowSet();

		final SubnetRowSet<R> rowSet = container.queryAndFetch(
				resultType, mapper, formatter, parameters,
				QueryParameters.isForUpdate(parameters, forUpdate));

		if (rowSet != null) {
			rowSet.prepareFollowing();
			lastRowSet = rowSet;
			lastRowSetVisible = true;
		}

		return lastRowSet;
	}

	@Override
	public SubnetRowSet<R> getRowSet() throws GSException {
		checkOpened();

		if (!lastRowSetVisible) {
			return null;
		}

		lastRowSet.prepareFollowing();
		lastRowSetVisible = false;

		return lastRowSet;
	}

	private void clearLastRowSet() throws GSException {
		final SubnetRowSet<R> rowSet = lastRowSet;
		lastRowSet = null;
		lastRowSetVisible = false;

		if (rowSet == null) {
			return;
		}

		if (!GridStoreChannel.v10ResourceCompatible) {
			rowSet.close();
		}
	}

	public static boolean isFetchSizeEnabled(int protocolVersion) {
		return (protocolVersion >= 2 &&
				(GridStoreChannel.v1ProtocolCompatible == null ||
				!GridStoreChannel.v1ProtocolCompatible));
	}

	public long getFetchLimit() throws GSException {
		checkOpened();
		return QueryParameters.get(parameters).fetchLimit;
	}

	void setForUpdate(boolean forUpdate) {
		if (forUpdate != QueryParameters.get(parameters).forUpdate) {
			parameters = QueryParameters.resolve(parameters);
			parameters.forUpdate = forUpdate;
		}
	}

	boolean isForUpdate() {
		return QueryParameters.isForUpdate(parameters, false);
	}

	void makeRequest(BasicBuffer req, boolean noUUID) throws GSException {
		checkOpened();
		clearLastRowSet();

		final boolean forUpdate =
				QueryParameters.isForUpdate(parameters, false);
		container.makeQueryRequest(
				formatter, parameters, forUpdate, req, noUUID);
	}

	void acceptResponse(
			BasicBuffer resp, Object targetConnection,
			boolean bufSwapAllowed) throws GSException {
		clearLastRowSet();

		final boolean forUpdate =
				QueryParameters.isForUpdate(parameters, false);
		final SubnetRowSet<R> rowSet = container.acceptQueryResponse(
				resultType, mapper, formatter, parameters, forUpdate, resp,
				targetConnection, bufSwapAllowed);

		if (rowSet != null) {
			lastRowSet = rowSet;
			lastRowSetVisible = true;
		}
	}

	@Override
	public void applyOptionsTo(
			Extensibles.AsQuery<?> dest) throws GSException {
		checkOpened();
		if (parameters == null) {
			return;
		}
		parameters.applyOptionsTo(dest);
	}

	@Override
	public void setExtOption(
			int key, byte[] value, boolean followingInheritable)
			throws GSException {
		checkOpened();
		parameters = QueryParameters.resolve(parameters);
		parameters.setExtOption(key, value, followingInheritable);
	}

	@Override
	public void setAcceptableResultKeys(Set<Integer> keys)
			throws GSException {
		checkOpened();
		parameters = QueryParameters.resolve(parameters);
		parameters.acceptableResultKeys = keys;
	}

	@Override
	public void setContainerLostAcceptable(boolean acceptable)
			throws GSException {
		checkOpened();
		parameters = QueryParameters.resolve(parameters);
		parameters.containerLostAcceptable = acceptable;
	}

	static class QueryParameters implements OptionalRequestSource {

		private static final QueryParameters DEFAULT_INSTANCE =
				new QueryParameters();

		private static final long DEFAULT_SIZE_OPTION_VALUE = Long.MAX_VALUE;

		long fetchLimit = DEFAULT_SIZE_OPTION_VALUE;

		long fetchSize = DEFAULT_SIZE_OPTION_VALUE;

		PartialExecutionStatus executionStatus =
				PartialExecutionStatus.DISABLED;

		boolean executionPartial;

		boolean containerLostAcceptable;

		boolean forUpdate;

		Long initialTransactionId;

		Map<Integer, byte[]> extOptions;

		Set<Integer> inheritableOptionKeys;

		Set<Integer> acceptableResultKeys = Collections.emptySet();

		QueryParameters() {
		}

		QueryParameters(QueryParameters src) {
			this.fetchLimit = src.fetchLimit;
			this.fetchSize = src.fetchSize;
			this.executionStatus = src.executionStatus;
			this.executionPartial = src.executionPartial;
			this.containerLostAcceptable = src.containerLostAcceptable;

			this.forUpdate = src.forUpdate;
			this.initialTransactionId = src.initialTransactionId;

			if (src.extOptions != null) {
				this.extOptions = new HashMap<Integer, byte[]>(src.extOptions);
			}
			if (src.inheritableOptionKeys != null) {
				this.inheritableOptionKeys =
						new HashSet<Integer>(src.inheritableOptionKeys);
			}
			this.acceptableResultKeys = src.acceptableResultKeys;
		}

		@Override
		public boolean hasOptions() {
			return extOptions != null;
		}

		@Override
		public void putOptions(OptionalRequest optionalRequest) {
			if (extOptions == null) {
				return;
			}
			for (Map.Entry<Integer, byte[]> entry : extOptions.entrySet()) {
				optionalRequest.putExt(entry.getKey(), entry.getValue());
			}
		}

		public void applyOptionsTo(
				Extensibles.AsQuery<?> dest) throws GSException {
			if (fetchLimit != DEFAULT_SIZE_OPTION_VALUE) {
				dest.setFetchOption(FetchOption.LIMIT, fetchLimit);
			}
			if (fetchSize != DEFAULT_SIZE_OPTION_VALUE) {
				@SuppressWarnings("deprecation")
				final FetchOption option = FetchOption.SIZE;
				dest.setFetchOption(option, fetchSize);
			}
			if (isPartialExecutionConfigured()) {
				dest.setFetchOption(FetchOption.PARTIAL_EXECUTION, true);
			}
			if (containerLostAcceptable) {
				dest.setContainerLostAcceptable(true);
			}
			if (extOptions != null) {
				for (Map.Entry<Integer, byte[]> entry :
						extOptions.entrySet()) {
					final Integer key = entry.getKey();
					final boolean inheritable =
							(inheritableOptionKeys != null &&
							inheritableOptionKeys.contains(key));
					dest.setExtOption(key, entry.getValue(), inheritable);
				}
			}
			if (acceptableResultKeys != null) {
				dest.setAcceptableResultKeys(acceptableResultKeys);
			}
		}

		void putFixed(BasicBuffer out) {
			out.putLong(fetchLimit);
			if (isFetchSizeEnabled(NodeConnection.getProtocolVersion())) {
				out.putLong(fetchSize);
			}
			if (SubnetGridStore.isQueryOptionsExtensible()) {
				executionStatus.put(out);
			}
		}

		void setFetchLimit(long fetchLimit) throws GSException {
			this.fetchLimit = fetchLimit;
		}

		void setFetchSize(long fetchSize) throws GSException {
			checkPartialOptions(fetchSize, executionStatus);
			this.fetchSize = fetchSize;
		}

		void setPartialExecutionEnabled(boolean enabled) throws GSException {
			final PartialExecutionStatus executionStatus = (enabled ?
					PartialExecutionStatus.ENABLED_INITIAL :
					PartialExecutionStatus.DISABLED);

			checkPartialOptions(fetchSize, executionStatus);
			this.executionStatus = executionStatus;
			this.executionPartial = enabled;
		}

		boolean isPartialExecutionConfigured() {
			return executionPartial;
		}

		void setExtOption(
				int key, byte[] value, boolean followingInheritable)
				throws GSException {
			if (extOptions == null) {
				extOptions = new HashMap<Integer, byte[]>();
			}
			extOptions.put(key, value);

			if (followingInheritable) {
				if (inheritableOptionKeys == null) {
					inheritableOptionKeys = new HashSet<Integer>();
				}
				inheritableOptionKeys.add(key);
			}
		}

		void checkResultType(int type) throws GSException {
			if (!acceptableResultKeys.contains(type)) {
				throw new GSException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by unexpected query result type (" +
						"type=" + type + ")");
			}
		}

		static void checkPartialOptions(
				long fetchSize, PartialExecutionStatus executionStatus)
				throws GSException {
			if (isPartialFetch(fetchSize) && executionStatus.isEnabled()) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"Partial fetch and partial execution " +
						"cannot be enabled at the same time");
			}
		}

		static boolean isPartialFetch(long fetchSize) {
			return fetchSize != DEFAULT_SIZE_OPTION_VALUE;
		}

		static boolean isForUpdate(QueryParameters src, boolean forUpdate) {
			return get(src).forUpdate || forUpdate;
		}

		static long resolveInitialTransactionId(QueryParameters src) {
			final Long transactionId = findInitialTransactionId(src);
			return (transactionId == null ? 0 : transactionId);
		}

		static Long findInitialTransactionId(QueryParameters src) {
			return get(src).initialTransactionId;
		}

		static QueryParameters get(QueryParameters src) {
			return (src == null ? DEFAULT_INSTANCE : src);
		}

		static QueryParameters resolve(QueryParameters src) {
			return (src == null ? new QueryParameters() : src);
		}

		static QueryParameters inherit(
				QueryParameters src, boolean forUpdate, long transactionId,
				PartialExecutionStatus executionStatus) throws GSException {
			if (src == null && !forUpdate && transactionId == 0 &&
					!PartialExecutionStatus.isEnabled(executionStatus)) {
				return null;
			}

			final QueryParameters dest = (src == null ?
					new QueryParameters() : new QueryParameters(src));

			dest.forUpdate = isForUpdate(src, forUpdate);
			if (dest.initialTransactionId != null &&
					dest.initialTransactionId != transactionId) {
				throw new Error();
			}
			dest.initialTransactionId = transactionId;

			dest.executionStatus = PartialExecutionStatus.inherit(
					dest.executionStatus, executionStatus);

			if (src != null && src.extOptions != null &&
					dest.inheritableOptionKeys != null) {
				dest.extOptions.keySet().retainAll(dest.inheritableOptionKeys);
			}

			return dest;
		}

	}

}
