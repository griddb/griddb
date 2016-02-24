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

import com.toshiba.mwcloud.gs.FetchOption;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.common.BasicBuffer;
import com.toshiba.mwcloud.gs.common.GSErrorCode;
import com.toshiba.mwcloud.gs.common.RowMapper;
import com.toshiba.mwcloud.gs.common.Statement;
import com.toshiba.mwcloud.gs.subnet.SubnetContainer.QueryFormatter;

public class SubnetQuery<R> implements Query<R>, QueryFormatter {

	private final SubnetContainer<?, ?> container;

	private final Class<R> resultType;

	private final RowMapper mapper;

	private final Statement statement;

	private final QueryFormatter formatter;

	private long fetchLimit = Long.MAX_VALUE;

	private long fetchSize = Long.MAX_VALUE;

	private SubnetRowSet<R> lastRowSet;

	private boolean forUpdate;

	private boolean lastRowSetVisible;

	private boolean closed;

	public SubnetQuery(SubnetContainer<?, ?> container, Class<R> resultType,
			RowMapper mapper, Statement statement, QueryFormatter formatter) {
		this.container = container;
		this.resultType = resultType;
		this.mapper = mapper;
		this.statement = statement;
		this.formatter = formatter;
	}

	SubnetContainer<?, ?> getContainer() {
		return container;
	}

	Statement getStatement() {
		return statement;
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

		final long longValue;
		switch (option) {
		case LIMIT:
			if (value instanceof Integer) {
				longValue = (long) (Integer) value;
			}
			else if (value instanceof Long) {
				longValue = (Long) value;
			}
			else {
				throw new GSException(
						GSErrorCode.UNSUPPORTED_FIELD_TYPE,
						"Unsupported limit value type " +
						"(class=" + value.getClass() + ")");
			}
			if (longValue < 0) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"Unsupported limit value (value=" + value + ")");
			}
			fetchLimit = longValue;
			break;
		case SIZE:
			if (value instanceof Integer) {
				longValue = (long) (Integer) value;
			}
			else if (value instanceof Long) {
				longValue = (Long) value;
			}
			else {
				throw new GSException(
						GSErrorCode.UNSUPPORTED_FIELD_TYPE,
						"Unsupported size value type " +
						"(class=" + value.getClass() + ")");
			}
			if (longValue < 0) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"Unsupported size value (value=" + value + ")");
			}
			fetchSize = longValue;
			break;
		default:
			throw new InternalError();
		}
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

		lastRowSet = container.queryAndFetch(
				statement, resultType, mapper, this,
				this.forUpdate || forUpdate, fetchSize);
		lastRowSetVisible = true;

		return lastRowSet;
	}

	@Override
	public void format(BasicBuffer inBuf) throws GSException {
		inBuf.putLong(fetchLimit);
		if (isFetchSizeEnabled(NodeConnection.getProtocolVersion())) {
			inBuf.putLong(fetchSize);
		}
		formatter.format(inBuf);
	}

	@Override
	public String getQueryString() {
		return formatter.getQueryString();
	}

	@Override
	public RowSet<R> getRowSet() throws GSException {
		checkOpened();

		if (!lastRowSetVisible) {
			return null;
		}

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

		if (!GridStoreChannel.v10ResourceCompatible || rowSet.isPartial()) {
			rowSet.close();
		}
	}

	public static boolean isFetchSizeEnabled(int protocolVersion) {
		return (protocolVersion >= 2 &&
				(GridStoreChannel.v1ProtocolCompatible == null ||
				!GridStoreChannel.v1ProtocolCompatible));
	}

	public long getFetchLimit() {
		return fetchLimit;
	}

	public void setForUpdate(boolean forUpdate) {
		this.forUpdate = forUpdate;
	}

	public boolean isForUpdate() {
		return forUpdate;
	}

	void makeRequest(BasicBuffer req, boolean noUUID) throws GSException {
		checkOpened();
		clearLastRowSet();

		container.makeQueryRequest(
				statement, this, forUpdate, fetchSize, req, noUUID);
	}

	void acceptResponse(
			BasicBuffer resp, Object targetConnection,
			boolean bufSwapAllowed) throws GSException {
		clearLastRowSet();

		lastRowSet = container.acceptQueryResponse(
				statement, resultType, mapper, forUpdate, fetchSize, resp,
				targetConnection, bufSwapAllowed);
		lastRowSetVisible = true;
	}

}
