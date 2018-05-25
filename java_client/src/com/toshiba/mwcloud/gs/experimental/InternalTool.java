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
package com.toshiba.mwcloud.gs.experimental;

import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.FetchOption;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.subnet.SubnetContainer;
import com.toshiba.mwcloud.gs.subnet.SubnetQuery;

public class InternalTool {

	public static Query<Row> queryAllRows(
			Container<?, ?> container) throws GSException {
		if (!(container instanceof SubnetContainer)) {
			throw new GSException("Unsupported Container implementation");
		}

		final SubnetContainer<?, ?> subnetContainer =
				(SubnetContainer<?, ?>) container;
		final SubnetQuery<?> proxyQuery = subnetContainer.query("");

		return new Query<Row>() {

			long[] position = new long[1];

			@Override
			public void setFetchOption(FetchOption option, Object value)
					throws GSException {
				proxyQuery.setFetchOption(option, value);
			}

			@Override
			public RowSet<Row> fetch() throws GSException {
				return fetch(false);
			}

			@Override
			public RowSet<Row> fetch(boolean forUpdate) throws GSException {
				if (forUpdate) {
					throw new GSException(
							"forUpdate option can not be enabled " +
							"on this implementation");
				}

				return subnetContainer.getRowSet(
						position, proxyQuery.getFetchLimit());
			}

			@Override
			public void close() throws GSException {
				proxyQuery.close();
			}

			@Override
			public RowSet<Row> getRowSet() throws GSException {
				return null;
			}
		};
	}

}
