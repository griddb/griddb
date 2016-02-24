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

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.common.BasicBuffer;
import com.toshiba.mwcloud.gs.common.GSStatementException;

public class NodeConnectionPool implements Closeable {

	private static final int DEFAULT_MAX_SIZE = 16;

	private Map<SocketAddress, List<NodeConnection>> connectionMap =
			new HashMap<SocketAddress, List<NodeConnection>>();

	private Set<NodeConnection> connectionSet =
			new LinkedHashSet<NodeConnection>();

	private List<NodeConnection> exceededConnectionList =
			Collections.synchronizedList(new ArrayList<NodeConnection>());

	private int maxSize;

	public NodeConnectionPool() {
		this.maxSize = DEFAULT_MAX_SIZE;
	}

	public synchronized int getMaxSize() {
		return maxSize;
	}

	public void setMaxSize(int maxSize) {
		try {
			synchronized (this) {
				if (connectionMap != null) {
					adjustSize(maxSize);
				}
				this.maxSize = maxSize;
			}
		}
		finally {
			try {
				closeExceededConnections();
			}
			catch (GSException e) {
				
			}
		}
	}

	private void adjustSize(int maxSize) {
		while (connectionSet.size() > Math.max(maxSize, 0)) {
			final NodeConnection oldest = connectionSet.iterator().next();
			if (oldest != null) {
				final SocketAddress oldestKey = getKey(oldest);
				final List<NodeConnection> oldestList =
						connectionMap.get(oldestKey);
				if (oldestList != null) {
					oldestList.remove(oldest);
				}

				if (oldestList.isEmpty()) {
					connectionMap.remove(oldestKey);
				}
				connectionSet.remove(oldest);

				exceededConnectionList.add(oldest);
			}
		}
	}

	public void add(NodeConnection connection) {
		try {
			synchronized (this) {
				if (connectionMap == null) {
					exceededConnectionList.add(connection);
				}
				else if (connectionSet.add(connection)) {
					final SocketAddress key = getKey(connection);
					List<NodeConnection> list = connectionMap.get(key);
					if (list == null) {
						list = new LinkedList<NodeConnection>();
						connectionMap.put(key, list);
					}
					list.add(connection);

					adjustSize(maxSize);
				}
			}
		}
		finally {
			try {
				closeExceededConnections();
			}
			catch (GSException e) {
				
			}
		}
	}

	public synchronized NodeConnection pull(SocketAddress address) {
		if (connectionMap == null) {
			return null;
		}

		final List<NodeConnection> list = connectionMap.get(address);
		if (list == null || list.isEmpty()) {
			return null;
		}

		final NodeConnection connection = list.remove(0);
		if (list.isEmpty()) {
			connectionMap.remove(address);
		}
		connectionSet.remove(connection);

		return connection;
	}

	public NodeConnection resolve(InetSocketAddress address,
			BasicBuffer req, BasicBuffer resp,
			NodeConnection.Config config,
			NodeConnection.LoginInfo loginInfo,
			boolean preferCache) throws GSException {
		NodeConnection connection = (preferCache ? pull(address) : null);
		if (connection == null) {
			final NodeConnection newConnection =
					new NodeConnection(address, config);
			try {
				newConnection.connect(req, resp);
				newConnection.login(req, resp, loginInfo);
				connection = newConnection;
			}
			finally {
				if (connection == null) {
					newConnection.close();
				}
			}
		}
		else {
			boolean released = false;
			try {
				connection.reuse(req, resp, loginInfo);
				released = true;
			}
			catch (GSStatementException e) {
				released = true;
				add(connection);
				throw e;
			}
			finally {
				if (!released) {
					connection.close();
				}
			}
		}

		return connection;
	}

	private static SocketAddress getKey(NodeConnection connection) {
		return connection.getRemoteSocketAddress();
	}

	public void close() throws GSException {
		try {
			synchronized (this) {
				if (connectionSet == null) {
					return;
				}

				try {
					exceededConnectionList.addAll(connectionSet);
				}
				finally {
					connectionMap = null;
					connectionSet = null;
				}
			}
		}
		finally {
			closeExceededConnections();
		}
	}

	private void closeExceededConnections() throws GSException {
		final List<NodeConnection> connectionList;
		synchronized (exceededConnectionList) {
			if (exceededConnectionList.isEmpty()) {
				return;
			}

			connectionList = new ArrayList<NodeConnection>();
			connectionList.addAll(exceededConnectionList);
			exceededConnectionList.clear();
		}
		closeConnections(connectionList, false);
	}

	private static void closeConnections(
			List<NodeConnection> connectionList, boolean silent)
			throws GSException {
		if (connectionList == null || connectionList.isEmpty()) {
			return;
		}

		try {
			for (Iterator<NodeConnection> it = connectionList.iterator();
					it.hasNext();) {
				if (silent) {
					try {
						it.next().close();
					}
					catch (Throwable e) {
					}
				}
				else {
					it.next().close();
				}
				it.remove();
			}
		}
		finally {
			if (!connectionList.isEmpty() && !silent) {
				closeConnections(connectionList, true);
			}
		}
	}

}
