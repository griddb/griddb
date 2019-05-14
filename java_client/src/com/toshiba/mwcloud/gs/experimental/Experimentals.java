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

import java.util.Map;

import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowSet;

public class Experimentals {

	private Experimentals() {
	}

	public interface StoreProvider {

		public AsStore getExperimentalStore();

	}

	public interface AsStore {

		public void putUser(
				String name, UserInfo info,
				boolean modifiable) throws GSException;

		public void dropUser(String name) throws GSException;

		public Map<String, UserInfo> getUsers() throws GSException;

		public UserInfo getCurrentUser() throws GSException;

		public void putDatabase(
				String name, DatabaseInfo info,
				boolean modifiable) throws GSException;

		public void dropDatabase(String name) throws GSException;

		public Map<String, DatabaseInfo> getDatabases() throws GSException;

		public DatabaseInfo getCurrentDatabase() throws GSException;

		public void putPrivilege(
				String dbName, String userName,
				PrivilegeInfo info) throws GSException;

		public void dropPrivilege(
				String dbName, String userName,
				PrivilegeInfo info) throws GSException;

	}

	public interface AsContainer<K, R> extends Container<K, R> {

		public void removeRowById(
				long transactionId, long baseId) throws GSException;

		public void updateRowById(
				long transactionId, long baseId, R rowObj) throws GSException;

		public RowSet<Row> getRowSet(
				Object[] position, long fetchLimit) throws GSException;

	}

	public interface AsQuery<R> extends Query<R> {

		long getFetchLimit() throws GSException;

	}

	public interface AsRowSet<R> extends RowSet<R> {

		RowId getRowIdForUpdate() throws GSException;

	}

	public static AsStore get(GridStore store) {
		final StoreProvider provider;
		try {
			provider = (StoreProvider) store;
		}
		catch (ClassCastException e) {
			throw new IllegalArgumentException(e);
		}
		return provider.getExperimentalStore();
	}

	public static <K, R> AsContainer<K, R> get(Container<K, R> container) {
		final AsContainer<K, R> extensible;
		try {
			extensible = (AsContainer<K, R>) container;
		}
		catch (ClassCastException e) {
			throw new IllegalArgumentException(e);
		}
		return extensible;
	}

	public static <R> AsQuery<R> get(Query<R> query) {
		final AsQuery<R> extensible;
		try {
			extensible = (AsQuery<R>) query;
		}
		catch (ClassCastException e) {
			throw new IllegalArgumentException(e);
		}
		return extensible;
	}

	public static <R> AsRowSet<R> get(RowSet<R> rowSet) {
		final AsRowSet<R> extensible;
		try {
			extensible = (AsRowSet<R>) rowSet;
		}
		catch (ClassCastException e) {
			throw new IllegalArgumentException(e);
		}
		return extensible;
	}

	public static class RowId {

		final Container<?, ?> baseContainer;

		final long transactionId;

		final long baseId;

		public RowId(
				Container<?, ?> baseContainer, long transactionId,
				long baseId) {
			this.baseContainer = baseContainer;
			this.transactionId = transactionId;
			this.baseId = baseId;
		}

	}

}
