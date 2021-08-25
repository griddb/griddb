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

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.PartitionController;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.common.ContainerKeyConverter.ContainerKey;
import com.toshiba.mwcloud.gs.common.ContainerKeyPredicate;
import com.toshiba.mwcloud.gs.common.ContainerProperties;
import com.toshiba.mwcloud.gs.common.Extensibles;
import com.toshiba.mwcloud.gs.common.RowMapper;

/*
 * The current API might be changed in the next version.
 */
public class ExperimentalTool {

	private static final ContainerProperties.KeySet ALL_CONTAINER_PROP_KEYS;

	static {
		final Set<ContainerProperties.Key> set =
				EnumSet.allOf(ContainerProperties.Key.class);
		set.remove(ContainerProperties.Key.INDEX_DETAIL);
		ALL_CONTAINER_PROP_KEYS = new ContainerProperties.KeySet(set);
	}

	private ExperimentalTool() {
	}

	public static class RowId {

		private final Container<?, ?> baseContainer;

		private final long transactionId;

		private final long baseId;

		public RowId(
				Container<?, ?> baseContainer, long transactionId,
				long baseId) {
			this.baseContainer = baseContainer;
			this.transactionId = transactionId;
			this.baseId = baseId;
		}

		RowId(Experimentals.RowId base) {
			this.baseContainer = base.baseContainer;
			this.transactionId = base.transactionId;
			this.baseId = base.baseId;
		}

	}

	public static RowId getRowIdForUpdate(
			RowSet<?> rowSet) throws GSException {
		return new RowId(Experimentals.get(rowSet).getRowIdForUpdate());
	}

	public static <R> void updateRowById(
			Collection<?, R> container, RowId rowId, R rowObj)
			throws GSException {
		if (container != rowId.baseContainer) {
			throw new GSException("Unmatched container");
		}

		Experimentals.get(container).updateRowById(
				rowId.transactionId, rowId.baseId, rowObj);
	}

	public static void removeRowById(
			Collection<?, ?> container, RowId rowId) throws GSException {
		if (container != rowId.baseContainer) {
			throw new GSException("Unmatched container");
		}

		Experimentals.get(container).removeRowById(
				rowId.transactionId, rowId.baseId);
	}

	public static long getContainerCount(
			PartitionController partitionController, int partitionIndex,
			ContainerCondition cond) throws GSException {
		final boolean internalMode = true;
		final ContainerKeyPredicate pred = new ContainerKeyPredicate(cond);
		return Extensibles.get(partitionController).getContainerCount(
				partitionIndex, pred, internalMode);
	}

	public static List<String> getContainerNames(
			PartitionController partitionController, int partitionIndex,
			ContainerCondition cond, long start, Long limit)
			throws GSException {
		final boolean internalMode = true;
		final ContainerKeyPredicate pred = new ContainerKeyPredicate(cond);
		return Extensibles.get(partitionController).getContainerNames(
				partitionIndex, start, limit, pred, internalMode);
	}

	public static Container<Object, Row> putContainer(
			GridStore store, String name,
			ContainerInfo info, boolean modifiable) throws GSException {
		final boolean internalMode = true;
		final Extensibles.AsStore extStore = Extensibles.get(store);
		return extStore.putContainer(
				parseContainerKey(name, extStore, internalMode, true),
				RowMapper.BindingTool.createBindType(
						null, Row.class, Container.class),
				toContainerProperties(info),
				modifiable, internalMode);
	}

	public static void dropContainer(
			GridStore store, String name) throws GSException {
		final boolean internalMode = true;
		final Extensibles.AsStore extStore = Extensibles.get(store);
		extStore.dropContainer(
				parseContainerKey(name, extStore, internalMode, true),
				null, internalMode);
	}

	public static Container<Object, Row> getContainer(
			GridStore store, String name) throws GSException {
		final boolean internalMode = true;
		final Extensibles.AsStore extStore = Extensibles.get(store);
		return extStore.getContainer(
				parseContainerKey(name, extStore, internalMode, false),
				RowMapper.BindingTool.createBindType(
						null, Row.class, Container.class),
				null, internalMode);
	}

	public static ExtendedContainerInfo getExtendedContainerInfo(
			GridStore store, String name) throws GSException {
		final boolean internalMode = true;
		final Extensibles.AsStore extStore = Extensibles.get(store);
		final ContainerProperties props = extStore.getContainerProperties(
				parseContainerKey(name, extStore, internalMode, false),
				ALL_CONTAINER_PROP_KEYS, null, internalMode);
		return toExtendedContainerInfo(props);
	}

	public static void putUser(
			GridStore store, String name, UserInfo info, boolean modifiable)
			throws GSException {
		Experimentals.get(store).putUser(name, info, modifiable);
	}

	public static void dropUser(GridStore store, String name)
			throws GSException {
		Experimentals.get(store).dropUser(name);
	}

	public static void dropRole(GridStore store, String name)
			throws GSException {
	}

	public static void putRole(
			GridStore store, String name)
			throws GSException {
	}

	public static Map<String, UserInfo> getUsers(GridStore store)
			throws GSException {
		return Experimentals.get(store).getUsers();
	}

	public static UserInfo getCurrentUser(GridStore store) throws GSException {
		return Experimentals.get(store).getCurrentUser();
	}

	public static void putDatabase(
			GridStore store, String name, DatabaseInfo info,
			boolean modifiable) throws GSException {
		Experimentals.get(store).putDatabase(name, info, modifiable);
	}

	public static void dropDatabase(GridStore store, String name)
			throws GSException {
		Experimentals.get(store).dropDatabase(name);
	}

	public static Map<String, DatabaseInfo> getDatabases(GridStore store)
			throws GSException {
		return Experimentals.get(store).getDatabases();
	}

	public static DatabaseInfo getCurrentDatabase(
			GridStore store) throws GSException {
		return Experimentals.get(store).getCurrentDatabase();
	}

	public static void putPrivilege(
			GridStore store, String dbName, String userName,
			PrivilegeInfo info) throws GSException {
		Experimentals.get(store).putPrivilege(dbName, userName, info);
	}

	public static void dropPrivilege(
			GridStore store, String dbName, String userName,
			PrivilegeInfo info) throws GSException {
		Experimentals.get(store).dropPrivilege(dbName, userName, info);
	}

	private static ContainerProperties toContainerProperties(
			ContainerInfo info) {
		final ContainerProperties props = new ContainerProperties();
		props.setInfo(info);

		if (info instanceof ExtendedContainerInfo) {
			final ContainerAttribute attribute =
					((ExtendedContainerInfo) info).getAttribute();
			if (attribute != null) {
				props.setAttribute(attribute.flag());
			}
		}

		return props;
	}

	private static ExtendedContainerInfo toExtendedContainerInfo(
			ContainerProperties props) {
		final ContainerInfo baseInfo = ContainerProperties.findInfo(props);
		if (baseInfo == null) {
			return null;
		}

		final ExtendedContainerInfo info =
				new ExtendedContainerInfo(baseInfo);

		final Integer attribute = props.getAttribute();
		if (attribute != null) {
			info.setAttribute(ContainerAttribute.getAttribute(attribute));
		}

		return info;
	}

	private static ContainerKey parseContainerKey(
			String name, Extensibles.AsStore store, boolean internalMode,
			boolean forModification) throws GSException {
		return store.getContainerKeyConverter(
				internalMode, forModification).parse(name, false);
	}

}
