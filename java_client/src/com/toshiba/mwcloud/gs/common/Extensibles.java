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
package com.toshiba.mwcloud.gs.common;

import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.ContainerType;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.IndexInfo;
import com.toshiba.mwcloud.gs.PartitionController;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowKeyPredicate;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.common.ContainerKeyConverter.ContainerKey;
import com.toshiba.mwcloud.gs.common.Statement.GeneralStatement;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.OptionalRequestSource;

public class Extensibles {

	private Extensibles() {
	}

	public interface AsFactoryProvidable {

		public AsStoreFactory getExtensibleFactory(
				Set<Class<?>> chainProviderClasses,
				Set<Class<?>> visitedProviderClasses);

	}

	public abstract static class AsStoreFactory {

		public static final String VERSION_KEY = "version";

		public static AsStoreFactory newInstance(
				Set<Class<?>> chainProviderClasses,
				Set<Class<?>> visitedProviderClasses) {

			final AsFactoryProvidable provider =
					GridStoreFactoryProvider.getProvider(
							AsFactoryProvidable.class,
							AsStoreFactory.class,
							chainProviderClasses);

			return provider.getExtensibleFactory(
					chainProviderClasses, visitedProviderClasses);
		}

		public abstract AsStore getExtensibleStore(
				Properties properties,
				Properties extProperties) throws GSException;

		public abstract GridStoreFactory getBaseFactory();

	}

	public interface AsStore extends GridStore {

		public void executeStatement(
				GeneralStatement statement, int partitionIndex,
				StatementHandler handler) throws GSException;

		public long getDatabaseId() throws GSException;

		public ContainerKeyConverter getContainerKeyConverter(
				boolean internalMode, boolean forModification) throws GSException;

		public ContainerProperties getContainerProperties(
				ContainerKey key, ContainerProperties.KeySet propKeySet,
				Integer attribute, boolean internalMode) throws GSException;

		public List<ContainerProperties> getAllContainerProperties(
				List<ContainerKey> keyList,
				ContainerProperties.KeySet propKeySet,
				Integer attribute, boolean internalMode) throws GSException;

		public <K, R> AsContainer<K, R> getContainer(
				ContainerKey key, Container.BindType<K, R, ?> bindType,
				Integer attribute, boolean internalMode) throws GSException;

		public <K, R> Container<K, R> putContainer(
				ContainerKey key, Container.BindType<K, R, ?> bindType,
				ContainerProperties props, boolean modifiable,
				boolean internalMode) throws GSException;

		public void dropContainer(
				ContainerKey key, ContainerType containerType,
				boolean internalMode) throws GSException;

		public void removeContainerCache(
				ContainerKey key, boolean internalMode) throws GSException;

		public void fetchAll(
				MultiOperationContext<
				Integer, Query<?>, Query<?>> multiContext) throws GSException;

		public void multiPut(
				MultiOperationContext<
				ContainerKey, List<Row>, Void> multiContext,
				boolean internalMode) throws GSException;

		public void multiGet(
				MultiOperationContext<ContainerKey,
				? extends RowKeyPredicate<?>, List<Row>> multiContext,
				boolean internalMode) throws GSException;

		public void setContainerMonitoring(boolean monitoring);

		public boolean isContainerMonitoring();

		@Override
		public AsPartitionController getPartitionController()
				throws GSException;
	}

	public interface AsContainer<K, R> extends Container<K, R> {

		public Class<R> getRowType() throws GSException;

		public ContainerProperties getSchemaProperties() throws GSException;

		public Object getRowValue(Object rowObj, int column)
				throws GSException;

		public Object getRowKeyValue(Object keyObj, int keyColumn)
				throws GSException;

		public void createIndex(IndexInfo info, OptionalRequestSource source)
				throws GSException;

		public void dropIndex(IndexInfo info, OptionalRequestSource source)
				throws GSException;

		public ContainerKey getKey();

		@Override
		public <S> AsQuery<S> query(final String tql, Class<S> rowType)
				throws GSException;

	}

	public interface AsQuery<R> extends Query<R> {

		@Override
		public AsRowSet<R> getRowSet() throws GSException;

		public void applyOptionsTo(AsQuery<?> dest) throws GSException;

		public void setExtOption(
				int key, byte[] value, boolean followingInheritable)
				throws GSException;

		public void setAcceptableResultKeys(
				Set<Integer> keys) throws GSException;

		public void setContainerLostAcceptable(
				boolean acceptable) throws GSException;

		public QueryInfo getInfo();

		public Query<R> getBaseQuery();

		public AsContainer<?, ?> getContainer();

	}

	public interface AsRowSet<R> extends RowSet<R> {

		public Class<?> getMappingRowType() throws GSException;

		public Set<Integer> getExtOptionKeys() throws GSException;

		public byte[] getExtOption(int key) throws GSException;

		public QueryInfo getQueryInfo();

		public AsContainer<?, ?> getContainer();

	}

	public interface AsPartitionController extends PartitionController {

		public long getContainerCount(
				int partitionIndex, ContainerKeyPredicate pred,
				boolean internalMode) throws GSException;

		public List<String> getContainerNames(
				int partitionIndex, long start, Long limit,
				ContainerKeyPredicate pred, boolean internalMode) throws GSException;

		public int getPartitionIndexOfContainer(
				String containerName, boolean internalMode) throws GSException;

		public int getPartitionIndexOfContainer(
				ContainerKey containerKey, boolean internalMode)
				throws GSException;

	}

	public interface MultiTargetConsumer<K, V> {

		public void consume(
				K key, V value, Integer attribute,
				OptionalRequestSource source) throws GSException;

	}

	public interface MultiOperationContext<K, V, R> {

		public void listTarget(
				MultiTargetConsumer<K, V> consumer) throws GSException;

		public boolean acceptException(
				GSException exception) throws GSException;

		public void acceptCompletion(K key, R result) throws GSException;

		public boolean isRemaining() throws GSException;

	}

	public interface StatementHandler {

		void makeRequest(BasicBuffer buf) throws GSException;

		void acceptResponse(BasicBuffer buf) throws GSException;

	}

	public interface QueryInfo {

		String getQueryString();

	}

	public static AsStore get(GridStore store) {
		final AsStore extensible;
		try {
			extensible = (AsStore) store;
		}
		catch (ClassCastException e) {
			throw new IllegalArgumentException(e);
		}
		return extensible;
	}

	public static AsPartitionController get(
			PartitionController partitionController) {
		final AsPartitionController extensible;
		try {
			extensible = (AsPartitionController) partitionController;
		}
		catch (ClassCastException e) {
			throw new IllegalArgumentException(e);
		}
		return extensible;
	}

}
