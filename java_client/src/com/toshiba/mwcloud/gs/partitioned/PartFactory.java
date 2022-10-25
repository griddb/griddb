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
package com.toshiba.mwcloud.gs.partitioned;

import java.util.Properties;
import java.util.Set;

import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.common.Extensibles;
import com.toshiba.mwcloud.gs.common.Extensibles.TransportProvider;

public class PartFactory extends GridStoreFactory {

	private static final String REQUIRED_EXT_VERSION = "8";

	private final Extensibles.AsStoreFactory base;

	public PartFactory(
			Set<Class<?>> chainProviderClasses,
			Set<Class<?>> visitedProviderClasses,
			Extensibles.AsFactoryProvidable baseProvider) {
		base = baseProvider.getExtensibleFactory(
				chainProviderClasses, visitedProviderClasses);
	}

	@Override
	public GridStore getGridStore(Properties properties)
			throws GSException {
		final Properties extProperties = new Properties();
		extProperties.setProperty(
				Extensibles.AsStoreFactory.VERSION_KEY,
				REQUIRED_EXT_VERSION);
		return getExtensibleStore(properties, extProperties);
	}

	@Override
	public void setProperties(Properties properties) throws GSException {
		base.getBaseFactory().setProperties(properties);
	}

	@Override
	public void close() throws GSException {
		base.getBaseFactory().close();
	}

	private Extensibles.AsStore getExtensibleStore(
			Properties properties, Properties extProperties)
			throws GSException {
		final Properties advProperties = new Properties();
		final Properties storeProperties = new Properties();
		PartStore.ConfigKey.filter(
				properties, advProperties, storeProperties);

		return new PartStore(
				base.getExtensibleStore(storeProperties, extProperties),
				advProperties);
	}

	static class ExtensibleFactory extends Extensibles.AsStoreFactory {

		private final PartFactory base;

		public ExtensibleFactory(PartFactory base) {
			this.base = base;
		}

		@Override
		public Extensibles.AsStore getExtensibleStore(
				Properties properties, Properties extProperties)
				throws GSException {
			return base.getExtensibleStore(properties, extProperties);
		}

		@Override
		public GridStoreFactory getBaseFactory() {
			return base;
		}

		@Override
		public void setTransportProvider(TransportProvider provider) {
			base.base.setTransportProvider(provider);
		}

	}

}
