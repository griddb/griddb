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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.common.Extensibles;
import com.toshiba.mwcloud.gs.common.GridStoreFactoryProvider;
import com.toshiba.mwcloud.gs.common.ServiceProviderUtils;
import com.toshiba.mwcloud.gs.subnet.SubnetGridStoreFactoryProvider;

public class PartFactoryProvider extends GridStoreFactoryProvider
implements GridStoreFactoryProvider.ChainProvidable,
Extensibles.AsFactoryProvidable {

	@Override
	public GridStoreFactory getFactory() {
		return getFactory(
				Collections.<Class<?>>emptySet(),
				Collections.<Class<?>>emptySet());
	}

	@Override
	public GridStoreFactory getFactory(
			Set<Class<?>> chainProviderClasses,
			Set<Class<?>> visitedProviderClasses) {
		final Class<?> thisClass = PartFactoryProvider.class;

		final Set<Class<?>> exclusiveClasses = new HashSet<Class<?>>();
		exclusiveClasses.add(thisClass);
		exclusiveClasses.addAll(chainProviderClasses);
		exclusiveClasses.addAll(visitedProviderClasses);

		final List<ChainProvidable> providerList = ServiceProviderUtils.load(
				GridStoreFactoryProvider.class,
				ChainProvidable.class,
				ServiceProviderUtils.listClassLoaders(thisClass),
				exclusiveClasses);

		if (!providerList.isEmpty()) {
			final Set<Class<?>> nextVisitedClasses = new HashSet<Class<?>>();
			nextVisitedClasses.add(thisClass);
			nextVisitedClasses.addAll(visitedProviderClasses);

			return providerList.get(0).getFactory(
					chainProviderClasses, nextVisitedClasses);
		}

		return new PartFactory(
				ServiceProviderUtils.mergeChainClasses(
						chainProviderClasses, PartFactoryProvider.class),
				visitedProviderClasses,
				getBaseProvider());
	}

	@Override
	public Extensibles.AsStoreFactory getExtensibleFactory(
			Set<Class<?>> chainProviderClasses,
			Set<Class<?>> visitedProviderClasses) {
		final PartFactory base = new PartFactory(
				ServiceProviderUtils.mergeChainClasses(
						chainProviderClasses, PartFactoryProvider.class),
				visitedProviderClasses,
				getBaseProvider());
		return new PartFactory.ExtensibleFactory(base);
	}

	private static Extensibles.AsFactoryProvidable getBaseProvider() {
		return new SubnetGridStoreFactoryProvider();
	}

}
