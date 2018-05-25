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
import java.util.Set;

import com.toshiba.mwcloud.gs.GridStoreFactory;

public abstract class GridStoreFactoryProvider {

	private static final String BUILTIN_CLASS_NAME =
			"com.toshiba.mwcloud.gs.subnet.SubnetGridStoreFactoryProvider";

	public abstract GridStoreFactory getFactory();

	public interface ChainProvidable {

		public GridStoreFactory getFactory(
				Set<Class<?>> chainProviderClasses,
				Set<Class<?>> visitedProviderClasses);

	}

	public static <P> P getProvider(
			Class<P> providerClassSpec, Class<?> loaderBaseClass,
			Set<Class<?>> exclusiveClasses) {
		final List<P> providerList = ServiceProviderUtils.load(
				GridStoreFactoryProvider.class,
				providerClassSpec,
				ServiceProviderUtils.listClassLoaders(loaderBaseClass),
				exclusiveClasses);

		final P provider;
		if (providerList.isEmpty()) {
			final GridStoreFactoryProvider builtinProvider =
					GridStoreFactoryProvider.getBuiltinProvider();
			try {
				provider = providerClassSpec.cast(builtinProvider);
			}
			catch (ClassCastException e) {
				throw new Error(e);
			}
		}
		else {
			provider = providerList.get(0);
		}

		return provider;
	}

	public static GridStoreFactoryProvider getBuiltinProvider() {
		try {
			return (GridStoreFactoryProvider)
					Class.forName(BUILTIN_CLASS_NAME).newInstance();
		}
		catch (IllegalAccessException e) {
			throw new Error(e);
		}
		catch (ClassNotFoundException e) {
			throw new Error(e);
		}
		catch (InstantiationException e) {
			throw new Error(e);
		}
	}

}
