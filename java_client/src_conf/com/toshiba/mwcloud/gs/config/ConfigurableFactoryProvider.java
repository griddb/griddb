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
package com.toshiba.mwcloud.gs.config;

import java.util.Collections;
import java.util.Set;

import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.common.GridStoreFactoryProvider;
import com.toshiba.mwcloud.gs.common.Extensibles;
import com.toshiba.mwcloud.gs.common.ServiceProviderUtils;

public class ConfigurableFactoryProvider extends GridStoreFactoryProvider
implements GridStoreFactoryProvider.ChainProvidable,
Extensibles.ConfigProvidable {

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
		return Extensibles.AsStoreFactory.newConfigurableInstance(
				ServiceProviderUtils.mergeChainClasses(
						chainProviderClasses, ConfigurableFactoryProvider.class),
				visitedProviderClasses);
	}

}
