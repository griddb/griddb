/*
   Copyright (c) 2019 TOSHIBA Digital Solutions Corporation

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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.PartitionController;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.TimeSeries;
import com.toshiba.mwcloud.gs.common.Extensibles;
import com.toshiba.mwcloud.gs.common.GridStoreFactoryProvider;
import com.toshiba.mwcloud.gs.common.ServiceProviderUtils;
import com.toshiba.mwcloud.gs.experimental.Experimentals;

public abstract class GSInterceptorProvider {

	protected abstract Chain tryCreateChain(
			Chain chain, Class<?> targetType, Object target);

	public static abstract class Chain {

		private final Chain chain;

		protected Chain(Chain chain) {
			this.chain = chain;
		}

		protected Object invokeDefault(
				Class<?> targetType, Object target, Method method,
				Object[] args)
				throws IllegalAccessException, InvocationTargetException {
			return chain.invoke(targetType, target, method, args);
		}

		public abstract Object invoke(
				Class<?> targetType, Object target, Method method,
				Object[] args)
				throws IllegalAccessException, InvocationTargetException;

	}

	public static class FactoryProvider extends GridStoreFactoryProvider
	implements GridStoreFactoryProvider.ChainProvidable {

		private GridStoreFactory factory;

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
			GridStoreFactory factory = this.factory;
			if (factory == null) {
				final Set<Class<?>> providerClasses = new HashSet<Class<?>>();
				providerClasses.addAll(chainProviderClasses);
				providerClasses.add(FactoryProvider.class);

				factory = GSInterceptorFactory.getInterceptorInstance(
						providerClasses, visitedProviderClasses);
				this.factory = factory;
			}
			return factory;
		}

	}
}

class GSInterceptorFactory extends GridStoreFactory {

	private static final Class<?>[] BASIC_TYPE_LIST = {
		GridStore.class,
		Container.class,
		Collection.class,
		TimeSeries.class,
		Query.class,
		RowSet.class,
		PartitionController.class
	};

	private static final Class<?>[][] EXTRA_TYPE_LIST = {
		{ Extensibles.AsStore.class },
		{ Extensibles.AsContainer.class },
		{ Extensibles.AsQuery.class },
		{ Extensibles.AsRowSet.class },
		{ Extensibles.AsPartitionController.class },
		{ Experimentals.StoreProvider.class, GridStore.class },
		{ Experimentals.AsContainer.class },
		{ Experimentals.AsQuery.class },
		{ Experimentals.AsRowSet.class }
	};

	private final Map<Class<?>, Class<?>> basicTypeMap;

	private final Map<Class<?>, List<Class<?>>> subTypeMap;

	private final Map<Class<?>, Constructor<?>> proxyConstructorMap;

	private final GridStoreFactory base;

	private final List<GSInterceptorProvider> providerList;

	private GSInterceptorFactory(
			GridStoreFactory base, List<GSInterceptorProvider> providerList) {
		basicTypeMap = makeBasicTypeMap();
		subTypeMap = makeSubTypeMap(basicTypeMap);
		proxyConstructorMap =
				makeProxyConstructorMap(basicTypeMap, subTypeMap);

		this.base = base;
		this.providerList = providerList;
	}

	public static GridStoreFactory getInterceptorInstance(
			Set<Class<?>> chainProviderClasses,
			Set<Class<?>> visitedProviderClasses) {
		final Class<?> loaderClass = GSInterceptorFactory.class;
		final List<GSInterceptorProvider> providerList = loadProviderList(
				ServiceProviderUtils.listClassLoaders(loaderClass));

		final GridStoreFactoryProvider.ChainProvidable factoryProvidable =
				GridStoreFactoryProvider.getProvider(
						GridStoreFactoryProvider.ChainProvidable.class,
						loaderClass, chainProviderClasses);

		final GridStoreFactory base = factoryProvidable.getFactory(
				chainProviderClasses, visitedProviderClasses);
		if (providerList.isEmpty()) {
			return base;
		}
		else {
			return new GSInterceptorFactory(base, providerList);
		}
	}

	private static Map<Class<?>, Class<?>> makeBasicTypeMap() {
		final Map<Class<?>, Class<?>> map =
				new LinkedHashMap<Class<?>, Class<?>>();
		for (Class<?> type : BASIC_TYPE_LIST) {
			map.put(type, type);
		}
		for (Class<?>[] typePair : EXTRA_TYPE_LIST) {
			final Class<?> type = typePair[0];
			if (typePair.length > 1) {
				final Class<?> basicType = typePair[1];
				if (map.get(basicType) == null) {
					throw new IllegalStateException();
				}
				map.put(type, basicType);
			}
			else {
				Class<?> basicType = null;
				for (Class<?> interfaceType : type.getInterfaces()) {
					basicType = map.get(interfaceType);
					if (basicType != null) {
						break;
					}
				}
				if (basicType == null) {
					throw new IllegalStateException();
				}
				map.put(type, basicType);
			}
		}
		return map;
	}

	private static Map<Class<?>, List<Class<?>>> makeSubTypeMap(
			Map<Class<?>, Class<?>> basicTypeMap) {
		final Map<Class<?>, List<Class<?>>> map =
				new HashMap<Class<?>, List<Class<?>>>();
		for (Class<?> type : basicTypeMap.keySet()) {
			if (basicTypeMap.get(type) != type) {
				continue;
			}
			for (Class<?> interfaceType : type.getInterfaces()) {
				final Class<?> basicType = basicTypeMap.get(interfaceType);
				if (basicType == null || basicType != interfaceType) {
					continue;
				}
				List<Class<?>> list = map.get(basicType);
				if (list == null) {
					list = new ArrayList<Class<?>>();
					map.put(basicType, list);
				}
				list.add(type);
			}
		}
		return map;
	}

	private static Map<Class<?>, Constructor<?>> makeProxyConstructorMap(
			Map<Class<?>, Class<?>> basicTypeMap,
			Map<Class<?>, List<Class<?>>> subTypeMap) {
		final Map<Class<?>, List<Class<?>>> baseMap =
				new HashMap<Class<?>, List<Class<?>>>();

		for (Map.Entry<Class<?>, Class<?>> entry : basicTypeMap.entrySet()) {
			final Class<?> basicType = entry.getValue();
			List<Class<?>> list = baseMap.get(basicType);
			if (list == null) {
				list = new ArrayList<Class<?>>();
				baseMap.put(basicType, list);
			}
			list.add(entry.getKey());
		}

		for (Map.Entry<Class<?>, List<Class<?>>> entry :
				subTypeMap.entrySet()) {
			for (Class<?> subType : entry.getValue()) {
				baseMap.get(subType).addAll(baseMap.get(entry.getKey()));
			}
		}

		final Map<Class<?>, Constructor<?>> map =
				new HashMap<Class<?>, Constructor<?>>();
		for (Map.Entry<Class<?>, List<Class<?>>> entry : baseMap.entrySet()) {
			final Class<?> type = entry.getKey();
			final List<Class<?>> list = entry.getValue();
			final Class<?> proxyType = Proxy.getProxyClass(
					type.getClassLoader(),
					list.toArray(new Class<?>[list.size()]));

			final Constructor<?> constructor;
			try {
				constructor = proxyType.getConstructor(
						InvocationHandler.class);
			}
			catch (NoSuchMethodException e) {
				throw new IllegalStateException(e);
			}
			map.put(type, constructor);
		}

		return map;
	}

	private static List<GSInterceptorProvider> loadProviderList(
			List<ClassLoader> classLoaderList) {
		final List<GSInterceptorProvider> baseList = ServiceProviderUtils.load(
				GSInterceptorProvider.class,
				GSInterceptorProvider.class,
				classLoaderList,
				Collections.<Class<?>>emptySet());
		final Map<Class<?>, GSInterceptorProvider> providerMap =
				new HashMap<Class<?>, GSInterceptorProvider>();
		for (GSInterceptorProvider provider : baseList) {
			providerMap.put(provider.getClass(), provider);
		}
		return new ArrayList<GSInterceptorProvider>(providerMap.values());
	}

	@Override
	public GridStore getGridStore(Properties properties) throws GSException {
		return (GridStore) createInterceptiveInstance(
				GridStore.class, base.getGridStore(properties));
	}

	@Override
	public void setProperties(Properties properties) throws GSException {
		base.setProperties(properties);
	}

	@Override
	public void close() throws GSException {
		base.close();
	}

	private Object createInterceptiveInstance(
			Class<?> targetType, Object target) {
		Class<?> basicType = targetType;

		final List<Class<?>> subList = subTypeMap.get(basicType);
		if (subList != null) {
			for (Class<?> subType : subList) {
				if (subType.isInstance(target)) {
					basicType = subType;
					break;
				}
			}
		}

		final GSInterceptorProvider.Chain emptyChain =
				EmptyChain.getInstance();

		GSInterceptorProvider.Chain totalChain = emptyChain;
		for (GSInterceptorProvider provider : providerList) {
			final GSInterceptorProvider.Chain chain =
					provider.tryCreateChain(totalChain, targetType, target);
			if (chain != null) {
				totalChain = chain;
			}
		}

		if (totalChain == emptyChain) {
			return target;
		}

		final Constructor<?> constructor = proxyConstructorMap.get(basicType);
		if (constructor == null) {
			throw new IllegalStateException();
		}
		try {
			return constructor.newInstance(
					new Handler(totalChain, basicType, target));
		}
		catch (InstantiationException e) {
			throw new IllegalStateException(e);
		}
		catch (IllegalAccessException e) {
			throw new IllegalStateException(e);
		}
		catch (InvocationTargetException e) {
			throw new IllegalStateException(e);
		}
	}

	private class Handler implements InvocationHandler {

		private final GSInterceptorProvider.Chain chain;

		private final Class<?> targetType;

		private final Object target;

		private Handler(
				GSInterceptorProvider.Chain chain, Class<?> targetType,
				Object target) {
			this.chain = chain;
			this.targetType = targetType;
			this.target = target;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			final Class<?> methodType = method.getDeclaringClass();
			final boolean tageted =
					(basicTypeMap.get(methodType) == methodType);

			final Object returnObject;
			try {
				returnObject = tageted ?
						chain.invoke(targetType, target, method, args) :
						method.invoke(target, args);
			}
			catch (InvocationTargetException e) {
				Throwable cause = e.getCause();
				if (cause == null) {
					throw e;
				}
				throw cause;
			}

			if (returnObject != null && tageted) {
				final Class<?> returnType = method.getReturnType();
				final Class<?> targetType = basicTypeMap.get(returnType);
				if (targetType != null) {
					return createInterceptiveInstance(
							targetType, returnObject);
				}
			}

			return returnObject;
		}

	}

	private static class EmptyChain extends GSInterceptorProvider.Chain {

		private static final EmptyChain INSTANCE = new EmptyChain();

		private EmptyChain() {
			super(null);
		}

		static EmptyChain getInstance() {
			return INSTANCE;
		}

		@Override
		public Object invoke(
				Class<?> targetType, Object target, Method method,
				Object[] args)
				throws IllegalAccessException, InvocationTargetException {
			return method.invoke(target, args);
		}

	}

}
