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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;

public class ServiceProviderUtils {

	public static <S> List<S> load(
			Class<S> providerClass,
			List<ClassLoader> classLoaderList,
			Set<Class<?>> exclusiveClasses) {
		return load(
				providerClass, providerClass,
				classLoaderList, exclusiveClasses);
	}

	public static <S> List<S> load(
			Class<?> providerClass, Class<S> providerClassSpec,
			List<ClassLoader> classLoaderList,
			Set<Class<?>> exclusiveClasses) {

		final List<S> providerList = new ArrayList<S>();

		for (ClassLoader cl : classLoaderList) {
			for (Object provider : ServiceLoader.load(providerClass, cl)) {
				if (providerClassSpec.isInstance(provider) &&
						!exclusiveClasses.contains(provider.getClass())) {
					providerList.add(providerClassSpec.cast(provider));
				}
			}
		}

		return providerList;
	}

	public static List<ClassLoader> listClassLoaders(Class<?> baseClass) {
		return Arrays.asList(
				baseClass.getClassLoader(),
				Thread.currentThread().getContextClassLoader(),
				ClassLoader.getSystemClassLoader());
	}

	public static Set<Class<?>> mergeChainClasses(
			Set<Class<?>> lastChainClasses, Class<?> chainClass) {
		Set<Class<?>> nextChainClasses = new HashSet<Class<?>>();
		nextChainClasses.addAll(lastChainClasses);
		nextChainClasses.add(chainClass);
		return nextChainClasses;
	}

}
