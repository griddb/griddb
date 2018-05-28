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
package com.toshiba.mwcloud.gs.subnet;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.common.Extensibles;
import com.toshiba.mwcloud.gs.common.GSErrorCode;
import com.toshiba.mwcloud.gs.common.GridStoreFactoryProvider;
import com.toshiba.mwcloud.gs.common.GridStoreFactoryProvider.ChainProvidable;
import com.toshiba.mwcloud.gs.common.LoggingUtils;
import com.toshiba.mwcloud.gs.common.LoggingUtils.BaseGridStoreLogger;
import com.toshiba.mwcloud.gs.common.PropertyUtils.WrappedProperties;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.Key;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.Source;

public class SubnetGridStoreFactory extends GridStoreFactory {

	private Map<Key, GridStoreChannel> channelMap =
			new HashMap<Key, GridStoreChannel>();

	private final GridStoreChannel.Config channelConfig =
			new GridStoreChannel.Config();

	{
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					close();
				}
				catch (GSException e) {
				}
			}
		}));
	}

	public SubnetGridStoreFactory() {
		this(true);
	}

	public SubnetGridStoreFactory(Set<Class<?>> chainProviderClasses) {
		this(!isConfigurableProviderChained(chainProviderClasses));
	}

	private SubnetGridStoreFactory(boolean configured) {
		ConfigUtils.checkNewFactory(configured);
	}

	private static boolean isConfigurableProviderChained(
			Set<Class<?>> chainProviderClasses) {
		for (Class<?> c : chainProviderClasses) {
			if (ConfigProvidable.class.isAssignableFrom(c)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public synchronized SubnetGridStore getGridStore(
			Properties properties) throws GSException {
		if (channelMap == null) {
			throw new GSException(
					GSErrorCode.RESOURCE_CLOSED, "Already closed");
		}

		final WrappedProperties wrapped = new WrappedProperties(properties);
		final Source source;
		try {
			source = new Source(wrapped);
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(properties, "properties", e);
		}
		ConfigUtils.checkProperties(wrapped, "store");

		GridStoreChannel channel = channelMap.get(source.getKey());
		if (channel == null) {
			channel = new GridStoreChannel(channelConfig, source);
			channelMap.put(source.getKey(), channel);
		}
		else {
			channel.apply(channelConfig);
		}

		final SubnetGridStore store = new SubnetGridStore(
				channel, source.createContext());
		return store;
	}

	@Override
	public synchronized void setProperties(
			Properties properties) throws GSException {
		if (channelMap == null) {
			throw new GSException(
					GSErrorCode.RESOURCE_CLOSED, "Already closed");
		}

		if (properties == null) {
			throw GSErrorCode.checkNullParameter(
					properties, "properties", null);
		}

		final WrappedProperties wrapped = new WrappedProperties(properties);

		final String messageOptionStr =
				wrapped.getProperty("detailErrorMessageEnabled", true);
		if (messageOptionStr != null) {
			NodeConnection.setDetailErrorMessageEnabled(
					Boolean.parseBoolean(messageOptionStr));
		}

		final String transactionProtocolVersionStr =
				wrapped.getProperty("transactionProtocolVersion", true);
		if (transactionProtocolVersionStr != null) {
			NodeConnection.setProtocolVersion(
					Integer.parseInt(transactionProtocolVersionStr));
		}

		final String pathKeyOptionStr =
				wrapped.getProperty("pathKeyOperationEnabled", true);
		if (pathKeyOptionStr != null) {
			SubnetGridStore.setPathKeyOperationEnabled(
					Boolean.parseBoolean(pathKeyOptionStr));
		}

		if (channelConfig.set(wrapped)) {
			for (GridStoreChannel channel : channelMap.values()) {
				channel.apply(channelConfig);
			}
		}

		ConfigUtils.checkProperties(wrapped, "factory");
	}

	@Override
	public synchronized void close() throws GSException {
		if (channelMap == null) {
			return;
		}

		try {
			closeResources(channelMap.values(), false);
		}
		finally {
			channelMap = null;
		}
	}

	private <T extends Closeable> void closeResources(
			Iterable<T> iterable, boolean silent) throws GSException {
		try {
			for (Iterator<T> it = iterable.iterator(); it.hasNext();) {
				final T resource = it.next();
				if (silent) {
					try {
						resource.close();
					}
					catch (Throwable t) {
					}
				}
				else {
					resource.close();
				}
				it.remove();
			}
		}
		catch (IOException e) {
			throw new GSException(e);
		}
		finally {
			if (!silent) {
				closeResources(iterable, true);
			}
		}
	}

	public static class ExtensibleFactory extends Extensibles.AsStoreFactory {

		
		
		
		
		private static final String ACCEPTABLE_VERSION = "3";

		private final SubnetGridStoreFactory base;

		public ExtensibleFactory(Set<Class<?>> chainProviderClasses) {
			base = new SubnetGridStoreFactory(chainProviderClasses);
		}

		@Override
		public Extensibles.AsStore getExtensibleStore(
				Properties properties,
				Properties extProperties) throws GSException {

			final String version = extProperties.getProperty(VERSION_KEY, "");
			if (!version.equals(ACCEPTABLE_VERSION)) {
				throw new GSException(
						GSErrorCode.ILLEGAL_CONFIG,
						"Library version unmatched (required=" + version +
						", acceptable=" + ACCEPTABLE_VERSION + ")");
			}

			return base.getGridStore(properties);
		}

		@Override
		public GridStoreFactory getBaseFactory() {
			return base;
		}

	}

	public interface ConfigProvidable {
	}

	public static class ConfigurableFactory extends GridStoreFactory {

		private final GridStoreFactory base;

		private final Properties storeProperties = new Properties();

		private final GSException lastException;

		public ConfigurableFactory(
				Set<Class<?>> chainProviderClasses,
				Set<Class<?>> visitedProviderClasses) {
			final ChainProvidable provider =
					GridStoreFactoryProvider.getProvider(
							ChainProvidable.class,
							ConfigurableFactory.class,
							chainProviderClasses);
			base = provider.getFactory(
					chainProviderClasses, visitedProviderClasses);
			lastException =
					ConfigUtils.loadConfig(base, storeProperties);
		}

		@Override
		public GridStore getGridStore(Properties properties)
				throws GSException {
			checkConfig();
			final Properties mergedProperties = new Properties();
			mergedProperties.putAll(storeProperties);
			mergedProperties.putAll(properties);
			return base.getGridStore(mergedProperties);
		}

		@Override
		public void setProperties(Properties properties) throws GSException {
			checkConfig();
			base.setProperties(properties);
		}

		@Override
		public void close() throws GSException {
			base.close();
		}

		private void checkConfig() throws GSException {
			if (lastException != null) {
				throw new GSException(lastException);
			}
		}

	}

	private static class ConfigUtils {

		private static final String CONFIG_FILE_NAME = "gs_client.properties";

		private static final BaseGridStoreLogger LOGGER =
				LoggingUtils.getLogger("Config");

		static void checkNewFactory(boolean configured) {
			if (configured) {
				try {
					final List<URL> urlList = Collections.list(
							ConfigUtils.class.getClassLoader().getResources(
									CONFIG_FILE_NAME));
					if (!urlList.isEmpty()) {
						LOGGER.warn("config.unexpectedConfigFile", urlList);
					}
				}
				catch (IOException e) {
					throw new Error(e);
				}
				LOGGER.debug("config.notLoaded");
			}
		}

		static void checkProperties(WrappedProperties props, String type) {
			{
				final Set<String> names = props.getUnknownNames();
				if (!names.isEmpty()) {
					LOGGER.warn("config.unknownProperty", type, names);
				}
			}

			{
				final Set<String> names = props.getDeprecatedNames();
				if (!names.isEmpty()) {
					LOGGER.warn("config.deprecatedProperty", type, names);
				}
			}
		}

		static GSException loadConfig(
				GridStoreFactory factory, Properties storeProperties) {
			final Properties factoryProperties = new Properties();
			GSException lastException = null;
			try {
				final URL url = loadConfig(factoryProperties, storeProperties);
				factory.setProperties(factoryProperties);

				LOGGER.debug("config.loaded", url);
			}
			catch (GSException e) {
				lastException = e;
			}
			catch (IOException e) {
				lastException = new GSException(e);
			}

			if (lastException != null) {
				LOGGER.debug("config.loadFailed", lastException);
			}

			return lastException;
		}

		private static URL loadConfig(
				Properties factoryProperties, Properties storeProperties)
				throws IOException {
			final List<URL> urlList = Collections.list(
					ConfigUtils.class.getClassLoader().getResources(
							CONFIG_FILE_NAME));
			if (urlList.isEmpty()) {
				throw new GSException(GSErrorCode.ILLEGAL_CONFIG,
						"Config file not found in classpath (fileName=" +
								CONFIG_FILE_NAME + ")");
			}
			else if (urlList.size() != 1) {
				throw new GSException(GSErrorCode.ILLEGAL_CONFIG,
						"Multiple config files found in classpath (urlList=" +
								urlList + ")");
			}

			final URL url = urlList.get(0);
			final Properties properties = new Properties();
			final InputStream in = url.openStream();
			try {
				properties.load(in);
			}
			finally {
				in.close();
			}

			for (String key : properties.stringPropertyNames()) {
				final Properties target;
				if (key.startsWith("store.")) {
					target = storeProperties;
				}
				else if (key.startsWith("factory.")) {
					target = factoryProperties;
				}
				else {
					throw new GSException(GSErrorCode.ILLEGAL_CONFIG,
							"Illegal property key (key=" + key + ", url=" + url);
				}

				final String subKey = key.substring(key.indexOf(".") + 1);
				if (subKey.isEmpty()) {
					throw new GSException(GSErrorCode.ILLEGAL_CONFIG,
							"Illegal property key (key=" + key + ", url=" + url);
				}

				target.setProperty(subKey, properties.getProperty(key));
			}

			return url;
		}

	}

}
