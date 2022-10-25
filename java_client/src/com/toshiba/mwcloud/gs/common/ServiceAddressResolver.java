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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;

import com.toshiba.mwcloud.gs.GSException;

public class ServiceAddressResolver {

	private final Config config;

	private List<String> typeList = new ArrayList<String>();

	private Map<String, Integer> typeMap =
			new HashMap<String, Integer>();

	private Set<InetSocketAddress> addressSet =
			new HashSet<InetSocketAddress>();

	private List<Entry> entryList = new ArrayList<Entry>();

	private boolean initialized;

	private boolean firstUpdated;

	private boolean changed;

	private boolean normalized;

	public ServiceAddressResolver(Config config) {
		this.config = new Config(config);
	}

	public Config getConfig() {
		return new Config(config);
	}

	public void initializeType(ServiceAddressResolver resolver) {
		if (initialized) {
			throw new IllegalStateException();
		}

		final List<String> typeList = new ArrayList<String>(resolver.typeList);
		final Map<String, Integer> typeMap =
				new HashMap<String, Integer>(resolver.typeMap);

		this.typeList = typeList;
		this.typeMap = typeMap;
	}

	public void initializeType(int type, String name) {
		if (initialized) {
			throw new IllegalStateException();
		}

		if (name.isEmpty() || typeMap.containsKey(name)) {
			throw new IllegalArgumentException();
		}

		if (type < typeList.size() && typeList.get(type) != null) {
			throw new IllegalArgumentException();
		}

		while (type >= typeList.size()) {
			typeList.add(null);
		}

		typeMap.put(name, type);
		typeList.set(type, name);
	}

	public int getTypeCount() {
		return typeMap.size();
	}

	public int getType(String name) {
		final Integer type = typeMap.get(name);
		if (type == null) {
			throw new IllegalArgumentException();
		}

		return type;
	}

	public void update() throws GSException {
		completeInit();

		if (config.providerURL == null) {
			return;
		}

		HttpURLConnection connection = null;
		try {
			final URLConnection urlConnection =
					config.providerURL.openConnection();
			if (!(urlConnection instanceof HttpURLConnection)) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PROPERTY_ENTRY,
						"Provider URL is only supported for HTTP(S) (protocol=" +
								config.providerURL.getProtocol() + ")");
			}

			if (urlConnection instanceof HttpsURLConnection) {
				final SSLSocketFactory factory =
						config.getSecureSocketFactory();
				if (factory == null) {
					throw new GSException(
							GSErrorCode.ILLEGAL_PROPERTY_ENTRY,
							"HTTPS Provider not available because of " +
							"lack of extra library (url=" +
							config.providerURL + ")");
				}
				((HttpsURLConnection) urlConnection).setSSLSocketFactory(
						factory);
				((HttpsURLConnection) urlConnection).setHostnameVerifier(
						new HostnameVerifier() {
					@Override
					public boolean verify(String hostname, SSLSession session) {
						return true;
					}
				});
			}

			connection = (HttpURLConnection) urlConnection;
			connection.setConnectTimeout(config.timeoutMillis);
			connection.setReadTimeout(config.timeoutMillis);
			connection.setRequestMethod("GET");
			connection.setUseCaches(false);
			connection.connect();

			final int code = connection.getResponseCode();
			if (code != 200) {
				throw new GSException(
						GSErrorCode.HTTP_UNEXPECTED_MESSAGE,
						"Unexpected response status code (code=" + code + ")");
			}

			final String expectedType = "application/json";
			final String type = connection.getContentType();
			if (type == null) {
				throw new GSException(
						GSErrorCode.HTTP_UNEXPECTED_MESSAGE,
						"Content type is not specified (expected=" +
						expectedType + ")");
			}
			else if (!type.toLowerCase(Locale.US).equals(expectedType)) {
				throw new GSException(
						GSErrorCode.HTTP_UNEXPECTED_MESSAGE,
						"Content type does not match (expected=" +
						expectedType + ", actual=" + type + ")");
			}

			final int length = connection.getContentLength();
			final InputStream in = connection.getInputStream();
			final ByteArrayOutputStream out = new ByteArrayOutputStream(8192);
			final byte[] buf = new byte[8192];
			for (int rest = length; rest != 0;) {
				final int result = in.read(
						buf, 0,
						(rest < 0 ? buf.length : Math.min(rest, buf.length)));
				if (result < 0) {
					break;
				}
				out.write(buf, 0, result);
				if (rest > 0) {
					rest -= result;
				}
			}

			assign(JsonUtils.parse(
					new String(out.toByteArray(), Charset.forName("UTF-8"))));
			firstUpdated = true;
		}
		catch (GSException e) {
			throw new GSException(
					"Failed to access the address provider (" +
					"url=" + config.providerURL +
					", reason=" + e.getMessage() + ")", e);
		}
		catch (IOException e) {
			throw new GSException(
					GSErrorCode.BAD_CONNECTION,
					"Network problem occurred " +
					"while accessing the address provider (" +
					"url=" + config.providerURL +
					", reason=" + e.getMessage() + ")", e);
		}
		finally {
			if (connection != null) {
				connection.disconnect();
			}
		}
	}

	public boolean isChanged() {
		return changed;
	}

	public boolean isAvailable() {
		if (config.providerURL == null) {
			return true;
		}

		return firstUpdated;
	}

	public boolean isSameEntries(ServiceAddressResolver another) {
		return isSameEntries(
				entryList, normalized, another.entryList, another.normalized);
	}

	public int getEntryCount() {
		return entryList.size();
	}

	public InetSocketAddress getAddress(int index, int type) {
		checkType(type);
		checkEntry(index);

		final InetSocketAddress address = entryList.get(index).list.get(type);
		if (address == null) {
			throw new IllegalStateException();
		}

		return address;
	}

	public void setAddress(
			int index, int type, InetSocketAddress addr) throws GSException {
		completeInit();
		checkType(type);

		InetSocketAddress storedAddr = null;
		if (index < entryList.size()) {
			storedAddr = entryList.get(index).list.get(type);
		}

		if (addr != null) {
			if (addressSet.contains(addr)) {
				if (addr.equals(storedAddr)) {
					return;
				}
				throw new GSException(
						GSErrorCode.SA_ADDRESS_CONFLICTED,
						"Address conflicted (index=" + index +
						", type=" + getTypeName(type) +
						", address=" + addr + ")");
			}

			final boolean ipv6 = (addr.getAddress() instanceof Inet6Address);
			if (ipv6 ^ config.ipv6Expected) {
				throw new GSException(
						GSErrorCode.SA_INVALID_ADDRESS,
						"Address family unmatched (index=" + index +
						", type=" + getTypeName(type) +
						", address=" + addr +
						", expectedFamily=" +
						(config.ipv6Expected ? "IPv6" : "IPv4") + ")");
			}
		}

		final int typeCount = getTypeCount();
		while (index >= entryList.size()) {
			entryList.add(new Entry(typeCount));
		}

		if (addr != null) {
			addressSet.add(addr);
		}

		addressSet.remove(storedAddr);

		entryList.get(index).list.set(type, addr);
		normalized = false;
	}

	public void validate() throws GSException {
		if (!isAvailable() || entryList.isEmpty()) {
			throw new GSException(
					GSErrorCode.SA_ADDRESS_NOT_ASSIGNED,
					"No available address found");
		}

		if (!(typeList.size() * entryList.size() == addressSet.size())) {
			throw new GSException(
					GSErrorCode.SA_ADDRESS_NOT_ASSIGNED,
					"One or more addresses are not assigned");
		}
	}

	public void normalize() {
		completeInit();

		if (!normalized) {
			normalizeEntries(entryList);
			normalized = true;
		}
	}

	private InetSocketAddress makeSocketAddress(
			String host, long port) throws GSException {
		InetAddress selectedAddr = null;
		try {
			final InetAddress[] candidates = InetAddress.getAllByName(host);
			for (InetAddress addr : candidates) {
				if (!config.ipv6Expected && addr instanceof Inet4Address) {
					selectedAddr = addr;
				}
				else if (config.ipv6Expected && addr instanceof Inet6Address) {
					selectedAddr = addr;
				}
			}
			if (selectedAddr == null) {
				throw new GSException(
						GSErrorCode.SA_INVALID_ADDRESS,
						"Non suitable address can not be resolved (" +
						"host=" + host +
						", ipv6Expected=" + config.ipv6Expected +
						", candidates=" + Arrays.toString(candidates) + ")");
			}
		}
		catch (UnknownHostException e) {
			throw new GSException(
					GSErrorCode.SA_INVALID_ADDRESS, e);
		}

		if (port < 0 || port >= 1 << Short.SIZE) {
			throw new GSException(
					GSErrorCode.SA_INVALID_ADDRESS,
					"Port out of range (" +
					"host=" + host + ", port=" + port + ")");
		}

		return new InetSocketAddress(selectedAddr, (int) port);
	}

	private void completeInit() {
		if (initialized) {
			return;
		}

		if (typeList.isEmpty() || typeList.size() != typeMap.size()) {
			throw new IllegalStateException();
		}

		initialized = true;
	}

	private void checkEntry(int index) {
		if (index >= entryList.size()) {
			throw new IllegalArgumentException();
		}
	}

	private void checkType(int type) {
		if (type >= typeList.size() || typeList.get(type).isEmpty()) {
			throw new IllegalArgumentException();
		}
	}

	private String getTypeName(int type) throws GSException {
		checkType(type);
		return typeList.get(type);
	}

	private void assign(Object value) throws GSException {
		final Config config = new Config(this.config);
		config.providerURL = null;

		final ServiceAddressResolver another = new ServiceAddressResolver(config);
		another.initializeType(this);

		final List<Object> list = JsonUtils.asArray(value);
		for (int index = 0; index < list.size(); index++) {
			for (int type = 0; type < typeList.size(); type++) {
				final String typeName = typeList.get(type);

				final Object addrObj =
						JsonUtils.asObject(list.get(index), typeName);

				final String host =
						JsonUtils.as(String.class, addrObj, "address");
				final long port = JsonUtils.asLong(addrObj, "port");

				final InetSocketAddress addr = makeSocketAddress(host, port);
				another.setAddress(index, type, addr);
			}
		}
		another.normalize();
		another.validate();

		changed = !isSameEntries(another);

		addressSet = another.addressSet;
		entryList = another.entryList;
		normalized = another.normalized;
	}

	private static boolean isSameEntries(
			List<Entry> list1, boolean normalized1,
			List<Entry> list2, boolean normalized2) {
		final int size = list1.size();
		if (size != list2.size()) {
			return false;
		}

		if (!normalized1) {
			final List<Entry> normalizedList = new ArrayList<Entry>(list1);
			normalizeEntries(normalizedList);
			return isSameEntries(normalizedList, true, list2, normalized2);
		}

		if (!normalized2) {
			final List<Entry> normalizedList = new ArrayList<Entry>(list2);
			normalizeEntries(normalizedList);
			return isSameEntries(list1, normalized1, normalizedList, true);
		}

		for (int i = 0; i < size; i++) {
			if (list1.get(i).compareTo(list2.get(i)) != 0) {
				return false;
			}
		}

		return true;
	}

	private static void normalizeEntries(List<Entry> list) {
		Collections.sort(list);
	}

	public static class Config {

		private static final int DEFAULT_TIMEOUT_MILLIS = 60 * 1000;

		private URL providerURL;

		private boolean ipv6Expected;

		private int timeoutMillis = DEFAULT_TIMEOUT_MILLIS;

		private transient SSLSocketFactory secureSocketFactory;

		public Config() {
		}

		public Config(Config config) {
			this.providerURL = config.providerURL;
			this.ipv6Expected = config.ipv6Expected;
			this.timeoutMillis = config.timeoutMillis;
			this.secureSocketFactory = config.secureSocketFactory;
		}

		public URL getProviderURL() {
			return providerURL;
		}

		public void setProviderURL(String providerURL) throws GSException {
			if (providerURL == null) {
				this.providerURL = null;
				return;
			}

			try {
				this.providerURL = new URL(providerURL);
			}
			catch (MalformedURLException e) {
				if (providerURL.isEmpty()) {
					throw new GSException(
							GSErrorCode.EMPTY_PARAMETER, "Empty provider URL");
				}
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"Failed to parse provider URL (url=" + providerURL +
						", reason=" + e.getMessage() + ")", e);
			}

			final String protocol = this.providerURL.getProtocol();
			if (!protocol.toLowerCase(Locale.US).equals("http") &&
					!protocol.toLowerCase(Locale.US).equals("https")) {
				throw new GSException(
						GSErrorCode.SA_INVALID_CONFIG,
						"Only HTTP(S) is supported for provider URL " +
						"(url=" + providerURL +")");
			}

			if (this.providerURL.getHost().isEmpty()) {
				throw new GSException(
						GSErrorCode.SA_INVALID_CONFIG,
						"No host specified in provider URL " +
						"(url=" + providerURL +")");
			}
		}

		public boolean isIPv6Expected() {
			return ipv6Expected;
		}

		public void setIPv6Expected(boolean ipv6Expected) {
			this.ipv6Expected = ipv6Expected;
		}

		public int getTimeoutMillis() {
			return timeoutMillis;
		}

		public void setTimeoutMillis(int timeoutMillis) {
			if (timeoutMillis <= 0) {
				this.timeoutMillis = DEFAULT_TIMEOUT_MILLIS;
				return;
			}
			this.timeoutMillis = timeoutMillis;
		}

		public SSLSocketFactory getSecureSocketFactory() {
			return secureSocketFactory;
		}

		public void setSecureSocketFactory(SSLSocketFactory factory) {
			this.secureSocketFactory = factory;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (ipv6Expected ? 1231 : 1237);
			result = prime * result +
					((providerURL == null) ? 0 : providerURL.hashCode());
			result = prime * result + timeoutMillis;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Config other = (Config) obj;
			if (ipv6Expected != other.ipv6Expected)
				return false;
			if (providerURL == null) {
				if (other.providerURL != null)
					return false;
			}
			else if (!providerURL.equals(other.providerURL))
				return false;
			if (timeoutMillis != other.timeoutMillis)
				return false;
			return true;
		}

	}

	private static class Entry implements Comparable<Entry> {

		private final List<InetSocketAddress> list;

		private static final Comparator<InetSocketAddress> COMPARATOR =
				new SocketAddressComparator();

		public Entry(int typeCount) {
			list = new ArrayList<InetSocketAddress>(typeCount);
			for (int i = 0; i < typeCount; i++) {
				list.add(null);
			}
		}

		@Override
		public int compareTo(Entry o) {
			final int size = list.size();
			{
				final int comp = size - o.list.size();
				if (comp != 0) {
					return comp;
				}
			}

			for (int i = 0; i < size; i++) {
				final int comp =
						COMPARATOR.compare(list.get(i), o.list.get(i));
				if (comp != 0) {
					return comp;
				}
			}

			return 0;
		}

	}

	public static class SocketAddressComparator
	implements Comparator<InetSocketAddress> {

		@Override
		public int compare(InetSocketAddress o1, InetSocketAddress o2) {
			{
				final int comp = ((Integer) getFamily(
						o1.getAddress())).compareTo(getFamily(o2.getAddress()));
				if (comp != 0) {
					return comp;
				}
			}

			{
				final int comp = compareBytes(
						o1.getAddress().getAddress(),
						o2.getAddress().getAddress());
				if (comp != 0) {
					return comp;
				}
			}

			{
				final int comp = o1.getPort() - o2.getPort();
				if (comp != 0) {
					return comp;
				}
			}

			return 0;
		}

		private static int getFamily(InetAddress addr) {
			if (addr instanceof Inet4Address) {
				return 1;
			}
			else if (addr instanceof Inet6Address) {
				return 2;
			}
			else {
				return 3;
			}
		}

		private static int compareBytes(byte[] b1, byte[] b2) {
			for (int i = 0;; i++) {
				if (i >= b1.length || i >= b2.length) {
					return (b1.length - b2.length);
				}
				final int comp = (b1[i] & 0xff) - (b2[i] & 0xff);
				if (comp != 0) {
					return comp;
				}
			}
		}

	}

	public static InetAddress resolveAddress(
			String host, Boolean ipv6Expected, String key) throws GSException {
		if (host.isEmpty()) {
			throw new GSException(
					GSErrorCode.EMPTY_PARAMETER,
					"Empty host name or address specified" +
							(key == null ? "" : " (propertyName=" + key + ")"));
		}

		try {
			boolean found = false;
			for (InetAddress address : InetAddress.getAllByName(host)) {
				if (ipv6Expected == null ||
						!(address instanceof Inet6Address ^ ipv6Expected)) {
					return address;
				}
				found = true;
			}

			if (found) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"Address family unmatched (value=" + host +
						", expectedFamily=" +
						(ipv6Expected ? "IPv6" : "IPv4") +
						(key == null ? "" : ", propertyName=" + key) + ")");
			}
		}
		catch (UnknownHostException e) {
			throw errorOnUnknownHost(e, host, key);
		}

		throw errorOnUnknownHost(null, host, key);
	}

	private static GSException errorOnUnknownHost(
			UnknownHostException e, String host, String key) {
		return new GSException(
				GSErrorCode.ILLEGAL_PARAMETER,
				"Address not resolved (value=" + host +
						(key == null ? "" : ", propertyName=" + key) + ")", e);
	}

}
