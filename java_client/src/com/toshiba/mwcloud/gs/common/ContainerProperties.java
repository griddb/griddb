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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.common.ContainerKeyConverter.ContainerKey;

public class ContainerProperties {

	public static class ContainerIdInfo {

		public final int versionId;

		public final long containerId;

		public final ContainerKey remoteKey;

		public final MetaDistributionType metaDistType;

		public final long metaContainerId;

		public final MetaNamingType metaNamingType;

		public ContainerIdInfo(
				int versionId, long containerId, ContainerKey remoteKey,
				MetaDistributionType metaDistType, long metaContainerId,
				MetaNamingType metaNamingType) {
			this.versionId = versionId;
			this.containerId = containerId;
			this.remoteKey = remoteKey;
			this.metaDistType = metaDistType;
			this.metaContainerId = metaContainerId;
			this.metaNamingType = metaNamingType;
		}

	}

	private static final Key[] BUILTIN_KEYS = Key.values();

	private ContainerInfo info;

	private ContainerIdInfo idInfo;

	private Integer attribute;

	private Long reservedValue;

	private Map<Integer, byte[]> rawEntries;

	private Map<Integer, byte[]> schemaOptions;

	public ContainerProperties() {
	}

	public ContainerProperties(ContainerInfo info) {
		this.info = info;
	}

	public ContainerInfo getInfo() {
		return info;
	}

	public void setInfo(ContainerInfo info) {
		this.info = info;
	}

	public ContainerIdInfo getIdInfo() {
		return idInfo;
	}

	public void setIdInfo(ContainerIdInfo idInfo) {
		this.idInfo = idInfo;
	}

	public Integer getAttribute() {
		return attribute;
	}

	public void setAttribute(Integer attribute) {
		this.attribute = attribute;
	}

	public Long getReservedValue() {
		return reservedValue;
	}

	public void setReservedValue(Long reservedValue) {
		this.reservedValue = reservedValue;
	}

	public Map<Integer, byte[]> getRawEntries() {
		return rawEntries;
	}

	public void setRawEntries(Map<Integer, byte[]> rawEntries) {
		this.rawEntries = rawEntries;
	}

	public Map<Integer, byte[]> getSchemaOptions() {
		return schemaOptions;
	}

	public void setSchemaOptions(Map<Integer, byte[]> schemaOptions) {
		this.schemaOptions = schemaOptions;
	}

	public static Key tryResolveKey(int rawkey) {
		if (rawkey < BUILTIN_KEYS.length) {
			return BUILTIN_KEYS[rawkey];
		}

		return null;
	}

	public static ContainerInfo findInfo(ContainerProperties props) {
		if (props == null) {
			return null;
		}

		return props.getInfo();
	}

	public enum Key {

		ID,

		SCHEMA,

		INDEX,

		EVENT_NOTIFICATION,

		TRIGGER,

		ATTRIBUTE,

		INDEX_DETAIL

	}

	public static class KeySet {

		private final Integer[] rawKeys;

		public KeySet(Set<Key> keys, Integer ...rawKeys) {
			final SortedSet<Integer> allKeys = new TreeSet<Integer>();

			for (Key key : keys) {
				allKeys.add(key.ordinal());
			}

			if (rawKeys != null) {
				final List<Integer> rawKeyList = Arrays.asList(rawKeys);
				allKeys.removeAll(rawKeyList);
				allKeys.addAll(rawKeyList);
			}

			this.rawKeys = allKeys.toArray(new Integer[allKeys.size()]);
		}

		public Integer[] getRawKeys() {
			return rawKeys;
		}

	}

	public enum MetaDistributionType {
		NONE,
		FULL,
		NODE
	}

	public enum MetaNamingType {
		NEUTRAL,
		CONTAINER,
		TABLE
	}

	public enum ContainerVisibility {
		META,
		INTERNAL_META,
		SYSTEM_TOOL
	}

}
