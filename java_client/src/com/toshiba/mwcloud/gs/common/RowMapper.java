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

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.toshiba.mwcloud.gs.AggregationResult;
import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.ColumnInfo;
import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.Container.BindType;
import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.ContainerType;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GSType;
import com.toshiba.mwcloud.gs.Geometry;
import com.toshiba.mwcloud.gs.NotNull;
import com.toshiba.mwcloud.gs.Nullable;
import com.toshiba.mwcloud.gs.QueryAnalysisEntry;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowField;
import com.toshiba.mwcloud.gs.RowKey;
import com.toshiba.mwcloud.gs.RowKeyPredicate;
import com.toshiba.mwcloud.gs.TimePrecision;
import com.toshiba.mwcloud.gs.TimeSeries;
import com.toshiba.mwcloud.gs.TimeUnit;
import com.toshiba.mwcloud.gs.TimestampUtils;
import com.toshiba.mwcloud.gs.TransientRowField;
import com.toshiba.mwcloud.gs.common.BasicBuffer.BufferUtils;

public class RowMapper {

	protected static boolean acceptAggregationResultColumnId = false;

	protected static boolean restrictKeyOrderFirst = true;

	private static final boolean STRING_FIELD_ENCODING_STRICT = true;

	private static final RowMapper AGGREGATION_RESULT_MAPPER;

	static {
		try {
			AGGREGATION_RESULT_MAPPER = new RowMapper(
					AggregationResult.class,
					AggregationResultImpl.class.getDeclaredConstructor(),
					Collections.<String, Entry>emptyMap(),
					Collections.<Entry>emptyList(),
					null, null, false);
		}
		catch (NoSuchMethodException e) {
			throw new Error(e);
		}
	}

	private static final byte[] EMPTY_LONG_BYTES =
			new byte[Long.SIZE / Byte.SIZE];

	private static final int MAX_VAR_SIZE_LENGTH = 8;

	public enum MappingMode {
		NORMAL,
		ROWWISE_SEPARATED,
		ROWWISE_SEPARATED_V2,
		COLUMNWISE_SEPARATED,
		AGGREGATED
	}

	public interface BlobFactory {

		Blob createBlob(byte[] data) throws GSException;

	}

	private static final BlobFactory DIRECT_BLOB_FACTORY = new BlobFactory() {
		@Override
		public Blob createBlob(byte[] data) throws GSException {
			final BlobImpl blob = new BlobImpl(null);
			blob.setDataDirect(data);
			return blob;
		}
	};

	private static final Pattern METHOD_PATTERN =
			Pattern.compile("^((get)|(set)|(is))(.+)$");

	private static final Cache CACHE = new Cache();

	private static final byte ANY_NULL_TYPE = -1;

	private static final Config BASIC_CONFIG =
			new Config(false, false, true, true);

	private static final Config GENERAL_CONFIG =
			new Config(true, true, true, true);

	private static final List<Integer> EMPTY_KEY_LIST =
			Collections.emptyList();

	private static final List<Integer> SINGLE_FIRST_KEY_LIST =
			Collections.singletonList(0);

	private final Class<?> rowType;

	private transient final Constructor<?> rowConstructor;

	private transient final Map<String, Entry> entryMap;

	private final List<Entry> entryList;

	private final boolean forTimeSeries;

	private final boolean nullableAllowed;

	private transient final RowMapper keyMapper;

	private transient final int variableEntryCount;

	private transient Object[] emptyFieldArray;

	private RowMapper(
			Class<?> rowType, Constructor<?> rowConstructor,
			Map<String, Entry> entryMap,
			List<Entry> entryList, RowMapper keyMapper,
			ContainerType containerType, boolean nullableAllowed) {
		this.rowType = rowType;
		this.rowConstructor = rowConstructor;
		this.entryMap = entryMap;
		this.entryList = entryList;
		this.forTimeSeries = (containerType == ContainerType.TIME_SERIES);
		this.nullableAllowed = nullableAllowed;
		this.keyMapper = keyMapper;
		this.variableEntryCount = calculateVariableEntryCount(entryList);
	}

	private RowMapper(
			Class<?> rowType, ContainerType containerType,
			boolean keyInside, Config config) throws GSException {
		this.rowType = rowType;
		this.entryMap = new HashMap<String, Entry>();
		this.entryList = new ArrayList<Entry>();
		this.forTimeSeries = (containerType == ContainerType.TIME_SERIES);
		this.nullableAllowed = config.nullableAllowed;
		this.rowConstructor = getRowConstructor(rowType);

		final Set<String> transientRowFields = new HashSet<String>();
		for (Field field : rowType.getDeclaredFields()) {
			accept(field, transientRowFields, keyInside);
		}
		for (Method method : rowType.getDeclaredMethods()) {
			accept(method, transientRowFields, keyInside);
		}

		applyOrder(transientRowFields, restrictKeyOrderFirst, true);
		applyNullable(nullableAllowed);

		this.keyMapper = makeKeyMapper(entryList, containerType, config);
		this.variableEntryCount = calculateVariableEntryCount(entryList);
	}

	private RowMapper(
			ContainerType containerType, ContainerInfo containerInfo,
			Config config) throws GSException {
		final ContainerType anotherContainerType = containerInfo.getType();
		if (anotherContainerType != null &&
				anotherContainerType != containerType) {
			throw new GSException(
					GSErrorCode.INTERNAL_ERROR, "Inconsistent container type");
		}

		this.rowType = Row.class;
		this.entryMap = new HashMap<String, Entry>();
		this.entryList = new ArrayList<Entry>();
		this.forTimeSeries = (containerType == ContainerType.TIME_SERIES);
		this.nullableAllowed = config.nullableAllowed;
		this.rowConstructor = null;

		final int columnCount = containerInfo.getColumnCount();
		if (columnCount <= 0 && !config.anyTypeAllowed) {
			throw new GSException(
					GSErrorCode.ILLEGAL_SCHEMA, "Empty schema");
		}

		for (int i = 0; i < columnCount; i++) {
			accept(containerInfo.getColumnInfo(i), config.anyTypeAllowed);
		}
		acceptKeyAndNullable(containerInfo, nullableAllowed);

		this.keyMapper = makeKeyMapper(entryList, containerType, config);
		this.variableEntryCount = calculateVariableEntryCount(entryList);
	}

	private static int calculateVariableEntryCount(List<Entry> entryList) {
		int count = 0;
		for (Entry entry : entryList) {
			if (entry.getDetailType().hasVarDataPart()) {
				count++;
			}
		}

		return count;
	}

	public static RowMapper getInstance(
			Container.BindType<?, ?, ?> bindType, ContainerType containerType,
			Config config) throws GSException {
		final RowMapper mapper =
				getInstance(bindType.getRowClass(), containerType, config);
		mapper.checkKeyTypeMatched(bindType.getKeyClass());
		return mapper;
	}

	public static RowMapper getInstance(
			Container.BindType<?, ?, ?> bindType, ContainerType containerType,
			ContainerInfo containerInfo, Config config) throws GSException {
		final RowMapper mapper = getInstance(containerType, containerInfo, config);
		mapper.checkKeyTypeMatched(bindType.getKeyClass());
		return mapper;
	}

	public static RowMapper getInstance(
			Class<?> rowType, ContainerType containerType, Config config)
			throws GSException {
		if (rowType == AggregationResult.class) {
			throw new GSException(
					GSErrorCode.ILLEGAL_PARAMETER, "Illegal row type");
		}

		return CACHE.getInstance(rowType, containerType, config);
	}

	public static RowMapper getInstance(
			ContainerType containerType, ContainerInfo containerInfo,
			Config config) throws GSException {
		return CACHE.intern(new RowMapper(
				containerType, containerInfo, config));
	}

	public static RowMapper getInstance(Row row, Config config)
			throws GSException {
		if (row instanceof Provider) {
			return ((Provider) row).getRowMapper();
		}

		return getInstance(null, row.getSchema(), config);
	}

	public static RowMapper getInstance(RowKeyPredicate<?> pred, Config config)
			throws GSException {
		if (pred instanceof Provider) {
			return ((Provider) pred).getRowMapper();
		}

		return getInstance(null, pred.getKeySchema(), config);
	}

	public static RowMapper getAggregationResultMapper() {
		return AGGREGATION_RESULT_MAPPER;
	}

	public static RowMapper getInstance(
			BasicBuffer in, ContainerType containerType, Config config)
			throws GSException {
		final int columnCount = importColumnCount(in);
		List<Integer> keyList = importKeyListBegin(in, config, columnCount);

		final List<ColumnInfo> columnInfoList = new ArrayList<ColumnInfo>();
		for (int i = 0; i < columnCount; i++) {
			columnInfoList.add(importColumnSchema(in, config).toInfo());
		}

		keyList = importKeyListEnd(in, config, columnCount, keyList);
		final ContainerInfo info = new ContainerInfo();
		info.setType(containerType);
		info.setColumnInfoList(columnInfoList);
		info.setRowKeyColumnList(keyList);

		return getInstance(containerType, info, config);
	}

	public void checkSchemaMatched(RowMapper mapper) throws GSException {
		if (this == mapper) {
			return;
		}

		if (rowType == AggregationResult.class ||
				mapper.rowType == AggregationResult.class) {
			throw new IllegalArgumentException();
		}

		if (entryList.size() != mapper.entryList.size()) {
			throw new GSException(GSErrorCode.ILLEGAL_SCHEMA, "");
		}

		for (int i = 0; i < entryList.size(); i++) {
			final Entry thisEntry = this.getEntry(i);
			final Entry entry = mapper.getEntry(i);

			if (thisEntry.keyType != entry.keyType) {
				throw new GSException(GSErrorCode.ILLEGAL_SCHEMA, "");
			}

			if (thisEntry.getDetailType() != entry.getDetailType()) {
				throw new GSException(GSErrorCode.ILLEGAL_SCHEMA, "");
			}

			if (!thisEntry.columnName.equals(entry.columnName) &&
					!normalizeSymbolUnchecked(thisEntry.columnName).equals(
							normalizeSymbolUnchecked(entry.columnName))) {
				throw new GSException(GSErrorCode.ILLEGAL_SCHEMA, "");
			}
		}
	}

	public void checkKeySchemaMatched(RowMapper mapper) throws GSException {
		resolveKeyMapper().checkSchemaMatched(mapper.resolveKeyMapper());
	}

	public void checkKeyTypeMatched(Class<?> keyClass) throws GSException {
		if (keyClass == null || keyClass == Object.class ||
				(isGeneral() && keyClass == Row.Key.class)) {
			return;
		}

		final RowMapper keyMapper = findKeyMapper();
		final List<DetailElementType> keyTypeList;
		if (keyMapper == null) {
			if (keyClass == Void.class) {
				return;
			}
			keyTypeList = Collections.emptyList();
		}
		else {
			if (keyMapper.getColumnCount() > 1) {
				if (keyClass == keyMapper.rowType) {
					return;
				}
			}
			else {
				final DetailElementType elemType =
						DetailElementType.resolve(keyClass, false, false);
				if (keyMapper.getEntry(0).getDetailType().match(
						elemType, true)) {
					return;
				}
			}
			final List<DetailElementType> baseList =
					new ArrayList<DetailElementType>();
			for (Entry entry : keyMapper.entryList) {
				baseList.add(entry.getDetailType());
			}
			keyTypeList = baseList;
		}
		throw new GSException(
				GSErrorCode.KEY_NOT_ACCEPTED,
				"Unacceptable key class (keyClass=" + keyClass.getName() +
				", keyTypeList=" + keyTypeList + ")");
	}

	public RowMapper reorderBySchema(
			BasicBuffer in, Config config, boolean columnOrderIgnorable)
			throws GSException {
		final int size = importColumnCount(in);
		if (size != entryList.size()) {
			throw new GSException(
					GSErrorCode.ILLEGAL_SCHEMA,
					"Inconsistent remote schema (column count)");
		}

		List<Integer> keyList = importKeyListBegin(in, config, size);

		final Map<String, Entry> newEntryMap = new HashMap<String, Entry>();
		final List<Entry> newEntryList = new ArrayList<Entry>(size);
		for (int i = 0; i < size; i++) {
			newEntryList.add(null);
		}

		for (int i = 0; i < size; i++) {
			final Entry newEntry = new Entry(null);
			newEntry.importColumnSchema(in, i, nullableAllowed);

			final String normalizedName =
					normalizeSymbolUnchecked(newEntry.columnName);
			final Entry orgEntry = entryMap.get(normalizedName);
			if (orgEntry == null) {
				throw new GSException(
						GSErrorCode.ILLEGAL_SCHEMA,
						"Inconsistent remote schema (column not found)");
			}
			newEntry.importObjectMapping(orgEntry, columnOrderIgnorable);

			if (newEntryList.get(i) != null) {
				throw new GSException(
						GSErrorCode.ILLEGAL_SCHEMA,
						"Inconsistent remote schema (duplicate column)");
			}
			newEntryList.set(i, newEntry);
			newEntryMap.put(normalizedName, newEntry);
		}

		keyList = importKeyListEnd(in, config, size, keyList);

		final RowMapper keyMapper = findKeyMapper();
		final RowMapper newKeyMapper;
		if (keyMapper == null) {
			if (!keyList.isEmpty()) {
				throw new GSException(
						GSErrorCode.ILLEGAL_SCHEMA,
						"Remote schema must not have a key");
			}
			newKeyMapper = null;
		}
		else {
			if (keyList.isEmpty()) {
				throw new GSException(
						GSErrorCode.ILLEGAL_SCHEMA,
						"Remote schema must have a key");
			}

			final List<Entry> newKeyEntryList = new ArrayList<Entry>();
			for (Integer key : keyList) {
				final Entry newKeyEntry = newEntryList.get(key);

				final String normalizedName =
						normalizeSymbolUnchecked(newKeyEntry.columnName);
				final Entry orgKeyEntry = entryMap.get(normalizedName);

				if (!orgKeyEntry.keyType) {
					throw new GSException(
							GSErrorCode.ILLEGAL_SCHEMA,
							"Inconsistent remote schema (key column (name=" +
									newKeyEntry.columnName + "))");
				}
				newKeyEntry.keyType = true;
				newKeyEntryList.add(newKeyEntry);
			}

			newKeyMapper =
					makeKeyMapper(newKeyEntryList, getContainerType(), config);

			if (keyList.size() != keyMapper.getColumnCount() ||
					keyList.size() != newKeyMapper.getColumnCount()) {
				throw new GSException(
						GSErrorCode.ILLEGAL_SCHEMA,
						"Inconsistent remote schema (key column count)");
			}
		}

		return CACHE.intern(new RowMapper(
				rowType, rowConstructor, newEntryMap, newEntryList,
				newKeyMapper, getContainerType(), nullableAllowed));
	}

	public static int importColumnCount(BasicBuffer in) throws GSException {
		return BufferUtils.getNonNegativeInt(in.base());
	}

	public static void exportColumnCount(
			BasicBuffer out, int columnCount) throws GSException {
		out.putInt(columnCount);
	}

	public static List<Integer> importKeyListBegin(
			BasicBuffer in, Config config, int columnCount)
			throws GSException {
		if (!config.keyExtensible) {
			final int columnId = in.base().getInt();
			if (!(columnId == 0 || columnId == -1) ||
					columnId >= columnCount) {
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by illegal index of row key " +
						"column (keyColumn=" + columnId + ")");
			}
			return toSimpleKeyList(columnId >= 0);
		}

		return null;
	}

	public static List<Integer> importKeyListEnd(
			BasicBuffer in, Config config, int columnCount,
			List<Integer> lastKeyList) throws GSException {
		if (config.keyExtensible) {
			final int count = in.base().getShort();
			if (count < 0 || count > columnCount ||
					!config.keyComposable && !(count == 0 || count == 1)) {
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by illegal row key count (" +
						"count=" + count + ")");
			}

			final List<Integer> keyList = new ArrayList<Integer>();
			for (int i = 0; i < count; i++) {
				final int columnId = in.base().getShort();
				if (columnId < 0 || columnId >= columnCount ||
						(columnId != i && restrictKeyOrderFirst)) {
					throw new GSConnectionException(
							GSErrorCode.MESSAGE_CORRUPTED,
							"Protocol error by illegal index of row key " +
							"column (keyColumn=" + columnId + ")");
				}
				keyList.add(columnId);
			}

			return keyList;
		}

		return lastKeyList;
	}

	public static void exportKeyListBegin(
			BasicBuffer out, Config config, List<Integer> keyList)
			throws GSException {
		if (!config.keyExtensible) {
			out.putInt((keyList.isEmpty() ? -1 : 0));
		}
	}

	public static void exportKeyListEnd(
			BasicBuffer out, Config config, List<Integer> keyList)
			throws GSException {
		if (config.keyExtensible) {
			final int count = keyList.size();
			if (!config.keyComposable && count > 1) {
				throw new GSException(GSErrorCode.INTERNAL_ERROR, "");
			}
			out.putShort((short) count);
			for (int keyIndex : keyList) {
				if (!config.keyComposable && keyIndex != 0) {
					throw new GSException(GSErrorCode.INTERNAL_ERROR, "");
				}
				out.putShort((short) keyIndex);
			}
		}
	}

	public static ColumnInfo.Builder importColumnSchema(
			BasicBuffer in, Config config) throws GSException {
		final Entry entry = new Entry(null);
		entry.importColumnSchema(in, -1, config.nullableAllowed);
		entry.filterNullable(
				entry.columnNullable, null, config.nullableAllowed);

		if (config.anyTypeAllowed && entry.columnName.isEmpty()) {
			entry.columnName = null;
		}

		final boolean withKeyInfo = false;
		return entry.getColumnInfo(withKeyInfo);
	}

	public static List<ColumnInfo> toColumnInfoList(
			List<ColumnInfo.Builder> builderList) {
		final List<ColumnInfo> infoList =
				new ArrayList<ColumnInfo>(builderList.size());
		for (ColumnInfo.Builder builder : builderList) {
			infoList.add(builder.toInfo());
		}
		return infoList;
	}

	private static List<Integer> toKeyList(RowMapper keyMapper) {
		if (keyMapper == null) {
			return toKeyList(Collections.<Entry>emptyList());
		}
		else {
			return toKeyList(keyMapper.entryList);
		}
	}

	private static List<Integer> toKeyList(List<Entry> keyEntryList) {
		if (keyEntryList.isEmpty()) {
			return toSimpleKeyList(false);
		}
		else if (keyEntryList.size() == 1 && keyEntryList.get(0).order == 0) {
			return toSimpleKeyList(true);
		}
		else {
			final List<Integer> list = new ArrayList<Integer>();
			for (Entry entry : keyEntryList) {
				list.add(entry.order);
			}
			return list;
		}
	}

	private static List<Integer> toSimpleKeyList(boolean rowKeyAssigned) {
		if (rowKeyAssigned) {
			return SINGLE_FIRST_KEY_LIST;
		}
		else {
			return EMPTY_KEY_LIST;
		}
	}

	public RowMapper applyResultType(Class<?> rowType) throws GSException {
		if (getRowType() == rowType) {
			return this;
		}
		else if (rowType == AggregationResult.class) {
			return AGGREGATION_RESULT_MAPPER;
		}
		else if (rowType == QueryAnalysisEntry.class) {
			return getInstance(QueryAnalysisEntry.class, null, BASIC_CONFIG);
		}
		else if (rowType == null) {
			return null;
		}
		else {
			throw new GSException(
					GSErrorCode.ILLEGAL_PARAMETER, "Unsupported result type");
		}
	}

	public boolean hasKey() {
		return findKeyMapper() != null;
	}

	public KeyCategory getKeyCategory() {
		final RowMapper keyMapper = findKeyMapper();
		if (keyMapper == null) {
			return KeyCategory.NONE;
		}
		else if (keyMapper.getColumnCount() > 1) {
			return KeyCategory.COMPOSITE;
		}
		else {
			return KeyCategory.SINGLE;
		}
	}

	public Class<?> getRowType() {
		return rowType;
	}

	public boolean isForTimeSeries() {
		return forTimeSeries;
	}

	public GSType getFieldElementType(int columnId) {
		return DetailElementType.of(
				getEntry(columnId).getDetailType().base(), false).toFullType();
	}

	public boolean isArray(int columnId) {
		return getEntry(columnId).getDetailType().isForArray();
	}

	public boolean hasAnyTypeColumn() {
		for (Entry entry : entryList) {
			if (entry.getDetailType().isAny()) {
				return true;
			}
		}

		return false;
	}

	public boolean isDefaultValueSpecified() {
		for (Entry entry : entryList) {
			if (entry.initialValueSpecified) {
				return true;
			}
		}

		return false;
	}

	private int getVariableEntryCount() {
		return variableEntryCount;
	}

	public ContainerType getContainerType() {
		if (rowType == AggregationResult.class) {
			return null;
		}
		else if (forTimeSeries) {
			return ContainerType.TIME_SERIES;
		}
		else {
			return ContainerType.COLLECTION;
		}
	}

	public ContainerInfo getContainerInfo() {
		if (rowType == AggregationResult.class) {
			return null;
		}

		final boolean withKeyInfo = true;
		final List<ColumnInfo> columnInfoList =
				new ArrayList<ColumnInfo>(entryList.size());
		for (Entry entry : entryList) {
			columnInfoList.add(entry.getColumnInfo(withKeyInfo).toInfo());
		}

		
		final ContainerInfo info = new ContainerInfo();
		info.setColumnInfoList(columnInfoList);
		info.setRowKeyColumnList(toKeyList(findKeyMapper()));
		return info;
	}

	public ContainerInfo resolveKeyContainerInfo() throws GSException {
		return resolveKeyMapper().getContainerInfo();
	}

	public ContainerInfo peekNextRowSchema(Cursor cursor) throws GSException {
		if (cursor.getMode() == MappingMode.AGGREGATED) {
			return peekAggregationSchema(cursor);
		}
		return null;
	}

	public SchemaFeatureLevel getFeatureLevel() {
		SchemaFeatureLevel level = SchemaFeatureLevel.LEVEL1;
		for (Entry entry : entryList) {
			level = entry.getDetailType().getFeatureLevel().merge(level);
		}
		return level;
	}

	public void exportSchema(BasicBuffer out, Config config)
			throws GSException {
		if (rowType == AggregationResult.class) {
			throw new GSException(
					GSErrorCode.INTERNAL_ERROR,
					"Unexpected row type: AggregationResult");
		}

		final List<Integer> keyList = toKeyList(findKeyMapper());

		exportColumnCount(out, entryList.size());
		exportKeyListBegin(out, config, keyList);
		for (Entry entry : entryList) {
			entry.exportColumnSchema(out);
		}
		exportKeyListEnd(out, config, keyList);
	}

	public void exportKeySchemaSingle(BasicBuffer out)
			throws GSException {
		final RowMapper keyMapper = resolveKeyMapper();
		if (keyMapper.getColumnCount() > 1) {
			out.put((byte) -1);
		}
		else {
			keyMapper.getEntry(0).getDetailType().put(out);
		}
	}

	public void exportKeySchemaComposite(BasicBuffer out)
			throws GSException {
		final RowMapper keyMapper = resolveKeyMapper();
		final int columnCount = keyMapper.getColumnCount();
		if (columnCount > 1) {
			out.putInt(columnCount);
			for (Entry entry : keyMapper.entryList) {
				entry.getDetailType().put(out);
			}
		}
	}

	public int resolveColumnId(String name) throws GSException {
		final Entry entry = entryMap.get(normalizeSymbol(name, "column name"));
		if (entry == null) {
			throw new GSException(
					GSErrorCode.UNKNOWN_COLUMN_NAME,
					"Unknown column: \"" + name + "\"");
		}
		return entry.order;
	}

	public Object resolveField(Object rowObj, int column) throws GSException {
		return getEntry(column).getFieldObj(rowObj, getFieldAccessMode());
	}

	public Object resolveKeyField(Object keyObj, int keyColumn)
			throws GSException {
		if (!checkKeyComposed(keyObj)) {
			if (keyColumn != 0) {
				throw new IllegalArgumentException();
			}
			return keyObj;
		}

		final RowMapper keyMapper = resolveKeyMapper();
		return keyMapper.getEntry(keyColumn).getFieldObj(
				keyObj, getFieldAccessMode());
	}

	public Object resolveKey(Object lastKeyObj, Object rowObj) throws GSException {
		if (!restrictKeyOrderFirst) {
			throw new Error();
		}
		final RowMapper keyMapper = resolveKeyMapper();
		final FieldAccessMode accessMode = getFieldAccessMode();
		final int keyColumnCount = keyMapper.getColumnCount();
		if (keyColumnCount > 1) {
			final Object destKey;
			if (lastKeyObj == null) {
				destKey = keyMapper.createRow(accessMode.isGeneral());
			}
			else {
				destKey = lastKeyObj;
			}
			Object src = findEncodingKeyObj(null, rowObj);
			if (src == null) {
				src = rowObj;
			}
			for (int i = 0; i < keyColumnCount; i++) {
				final Object elem = getEntry(i).getFieldObj(src, accessMode);
				keyMapper.getEntry(i).setFieldObj(destKey, elem, accessMode);
			}
			return destKey;
		}
		else {
			return getEntry(0).getFieldObj(rowObj, accessMode);
		}
	}

	public Object resolveKey(String keyString) throws GSException {
		final RowMapper keyMapper = resolveKeyMapper();

		if (keyMapper.getColumnCount() != 1) {
			throw new GSException(
					GSErrorCode.UNSUPPORTED_KEY_TYPE,
					"Path key operation not supported for composite key");
		}

		final DetailElementType type = keyMapper.getEntry(0).getDetailType();
		if (!type.isForKey()) {
			throw new GSException(
					GSErrorCode.UNSUPPORTED_KEY_TYPE,
					"Unsupported key type (type=" + type + ")");
		}
		try {
			switch (type.base()) {
			case STRING:
				return keyString;
			case INTEGER:
				return Integer.parseInt(keyString);
			case LONG:
				return Long.parseLong(keyString);
			case TIMESTAMP:
				return TimestampUtils.getFormat().parse(keyString);
			default:
				throw new IllegalStateException();
			}
		}
		catch (NumberFormatException e) {
			throw new GSException(GSErrorCode.ILLEGAL_VALUE_FORMAT, e);
		}
		catch (ParseException e) {
			throw new GSException(GSErrorCode.ILLEGAL_VALUE_FORMAT, e);
		}
	}

	public Row createGeneralRow() throws GSException {
		return new ArrayRow(this, true, true);
	}

	public Object createRow(boolean general) throws GSException {
		if (general || isGeneral()) {
			return createGeneralRow();
		}

		return constructObj(rowConstructor);
	}

	public Row.Key createGeneralRowKey() throws GSException {
		return new ArrayRowKey(resolveKeyMapper(), true, true);
	}

	public static Row.Key createIdenticalRowKey(Row.Key key)
			throws GSException {
		return new IdenticalRowKey(key);
	}

	public void encodeKey(BasicBuffer buffer, Object keyObj, MappingMode mode)
			throws GSException {
		encodeKey(buffer, Collections.singleton(keyObj), mode, false, false);
	}

	public void encodeKey(
			BasicBuffer buffer, java.util.Collection<?> keyObjCollection,
			MappingMode mode, boolean withEncodedSize,
			boolean withKeyCount) throws GSException {
		final RowMapper keyMapper = resolveKeyMapper();
		final boolean composite = (keyMapper.getColumnCount() > 1);

		final int headPos = buffer.base().position();
		if (withEncodedSize && composite) {
			buffer.putInt(0);
		}
		final int bodyPos = buffer.base().position();

		final int keyCount = keyObjCollection.size();
		if (withKeyCount) {
			buffer.putInt(keyCount);
		}

		final Cursor compositeCursor;
		if (composite) {
			compositeCursor = keyMapper.createCursor(
					buffer, mode, keyCount, false, null);
		}
		else {
			compositeCursor = null;
		}

		final FieldAccessMode accessMode = keyMapper.getFieldAccessMode();
		for (Object keyObj : keyObjCollection) {
			final boolean objComposed = checkKeyComposed(keyObj);

			if (compositeCursor == null) {
				final Entry entry = keyMapper.getEntry(0);
				final Object keyElemObj;
				if (objComposed) {
					keyElemObj = entry.getFieldObj(keyObj, accessMode);
				}
				else {
					keyElemObj = keyObj;
				}
				putSingleField(
						buffer, keyElemObj, entry.getDetailType(), mode);
			}
			else {
				keyMapper.encode(compositeCursor, null, keyObj);
			}
		}

		if (withEncodedSize && composite) {
			final int endPos = buffer.base().position();
			buffer.base().position(headPos);
			buffer.putInt(endPos - bodyPos);
			buffer.base().position(endPos);
		}
	}

	public Cursor createCursor(BasicBuffer buffer, MappingMode mode,
			int rowCount, boolean rowIdIncluded, BlobFactory blobFactory) {
		return new Cursor(buffer, mode, rowCount, rowIdIncluded, blobFactory);
	}

	public void encode(BasicBuffer buffer, MappingMode mode,
			Object keyObj, Object rowObj) throws GSException {
		final Cursor cursor = new Cursor(buffer, mode, 1, false, null);
		encode(cursor, keyObj, rowObj, isGeneral());
	}

	public Object decode(BasicBuffer buffer, MappingMode mode,
			BlobFactory blobFactory) throws GSException {
		final Cursor cursor = new Cursor(buffer, mode, 1, false, blobFactory);
		return decode(cursor, isGeneral());
	}

	public void encode(
			Cursor cursor, Object keyObj, Object rowObj) throws GSException {
		encode(cursor, keyObj, rowObj, isGeneral());
	}

	public Object decode(Cursor cursor) throws GSException {
		return decode(cursor, isGeneral());
	}

	public void encode(
			Cursor cursor, Object keyObj, Object rowObj, boolean general)
			throws GSException {
		if (rowType == AggregationResult.class) {
			throw new GSException(
					GSErrorCode.INTERNAL_ERROR,
					"Unexpected row type: AggregationResult");
		}

		if (!rowType.isInstance(rowObj)) {
			if (rowObj == null) {
				throw new NullPointerException("The row object is null");
			}
			if (!general) {
				throw new ClassCastException("Inconsistent row type");
			}
		}

		final Object resolvedKeyObj = findEncodingKeyObj(keyObj, rowObj);
		boolean keyDecomposing;
		if (resolvedKeyObj == null) {
			keyDecomposing = false;
		}
		else {
			keyDecomposing = checkKeyComposed(resolvedKeyObj);
		}

		if (rowType == Row.class || general) {
			getInstance((Row) rowObj, GENERAL_CONFIG).checkSchemaMatched(this);
		}

		cursor.beginRowOutput();
		if (cursor.getMode() == MappingMode.AGGREGATED) {
			throw new IllegalArgumentException();
		}
		else {
			final FieldAccessMode accessMode = FieldAccessMode.of(general);
			for (Entry entry : entryList) {
				final Object keyElemObj;
				if (keyDecomposing && entry.keyType) {
					keyElemObj = entry.getFieldObj(resolvedKeyObj, accessMode);
				}
				else {
					keyElemObj = resolvedKeyObj;
				}
				entry.encode(cursor, keyElemObj, rowObj, accessMode);
			}
		}
		cursor.endRowOutput();
	}

	public Object decode(Cursor cursor, boolean general) throws GSException {
		final Object rowObj = (general ?
				new ArrayRow(this, false, false) : createRow(false));
		cursor.decode(general, rowObj);
		return rowObj;
	}

	
	public void extractSubRowSetAndCount(
			Cursor cursor, int rowOffset, int rowLimit,
			BasicBuffer out, boolean includeVarDataOffset) throws GSException {
		if (cursor.getMode() != MappingMode.ROWWISE_SEPARATED &&
				cursor.getMode() != MappingMode.ROWWISE_SEPARATED_V2) {
			throw new IllegalArgumentException();
		}

		if (rowOffset < 0 || rowLimit <= 0 || rowOffset >= cursor.rowCount) {
			return;
		}

		final int extractingCount =
				Math.min(cursor.rowCount - rowOffset, rowLimit);

		final BasicBuffer in = cursor.buffer;

		cursor.skipRowInput(rowOffset);
		final int fixedOffset = in.base().position();
		final int fixedLength = getFixedRowPartSize(
				cursor.rowIdIncluded, cursor.mode) * extractingCount;

		final int varOffset = scanVarDataStartOffset(cursor);
		cursor.skipRowInput(extractingCount - 2);
		final int varLength = scanVarDataEndOffset(cursor) - varOffset;

		cursor.skipRowInput(-(cursor.rowIndex + 1));

		if (includeVarDataOffset) {
			out.putLong(varOffset);
		}
		out.putLong(extractingCount);

		in.base().limit(fixedOffset + fixedLength);
		in.base().position(fixedOffset);
		out.prepare(fixedLength);
		out.base().put(in.base());

		in.base().limit(cursor.varDataTop + varOffset + varLength);
		in.base().position(cursor.varDataTop + varOffset);
		out.prepare(varLength);
		out.base().put(in.base());

		in.base().position(cursor.topPos);
	}

	private int scanVarDataStartOffset(Cursor cursor) throws GSException {
		final BasicBuffer in = cursor.buffer;

		if (cursor.mode == MappingMode.ROWWISE_SEPARATED_V2) {
			final int orgPos = in.base().position();
			if (cursor.isRowIdIncluded()) {
				in.base().getLong();
			}
			final int startOffset =
					(getVariableEntryCount() > 0 ? (int) in.base().getLong() : 0);
			in.base().position(orgPos);

			cursor.beginRowInput();
			for (int i = 0; i < entryList.size(); i++) {
				in.base().position(in.base().position() +
						getFixedFieldPartSize(i, cursor.mode));
			}
			cursor.endRowInput();

			return startOffset;
		}

		int startOffset = Integer.MAX_VALUE;
		cursor.beginRowInput();
		for (Entry entry : entryList) {
			cursor.beginField();
			if (entry.getDetailType().hasVarDataPart()) {
				startOffset = Math.min(startOffset, (int) in.base().getLong());
			}
			else {
				in.base().position(in.base().position() +
						getFixedFieldPartSize(entry.order, cursor.getMode()));
			}
		}
		cursor.endRowInput();

		if (startOffset == Integer.MAX_VALUE) {
			startOffset = 0;
		}

		return startOffset;
	}

	private int scanVarDataEndOffset(Cursor cursor) throws GSException {
		final BasicBuffer in = cursor.buffer;

		if (cursor.mode == MappingMode.ROWWISE_SEPARATED_V2) {
			cursor.beginRowInput();
			for (int i = 0; i < entryList.size(); i++) {
				in.base().position(in.base().position() +
						getFixedFieldPartSize(i, cursor.mode));
			}
			cursor.endRowInput();

			final int endOffset;
			if (cursor.hasNext()) {
				final int orgPos = in.base().position();
				if (cursor.isRowIdIncluded()) {
					in.base().getLong();
				}
				endOffset = (getVariableEntryCount() > 0 ?
						(int) in.base().getLong() : 0);
				in.base().position(orgPos);
			}
			else {
				endOffset = in.base().limit() - cursor.varDataTop;
			}

			return endOffset;
		}

		int endOffset = 0;

		cursor.beginRowInput();
		for (Entry entry : entryList) {
			cursor.beginField();
			if (entry.getDetailType().hasVarDataPart()) {
				final int offset = (int) (
						in.base().getLong() - cursor.varDataBaseOffset);
				final int orgPos = in.base().position();

				in.base().position(cursor.varDataTop + offset);
				final int size = in.base().getInt() + Integer.SIZE / Byte.SIZE;
				endOffset = Math.max(endOffset, offset + size);
				in.base().position(orgPos);
			}
			else {
				in.base().position(in.base().position() +
						getFixedFieldPartSize(entry.order, cursor.getMode()));
			}
		}
		cursor.endRowInput();

		return endOffset;
	}

	private FieldAccessMode getFieldAccessMode() {
		return FieldAccessMode.of(isGeneral());
	}

	private boolean isGeneral() {
		return (rowType == Row.class || rowType == Row.Key.class);
	}

	private static Constructor<?> getRowConstructor(
			Class<?> rowType) throws GSException {
		Constructor<?> defaultConstructor = null;
		for (Constructor<?> constructor : rowType.getDeclaredConstructors()) {
			if (constructor.getParameterTypes().length == 0) {
				defaultConstructor = constructor;
				break;
			}
		}
		if (defaultConstructor == null) {
			throw new GSException(
					GSErrorCode.ILLEGAL_PARAMETER,
					"Default constructor not found or specified class is non-static");
		}
		defaultConstructor.setAccessible(true);
		return defaultConstructor;
	}

	private void accept(
			Field field, Set<String> transientRowFields, boolean keyInside)
			throws GSException {
		final int modifier = field.getModifiers();
		if (Modifier.isFinal(modifier) || Modifier.isPrivate(modifier) ||
				Modifier.isStatic(modifier) || Modifier.isTransient(modifier)) {
			return;
		}

		final RowField rowField = field.getAnnotation(RowField.class);
		final String orgName = (rowField == null || rowField.name().isEmpty() ?
				field.getName() : rowField.name());
		final String name =
				normalizeSymbol(orgName, "field name of row class");
		if (field.getAnnotation(TransientRowField.class) != null) {
			transientRowFields.add(name);
			return;
		}

		final Class<?> objectType = field.getType();
		Entry compositeKeyEntry = null;
		if (DetailElementType.resolve(objectType, false, false) == null) {
			compositeKeyEntry = tryAcceptCompositeKey(
					objectType, field, null, false, keyInside);
			if (compositeKeyEntry == null) {
				return;
			}
		}

		final Entry entry = putEntry(orgName, rowField, compositeKeyEntry);
		if (entry.rowTypeField != null) {
			throw new GSException(
					GSErrorCode.ILLEGAL_SCHEMA,
					"Duplicate field name (" + name + ")");
		}
		entry.rowTypeField = field;
		entry.nameByField = orgName;
		entry.applyAccessibleObject(field);
		entry.setObjectType(field.getType());
	}

	private void accept(
			Method method, Set<String> transientRowFields, boolean keyInside)
			throws GSException {
		final int modifier = method.getModifiers();
		if (Modifier.isPrivate(modifier) || Modifier.isStatic(modifier)) {
			return;
		}

		final RowField rowField = method.getAnnotation(RowField.class);
		final Matcher matcher = METHOD_PATTERN.matcher(method.getName());
		if (!matcher.find()) {
			return;
		}

		final String orgName = (rowField == null || rowField.name().isEmpty() ?
				matcher.group(5) : rowField.name());
		final String name =
				normalizeSymbol(orgName, "method name of row class");
		if (method.getAnnotation(TransientRowField.class) != null) {
			transientRowFields.add(name);
			return;
		}

		final Class<?> objectType;
		final boolean forGetter;
		if (matcher.group(2) != null) {
			if (method.getParameterTypes().length != 0) {
				return;
			}
			objectType = method.getReturnType();
			forGetter = true;
		}
		else if (matcher.group(3) != null) {
			Class<?>[] parameterTypes = method.getParameterTypes();
			if (parameterTypes.length != 1) {
				return;
			}
			objectType = parameterTypes[0];
			forGetter = false;
		}
		else {
			if (method.getParameterTypes().length != 0 ||
					method.getReturnType() != boolean.class) {
				return;
			}
			objectType = boolean.class;
			forGetter = true;
		}

		Entry compositeKeyEntry = null;
		if (DetailElementType.resolve(objectType, false, false) == null) {
			compositeKeyEntry = tryAcceptCompositeKey(
					objectType, null, method, forGetter, keyInside);
			if (compositeKeyEntry == null) {
				return;
			}
		}

		final Entry entry = putEntry(orgName, rowField, compositeKeyEntry);
		if (forGetter) {
			if (entry.getterMethod != null) {
				throw new GSException(
						GSErrorCode.ILLEGAL_SCHEMA,
						"Duplicate getter name (" + name + ")");
			}
			entry.getterMethod = method;
			entry.nameByGetter = orgName;
		}
		else{
			if (entry.setterMethod != null) {
				throw new GSException(
						GSErrorCode.ILLEGAL_SCHEMA,
						"Duplicate setter name (" + name + ")");
			}
			entry.setterMethod = method;
		}
		entry.applyAccessibleObject(method);
		entry.setObjectType(objectType);
	}

	private void accept(
			ColumnInfo columnInfo, boolean anyTypeAllowed) throws GSException {
		final String orgName = columnInfo.getName();
		final String normalizedName =
				(orgName == null ? null : normalizeSymbolUnchecked(orgName));
		if (!anyTypeAllowed || orgName != null) {
			RowMapper.checkSymbol(orgName, "column name");
		}

		if (entryMap.containsKey(normalizedName)) {
			throw new GSException(
					GSErrorCode.ILLEGAL_SCHEMA,
					"Duplicate column name (" + orgName + ")");
		}

		final Entry entry = new Entry(orgName);
		final GSType type = columnInfo.getType();

		final DetailElementType detailType = DetailElementType.of(
				type, DetailElementType.resolvePrecisionLevel(columnInfo),
				anyTypeAllowed);
		final int order = entryList.size();
		entry.setColumnType(detailType, order);

		if (normalizedName != null) {
			entryMap.put(normalizedName, entry);
		}
		entryList.add(entry);
	}

	private void acceptKeyAndNullable(
			ContainerInfo containerInfo, boolean nullableAllowed)
			throws GSException {
		final List<Integer> rowKeyColumnList = containerInfo.getRowKeyColumnList();
		int expectedColumn = 0;
		for (int column : rowKeyColumnList) {
			if (column < 0 || column >= entryList.size()) {
				throw new GSException(
						GSErrorCode.KEY_NOT_ACCEPTED,
						"Out of range for row key column number (" +
						"column=" + column +
						", columnCount=" + entryList.size() +")");
			}

			if (column != expectedColumn) {
				throw new GSException(
						GSErrorCode.KEY_NOT_ACCEPTED,
						"Row key column must be ordered as coulmn schema");
			}

			entryList.get(column).keyType = true;
			expectedColumn++;
		}

		final int columnCount = entryList.size();
		for (int i = 0; i < columnCount; i++) {
			final ColumnInfo columnInfo = containerInfo.getColumnInfo(i);
			final Entry entry = entryList.get(i);

			entry.setNullableGeneral(columnInfo.getNullable(), nullableAllowed);
			entry.setInitialValueNull(columnInfo.getDefaultValueNull());
		}
	}

	private Entry putEntry(
			String name, RowField rowField, Entry compositeKeyEntry)
			throws GSException {
		final String normalizedName = normalizeSymbolUnchecked(name);
		Entry entry;
		if (compositeKeyEntry == null) {
			entry = entryMap.get(normalizedName);
			if (entry == null) {
				entry = new Entry(name);
				entryMap.put(normalizedName, entry);
				entryList.add(entry);
			}
		}
		else if (compositeKeyEntry.columnName == null) {
			compositeKeyEntry.columnName = name;
			entry = compositeKeyEntry;
		}
		else {
			if (!normalizedName.equals(
					normalizeSymbolUnchecked(compositeKeyEntry.columnName))) {
				throw new GSException(
						GSErrorCode.ILLEGAL_SCHEMA,
						"Composite row key names unmatched (names=[" +
						name + ", " + compositeKeyEntry.columnName + "])");
			}
			entry = compositeKeyEntry;
		}

		if (rowField != null) {
			if (compositeKeyEntry != null) {
				throw new GSException(
						GSErrorCode.ILLEGAL_SCHEMA,
						"RowField annotation cannot be specified for " +
						"composite row key");
			}
			final int order = rowField.columnNumber();
			if (order >= 0) {
				if (entry.order >= 0) {
					if (entry.order != order) {
						throw new GSException(
								GSErrorCode.ILLEGAL_SCHEMA,
								"Illegal column number");
					}
				}
				else {
					entry.order = order;
					entry.orderSpecified = true;
				}
			}
		}

		return entry;
	}

	private Entry tryAcceptCompositeKey(
			Class<?> objectType, Field field, Method method,
			boolean forGetter, boolean keyInside) throws GSException {
		final AccessibleObject ao = (method == null ? field : method);
		if (ao.getAnnotation(RowKey.class) == null) {
			return null;
		}

		if (keyInside) {
			throw new GSException(
					GSErrorCode.ILLEGAL_SCHEMA,
					"Nested composite row key specified");
		}

		for (Entry entry : entryList) {
			final CompositeKeyEntry keyEntry = entry.compositeKeyEntry;
			if (keyEntry != null) {
				if (keyEntry.keyClass != objectType) {
					throw new GSException(
							GSErrorCode.ILLEGAL_SCHEMA,
							"Inconsistent classes for composite row key(" +
							"classes=[" + keyEntry.keyClass.getName() + ", " +
							objectType.getName() + "])");
				}
				return keyEntry.base;
			}
		}

		final boolean keyInsideSub = true;;
		final RowMapper keyMapper;
		try {
			keyMapper = new RowMapper(
					objectType, ContainerType.COLLECTION, keyInsideSub,
					GENERAL_CONFIG);
		}
		catch (GSException e) {
			throw new GSException(
					GSErrorCode.getDescription(e) +
					" on checking composite key class (" +
					"keyClass=" + objectType.getName() + ")", e);
		}

		if (keyMapper.keyMapper != null) {
			throw new GSException(
					GSErrorCode.ILLEGAL_SCHEMA,
					"Nested single row key specified in composite row key");
		}

		final CompositeKeyEntry keyEntry = new CompositeKeyEntry(
				new Entry(null), keyMapper.rowType, keyMapper.rowConstructor);

		for (Map.Entry<String, Entry> entry : keyMapper.entryMap.entrySet()) {
			final Entry subEntry = entry.getValue();
			if (entryMap.keySet().contains(entry.getKey())) {
				throw new GSException(
						GSErrorCode.ILLEGAL_SCHEMA,
						"Duplicate field name specified between entries of " +
						"composite row key and other (name=" +
						subEntry.columnName + ")");
			}
			subEntry.compositeKeyEntry = keyEntry;
			subEntry.keyType = true;
		}

		if (keyMapper.entryList.size() != keyMapper.entryMap.size()) {
			throw new Error();
		}

		if (keyMapper.entryList.size() <= 1) {
			throw new GSException(
					GSErrorCode.KEY_NOT_ACCEPTED,
					"Composite row key must have multiple row fields (" +
					"keyClass=" + objectType.getName() + ")");
		}

		entryList.addAll(keyMapper.entryList);
		entryMap.putAll(keyMapper.entryMap);
		return keyEntry.base;
	}

	private void applyOrder(
			Set<String> transientRowFields, boolean keyFirst,
			boolean keyUnified) throws GSException {
		boolean specified = false;
		List<Entry> keyEntryList = Collections.emptyList();
		for (Iterator<Entry> i = entryList.iterator(); i.hasNext();) {
			final Entry entry = i.next();
			final String normalizedName =
					normalizeSymbolUnchecked(entry.columnName);
			if (!entry.reduceByAccessors() ||
					transientRowFields.contains(normalizedName)) {
				entryMap.remove(normalizedName);
				i.remove();
			}
			specified |= entry.orderSpecified;
			if (entry.keyType) {
				if (keyEntryList.isEmpty()) {
					keyEntryList = new ArrayList<Entry>();
				}
				keyEntryList.add(entry);
			}
		}

		if (entryList.isEmpty()) {
			throw new GSException(
					GSErrorCode.ILLEGAL_SCHEMA, "Empty schema");
		}

		if (!specified && (!keyFirst || keyEntryList.isEmpty())) {
			int order = 0;
			for (Entry entry : entryList) {
				entry.order = order;
				order++;
			}
			return;
		}

		final List<Entry> orgList = new ArrayList<Entry>(entryList);
		Collections.fill(entryList, null);
		int rest = orgList.size();
		for (Entry entry : orgList) {
			final int order = entry.order;
			if (order >= 0) {
				if (order >= orgList.size() || entryList.get(order) != null) {
					throw new GSException(
							GSErrorCode.ILLEGAL_SCHEMA, "Illegal order");
				}
				else if (keyUnified && entry.keyType &&
						order >= keyEntryList.size()) {
					throw new GSException(
							GSErrorCode.ILLEGAL_SCHEMA, "Illegal key order");
				}
				entryList.set(order, entry);
				rest--;
			}
		}

		if (rest > 0) {
			ListIterator<Entry> it = entryList.listIterator();
			boolean keyConsumed = false;
			if (keyFirst && !keyEntryList.isEmpty()) {
				for (Entry keyEntry : keyEntryList) {
					if (keyEntry.order >= 0) {
						continue;
					}
					while (it.next() != null) {
					}
					keyEntry.order = it.previousIndex();
					it.set(keyEntry);
					rest--;
					keyConsumed = true;
				}
			}
			for (Entry entry : orgList) {
				if ((entry.keyType && keyConsumed) || entry.order >= 0) {
					continue;
				}
				while (it.next() != null) {
				}
				entry.order = it.previousIndex();
				it.set(entry);
				if (--rest == 0) {
					break;
				}
			}
		}

		if (rest != 0) {
			throw new Error();
		}
	}

	private void applyNullable(boolean nullableAllowed) throws GSException {
		Boolean nullableDefault = null;

		for (Class<?> type = rowType;
				type != null; type = type.getEnclosingClass()) {
			nullableDefault =
					Entry.findNullableFromAccessor(type, null, null, null);
			if (nullableDefault != null) {
				break;
			}
		}

		if (nullableDefault == null) {
			final AnnotatedElement target = rowType.getPackage();
			if (target != null) {
				nullableDefault = Entry.findNullableFromAccessor(
						target, null, null, null);
			}
		}

		for (Entry entry : entryList) {
			entry.setOptionByAccessors(nullableDefault, nullableAllowed);
		}
	}

	private static RowMapper makeKeyMapper(
			List<Entry> entryList, ContainerType containerType, Config config)
			throws GSException {
		final boolean forTimeSeries =
				(containerType == ContainerType.TIME_SERIES);

		List<Entry> keyEntryList = Collections.emptyList();
		Map<String, Entry> entryMap = Collections.emptyMap();
		CompositeKeyEntry compositeKeyEntry = null;
		boolean general = true;
		boolean nested = false;
		boolean flatten = false;

		for (Entry entry : entryList) {
			if (!entry.keyType) {
				continue;
			}
			if (entry.order != keyEntryList.size()) {
				if (restrictKeyOrderFirst) {
					throw new GSException(
							GSErrorCode.ILLEGAL_SCHEMA,
							"Key must be ordered before non key coulumns");
				}
				throw new Error();
			}

			final DetailElementType type = entry.getDetailType();
			if (type.isForArray()) {
				throw new GSException(
						GSErrorCode.UNSUPPORTED_KEY_TYPE,
						"Key type must not be array");
			}

			if (forTimeSeries) {
				if (type.base() != ElementType.TIMESTAMP) {
					throw new GSException(
							GSErrorCode.UNSUPPORTED_KEY_TYPE,
							"Illegal key type for time series (" +
							"type=" + type + ")");
				}
			}
			else if (!type.isForKey()) {
				if (type.isAny()) {
					throw new GSException(
							GSErrorCode.UNSUPPORTED_KEY_TYPE,
							"Key must not be any type");
				}
				else {
					throw new GSException(
							GSErrorCode.UNSUPPORTED_KEY_TYPE,
							"Illegal key type for collection (" +
							"type=" + type + ")");
				}
			}

			if (keyEntryList.isEmpty()) {
				keyEntryList = new ArrayList<Entry>();
				entryMap = new HashMap<String, Entry>();
			}

			general &=
					(entry.getterMethod == null && entry.rowTypeField == null &&
					entry.compositeKeyEntry == null);

			keyEntryList.add(entry);
			if (entry.columnName != null) {
				entryMap.put(
						normalizeSymbolUnchecked(entry.columnName), entry);
			}

			if (entry.compositeKeyEntry == null) {
				flatten = true;
			}
			else {
				compositeKeyEntry = entry.compositeKeyEntry;
				nested = true;
			}
		}

		if (keyEntryList.isEmpty()) {
			if (forTimeSeries) {
				throw new GSException(
						GSErrorCode.ILLEGAL_SCHEMA,
						"Key must be required for time series");
			}
			return null;
		}

		if (keyEntryList.size() > 1) {
			if (forTimeSeries) {
				throw new GSException(
						GSErrorCode.MULTIPLE_KEYS_FOUND,
						"Multiple keys found for time series");
			}
			else if (!general && !config.keyComposable) {
				throw new GSException(
						GSErrorCode.MULTIPLE_KEYS_FOUND,
						"Multiple keys found");
			}
		}

		if (flatten && nested) {
			throw new GSException(
					GSErrorCode.ILLEGAL_SCHEMA,
					"Both composite key and single key found");
		}

		final Class<?> rowType;
		final Constructor<?> constructor;
		if (compositeKeyEntry == null) {
			rowType = (general ? Row.Key.class : null);
			constructor = null;
		}
		else {
			rowType = compositeKeyEntry.keyClass;
			constructor = compositeKeyEntry.keyConstructor;
		}
		final boolean nullableAllowed = false;
		return new RowMapper(
				rowType, constructor, entryMap, keyEntryList, null,
				containerType, nullableAllowed);
	}

	private ContainerInfo peekAggregationSchema(Cursor baseCursor)
			throws GSException {
		if (!baseCursor.hasNext()) {
			return null;
		}

		final Entry[] entryRef = new Entry[1];
		final Entry entry = new Entry(null);
		entry.detailType =
				decodeAggregationHead(baseCursor.asReadOnly(), entryRef);
		return new ContainerInfo(
				null, null, Arrays.asList(entry.getColumnInfo(false).toInfo()), false);
	}

	private void decodeAggregation(
			Cursor cursor, Object rowObj, FieldAccessMode accessMode)
			throws GSException {
		if (rowType == AggregationResult.class) {
			final DetailElementType type = decodeAggregationHead(cursor, null);
			final Set<ElementTypeAttribute> typeAttrs = type.attributes();

			final Object orgValue = getField(cursor, type, accessMode);
			final Object value;
			if (!typeAttrs.contains(ElementTypeAttribute.AGGREGATABLE)) {
				throw new GSException(
						GSErrorCode.UNSUPPORTED_FIELD_TYPE,
						"Unsupported aggregation result type (type=" +
						type + ")");
			}
			else if (typeAttrs.contains(ElementTypeAttribute.INTEGRAL)) {
				value = ((Number) orgValue).longValue();
			}
			else if (typeAttrs.contains(ElementTypeAttribute.FLOATING)) {
				value = ((Number) orgValue).doubleValue();
			}
			else {
				value = orgValue;
			}

			try {
				((AggregationResultImpl) rowObj).setValue(value);
			}
			catch (ClassCastException e) {
				throw new GSException(GSErrorCode.INTERNAL_ERROR,
						"Internal error by inconsistent aggregation result type", e);
			}

			return;
		}

		

		final Entry[] entryRef = new Entry[1];
		final DetailElementType type = decodeAggregationHead(cursor, null);
		final Entry entry = entryRef[0];

		if (type == entry.getDetailType()) {
			entry.decode(cursor, null, rowObj, accessMode);
		}
		else {
			final Object orgValue = getField(cursor, type, accessMode);
			if (!(orgValue instanceof Long || orgValue instanceof Double)) {
				throw new GSException(
						GSErrorCode.UNSUPPORTED_ROW_MAPPING,
						"Unacceptable result type");
			}
			final Object fieldObj;
			switch (type.base()) {
			case BYTE:
				fieldObj = (byte) (orgValue instanceof Long ?
						(long) (Long) orgValue : (double) (Double) orgValue);
				break;
			case SHORT:
				fieldObj = (short) (orgValue instanceof Long ?
						(long) (Long) orgValue : (double) (Double) orgValue);
				break;
			case INTEGER:
				fieldObj = (int) (orgValue instanceof Long ?
						(long) (Long) orgValue : (double) (Double) orgValue);
				break;
			case LONG:
				fieldObj = (long) (orgValue instanceof Long ?
						(long) (Long) orgValue : (double) (Double) orgValue);
				break;
			case FLOAT:
				fieldObj = (float) (orgValue instanceof Long ?
						(long) (Long) orgValue : (double) (Double) orgValue);
				break;
			case DOUBLE:
				fieldObj = (double) (orgValue instanceof Long ?
						(long) (Long) orgValue : (double) (Double) orgValue);
				break;
			default:
				throw new GSException(
						GSErrorCode.UNSUPPORTED_FIELD_TYPE,
						"Unacceptable result type");
			}
			entry.setFieldObj(rowObj, fieldObj, accessMode);
		}

		cursor.endRowInput();
	}

	private DetailElementType decodeAggregationHead(
			Cursor cursor, Entry[] entryRef) throws GSException {
		if (rowType == AggregationResult.class) {
			if (cursor.isRowIdIncluded()) {
				throw new GSException(
						GSErrorCode.UNSUPPORTED_ROW_MAPPING,
						"Illegal result type");
			}

			cursor.beginRowInput();
			final BasicBuffer in = cursor.getBuffer();
			if (acceptAggregationResultColumnId) {
				in.base().getInt();	
			}

			return DetailElementType.get(in);
		}

		if (!acceptAggregationResultColumnId) {
			throw new GSException(GSErrorCode.UNSUPPORTED_ROW_MAPPING, "");
		}

		

		cursor.beginRowInput();

		final BasicBuffer in = cursor.getBuffer();
		final int column = in.base().getInt();
		if (column < 0 || column >= entryList.size()) {
			if (column == -1) {
				throw new GSException(
						GSErrorCode.UNSUPPORTED_ROW_MAPPING,
						"Unable to map non columnwise aggregation (ex. COUNT())");
			}
			throw new GSException(
					GSErrorCode.UNSUPPORTED_ROW_MAPPING, "Illegal column ID");
		}
		entryRef[0] = getEntry(column);
		return DetailElementType.get(in);
	}

	private int getNullsByteSize(int fieldNum) {
		return (int)((fieldNum + 7) / 8);
	}

	private int getFixedRowPartSize(boolean rowIdIncluded, MappingMode mode) {
		int size = (rowIdIncluded ? Long.SIZE / Byte.SIZE : 0);
		if (mode == MappingMode.ROWWISE_SEPARATED_V2) { 
			size += getNullsByteSize(entryList.size());
		}
		boolean hasVarDataPart = false;
		for (final Entry entry : entryList) {
			size += entry.getDetailType().getFixedEncodedSize(mode);
			if (!hasVarDataPart && entry.getDetailType().hasVarDataPart()) {
				hasVarDataPart = true;
			}
		}
		if (mode == MappingMode.ROWWISE_SEPARATED_V2 && hasVarDataPart) {
			
			size += Long.SIZE / Byte.SIZE;
		}
		return size;
	}

	private int getFixedFieldPartSize(int columnId, MappingMode mode) {
		return getEntry(columnId).getDetailType().getFixedEncodedSize(mode);
	}

	private int getColumnCount() {
		return entryList.size();
	}

	private Entry getEntry(int index) {
		return entryList.get(index);
	}

	private void getAllInitialValue(
			boolean nullable, Object[] dest, FieldAccessMode accessMode) {
		Object[] target = emptyFieldArray;
		if (target == null || nullable) {
			final int count = getColumnCount();
			target = (nullable ? dest : new Object[count]);
			for (int i = 0; i < count; i++) {
				target[i] = getEntry(i).getInitialObj(nullable, accessMode);
			}
			if (nullable) {
				return;
			}
			emptyFieldArray = target;
		}
		System.arraycopy(target, 0, dest, 0, target.length);
	}

	private boolean checkKeyComposed(Object keyObj) throws GSException {
		final RowMapper keyMapper = findKeyMapper();
		if (keyMapper == null) {
			throw new GSException(
					GSErrorCode.KEY_NOT_ACCEPTED,
					"Key must not be specified");
		}

		final boolean composed = (DetailElementType.resolve(
				keyObj.getClass(), true, false) == null);
		if (!composed && keyMapper.getColumnCount() > 1) {
			throw new GSException(
					GSErrorCode.KEY_NOT_ACCEPTED,
					"Unacceptable key type for composite key (" +
					"keyClass=" + keyObj.getClass().getName() + ")");
		}

		return composed;
	}

	private Object findEncodingKeyObj(
			Object specifiedKeyObj, Object rowObj) throws GSException {
		if (specifiedKeyObj != null) {
			return specifiedKeyObj;
		}

		final CompositeKeyEntry keyEntry = findCompositeKeyEntry();
		if (keyEntry == null) {
			return null;
		}

		final FieldAccessMode accessMode = FieldAccessMode.SPECIFIC;
		final Object keyObj = keyEntry.base.getFieldObj(rowObj, accessMode);
		if (keyObj == null) {
			throw new GSException(
					GSErrorCode.EMPTY_ROW_FIELD, "Empty composite row key");
		}
		return keyObj;
	}

	private Object preapereDecodingKeyObj(Object rowObj) throws GSException {
		final CompositeKeyEntry keyEntry = findCompositeKeyEntry();
		if (keyEntry == null) {
			return null;
		}

		final FieldAccessMode accessMode = FieldAccessMode.SPECIFIC;
		final Object existingKeyObj =
				keyEntry.base.getFieldObj(rowObj, accessMode);
		if (existingKeyObj != null) {
			return existingKeyObj;
		}

		final Object keyObj = constructObj(keyEntry.keyConstructor);
		keyEntry.base.setFieldObj(rowObj, keyObj, accessMode);
		return keyObj;
	}

	private static Object constructObj(Constructor<?> constructor)
			throws GSException {
		try {
			return constructor.newInstance();
		}
		catch (InstantiationException e) {
			throw new GSException(GSErrorCode.INTERNAL_ERROR, e);
		}
		catch (IllegalAccessException e) {
			throw new GSException(GSErrorCode.INTERNAL_ERROR, e);
		}
		catch (InvocationTargetException e) {
			throw new GSException(e);
		}
	}

	private CompositeKeyEntry findCompositeKeyEntry() throws GSException {
		final RowMapper keyMapper = findKeyMapper();
		if (keyMapper == null || keyMapper == this) {
			return null;
		}

		final CompositeKeyEntry keyEntry =
				keyMapper.getEntry(0).compositeKeyEntry;
		if (keyEntry == null) {
			return null;
		}

		return keyEntry;
	}

	private RowMapper resolveKeyMapper() throws GSException {
		final RowMapper keyMapper = findKeyMapper();
		if (keyMapper == null) {
			throw new GSException(
					GSErrorCode.KEY_NOT_FOUND, "Row key does not exist");
		}
		return keyMapper;
	}

	private RowMapper findKeyMapper() {
		if (keyMapper == null && rowType == Row.Key.class) {
			return this;
		}

		return keyMapper;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result +
				((entryList == null) ? 0 : entryList.hashCode());
		result = prime * result + (forTimeSeries ? 1231 : 1237);
		result = prime * result + (nullableAllowed ? 1231 : 1237);
		result = prime * result + ((rowType == null) ? 0 : rowType.hashCode());
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
		RowMapper other = (RowMapper) obj;
		if (entryList == null) {
			if (other.entryList != null)
				return false;
		}
		else if (!entryList.equals(other.entryList))
			return false;
		if (forTimeSeries != other.forTimeSeries)
			return false;
		if (nullableAllowed != other.nullableAllowed)
			return false;
		if (rowType == null) {
			if (other.rowType != null)
				return false;
		}
		else if (!rowType.equals(other.rowType))
			return false;
		return true;
	}

	private static void putArraySizeInfo(
			Cursor cursor, int elementSize, int elementCount) throws GSException {
		final BasicBuffer out = cursor.buffer;
		if (cursor.getMode() == MappingMode.ROWWISE_SEPARATED_V2) {
			putVarSize(out, elementSize * elementCount / Byte.SIZE +
					getEncodedLength((elementCount)));
			putVarSize(out, elementCount);
		}
		else {
			out.putInt((Integer.SIZE + elementSize * elementCount) / Byte.SIZE);
			out.putInt(elementCount);
		}
	}

	private static void putSingleField(
			BasicBuffer buffer, Object value, DetailElementType type,
			MappingMode mode) throws GSException {
		switch (type.base()) {
		case STRING:
			putString(buffer, (String) value,
					(mode == MappingMode.ROWWISE_SEPARATED_V2));
			break;
		case BYTE:
			buffer.put((Byte) value);
			break;
		case SHORT:
			buffer.putShort((Short) value);
			break;
		case INTEGER:
			buffer.putInt((Integer) value);
			break;
		case LONG:
			buffer.putLong((Long) value);
			break;
		case TIMESTAMP:
			buffer.putDate((Date) value);
			break;
		case MICRO_TIMESTAMP:
			putMicroTimestamp(buffer, (Timestamp) value);
			break;
		case NANO_TIMESTAMP:
			putFixedNanoTimestamp(buffer, (Timestamp) value);
			break;
		default:
			throw new IllegalArgumentException();
		}
	}

	private static Object getSingleField(
			BasicBuffer buffer, DetailElementType type) throws GSException {
		switch (type.base()) {
		case BYTE:
			return buffer.base().get();
		case SHORT:
			return buffer.base().getShort();
		case INTEGER:
			return buffer.base().getInt();
		case LONG:
			return buffer.base().getLong();
		case TIMESTAMP:
			return buffer.getDate();
		case MICRO_TIMESTAMP:
			return getMicroTimestamp(buffer);
		case NANO_TIMESTAMP:
			return getFixedNanoTimestamp(buffer);
		default:
			throw new GSException(
					GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by unexpected column type");
		}
	}

	private static void putField(
			Cursor cursor, Object fieldObj, DetailElementType type)
			throws GSException {
		cursor.beginField();
		final BasicBuffer out = cursor.buffer;
		if (type.isForArray()) {
			cursor.beginVarDataOutput();
			switch (type.base()) {
			case STRING: {
				final String[] rawArray = (String[]) fieldObj;
				final int orgPos = out.base().position();
				if (cursor.isVarSizeMode()) {
					final int estimatedTotalSize = rawArray.length * 32;
					final int estimatedHeadSize =
							getEncodedLength(estimatedTotalSize);

					out.prepare(MAX_VAR_SIZE_LENGTH * 2);
					putVarSizePrepared(out, estimatedTotalSize);
					putVarSizePrepared(out, rawArray.length);

					for (String element : rawArray) {
						putString(out, element, true);
					}

					int endPos = out.base().position();
					final int totalSize = endPos - (orgPos + estimatedHeadSize);
					final int actualHeadSize = getEncodedLength(totalSize);

					if (estimatedHeadSize != actualHeadSize) {
						out.prepare(actualHeadSize - estimatedHeadSize);
						final byte[] rawBuf = out.base().array();
						System.arraycopy(
								rawBuf, orgPos + estimatedHeadSize,
								rawBuf, orgPos + actualHeadSize, totalSize);
						endPos = orgPos + (actualHeadSize + totalSize);
					}

					out.base().position(orgPos);
					putVarSizePrepared(out, totalSize);
					out.base().position(endPos);
				}
				else {
					out.putInt(0);	
					out.putInt(rawArray.length);
					for (int i = 0; i < rawArray.length; i++) {
						out.putString(rawArray[i]);
					}

					final int endPos = out.base().position();
					out.base().position(orgPos);
					out.putInt(endPos - (orgPos + Integer.SIZE / Byte.SIZE));	
					out.base().position(endPos);
				}
				break;
			}
			case BOOL: {
				boolean[] rawArray = (boolean[]) fieldObj;
				putArraySizeInfo(cursor, Byte.SIZE, rawArray.length);
				for (int i = 0; i < rawArray.length; i++) {
					out.putBoolean(rawArray[i]);
				}
				break;
			}
			case BYTE: {
				byte[] rawArray = (byte[]) fieldObj;
				putArraySizeInfo(cursor, Byte.SIZE, rawArray.length);
				out.prepare(rawArray.length);
				out.base().put(rawArray);
				break;
			}
			case SHORT: {
				short[] rawArray = (short[]) fieldObj;
				putArraySizeInfo(cursor, Short.SIZE, rawArray.length);
				for (int i = 0; i < rawArray.length; i++) {
					out.putShort(rawArray[i]);
				}
				break;
			}
			case INTEGER: {
				int[] rawArray = (int[]) fieldObj;
				putArraySizeInfo(cursor, Integer.SIZE, rawArray.length);
				for (int i = 0; i < rawArray.length; i++) {
					out.putInt(rawArray[i]);
				}
				break;
			}
			case LONG: {
				long[] rawArray = (long[]) fieldObj;
				putArraySizeInfo(cursor, Long.SIZE, rawArray.length);
				for (int i = 0; i < rawArray.length; i++) {
					out.putLong(rawArray[i]);
				}
				break;
			}
			case FLOAT: {
				float[] rawArray = (float[]) fieldObj;
				putArraySizeInfo(cursor, Float.SIZE, rawArray.length);
				for (int i = 0; i < rawArray.length; i++) {
					out.putFloat(rawArray[i]);
				}
				break;
			}
			case DOUBLE: {
				double[] rawArray = (double[]) fieldObj;
				putArraySizeInfo(cursor, Double.SIZE, rawArray.length);
				for (int i = 0; i < rawArray.length; i++) {
					out.putDouble(rawArray[i]);
				}
				break;
			}
			case TIMESTAMP: {
				Date[] rawArray = (Date[]) fieldObj;
				putArraySizeInfo(cursor, Long.SIZE, rawArray.length);
				for (int i = 0; i < rawArray.length; i++) {
					out.putDate(rawArray[i]);
				}
				break;
			}
			case GEOMETRY:
				throw new GSException(
						GSErrorCode.UNSUPPORTED_FIELD_TYPE,
						"Illegal array type");
			case BLOB:
				throw new GSException(
						GSErrorCode.UNSUPPORTED_FIELD_TYPE,
						"Illegal array type");
			default:
				throw new Error();
			}
			cursor.endVarData();
		}
		else if (!type.isAny()) {
			switch (type.base()) {
			case STRING:
				cursor.beginVarDataOutput();
				putString(out, (String) fieldObj, cursor.isVarSizeMode());
				cursor.endVarData();
				break;
			case BOOL:
				out.putBooleanPrepared((Boolean) fieldObj);
				break;
			case BYTE:
				out.base().put((Byte) fieldObj);
				break;
			case SHORT:
				out.base().putShort((Short) fieldObj);
				break;
			case INTEGER:
				out.base().putInt((Integer) fieldObj);
				break;
			case LONG:
				out.base().putLong((Long) fieldObj);
				break;
			case FLOAT:
				out.base().putFloat((Float) fieldObj);
				break;
			case DOUBLE:
				out.base().putDouble((Double) fieldObj);
				break;
			case TIMESTAMP:
				out.putDatePrepared((Date) fieldObj);
				break;
			case MICRO_TIMESTAMP:
				putMicroTimestamp(out, (Timestamp) fieldObj);
				break;
			case NANO_TIMESTAMP:
				putNanoTimestampPrepared(cursor, out, (Timestamp) fieldObj);
				break;
			case GEOMETRY:
				cursor.beginVarDataOutput();
				final int dataSize = GeometryUtils.getBytesLength((Geometry) fieldObj);
				if (cursor.isVarSizeMode()) {
					putVarSize(out, dataSize);
				}
				else {
					out.putInt(dataSize);
				}
				GeometryUtils.putGeometry(out, (Geometry) fieldObj);
				cursor.endVarData();
				break;
			case BLOB: {
				cursor.beginVarDataOutput();
				final byte[] rawArray;
				try {
					if (fieldObj instanceof BlobImpl) {
						rawArray = ((BlobImpl) fieldObj).getDataDirect();
					}
					else {
						final Blob blob = ((Blob) fieldObj);
							final long length = blob.length();
							if (length > Integer.MAX_VALUE) {
								throw new GSException(
										GSErrorCode.SIZE_VALUE_OUT_OF_RANGE,
										"Blob size limit exceeded");
							}
							if (length > 0) {
								rawArray = blob.getBytes(1, (int) length);
							}
							else {
								rawArray = new byte[0];
							}
					}
				}
				catch (SQLException e) {
					throw new GSException(e);
				}
				if (cursor.isVarSizeMode()) {
					putVarSize(out, rawArray.length);
				}
				else {
					out.putInt(rawArray.length);
				}
				out.prepare(rawArray.length);
				out.base().put(rawArray);
				cursor.endVarData();
				break;
			}
			default:
				throw new Error();
			}
		}
		else {
			if (fieldObj == null) {
				DetailElementType.ANY.put(out);
				out.base().putLong(0);
			}
			else {
				final DetailElementType actualElementType =
						DetailElementType.resolveByObject(fieldObj, true, true);
				actualElementType.put(out);

				final int lastPos = out.base().position();
				cursor.checkOutputAnyType(actualElementType);
				cursor.beginAnyData();
				putField(cursor, fieldObj, actualElementType);
				cursor.endAnyData();

				final int fixedGap = lastPos + Long.SIZE / Byte.SIZE -
						out.base().position();
				out.base().put(EMPTY_LONG_BYTES, 0, fixedGap);
			}
		}
	}

	private static Object getField(
			Cursor cursor, DetailElementType type, FieldAccessMode accessMode)
			throws GSException {
		cursor.beginField();
		final BasicBuffer in = cursor.buffer;
		final Object result;
		if (type.isForArray()) {
			cursor.beginVarDataInput();
			final int length;
			if (cursor.isVarSizeMode()) {
				getVarSize(in);	
				length = getVarSize(in);
			} else {
				in.base().getInt();	
				length = in.base().getInt();
			}
			switch (type.base()) {
			case STRING: {
				final String[] rawArray = new String[length];
				for (int i = 0; i < length; i++) {
					rawArray[i] = getString(in, cursor.isVarSizeMode());
				}
				result = rawArray;
				break;
			}
			case BOOL: {
				boolean[] rawArray = new boolean[length];
				for (int i = 0; i < length; i++) {
					rawArray[i] = in.getBoolean();
				}
				result = rawArray;
				break;
			}
			case BYTE: {
				byte[] rawArray = new byte[length];
				in.base().get(rawArray);
				result = rawArray;
				break;
			}
			case SHORT: {
				short[] rawArray = new short[length];
				for (int i = 0; i < length; i++) {
					rawArray[i] = in.base().getShort();
				}
				result = rawArray;
				break;
			}
			case INTEGER: {
				int[] rawArray = new int[length];
				for (int i = 0; i < length; i++) {
					rawArray[i] = in.base().getInt();
				}
				result = rawArray;
				break;
			}
			case LONG: {
				long[] rawArray = new long[length];
				for (int i = 0; i < length; i++) {
					rawArray[i] = in.base().getLong();
				}
				result = rawArray;
				break;
			}
			case FLOAT: {
				float[] rawArray = new float[length];
				for (int i = 0; i < length; i++) {
					rawArray[i] = in.base().getFloat();
				}
				result = rawArray;
				break;
			}
			case DOUBLE: {
				double[] rawArray = new double[length];
				for (int i = 0; i < length; i++) {
					rawArray[i] = in.base().getDouble();
				}
				result = rawArray;
				break;
			}
			case TIMESTAMP: {
				Date[] rawArray = new Date[length];
				for (int i = 0; i < length; i++) {
					rawArray[i] = in.getDate();
				}
				result = rawArray;
				break;
			}
			case GEOMETRY:
				throw new GSException(
						GSErrorCode.UNSUPPORTED_FIELD_TYPE,
						"Illegal array type");
			case BLOB:
				throw new GSException(
						GSErrorCode.UNSUPPORTED_FIELD_TYPE,
						"Illegal array type");
			default:
				throw new Error();
			}
			cursor.endVarData();
			return result;
		}
		else if (!type.isAny()) {
			switch (type.base()) {
			case STRING:
				cursor.beginVarDataInput();
				result = getString(in, cursor.isVarSizeMode());
				cursor.endVarData();
				return result;
			case BOOL:
				return in.getBoolean();
			case BYTE:
				return in.base().get();
			case SHORT:
				return in.base().getShort();
			case INTEGER:
				return in.base().getInt();
			case LONG:
				return in.base().getLong();
			case FLOAT:
				return in.base().getFloat();
			case DOUBLE:
				return in.base().getDouble();
			case TIMESTAMP:
				return in.getDate();
			case MICRO_TIMESTAMP: {
				final Timestamp ts = getMicroTimestamp(in);
				if (accessMode == FieldAccessMode.GENERAL_DIRECT) {
					return new PreciseTimestamp(ts);
				}
				return ts;
			}
			case NANO_TIMESTAMP: {
				final Timestamp ts = getNanoTimestamp(cursor, in);
				if (accessMode == FieldAccessMode.GENERAL_DIRECT) {
					return new PreciseTimestamp(ts);
				}
				return ts;
			}
			case GEOMETRY: {
				cursor.beginVarDataInput();
				final int length;
				if (cursor.isVarSizeMode()) {
					length = getVarSize(in);
				}
				else {
					length = in.base().getInt();
				}
				result = GeometryUtils.getGeometry(in, length);
				cursor.endVarData();
				return result;
			}
			case BLOB: {
				cursor.beginVarDataInput();
				final int length;
				if (cursor.isVarSizeMode()) {
					length = getVarSize(in);
				}
				else {
					length = in.base().getInt();
				}
				final byte[] rawArray = new byte[length];
				in.base().get(rawArray);
				final Blob blob;
				if (cursor.blobFactory == null) {
					blob = DIRECT_BLOB_FACTORY.createBlob(rawArray);
				}
				else {
					blob = cursor.blobFactory.createBlob(rawArray);
				}
				cursor.endVarData();
				return blob;
			}
			default:
				throw new Error();
			}
		}
		else {
			final DetailElementType actualType = DetailElementType.get(in);
			if (actualType.isAny()) {
				in.base().getLong();
				return null;
			}
			else {
				final int lastPos = in.base().position();
				cursor.beginAnyData();
				final Object value = getField(cursor, actualType, accessMode);
				cursor.endAnyData();

				final int fixedGap = lastPos + Long.SIZE / Byte.SIZE -
						in.base().position();
				if (fixedGap < 0 || fixedGap > Long.SIZE / Byte.SIZE) {
					throw new Error();
				}

				for (int r = fixedGap; r > 0; r--) {
					in.base().get();
				}
				return value;
			}
		}
	}

	public static String normalizeSymbol(String symbol, String typeName)
			throws GSException {
		checkSymbol(symbol, typeName);
		return normalizeSymbolUnchecked(symbol);
	}

	public static String normalizeSymbolUnchecked(String symbol) {
		return symbol.toLowerCase(Locale.ROOT);
	}

	public static void checkSymbol(String symbol, String typeName)
			throws GSException {
		if (symbol == null || symbol.isEmpty()) {
			throw new GSException(
					GSErrorCode.EMPTY_PARAMETER, "Empty " + typeName);
		}
		checkString(symbol, typeName);
	}

	public static void checkString(String value, String typeName)
			throws GSException {
		boolean highSurrogateLast = false;
		boolean surrogateIllegal = false;

		final int length = value.length();
		for (int i = 0; i < length; i++) {
			final char ch = value.charAt(i);
			if (ch == '\0') {
				throw new GSException(
						GSErrorCode.ILLEGAL_SYMBOL_CHARACTER,
						"Illegal '\\0' character found in " + typeName);
			}
			else if (highSurrogateLast) {
				if (!Character.isLowSurrogate(ch)) {
					surrogateIllegal = true;
					break;
				}
				highSurrogateLast = false;
			}
			else if (Character.isHighSurrogate(ch)) {
				if (highSurrogateLast) {
					surrogateIllegal = true;
					break;
				}
				highSurrogateLast = true;
			}
			else if (Character.isLowSurrogate(ch)) {
				surrogateIllegal = true;
				break;
			}
		}

		if (highSurrogateLast || surrogateIllegal) {
			throw new GSException(
					GSErrorCode.ILLEGAL_SYMBOL_CHARACTER,
					"Illegal surrogate character found in " + typeName);
		}
	}

	public static BlobFactory getDirectBlobFactory() {
		return DIRECT_BLOB_FACTORY;
	}

	public static class Config {

		public boolean anyTypeAllowed;

		public boolean nullableAllowed;

		public boolean keyExtensible;

		public boolean keyComposable;

		public Config(
				boolean anyTypeAllowed,
				boolean nullableAllowed,
				boolean keyExtensible,
				boolean keyComposable) {
			this.anyTypeAllowed = anyTypeAllowed;
			this.nullableAllowed = nullableAllowed;
			this.keyExtensible = keyExtensible;
			this.keyComposable = keyComposable;
		}

	}

	public enum SchemaFeatureLevel {

		
		LEVEL1,

		
		LEVEL2

		;

		public SchemaFeatureLevel merge(SchemaFeatureLevel another) {
			if (another != null && another.ordinal() > ordinal()) {
				return another;
			}
			return this;
		}

	}

	private static class Entry {

		private static final int COLUMN_FLAG_ARRAY = 1 << 0;

		private static final int COLUMN_FLAG_NOT_NULL = 1 << 2;

		String columnName;
		transient String nameByField;
		transient String nameByGetter;
		transient Field rowTypeField;
		transient Method getterMethod;
		transient Method setterMethod;
		transient CompositeKeyEntry compositeKeyEntry;

		DetailElementType detailType;
		boolean keyType;
		transient int order = -1;
		transient boolean orderSpecified;

		boolean columnNullable;
		boolean objectNullable;

		boolean initialValueSpecified;
		boolean initialValueNull;

		Entry(String columnName) {
			this.columnName = columnName;
		}

		void applyAccessibleObject(AccessibleObject ao) {
			keyType |= (ao.getAnnotation(RowKey.class) != null);
			ao.setAccessible(true);
		}

		boolean reduceByAccessors() throws GSException {
			if (getterMethod != null || setterMethod != null) {
				if (getterMethod != null && setterMethod != null) {
					rowTypeField = null;
					if (nameByGetter == null) {
						throw new Error();
					}
					columnName = nameByGetter;
					return true;
				}
				if (getterMethod == null &&
						findMappingAnnotations(setterMethod) ||
						setterMethod == null &&
						findMappingAnnotations(getterMethod)) {
					throw new GSException(
							GSErrorCode.ILLEGAL_SCHEMA,
							"Inconsistent annotation");
				}
				getterMethod = null;
				setterMethod = null;
			}

			if (rowTypeField != null) {
				if (nameByField == null) {
					throw new Error();
				}
				columnName = nameByField;
				return true;
			}

			return false;
		}

		static boolean findMappingAnnotations(AccessibleObject ao) {
			return ao.getAnnotation(RowField.class) != null ||
					ao.getAnnotation(Nullable.class) != null ||
					ao.getAnnotation(NotNull.class) != null;
		}

		void setOptionByAccessors(
				Boolean nullableDefault, boolean nullableAllowed)
				throws GSException {
			Boolean nullable = null;
			TimeUnit timePrecision = null;

			AccessibleObject lastFoundNullablity = null;
			AccessibleObject lastFoundTimePrecision = null;

			for (int i = 0; i < 3; i++) {
				final AccessibleObject target = (i == 0 ? rowTypeField :
						(i == 1 ? getterMethod : setterMethod));
				if (target == null) {
					continue;
				}

				{
					final Boolean next = findNullableFromAccessor(
							target, lastFoundNullablity, nullable, this);
					if (nullable == null && next != null) {
						lastFoundNullablity = target;
						nullable = next;
					}
				}

				{
					final TimeUnit next = findTimePrecisionFromAccessor(
							target, lastFoundTimePrecision, timePrecision,
							this);
					if (timePrecision == null && next != null) {
						lastFoundNullablity = target;
						timePrecision = next;
					}
				}
			}

			detailType = filterObjectType(detailType, timePrecision);
			columnNullable =
					filterNullable(nullable, nullableDefault, nullableAllowed);
		}

		static Boolean findNullableFromAccessor(
				AnnotatedElement target, AnnotatedElement lastFoundTarget,
				Boolean lastNullable, Entry entry) throws GSException {
			Boolean nullable = lastNullable;
			boolean curAccepted = false;
			do {
				if (target.getAnnotation(NotNull.class) != null) {
					if (nullable != null && nullable) {
						break;
					}
					nullable = false;
					curAccepted = true;
				}

				if (target.getAnnotation(Nullable.class) != null) {
					if (nullable != null && !nullable) {
						break;
					}
					nullable = true;
				}

				return nullable;
			}
			while (false);

			throw new GSException(
					GSErrorCode.ILLEGAL_SCHEMA,
					"Inconsistent nullability annotations specified (" +
					(entry == null ?
							"" : "column=" + entry.columnName + ", ") +
					"target=" + target +
					(!curAccepted && lastFoundTarget != null ?
							", conflictingTarget=" + lastFoundTarget : "") +
					")");
		}

		static TimeUnit findTimePrecisionFromAccessor(
				AnnotatedElement target, AnnotatedElement lastFoundTarget,
				TimeUnit lastPrecision, Entry entry) throws GSException {
			final TimePrecision curAnnotation =
					target.getAnnotation(TimePrecision.class);
			if (curAnnotation == null) {
				return lastPrecision;
			}

			final TimeUnit curPrecision = curAnnotation.value();
			if (curPrecision == lastPrecision || lastPrecision == null) {
				return curPrecision;
			}

			throw new GSException(
					GSErrorCode.ILLEGAL_SCHEMA,
					"Inconsistent time precision annotations specified (" +
					(entry == null ?
							"" : "column=" + entry.columnName + ", ") +
					"target=" + target +
					", conflictingTarget=" + lastFoundTarget +
					", precision=" + curPrecision +
					", conflictingPrecision=" + lastPrecision + ")");
		}

		static DetailElementType filterObjectType(
				DetailElementType baseType, TimeUnit timePrecision)
				throws GSException {
			final int precisionLevel =
					DetailElementType.resolvePrecisionLevel(timePrecision);
			final boolean forObject = true;
			return (baseType == null ?
					null : baseType.withPrecision(precisionLevel, forObject));
		}

		void setNullableGeneral(Boolean nullable, boolean nullableAllowed)
				throws GSException {
			columnNullable = filterNullable(nullable, null, nullableAllowed);
			objectNullable = true;
		}

		void setInitialValueNull(Boolean valueNull) throws GSException {
			if (valueNull != null) {
				if (valueNull && !columnNullable) {
					throw new GSException(
							GSErrorCode.ILLEGAL_SCHEMA,
							"Default value cannot set be null for " +
							"non-nullable column (" +
							"column=" + columnName + ")");
				}
				initialValueSpecified = true;
				initialValueNull = valueNull;
			}
		}

		boolean filterNullable(
				Boolean nullable, Boolean nullableDefault,
				boolean nullableAllowed) throws GSException {
			if (nullable != null && nullable) {
				if (!nullableAllowed) {
					throw new GSException(
							GSErrorCode.ILLEGAL_SCHEMA,
							"Nullable column is not currently available (" +
							"column=" + columnName + ")");
				}

				if (keyType) {
					throw new GSException(
							GSErrorCode.ILLEGAL_SCHEMA,
							"Row key cannot be null (" +
							"column=" + columnName + ")");
				}
			}

			if (nullable == null) {
				if (keyType) {
					return false;
				}
				if (nullableDefault != null) {
					return nullableDefault;
				}
				return nullableAllowed;
			}

			return nullable;
		}

		void setColumnType(DetailElementType type, int order) {
			this.detailType = type;
			this.order = order;
		}

		void setObjectType(Class<?> objectType) throws GSException {
			detailType = DetailElementType.resolve(objectType, false, false);
			objectNullable = !objectType.isPrimitive();
		}

		GSType getFullType() {
			return getDetailType().toFullType();
		}

		DetailElementType getDetailType() {
			return detailType;
		}

		Boolean getInitialValueNull() {
			if (initialValueSpecified) {
				return initialValueNull;
			}
			return null;
		}

		ColumnInfo.Builder getColumnInfo(boolean withKeyInfo) {
			final ColumnInfo.Builder info = new ColumnInfo.Builder();
			info.setName(columnName);
			info.setType(getFullType());
			info.setNullable(columnNullable);
			info.setDefaultValueNull(getInitialValueNull());
			info.setTimePrecision(getDetailType().getTimePrecision());
			return info;
		}

		void exportColumnSchema(BasicBuffer out) throws GSException {
			out.putString((columnName == null ? "" : columnName));

			final DetailElementType type = getDetailType();
			DetailElementType.of(type.base(), false).put(out);

			byte flags = 0;
			flags |= (type.isForArray() ? COLUMN_FLAG_ARRAY : 0);
			flags |= (columnNullable ? 0 : COLUMN_FLAG_NOT_NULL);
			out.put(flags);
		}

		void importColumnSchema(
				BasicBuffer in, int order, boolean nullableAllowed)
				throws GSException {
			columnName = in.getString();
			final ElementType baseType = DetailElementType.get(in).base();

			final byte flags = in.base().get();
			final boolean forArray = (flags & COLUMN_FLAG_ARRAY) != 0;
			columnNullable = (nullableAllowed &&
					(flags & COLUMN_FLAG_NOT_NULL) == 0);

			setColumnType(
					DetailElementType.resolve(baseType, forArray), order);
		}

		void importObjectMapping(
				Entry orgEntry, boolean orderIgnorable) throws GSException {
			if (order != orgEntry.order &&
					(orgEntry.orderSpecified || !orderIgnorable)) {
				throw new GSException(
						GSErrorCode.ILLEGAL_SCHEMA,
						"Inconsistent column order (name=" + columnName +
						", localOrder=" + orgEntry.order +
						", remoteOrder=" + order + ")");
			}
			order = orgEntry.order;

			if (!normalizeSymbolUnchecked(columnName).equals(
					normalizeSymbolUnchecked(orgEntry.columnName)) ||
					getDetailType() != orgEntry.getDetailType() ||
					columnNullable != orgEntry.columnNullable) {
				throw new GSException(
						GSErrorCode.ILLEGAL_SCHEMA,
						"Inconsistent remote column");
			}

			rowTypeField = orgEntry.rowTypeField;
			getterMethod = orgEntry.getterMethod;
			setterMethod = orgEntry.setterMethod;
			compositeKeyEntry = orgEntry.compositeKeyEntry;
			objectNullable = orgEntry.objectNullable;
		}

		void encode(
				Cursor cursor, Object keyObj, Object rowObj,
				FieldAccessMode accessMode) throws GSException {
			Object fieldObj = (!keyType || keyObj == null ?
					getFieldObj(rowObj, accessMode) : keyObj);

			if (fieldObj == null && !getDetailType().isAny()) {
				if (!columnNullable) {
					throw new NullPointerException(
							"Null field (columnName=" + columnName +
							", type=" + getFullType() + ")");
				}
				cursor.setNull(order);
				fieldObj = getInitialObj(false, accessMode);
			}

			putField(cursor, fieldObj, getDetailType());
		}

		void decode(
				Cursor cursor, Object keyObj, Object rowObj,
				FieldAccessMode accessMode) throws GSException {
			Object fieldObj = getField(cursor, getDetailType(), accessMode);
			if (cursor.isNull(order)) {
				if (objectNullable && columnNullable) {
					fieldObj = null;
				}
				else {
					fieldObj = getInitialObj(false, accessMode);
				}
			}
			setFieldObj(
					(compositeKeyEntry == null ? rowObj : keyObj),
					fieldObj, accessMode);
		}

		void decodeNoNull(
				Cursor cursor, Object keyObj, Object rowObj,
				FieldAccessMode accessMode) throws GSException {
			final Object fieldObj =
					getField(cursor, getDetailType(), accessMode);
			setFieldObj(
					(compositeKeyEntry == null ? rowObj : keyObj),
					fieldObj, accessMode);
		}

		Object getFieldObj(Object rowObj, FieldAccessMode accessMode)
				throws GSException {
			if (accessMode.isGeneral()) {
				return ((Row) rowObj).getValue(order);
			}

			try {
				if (rowTypeField != null) {
					return rowTypeField.get(rowObj);
				}
				else {
					return getterMethod.invoke(rowObj, new Object[0]);
				}
			}
			catch (IllegalAccessException e) {
				throw new GSException(GSErrorCode.INTERNAL_ERROR, e);
			}
			catch (InvocationTargetException e) {
				throw new GSException(e);
			}
		}

		void setFieldObj(
				Object rowObj, Object fieldObj, FieldAccessMode accessMode)
				throws GSException {
			if (accessMode.isGeneral()) {
				if (accessMode == FieldAccessMode.GENERAL_DIRECT) {
					((ArrayRow) rowObj).setAnyValueDirect(order, fieldObj);
				}
				else {
					((Row) rowObj).setValue(order, fieldObj);
				}
				return;
			}

			try {
				if (rowTypeField != null) {
					rowTypeField.set(rowObj, fieldObj);
				}
				else {
					setterMethod.invoke(rowObj, fieldObj);
				}
			}
			catch (IllegalAccessException e) {
				throw new GSException(GSErrorCode.INTERNAL_ERROR, e);
			}
			catch (InvocationTargetException e) {
				throw new GSException(e);
			}
		}

		Object getInitialObj(boolean nullable, FieldAccessMode accessMode) {
			if (nullable && (accessMode.isGeneral() || objectNullable) &&
					columnNullable && initialValueNull) {
				return null;
			}

			return getDetailType().getEmptyObjectAccessor().get(accessMode);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result +
					((columnName == null) ? 0 : columnName.hashCode());
			result = prime * result + (columnNullable ? 1231 : 1237);
			result = prime * result +
					((detailType == null) ? 0 : detailType.hashCode());
			result = prime * result + (initialValueNull ? 1231 : 1237);
			result = prime * result + (initialValueSpecified ? 1231 : 1237);
			result = prime * result + (keyType ? 1231 : 1237);
			result = prime * result + (objectNullable ? 1231 : 1237);
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
			Entry other = (Entry) obj;
			if (columnName == null) {
				if (other.columnName != null)
					return false;
			}
			else if (!columnName.equals(other.columnName))
				return false;
			if (columnNullable != other.columnNullable)
				return false;
			if (detailType == null) {
				if (other.detailType != null)
					return false;
			}
			else if (!detailType.equals(other.detailType))
				return false;
			if (initialValueNull != other.initialValueNull)
				return false;
			if (initialValueSpecified != other.initialValueSpecified)
				return false;
			if (keyType != other.keyType)
				return false;
			if (objectNullable != other.objectNullable)
				return false;
			return true;
		}

	}

	private static class CompositeKeyEntry {
		final Entry base;
		final Class<?> keyClass;
		final Constructor<?> keyConstructor;
		CompositeKeyEntry(
				Entry base, Class<?> keyClass, Constructor<?> keyConstructor) {
			this.base = base;
			this.keyClass = keyClass;
			this.keyConstructor = keyConstructor;
		}
	}

	public class Cursor {

		private BasicBuffer buffer;

		private final int rowCount;

		private final MappingMode mode;

		private final boolean rowIdIncluded;

		private final BlobFactory blobFactory;

		private int rowIndex = -1;

		private int fieldIndex = -1;

		private final int fixedRowPartSize;

		private final int topPos;

		private final int varDataTop;

		private int partialVarDataOffset;

		private int varDataLast;

		private int pendingPos = -1;

		private long lastRowId = -1;

		private long varDataBaseOffset;

		private byte[] nullsBytes;

		private boolean nullFound;

		private boolean onAnyData;

		private SchemaFeatureLevel outputFeatureLevel;

		private Cursor(
				BasicBuffer buffer, MappingMode mode, int rowCount,
				boolean rowIdIncluded, BlobFactory blobFactory) {
			this.buffer = buffer;
			this.mode = mode;
			this.rowCount = rowCount;
			this.rowIdIncluded = rowIdIncluded;
			this.blobFactory = blobFactory;

			this.fixedRowPartSize = getFixedRowPartSize(rowIdIncluded, mode);
			this.topPos = buffer.base().position();
			switch (mode) {
			case ROWWISE_SEPARATED:
				varDataTop = topPos + fixedRowPartSize * rowCount;
				break;
			case ROWWISE_SEPARATED_V2:
				varDataTop = topPos + fixedRowPartSize * rowCount;
				nullsBytes = new byte[getNullsByteSize(entryList.size())];
				break;
			case COLUMNWISE_SEPARATED:
				if (rowIdIncluded) {
					throw new IllegalArgumentException();
				}
				varDataTop = topPos + fixedRowPartSize * rowCount;
				break;
			default:
				varDataTop = -1;
				break;
			}
			varDataLast = varDataTop;
			partialVarDataOffset = 0;
		}

		public Cursor asReadOnly() {
			return new Cursor(
					buffer.asReadOnlyBuffer(),
					mode, rowCount, rowIdIncluded, blobFactory);
		}

		public void setVarDataBaseOffset(long varDataBaseOffset) {
			this.varDataBaseOffset = varDataBaseOffset;
		}

		public int getRowCount() {
			return rowCount;
		}

		public long getLastRowId() {
			return lastRowId;
		}

		public void setRowId(long rowId) {
			lastRowId = rowId;
		}

		public boolean isRowIdIncluded() {
			return rowIdIncluded;
		}

		public boolean hasNext() {
			return (rowIndex + 1 < rowCount);
		}

		public boolean isInRange() {
			return (0 <= rowIndex && rowIndex < rowCount);
		}

		public void reset() {
			rowIndex = -1;
			fieldIndex = -1;

			buffer.base().position(topPos);
			if (varDataLast >= 0) {
				buffer.base().limit(varDataLast);
			}
			varDataLast = varDataTop;
			partialVarDataOffset = 0;
			pendingPos = -1;
			lastRowId = -1;

			if (nullsBytes != null) {
				Arrays.fill(nullsBytes, (byte) 0);
			}
			nullFound = false;
		}

		public void resetBuffer() {
			buffer = null;
		}

		public void decode(boolean general, Object rowObj) throws GSException {
			final FieldAccessMode accessMode = FieldAccessMode.of(general, rowObj);
			if (mode == MappingMode.AGGREGATED) {
				decodeAggregation(this, rowObj, accessMode);
			}
			else {
				if (rowType == AggregationResult.class) {
					throw new GSException(
							GSErrorCode.INTERNAL_ERROR,
							"Unexpected row type: AggregationResult");
				}

				final Object keyObj = preapereDecodingKeyObj(rowObj);
				beginRowInput();
				if (nullFound) {
					for (Entry entry : entryList) {
						entry.decode(this, keyObj, rowObj, accessMode);
					}
				}
				else {
					for (Entry entry : entryList) {
						entry.decodeNoNull(this, keyObj, rowObj, accessMode);
					}
				}
				endRowInput();
			}
		}

		public int getRowIndex() {
			return rowIndex;
		}

		public SchemaFeatureLevel getOutputFeatureLevel(RowMapper mapper) {
			return mapper.getFeatureLevel().merge(outputFeatureLevel);
		}

		private BasicBuffer getBuffer() {
			return buffer;
		}

		private MappingMode getMode() {
			return mode;
		}

		private boolean isVarSizeMode() {
			return (mode == MappingMode.ROWWISE_SEPARATED_V2);
		}

		private void prepareOutput() {
			final int limit = buffer.base().limit();
			if (limit < varDataTop) {
				buffer.prepare(varDataTop - buffer.base().position());
			}
		}

		private void skipRowInput(int skipCount) {
			if (rowIndex + skipCount < -1) {
				throw new IllegalStateException();
			}

			if (mode == MappingMode.COLUMNWISE_SEPARATED &&
					rowIdIncluded) {
				throw new IllegalStateException();
			}

			rowIndex += skipCount;
			buffer.base().position(topPos + fixedRowPartSize * (rowIndex + 1));
		}

		private void beginRowInput() throws GSException {
			rowIndex++;
			fieldIndex = -1;
			if (rowIdIncluded) {
				lastRowId = buffer.base().getLong();
			}
			if (mode == MappingMode.ROWWISE_SEPARATED_V2) {
				if (getVariableEntryCount() > 0) {
					final long varDataOffset = buffer.base().getLong();
					if (rowIndex == 0) {
						partialVarDataOffset = (int)varDataOffset; 
						
					}
					varDataLast = varDataTop + (int)varDataOffset - partialVarDataOffset;
					
					final int savePos = buffer.base().position();
					buffer.base().position(varDataLast);
					getVarSize(buffer);
					varDataLast = buffer.base().position();
					buffer.base().position(savePos);
				}
				buffer.base().get(nullsBytes);
				nullFound = false;
				for (int i = 0; i < nullsBytes.length; i++) {
					if (nullsBytes[i] != 0) {
						nullFound = true;
						break;
					}
				}
			}
		}

		private void beginRowOutput() {
			if (rowIndex < 0) {
				prepareOutput();
			}

			if (rowIdIncluded) {
				if (lastRowId <= 0) {
					throw new IllegalStateException();
				}
				buffer.putLong(lastRowId);
				lastRowId = -1;
			}
			if (mode == MappingMode.ROWWISE_SEPARATED_V2) {
				if (getVariableEntryCount() > 0) {
					final long varOffset = varDataLast - varDataTop;
					buffer.putLong(varOffset);
					
					final int savePos = buffer.base().position();
					buffer.base().position(varDataLast);
					putVarSize(buffer, getVariableEntryCount());
					varDataLast = buffer.base().position();
					buffer.base().position(savePos);
				}
				
				buffer.prepare(nullsBytes.length);
				buffer.base().put(nullsBytes);
			}
			rowIndex++;
			fieldIndex = -1;
		}

		private void endRowInput() {
			if (varDataLast >= 0 && rowIndex + 1 >= rowCount) {
				buffer.base().position(varDataLast);
			}
		}

		private void endRowOutput() {
			if (nullFound) {
				final int lastPos = buffer.base().position();

				final int nullsOffset = (Long.SIZE / Byte.SIZE) *
						((rowIdIncluded ? 1 : 0) +
						(getVariableEntryCount() > 0 ? 1 : 0));
				buffer.base().position(
						lastPos - fixedRowPartSize + nullsOffset);

				buffer.base().put(nullsBytes);
				buffer.base().position(lastPos);

				Arrays.fill(nullsBytes, (byte) 0);
				nullFound = false;
			}

			endRowInput();
		}

		private void beginField() {
			if (mode == MappingMode.COLUMNWISE_SEPARATED) {
				fieldIndex++;

				final int pos;
				if (fieldIndex == 0) {
					pos = topPos + getFixedFieldPartSize(0, mode) * rowIndex;
				}
				else {
					pos = buffer.base().position() +
							getFixedFieldPartSize(fieldIndex - 1, mode) * (
									rowCount - rowIndex - 1) +
							getFixedFieldPartSize(fieldIndex, mode) * rowIndex;
				}
				buffer.base().position(pos);
			}
		}

		private void beginVarDataInput() {
			if (varDataTop >= 0) {
				if (pendingPos >= 0) {
					throw new IllegalStateException();
				}
				if (mode == MappingMode.ROWWISE_SEPARATED_V2) {
					pendingPos = buffer.base().position();
					buffer.base().position(varDataLast);
				} else {
					final int offset =
							(int) (buffer.base().getLong() - varDataBaseOffset);
					pendingPos = buffer.base().position();
					buffer.base().position(varDataTop + offset);
				}
			}
		}

		private void beginVarDataOutput() {
			if (varDataTop >= 0) {
				if (pendingPos >= 0) {
					throw new IllegalStateException();
				}
				if (mode == MappingMode.ROWWISE_SEPARATED_V2) {
					pendingPos = buffer.base().position();
					buffer.base().position(varDataLast);
				} else {
					final int offset = varDataLast - varDataTop;
					buffer.base().putLong(offset);
					pendingPos = buffer.base().position();
					buffer.base().position(varDataLast);
				}
			}
		}

		private void endVarData() {
			if (varDataTop >= 0) {
				if (pendingPos < 0) {
					throw new IllegalStateException();
				}
				
				varDataLast = buffer.base().position();
				buffer.base().position(pendingPos);
				pendingPos = -1;
			}
		}

		private void checkOutputAnyType(DetailElementType type) {
			outputFeatureLevel = type.getFeatureLevel().merge(outputFeatureLevel);
		}

		private void beginAnyData() {
			onAnyData = true;
		}

		private void endAnyData() {
			onAnyData = false;
		}

		private boolean isOnAnyData() {
			return onAnyData;
		}

		private boolean isNull(int ordinal) {
			return (nullsBytes[ordinal / Byte.SIZE] &
					(1 << (ordinal % Byte.SIZE))) != 0;
		}

		private void setNull(int ordinal) {
			nullsBytes[ordinal / Byte.SIZE] |= (1 << (ordinal % Byte.SIZE));
			nullFound = true;
		}

	}

	private enum FieldAccessMode {

		SPECIFIC,
		GENERAL,
		GENERAL_DIRECT

		;

		static FieldAccessMode of(boolean general) {
			return (general ? GENERAL : SPECIFIC);
		}

		static FieldAccessMode of(boolean general, Object rowObj) {
			return (general ?
					(rowObj.getClass() == ArrayRow.class ?
							GENERAL_DIRECT : GENERAL) :
					SPECIFIC);
		}

		boolean isGeneral() {
			return this != SPECIFIC;
		}

	}

	private static abstract class EmptyObjectAccessor {

		public abstract Object get(FieldAccessMode accessMode);

		static EmptyObjectAccessor immutableOf(final Object obj) {
			return new EmptyObjectAccessor() {
				@Override
				public Object get(FieldAccessMode accessMode) {
					return obj;
				}
			};
		}

	}

	public interface Provider {

		public RowMapper getRowMapper();

	}

	private static class ArrayRow implements Row, Provider {

		private final RowMapper mapper;

		private final Object[] fieldArray;

		ArrayRow(
				RowMapper mapper, boolean initializing, boolean nullable)
				throws GSException {
			if (mapper.getContainerType() == null) {
				throw new GSException(GSErrorCode.UNSUPPORTED_OPERATION, "");
			}

			this.mapper = mapper;
			this.fieldArray = new Object[mapper.getColumnCount()];
			if (initializing) {
				mapper.getAllInitialValue(
						nullable, fieldArray, FieldAccessMode.GENERAL_DIRECT);
			}
		}

		private void setValueAs(
				int column, Object fieldValue, DetailElementType type,
				boolean onAny) throws GSException {
			try {
				if (type.isForArray()) {
					switch (type.base()) {
					case STRING:
						setStringArray(column, (String[]) fieldValue);
						break;
					case BOOL:
						setBoolArray(column, (boolean[]) fieldValue);
						break;
					case BYTE:
						setByteArray(column, (byte[]) fieldValue);
						break;
					case SHORT:
						setShortArray(column, (short[]) fieldValue);
						break;
					case INTEGER:
						setIntegerArray(column, (int[]) fieldValue);
						break;
					case LONG:
						setLongArray(column, (long[]) fieldValue);
						break;
					case FLOAT:
						setFloatArray(column, (float[]) fieldValue);
						break;
					case DOUBLE:
						setDoubleArray(column, (double[]) fieldValue);
						break;
					case TIMESTAMP:
						setTimestampArray(column, (Date[]) fieldValue);
						break;
					default:
						throw new Error();
					}
				}
				else if (!type.isAny()) {
					switch (type.base()) {
					case STRING:
						setAnyValueDirect(column, (String) fieldValue);
						break;
					case BOOL:
						setAnyValueDirect(column, (Boolean) fieldValue);
						break;
					case BYTE:
						setAnyValueDirect(column, (Byte) fieldValue);
						break;
					case SHORT:
						setAnyValueDirect(column, (Short) fieldValue);
						break;
					case INTEGER:
						setAnyValueDirect(column, (Integer) fieldValue);
						break;
					case LONG:
						setAnyValueDirect(column, (Long) fieldValue);
						break;
					case FLOAT:
						setAnyValueDirect(column, (Float) fieldValue);
						break;
					case DOUBLE:
						setAnyValueDirect(column, (Double) fieldValue);
						break;
					case TIMESTAMP:
						setAnyValueDirect(column, (Date) fieldValue);
						break;
					case GEOMETRY:
						setAnyValueDirect(column, (Geometry) fieldValue);
						break;
					case BLOB:
						setAnyValueDirect(
								column, shareBlob((Blob) fieldValue));
						break;
					case MICRO_TIMESTAMP:
						setAnyValueDirect(
								column, (onAny ? fieldValue :
										new PreciseTimestamp((Timestamp) fieldValue)));
						break;
					case NANO_TIMESTAMP:
						setAnyValueDirect(
								column, (onAny ? fieldValue :
										new PreciseTimestamp((Timestamp) fieldValue)));
						break;
					default:
						throw new Error();
					}
				}
				else {
					final Class<?> objectType = fieldValue.getClass();
					final DetailElementType resolvedType =
							DetailElementType.resolve(objectType, true, true);
					if (resolvedType.attributes.contains(
							ElementTypeAttribute.DEEP_COPY_ALWAYS)) {
						setValueAs(column, fieldValue, resolvedType, true);
					}
					else {
						setAnyValueDirect(column, fieldValue);
					}
				}
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(column, null, fieldValue, e);
			}
		}

		private void setAnyValueDirect(int column, Object fieldValue) {
			fieldArray[column] = fieldValue;
		}

		private Object getAnyValue(int column) throws GSException {
			try {
				return fieldArray[column];
			}
			catch (IndexOutOfBoundsException e) {
				throw errorColumnNumber(column, e);
			}
		}

		private void checkNullable(Entry entry) throws GSException {
			if (!entry.columnNullable && !entry.getDetailType().isAny()) {
				throw errorNull(entry.order, null);
			}
		}

		private boolean trySetNull(
				int column, Object fieldValue, DetailElementType elementType)
				throws GSException {
			final Entry entry = getEntry(column);

			if (fieldValue == null) {
				checkNullable(entry);
				checkType(entry, elementType);
				setAnyValueDirect(column, null);
				return true;
			}

			checkType(entry, elementType);
			return false;
		}

		private void checkType(int column, DetailElementType elementType)
				throws GSException {
			final Entry entry = getEntry(column);
			checkType(entry, elementType);
		}

		private void checkType(Entry entry, DetailElementType elementType)
				throws GSException {
			if (!entry.getDetailType().match(elementType, true)) {
				throw errorTypeUnmatch(entry.order, elementType, null, null);
			}
		}

		private void checkObjectArrayElements(
				int column, Object[] arrayElements) throws GSException {
			for (int i = 0; i < arrayElements.length; i++) {
				if (arrayElements[i] == null) {
					throw errorNull(column, i);
				}
			}
		}

		private Entry getEntry(int column) throws GSException {
			try {
				return mapper.getEntry(column);
			}
			catch (IndexOutOfBoundsException e) {
				throw errorColumnNumber(column, e);
			}
		}

		private GSException errorNull(int column, Integer arrayIndex) {
			final Entry entry = (column < mapper.getColumnCount() ?
					mapper.getEntry(column) : null);
			return new GSException(
					GSErrorCode.EMPTY_PARAMETER,
					"Null is not allowed" +
					(arrayIndex == null ? "" : " for array element") + " (" +
					(arrayIndex == null ?
							"" : "arrayIndex=" + arrayIndex + ", ") +
					(entry == null ?
							"" : "column=" + entry.columnName + ", ") +
					"columnNumber=" + column + ")");
		}

		private GSException errorColumnNumber(
				int column, IndexOutOfBoundsException cause) {
			return new GSException(
					GSErrorCode.ILLEGAL_PARAMETER,
					"Column number out of bounds (" +
					"columnNumber=" + column + ", " +
					"columnCount=" + mapper.getColumnCount() + ")",
					cause);
		}

		private GSException errorTypeUnmatch(
				int column, DetailElementType specifiedType,
				ClassCastException cause) {
			return errorTypeUnmatch(column, specifiedType, null, cause);
		}

		private GSException errorTypeUnmatch(
				int column, DetailElementType specifiedType,
				Object specifiedValue, ClassCastException cause) {
			final String specifiedTypeName = (specifiedType == null ?
					(specifiedValue == null ?
							"null" : specifiedValue.getClass().getName()) :
					specifiedType.name());
			final Entry entry = (column < mapper.getColumnCount() ?
					mapper.getEntry(column) : null);
			return new GSException(
					GSErrorCode.ILLEGAL_PARAMETER,
					"Column type unmatched (" +
					(specifiedTypeName == null ?
							"" : "specifiedType=" + specifiedTypeName + ", ") +
					(entry == null ?
							"" : "actualType=" + entry.getDetailType().name() +
							", ") +
					(entry == null ?
							"" : "column=" + entry.columnName + ", ") +
					"columnNumber=" + column + ")",
					cause);
		}

		private Date[] copyTimestampArray(Date[] src, int column)
				throws GSException {
			final Date[] dest = new Date[src.length];
			for (int i = 0; i < src.length; i++) {
				final long time;
				try {
					time = src[i].getTime();
				}
				catch (NullPointerException e) {
					throw errorNull(column, i);
				}
				dest[i] = new Date(time);
			}
			return dest;
		}

		private static Blob shareBlob(Blob src) throws GSException {
			try {
				return BlobImpl.share(src);
			}
			catch (SQLException e) {
				throw new GSException(e);
			}
		}

		@Override
		public ContainerInfo getSchema() throws GSException {
			return mapper.getContainerInfo();
		}

		@Override
		public void setValue(int column, Object fieldValue)
				throws GSException {
			if (fieldValue == null) {
				setNull(column);
				return;
			}

			final Entry entry = getEntry(column);
			setValueAs(column, fieldValue, entry.getDetailType(), false);
		}

		@Override
		public Object getValue(int column) throws GSException {
			final DetailElementType type = getEntry(column).getDetailType();
			if (type.isForArray()) {
				switch (type.base()) {
				case STRING:
					return getStringArray(column);
				case BOOL:
					return getBoolArray(column);
				case BYTE:
					return getByteArray(column);
				case SHORT:
					return getShortArray(column);
				case INTEGER:
					return getIntegerArray(column);
				case LONG:
					return getLongArray(column);
				case FLOAT:
					return getFloatArray(column);
				case DOUBLE:
					return getDoubleArray(column);
				case TIMESTAMP:
					return getTimestampArray(column);
				default:
					throw new Error();
				}
			}
			else if (!type.isAny()) {
				switch (type.base()) {
				case TIMESTAMP:
					return getTimestamp(column);
				case BLOB:
					return getBlob(column);
				case MICRO_TIMESTAMP:
					return getPreciseTimestamp(column);
				case NANO_TIMESTAMP:
					return getPreciseTimestamp(column);
				default:
					break;
				}
			}
			return getAnyValue(column);
		}

		@Override
		public void setString(int column, String fieldValue)
				throws GSException {
			if (trySetNull(column, fieldValue, DetailElementType.STRING)) {
				return;
			}
			setAnyValueDirect(column, fieldValue);
		}

		@Override
		public String getString(int column) throws GSException {
			final String src;
			try {
				src = (String) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(column, DetailElementType.STRING, e);
			}
			return (src == null ? null : src);
		}

		@Override
		public void setBool(int column, boolean fieldValue)
				throws GSException {
			checkType(column, DetailElementType.BOOL);
			setAnyValueDirect(column, fieldValue);
		}

		@Override
		public boolean getBool(int column) throws GSException {
			final Boolean src;
			try {
				src = (Boolean) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(column, DetailElementType.BOOL, e);
			}
			return (src == null ? false : src);
		}

		@Override
		public void setByte(int column, byte fieldValue)
				throws GSException {
			checkType(column, DetailElementType.BYTE);
			setAnyValueDirect(column, fieldValue);
		}

		@Override
		public byte getByte(int column) throws GSException {
			final Byte src;
			try {
				src = (Byte) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(column, DetailElementType.BYTE, e);
			}
			return (src == null ? 0 : src);
		}

		@Override
		public void setShort(int column, short fieldValue)
				throws GSException {
			checkType(column, DetailElementType.SHORT);
			setAnyValueDirect(column, fieldValue);
		}

		@Override
		public short getShort(int column) throws GSException {
			final Short src;
			try {
				src = (Short) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(column, DetailElementType.SHORT, e);
			}
			return (src == null ? 0 : src);
		}

		@Override
		public void setInteger(int column, int fieldValue)
				throws GSException {
			checkType(column, DetailElementType.INTEGER);
			setAnyValueDirect(column, fieldValue);
		}

		@Override
		public int getInteger(int column) throws GSException {
			final Integer src;
			try {
				src = (Integer) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(column, DetailElementType.INTEGER, e);
			}
			return (src == null ? 0 : src);
		}

		@Override
		public void setLong(int column, long fieldValue)
				throws GSException {
			checkType(column, DetailElementType.LONG);
			setAnyValueDirect(column, fieldValue);
		}

		@Override
		public long getLong(int column) throws GSException {
			final Long src;
			try {
				src = (Long) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(column, DetailElementType.LONG, e);
			}
			return (src == null ? 0 : src);
		}

		@Override
		public void setFloat(int column, float fieldValue)
				throws GSException {
			checkType(column, DetailElementType.FLOAT);
			setAnyValueDirect(column, fieldValue);
		}

		@Override
		public float getFloat(int column) throws GSException {
			final Float src;
			try {
				src = (Float) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(column, DetailElementType.FLOAT, e);
			}
			return (src == null ? 0 : src);
		}

		@Override
		public void setDouble(int column, double fieldValue)
				throws GSException {
			checkType(column, DetailElementType.DOUBLE);
			setAnyValueDirect(column, fieldValue);
		}

		@Override
		public double getDouble(int column) throws GSException {
			final Double src;
			try {
				src = (Double) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(column, DetailElementType.DOUBLE, e);
			}
			return (src == null ? 0 : src);
		}

		@Override
		public void setTimestamp(int column, Date fieldValue)
				throws GSException {
			if (trySetNull(column, fieldValue, DetailElementType.TIMESTAMP)) {
				return;
			}
			setAnyValueDirect(column, fieldValue);
		}

		@Override
		public Date getTimestamp(int column) throws GSException {
			final Date src;
			try {
				src = (Date) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(column, DetailElementType.TIMESTAMP, e);
			}
			return (src == null ? null : new Date(src.getTime()));
		}

		@Override
		public void setPreciseTimestamp(
				int column, Timestamp fieldValue) throws GSException {
			if (trySetNull(
					column, fieldValue, DetailElementType.NANO_TIMESTAMP)) {
				return;
			}
			setAnyValueDirect(
					column, (getEntry(column).getDetailType().isAny() ?
							fieldValue : new PreciseTimestamp(fieldValue)));
		}

		@Override
		public Timestamp getPreciseTimestamp(int column) throws GSException {
			final PreciseTimestamp src;
			try {
				src = (PreciseTimestamp) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(
						column, DetailElementType.NANO_TIMESTAMP, e);
			}
			return (src == null ? null : src.toTimestamp());
		}

		@Override
		public void setGeometry(int column, Geometry fieldValue)
				throws GSException {
			if (trySetNull(column, fieldValue, DetailElementType.GEOMETRY)) {
				return;
			}
			setAnyValueDirect(column, fieldValue);
		}

		@Override
		public Geometry getGeometry(int column) throws GSException {
			final Geometry src;
			try {
				src = (Geometry) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(column, DetailElementType.GEOMETRY, e);
			}
			return (src == null ? null : src);
		}

		@Override
		public void setBlob(int column, Blob fieldValue) throws GSException {
			if (trySetNull(column, fieldValue, DetailElementType.BLOB)) {
				return;
			}
			setAnyValueDirect(column, shareBlob(fieldValue));
		}

		@Override
		public Blob getBlob(int column) throws GSException {
			final BlobImpl src;
			try {
				src = (BlobImpl) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(column, DetailElementType.BLOB, e);
			}
			if (src == null) {
				return null;
			}
			return BlobImpl.shareDirect(src);
		}

		@Override
		public void setStringArray(int column, String[] fieldValue)
				throws GSException {
			if (trySetNull(column, fieldValue, DetailElementType.STRING_ARRAY)) {
				return;
			}
			final String[] dest = new String[fieldValue.length];
			System.arraycopy(fieldValue, 0, dest, 0, fieldValue.length);
			checkObjectArrayElements(column, dest);
			setAnyValueDirect(column, dest);
		}

		@Override
		public String[] getStringArray(int column) throws GSException {
			final String[] src;
			try {
				src = (String[]) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(
						column, DetailElementType.STRING_ARRAY, e);
			}
			if (src == null) {
				return null;
			}
			final String[] dest = new String[src.length];
			System.arraycopy(src, 0, dest, 0, src.length);
			return dest;
		}

		@Override
		public void setBoolArray(int column, boolean[] fieldValue)
				throws GSException {
			if (trySetNull(column, fieldValue, DetailElementType.BOOL_ARRAY)) {
				return;
			}
			final boolean[] dest = new boolean[fieldValue.length];
			System.arraycopy(fieldValue, 0, dest, 0, fieldValue.length);
			setAnyValueDirect(column, dest);
		}

		@Override
		public boolean[] getBoolArray(int column) throws GSException {
			final boolean[] src;
			try {
				src = (boolean[]) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(
						column, DetailElementType.BOOL_ARRAY, e);
			}
			if (src == null) {
				return null;
			}
			final boolean[] dest = new boolean[src.length];
			System.arraycopy(src, 0, dest, 0, src.length);
			return dest;
		}

		@Override
		public void setByteArray(int column, byte[] fieldValue)
				throws GSException {
			if (trySetNull(column, fieldValue, DetailElementType.BYTE_ARRAY)) {
				return;
			}
			final byte[] dest = new byte[fieldValue.length];
			System.arraycopy(fieldValue, 0, dest, 0, fieldValue.length);
			setAnyValueDirect(column, dest);
		}

		@Override
		public byte[] getByteArray(int column) throws GSException {
			final byte[] src;
			try {
				src = (byte[]) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(
						column, DetailElementType.BYTE_ARRAY, e);
			}
			if (src == null) {
				return null;
			}
			final byte[] dest = new byte[src.length];
			System.arraycopy(src, 0, dest, 0, src.length);
			return dest;
		}

		@Override
		public void setShortArray(int column, short[] fieldValue)
				throws GSException {
			if (trySetNull(column, fieldValue, DetailElementType.SHORT_ARRAY)) {
				return;
			}
			final short[] dest = new short[fieldValue.length];
			System.arraycopy(fieldValue, 0, dest, 0, fieldValue.length);
			setAnyValueDirect(column, dest);
		}

		@Override
		public short[] getShortArray(int column) throws GSException {
			final short[] src;
			try {
				src = (short[]) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(
						column, DetailElementType.SHORT_ARRAY, e);
			}
			if (src == null) {
				return null;
			}
			final short[] dest = new short[src.length];
			System.arraycopy(src, 0, dest, 0, src.length);
			return dest;
		}

		@Override
		public void setIntegerArray(int column, int[] fieldValue)
				throws GSException {
			if (trySetNull(column, fieldValue, DetailElementType.INTEGER_ARRAY)) {
				return;
			}
			final int[] dest = new int[fieldValue.length];
			System.arraycopy(fieldValue, 0, dest, 0, fieldValue.length);
			setAnyValueDirect(column, dest);
		}

		@Override
		public int[] getIntegerArray(int column) throws GSException {
			final int[] src;
			try {
				src = (int[]) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(
						column, DetailElementType.INTEGER_ARRAY, e);
			}
			if (src == null) {
				return null;
			}
			final int[] dest = new int[src.length];
			System.arraycopy(src, 0, dest, 0, src.length);
			return dest;
		}

		@Override
		public void setLongArray(int column, long[] fieldValue)
				throws GSException {
			if (trySetNull(column, fieldValue, DetailElementType.LONG_ARRAY)) {
				return;
			}
			final long[] dest = new long[fieldValue.length];
			System.arraycopy(fieldValue, 0, dest, 0, fieldValue.length);
			setAnyValueDirect(column, dest);
		}

		@Override
		public long[] getLongArray(int column) throws GSException {
			final long[] src;
			try {
				src = (long[]) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(
						column, DetailElementType.LONG_ARRAY, e);
			}
			if (src == null) {
				return null;
			}
			final long[] dest = new long[src.length];
			System.arraycopy(src, 0, dest, 0, src.length);
			return dest;
		}

		@Override
		public void setFloatArray(int column, float[] fieldValue)
				throws GSException {
			if (trySetNull(column, fieldValue, DetailElementType.FLOAT_ARRAY)) {
				return;
			}
			final float[] dest = new float[fieldValue.length];
			System.arraycopy(fieldValue, 0, dest, 0, fieldValue.length);
			setAnyValueDirect(column, dest);
		}

		@Override
		public float[] getFloatArray(int column) throws GSException {
			final float[] src;
			try {
				src = (float[]) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(
						column, DetailElementType.FLOAT_ARRAY, e);
			}
			if (src == null) {
				return null;
			}
			final float[] dest = new float[src.length];
			System.arraycopy(src, 0, dest, 0, src.length);
			return dest;
		}

		@Override
		public void setDoubleArray(int column, double[] fieldValue)
				throws GSException {
			if (trySetNull(column, fieldValue, DetailElementType.DOUBLE_ARRAY)) {
				return;
			}
			final double[] dest = new double[fieldValue.length];
			System.arraycopy(fieldValue, 0, dest, 0, fieldValue.length);
			setAnyValueDirect(column, dest);
		}

		@Override
		public double[] getDoubleArray(int column) throws GSException {
			final double[] src;
			try {
				src = (double[]) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(
						column, DetailElementType.DOUBLE_ARRAY, e);
			}
			if (src == null) {
				return null;
			}
			final double[] dest = new double[src.length];
			System.arraycopy(src, 0, dest, 0, src.length);
			return dest;
		}

		@Override
		public void setTimestampArray(int column, Date[] fieldValue)
				throws GSException {
			if (trySetNull(
					column, fieldValue, DetailElementType.TIMESTAMP_ARRAY)) {
				return;
			}
			setAnyValueDirect(column, copyTimestampArray(fieldValue, column));
		}

		@Override
		public Date[] getTimestampArray(int column) throws GSException {
			final Date[] src;
			try {
				src = (Date[]) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(
						column, DetailElementType.TIMESTAMP_ARRAY, e);
			}
			if (src == null) {
				return null;
			}
			return copyTimestampArray(src, column);
		}

		@Override
		public void setNull(int column) throws GSException {
			checkNullable(getEntry(column));
			setAnyValueDirect(column, null);
		}

		@Override
		public boolean isNull(int column) throws GSException {
			return getAnyValue(column) == null;
		}

		@Override
		public Row createRow() throws GSException {
			final ArrayRow dest = new ArrayRow(mapper, false, false);
			System.arraycopy(
					fieldArray, 0, dest.fieldArray, 0, fieldArray.length);
			return dest;
		}

		@Override
		public Key createKey() throws GSException {
			final RowMapper keyMapper = mapper.resolveKeyMapper();

			if (!restrictKeyOrderFirst) {
				throw new Error();
			}

			final ArrayRowKey dest = new ArrayRowKey(keyMapper, false, false);
			final ArrayRow destRow = dest;
			for (Entry entry : keyMapper.entryList) {
				destRow.fieldArray[entry.order] = fieldArray[entry.order];
			}
			return dest;
		}

		@Override
		public RowMapper getRowMapper() {
			return mapper;
		}

		void setRow(Row src) throws GSException {
			if (src instanceof ArrayRow) {
				final ArrayRow srcArrayRow = (ArrayRow) src;
				if (mapper == srcArrayRow.mapper) {
					System.arraycopy(
							srcArrayRow.fieldArray, 0, fieldArray, 0,
							fieldArray.length);
					return;
				}
			}
			final int columnCount = mapper.getColumnCount();
			for (int i = 0; i < columnCount; i++) {
				setValue(i, src.getValue(i));
			}
		}

		int getKeyHashCode() {
			checkKeyType();
			return Arrays.hashCode(fieldArray);
		}

		boolean equalsKey(Object another) {
			if (!(another instanceof ArrayRowKey)) {
				return false;
			}
			return Arrays.equals(fieldArray, ((ArrayRow) another).fieldArray);
		}

		private void checkKeyType() {
			if (mapper.rowType != Row.Key.class) {
				throw new IllegalStateException();
			}
			for (Entry entry : mapper.entryList) {
				if (!entry.getDetailType().isForKey()) {
					throw new IllegalStateException();
				}
			}
		}

	}

	private static class ArrayRowKey extends ArrayRow implements Row.Key {

		ArrayRowKey(
				RowMapper mapper, boolean initializing, boolean nullable)
				throws GSException {
			super(mapper, initializing, nullable);
		}

	}

	private static class PreciseTimestamp {

		private final Timestamp base;

		PreciseTimestamp(Timestamp base) {
			this.base = base;
		}

		Timestamp toTimestamp() {
			return ValueUtils.duplicatePreciseTimestamp(base);
		}

	}

	private static class IdenticalRowKey extends ArrayRowKey {

		final int hashCode;

		IdenticalRowKey(Row.Key key) throws GSException {
			super(
					getInstance(key, GENERAL_CONFIG).resolveKeyMapper(),
					true, true);
			setRow(key);
			hashCode = getKeyHashCode();
		}

		@Override
		public int hashCode() {
			return hashCode;
		}

		@Override
		public boolean equals(Object obj) {
			return equalsKey(obj);
		}

	}

	private static class Cache {

		private final Map<Class<?>, RowMapper> timeSeriesMap =
				new WeakHashMap<Class<?>, RowMapper>();

		private final Map<Class<?>, RowMapper> collectionMap =
				new WeakHashMap<Class<?>, RowMapper>();

		private final InternPool<RowMapper> internPool =
				new InternPool<RowMapper>();

		synchronized RowMapper getInstance(
				Class<?> rowType, ContainerType containerType, Config config)
				throws GSException {
			final Map<Class<?>, RowMapper> map =
					(containerType == ContainerType.TIME_SERIES ?
							timeSeriesMap : collectionMap);

			RowMapper mapper = map.get(rowType);
			if (mapper == null ||
					mapper.nullableAllowed != config.nullableAllowed) {
				final boolean keyInside = false;
				mapper = new RowMapper(
						rowType, containerType, keyInside, config);
				map.put(rowType, mapper);
				internPool.intern(mapper);
			}

			return mapper;
		}

		synchronized RowMapper intern(RowMapper mapper) {
			return internPool.intern(mapper);
		}

	}

	public enum KeyCategory {
		NONE,
		SINGLE,
		COMPOSITE
	}

	public static class Tool {

		private Tool() {
		}

		/*
		 * For c-client adapter
		 */

		public static Class<?> getRowType(RowMapper mapper) {
			return mapper.getRowType();
		}

		public static ContainerInfo getContainerSchema(RowMapper mapper) {
			return mapper.getContainerInfo();
		}

		public static RowMapper resolveMapper(
				ContainerInfo containerSchema) throws GSException {
			return getInstance(
					containerSchema.getType(), containerSchema,
					GENERAL_CONFIG);
		}

		public static int getColumnCount(RowMapper mapper) {
			return mapper.getColumnCount();
		}

		public static String getColumnName(RowMapper mapper, int index) {
			return mapper.getEntry(index).columnName;
		}

		public static GSType getElementType(RowMapper mapper, int index) {
			return mapper.getFieldElementType(index);
		}

		public static boolean isArrayColumn(RowMapper mapper, int index) {
			return mapper.isArray(index);
		}

		public static boolean isColumnNullable(RowMapper mapper, int index) {
			return mapper.getEntry(index).columnNullable;
		}

		public static int getPrecisionLevel(RowMapper mapper, int index) {
			return mapper.getEntry(index).getDetailType().getPrecisionLevel();
		}

		public static int getKeyColumnId(RowMapper mapper) {
			for (int i = 0; i < mapper.getColumnCount(); i++) {
				if (mapper.getEntry(i).keyType) {
					return i;
				}
			}
			return -1;
		}

		public static Object createRowObject(
				RowMapper mapper) throws GSException {
			return mapper.createRow(mapper.isGeneral());
		}

		public static Object getFieldObj(
				RowMapper mapper, int index, Object rowObj) throws GSException {
			return mapper.getEntry(index).getFieldObj(
					rowObj, mapper.getFieldAccessMode());
		}

		public static void setFieldObj(
				RowMapper mapper, int index, Object rowObj, Object fieldObj)
				throws GSException {
			mapper.getEntry(index).setFieldObj(
					rowObj, fieldObj, mapper.getFieldAccessMode());
		}

		public static Blob createBlob(byte[] bytes) throws GSException {
			return DIRECT_BLOB_FACTORY.createBlob(bytes);
		}

		

		public static GSType toArrayElementType(GSType type) {
			final DetailElementType detailType;
			try {
				detailType = DetailElementType.of(type, 0, false);
			}
			catch (GSException e) {
				throw new IllegalArgumentException(e);
			}

			if (!detailType.isForArray()) {
				return null;
			}
			return DetailElementType.of(detailType.base(), false).toFullType();
		}
	}

	public static abstract class BindingTypeFactory {

		public abstract
		<K, R, C extends Container<K, R>, D extends Container<?, ?>>
		BindType<K, R, C> create(
				Class<K> keyType, Class<R> rowType, Class<D> containerType)
				throws GSException;

	}

	public static class BindingTool {

		private static AtomicReference<BindingTypeFactory> TYPE_FACTORY =
				new AtomicReference<BindingTypeFactory>();

		public static <K, R, C extends Container<?, ?>>
		BindType<K, R, ? extends Container<K, R>> createBindType(
				Class<K> keyType, Class<R> rowType, Class<C> containerType)
				throws GSException {
			return createBindType(
					keyType, rowType, containerType,
					(BindType<K, R, ? extends Container<K, R>>) null);
		}

		public static <K, R>
		BindType<K, R, Collection<K, R>> createCollectionBindType(
				Class<K> keyType, Class<R> rowType) throws GSException {
			return createBindType(
					keyType, rowType, Collection.class,
					(BindType<K, R, Collection<K, R>>) null);
		}

		public static <R>
		BindType<Date, R, TimeSeries<R>> createTimeSeriesBindType(
				Class<R> rowType) throws GSException {
			return createBindType(
					Date.class, rowType, TimeSeries.class,
					(BindType<Date, R, TimeSeries<R>>) null);
		}

		public static <K, R>
		BindType<K, R, ? extends Container<K, R>> rebindKey(
				Class<K> keyClass,
				BindType<?, R, ? extends Container<?, ?>> bindType)
				throws GSException {
			return createBindType(
					keyClass, bindType.getRowClass(),
					bindType.getContainerClass());
		}

		public static <K, R>
		BindType<K, R, ? extends Container<K, R>> rebindRow(
				Class<R> rowClass,
				BindType<K, ?, ? extends Container<?, ?>> bindType)
				throws GSException {
			return createBindType(
					bindType.getKeyClass(), rowClass,
					bindType.getContainerClass());
		}

		public static <K, R extends Row.WithKey<K>> Class<K> resolveKeyClass(
				Class<R> rowClass) throws GSException {
			@SuppressWarnings("unchecked")
			final Class<K> keyClass = (Class<K>) findKeyClassGeneral(rowClass);
			if (keyClass == null) {
				throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
						"Row key class not found (rowClass=" +
						rowClass.getName() + ")");
			}
			return keyClass;
		}

		public static <K> Class<K> checkKeyClass(
				Class<K> keyClass, Class<?> rowClass) throws GSException {
			final Class<?> foundKeyClass = findKeyClassGeneral(rowClass);
			if (foundKeyClass != null && (keyClass != foundKeyClass)) {
				throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
						"Row key class unmatched (" +
						"specifiedKeyClass=" + keyClass.getName() +
						", keyClassFromRow=" + foundKeyClass.getName() +
						", rowClass=" + rowClass.getName() + ")");
			}
			return keyClass;
		}

		public static ContainerType findContainerType(
				BindType<?, ?, ?> bindType) {
			return findContainerType(bindType.getContainerClass());
		}

		public static ContainerType findContainerType(
				Class<? extends Container<?, ?>> containerClass) {
			if (Collection.class.isAssignableFrom(containerClass)) {
				return ContainerType.COLLECTION;
			}
			else if (TimeSeries.class.isAssignableFrom(containerClass)) {
				return ContainerType.TIME_SERIES;
			}
			else {
				return null;
			}
		}

		public static void setFactory(BindingTypeFactory factory) {
			if (!BindingTool.TYPE_FACTORY.compareAndSet(null, factory)) {
				throw new Error();
			}
		}

		private static
		<K, R, C extends Container<K, R>, D extends Container<?, ?>>
		BindType<K, R, C> createBindType(
				Class<K> keyType, Class<R> rowType, Class<D> containerType,
				BindType<K, R, C> baseType) throws GSException {
			return getFactory().<K, R, C, D>create(
					keyType, rowType, containerType);
		}

		private static BindingTypeFactory getFactory() {
			try {
				Container.BindType.of(Row.Key.class, Row.class);
			}
			catch (GSException e) {
				throw new Error();
			}
			final BindingTypeFactory factory = BindingTool.TYPE_FACTORY.get();
			if (factory == null) {
				throw new Error();
			}
			return factory;
		}

		private static Class<?> findKeyClassGeneral(Class<?> baseClass)
				throws GSException {
			if (baseClass == null) {
				return null;
			}
			for (Type anyType : baseClass.getGenericInterfaces()) {
				if (anyType instanceof Class<?>) {
					final Class<?> sub = findKeyClassGeneral((Class<?>) anyType);
					if (sub != null) {
						return sub;
					}
					continue;
				}
				else if (!(anyType instanceof ParameterizedType)) {
					continue;
				}
				final ParameterizedType type = (ParameterizedType) anyType;
				if (type.getRawType() != Row.WithKey.class) {
					continue;
				}
				final Type[] argList = type.getActualTypeArguments();
				if (argList.length != 1) {
					throw new GSException(GSErrorCode.ILLEGAL_PARAMETER, "");
				}
				if (!(argList[0] instanceof Class<?>)) {
					return null;
				}
				return (Class<?>) argList[0];
			}
			return findKeyClassGeneral(baseClass.getSuperclass());
		}

	}

	enum ElementTypeAttribute {
		FOR_ARRAY,
		FOR_ROW_KEY,
		INTEGRAL,
		FLOATING,
		NUMERIC,
		TIMESTAMP_FAMILY,
		AGGREGATABLE,
		DEEP_COPY_ALWAYS,
		IMMUTABLE,
		FEATURE_LEVEL2
	}

	enum ElementType {

		STRING,
		BOOL,
		BYTE,
		SHORT,
		INTEGER,
		LONG,
		FLOAT,
		DOUBLE,
		TIMESTAMP,
		GEOMETRY,
		BLOB,
		MICRO_TIMESTAMP,
		NANO_TIMESTAMP

		;

		static final Set<ElementType> ROW_KEY_TYPES =
				Collections.unmodifiableSet(EnumSet.of(
						STRING,
						INTEGER,
						LONG,
						TIMESTAMP,
						MICRO_TIMESTAMP,
						NANO_TIMESTAMP));

		static final Set<ElementType> INTEGRAL_TYPES =
				Collections.unmodifiableSet(EnumSet.of(
						BYTE,
						SHORT,
						INTEGER,
						LONG));

		static final Set<ElementType> FLOATING_TYPES =
				Collections.unmodifiableSet(EnumSet.of(
						FLOAT,
						DOUBLE));

		static final Set<ElementType> ALWAYS_DEEP_COPY_TYPES =
				Collections.unmodifiableSet(EnumSet.of(
						MICRO_TIMESTAMP,
						NANO_TIMESTAMP,
						BLOB));

		static final Set<ElementType> EXTRA_IMMUTABLE_TYPES =
				Collections.unmodifiableSet(EnumSet.of(
						STRING,
						BOOL,
						GEOMETRY));

		static final Set<ElementType> FEATURE_LEVEL2_TYPES =
				Collections.unmodifiableSet(EnumSet.of(
						MICRO_TIMESTAMP,
						NANO_TIMESTAMP));

		static Map<ElementType, GSType> makeFullTypeMap(
				boolean forArray) {
			final Map<ElementType, GSType> map =
					new EnumMap<ElementType, GSType>(ElementType.class);
			if (forArray) {
				map.put(STRING, GSType.STRING_ARRAY);
				map.put(BOOL, GSType.BOOL_ARRAY);
				map.put(BYTE, GSType.BYTE_ARRAY);
				map.put(SHORT, GSType.SHORT_ARRAY);
				map.put(INTEGER, GSType.INTEGER_ARRAY);
				map.put(LONG, GSType.LONG_ARRAY);
				map.put(FLOAT, GSType.FLOAT_ARRAY);
				map.put(DOUBLE, GSType.DOUBLE_ARRAY);
				map.put(TIMESTAMP, GSType.TIMESTAMP_ARRAY);
			}
			else {
				map.put(STRING, GSType.STRING);
				map.put(BOOL, GSType.BOOL);
				map.put(BYTE, GSType.BYTE);
				map.put(SHORT, GSType.SHORT);
				map.put(INTEGER, GSType.INTEGER);
				map.put(LONG, GSType.LONG);
				map.put(FLOAT, GSType.FLOAT);
				map.put(DOUBLE, GSType.DOUBLE);
				map.put(TIMESTAMP, GSType.TIMESTAMP);
				map.put(GEOMETRY, GSType.GEOMETRY);
				map.put(BLOB, GSType.BLOB);
				map.put(MICRO_TIMESTAMP, GSType.TIMESTAMP);
				map.put(NANO_TIMESTAMP, GSType.TIMESTAMP);
			}
			return map;
		}

		static Map<ElementType, Class<?>> makeObjectTypeMap(
				boolean primitive) {
			final Map<ElementType, Class<?>> map =
					new EnumMap<ElementType, Class<?>>(ElementType.class);
			if (primitive) {
				map.put(BOOL, boolean.class);
				map.put(BYTE, byte.class);
				map.put(SHORT, short.class);
				map.put(INTEGER, int.class);
				map.put(LONG, long.class);
				map.put(FLOAT, float.class);
				map.put(DOUBLE, double.class);
			}
			else {
				map.put(STRING, String.class);
				map.put(BOOL, Boolean.class);
				map.put(BYTE, Byte.class);
				map.put(SHORT, Short.class);
				map.put(INTEGER, Integer.class);
				map.put(LONG, Long.class);
				map.put(FLOAT, Float.class);
				map.put(DOUBLE, Double.class);
				map.put(TIMESTAMP, Date.class);
				map.put(GEOMETRY, Geometry.class);
				map.put(BLOB, Blob.class);
				map.put(MICRO_TIMESTAMP, Timestamp.class);
				map.put(NANO_TIMESTAMP, Timestamp.class);
			}
			return map;
		}

		static Map<ElementType, Integer> makeBaseFixedSizeMap() {
			final Map<ElementType, Integer> map =
					new EnumMap<ElementType, Integer>(ElementType.class);
			final int boolBitSize = Byte.SIZE;
			map.put(STRING, 0);
			map.put(BOOL, boolBitSize / Byte.SIZE);
			map.put(BYTE, Byte.SIZE / Byte.SIZE);
			map.put(SHORT, Short.SIZE / Byte.SIZE);
			map.put(INTEGER, Integer.SIZE / Byte.SIZE);
			map.put(LONG, Long.SIZE / Byte.SIZE);
			map.put(FLOAT, Float.SIZE / Byte.SIZE);
			map.put(DOUBLE, Double.SIZE / Byte.SIZE);
			map.put(TIMESTAMP, Long.SIZE / Byte.SIZE);
			map.put(GEOMETRY, 0);
			map.put(BLOB, 0);
			map.put(MICRO_TIMESTAMP, Long.SIZE / Byte.SIZE);
			map.put(NANO_TIMESTAMP, (Byte.SIZE + Long.SIZE) / Byte.SIZE);
			return map;
		}

		static Map<ElementType, EmptyObjectAccessor> makeEmptyObjectMap(
				boolean forArray) {
			final Map<ElementType, EmptyObjectAccessor> map =
					new EnumMap<ElementType, EmptyObjectAccessor>(
							ElementType.class);
			for (ElementType type : values()) {
				final EmptyObjectAccessor accessor =
						makeEmptyObjectAccessor(type, forArray);
				if (accessor != null) {
					map.put(type, accessor);
				}
			}
			return map;
		}

		static EmptyObjectAccessor makeEmptyObjectAccessor(
				ElementType type, boolean forArray) {
			final Object obj;
			switch (type) {
			case STRING:
				obj = (forArray ? new String[0] : "");
				break;
			case BOOL:
				obj = (forArray ? new boolean[0] : false);
				break;
			case BYTE:
				obj = (forArray ? new byte[0] : (byte) 0);
				break;
			case SHORT:
				obj = (forArray ? new short[0] : (short) 0);
				break;
			case INTEGER:
				obj = (forArray ? new int[0] : (int) 0);
				break;
			case LONG:
				obj = (forArray ? new long[0] : (long) 0);
				break;
			case FLOAT:
				obj = (forArray ? new float[0] : (float) 0);
				break;
			case DOUBLE:
				obj = (forArray ? new double[0] : (double) 0);
				break;
			case TIMESTAMP:
				if (forArray) {
					obj = new Date[0];
					break;
				}
				return new EmptyObjectAccessor() {
					@Override
					public Object get(FieldAccessMode accessMode) {
						return new Date(0);
					}
				};
			case GEOMETRY:
				obj = (forArray ? null : Geometry.valueOf("POINT(EMPTY)"));
				break;
			case BLOB:
				return (forArray ? null : new EmptyObjectAccessor() {
					@Override
					public Object get(FieldAccessMode accessMode) {
						return new BlobImpl();
					}
				});
			case MICRO_TIMESTAMP:
				return makeEmptyObjectAccessor(NANO_TIMESTAMP, forArray);
			case NANO_TIMESTAMP:
				return (forArray ? null : new EmptyObjectAccessor() {
					@Override
					public Object get(FieldAccessMode accessMode) {
						final Timestamp ts = new Timestamp(0);
						if (accessMode == FieldAccessMode.GENERAL_DIRECT) {
							return new PreciseTimestamp(ts);
						}
						return ts;
					}
				});
			default:
				throw new Error();
			}
			if (obj == null) {
				return null;
			}
			return EmptyObjectAccessor.immutableOf(obj);
		}

		static Map<ElementType, TimeUnit> makeTimePrecisionMap() {
			final Map<ElementType, TimeUnit> map =
					new EnumMap<ElementType, TimeUnit>(ElementType.class);
			map.put(TIMESTAMP, TimeUnit.MILLISECOND);
			map.put(MICRO_TIMESTAMP, TimeUnit.MICROSECOND);
			map.put(NANO_TIMESTAMP, TimeUnit.NANOSECOND);
			return map;
		}

		static Map<ElementType, Integer> makePrecisionLevelMap() {
			final Map<ElementType, Integer> map =
					new EnumMap<ElementType, Integer>(ElementType.class);
			map.put(TIMESTAMP, 3);
			map.put(MICRO_TIMESTAMP, 6);
			map.put(NANO_TIMESTAMP, 9);
			return map;
		}

		static Map<ElementType, ElementTypeInfo> makeBaseInfoMap(
				boolean forArray) {
			final Map<ElementType, GSType> fullTypeMap =
					makeFullTypeMap(forArray);
			final Map<ElementType, Class<?>> objectTypeMap =
					makeObjectTypeMap(false);
			final Map<ElementType, Class<?>> objectPrimitiveTypeMap =
					makeObjectTypeMap(true);
			final Map<ElementType, Integer> baseFixedSizeMap =
					makeBaseFixedSizeMap();
			final Map<ElementType, EmptyObjectAccessor> emptyObjectMap =
					makeEmptyObjectMap(forArray);
			final Map<ElementType, TimeUnit> timePrecisionMap = (forArray ?
					null : makeTimePrecisionMap());
			final Map<ElementType, Integer> precisionLevelMap = (forArray ?
					null : makePrecisionLevelMap());
			final Map<ElementType, ElementTypeInfo> map = new EnumMap<
					ElementType, ElementTypeInfo>(ElementType.class);
			for (ElementType type : ElementType.values()) {
				final ElementTypeInfo info = new ElementTypeInfo();
				info.baseType = type;
				info.fullType = fullTypeMap.get(type);
				if (forArray) {
					if (info.fullType == null) {
						continue;
					}
				}
				else {
					info.timePrecision = timePrecisionMap.get(type);
					if (info.timePrecision != null) {
						info.precisionLevel = precisionLevelMap.get(type);
					}
				}
				info.baseFixedSize = baseFixedSizeMap.get(type);
				info.objectType = objectTypeMap.get(type);
				info.objectPrimitiveType = objectPrimitiveTypeMap.get(type);
				info.emptyObjectAccessor = emptyObjectMap.get(type);
				info.attributes = type.makeAttributes(forArray, timePrecisionMap);
				map.put(type, info);
			}
			return map;
		}

		private Set<ElementTypeAttribute> makeAttributes(
				boolean forArray,
				Map<ElementType, TimeUnit> timePrecisionMap) {
			final Set<ElementTypeAttribute> attrs =
					EnumSet.noneOf(ElementTypeAttribute.class);

			if (forArray) {
				attrs.add(ElementTypeAttribute.FOR_ARRAY);
			}

			if (!forArray && ROW_KEY_TYPES.contains(this)) {
				attrs.add(ElementTypeAttribute.FOR_ROW_KEY);
			}

			final boolean integral = INTEGRAL_TYPES.contains(this);
			if (integral) {
				attrs.add(ElementTypeAttribute.INTEGRAL);
			}

			final boolean floating = FLOATING_TYPES.contains(this);
			if (floating) {
				attrs.add(ElementTypeAttribute.FLOATING);
			}

			final boolean numeric = (integral || floating);
			if (numeric) {
				attrs.add(ElementTypeAttribute.NUMERIC);
			}

			final boolean timestampFamily = (timePrecisionMap != null &&
					timePrecisionMap.containsKey(this));
			if (timestampFamily) {
				attrs.add(ElementTypeAttribute.TIMESTAMP_FAMILY);
			}

			if (!forArray && (numeric || timestampFamily)) {
				attrs.add(ElementTypeAttribute.AGGREGATABLE);
			}

			if (forArray || ALWAYS_DEEP_COPY_TYPES.contains(this)) {
				attrs.add(ElementTypeAttribute.DEEP_COPY_ALWAYS);
			}

			if (numeric || EXTRA_IMMUTABLE_TYPES.contains(this)) {
				attrs.add(ElementTypeAttribute.IMMUTABLE);
			}

			if (FEATURE_LEVEL2_TYPES.contains(this)) {
				attrs.add(ElementTypeAttribute.FEATURE_LEVEL2);
			}

			return Collections.unmodifiableSet(attrs);
		}

	}

	static class ElementTypeInfo {
		ElementType baseType;
		GSType fullType;
		TimeUnit timePrecision;
		int precisionLevel;
		int baseFixedSize;
		Class<?> objectType;
		Class<?> objectPrimitiveType;
		EmptyObjectAccessor emptyObjectAccessor =
				EmptyObjectAccessor.immutableOf(null);
		Set<ElementTypeAttribute> attributes = Collections.emptySet();
	}

	static class DetailElementType {

		private static final ElementType[] TYPE_CONSTANTS = ElementType.values();

		private static final Map<
				ElementType, DetailElementType> NORMAL_TYPE_MAP = makeTypeMap(false);

		private static final Map<
				ElementType, DetailElementType> ARRAY_TYPE_MAP = makeTypeMap(true);

		private static final Map<GSType, DetailElementType> FULL_TYPE_MAP =
				makeFullTypeMap(NORMAL_TYPE_MAP, ARRAY_TYPE_MAP);

		private static final Map<Class<?>, ElementType> OBJECT_TYPE_MAP =
				makeObjectTypeMap(NORMAL_TYPE_MAP, false);

		private static final Map<Class<?>, ElementType> INHERITABLE_OBJECT_TYPE_MAP =
				makeObjectTypeMap(NORMAL_TYPE_MAP, true);

		private static final Map<TimeUnit, Integer> TIME_PRECISION_MAP =
				makeTimePrecisionMap(NORMAL_TYPE_MAP);

		static final DetailElementType ANY = makeAny();

		static final DetailElementType STRING = of(ElementType.STRING, false);
		static final DetailElementType BOOL = of(ElementType.BOOL, false);
		static final DetailElementType BYTE = of(ElementType.BYTE, false);
		static final DetailElementType SHORT = of(ElementType.SHORT, false);
		static final DetailElementType INTEGER = of(ElementType.INTEGER, false);
		static final DetailElementType LONG = of(ElementType.LONG, false);
		static final DetailElementType FLOAT = of(ElementType.FLOAT, false);
		static final DetailElementType DOUBLE = of(ElementType.DOUBLE, false);
		static final DetailElementType TIMESTAMP = of(
				ElementType.TIMESTAMP, false);
		static final DetailElementType GEOMETRY = of(
				ElementType.GEOMETRY, false);
		static final DetailElementType BLOB = of(ElementType.BLOB, false);
		static final DetailElementType MICRO_TIMESTAMP = of(
				ElementType.MICRO_TIMESTAMP, false);
		static final DetailElementType NANO_TIMESTAMP = of(
				ElementType.NANO_TIMESTAMP, false);

		static final DetailElementType STRING_ARRAY = of(
				ElementType.STRING, true);
		static final DetailElementType BOOL_ARRAY = of(ElementType.BOOL, true);
		static final DetailElementType BYTE_ARRAY = of(ElementType.BYTE, true);
		static final DetailElementType SHORT_ARRAY = of(
				ElementType.SHORT, true);
		static final DetailElementType INTEGER_ARRAY = of(
				ElementType.INTEGER, true);
		static final DetailElementType LONG_ARRAY = of(ElementType.LONG, true);
		static final DetailElementType FLOAT_ARRAY = of(
				ElementType.FLOAT, true);
		static final DetailElementType DOUBLE_ARRAY = of(
				ElementType.DOUBLE, true);
		static final DetailElementType TIMESTAMP_ARRAY = of(
				ElementType.TIMESTAMP, true);

		private final ElementType baseType;
		private final GSType fullType;
		private final TimeUnit timePrecision;
		private final int precisionLevel;
		private final int baseFixedSize;
		private final Class<?> objectType;
		private final Class<?> objectPrimitiveType;
		private final EmptyObjectAccessor emptyObjectAccessor;
		private final Set<ElementTypeAttribute> attributes;
		private SortedMap<Integer, DetailElementType> precisionMap;
		private DetailElementType objectMappedType;
		private String name;

		private DetailElementType(ElementTypeInfo info) {
			this.baseType = info.baseType;
			this.fullType = info.fullType;
			this.timePrecision = info.timePrecision;
			this.precisionLevel = info.precisionLevel;
			this.baseFixedSize = info.baseFixedSize;
			this.objectType = info.objectType;
			this.objectPrimitiveType = info.objectPrimitiveType;
			this.emptyObjectAccessor = info.emptyObjectAccessor;
			this.attributes = info.attributes;
		}

		private static DetailElementType makeAny() {
			final ElementTypeInfo anyInfo = new ElementTypeInfo();

			final Map<ElementType, ElementTypeInfo> baseMap =
					newMap(ElementType.class, true);
			baseMap.put(null, anyInfo);

			return makeTypeMap(baseMap, true).values().iterator().next();
		}

		private static Map<ElementType, DetailElementType> makeTypeMap(
				boolean forArray) {
			final Map<ElementType, ElementTypeInfo> baseMap =
					ElementType.makeBaseInfoMap(forArray);
			return makeTypeMap(baseMap, false);
		}

		private static Map<ElementType, DetailElementType> makeTypeMap(
				Map<ElementType, ElementTypeInfo> baseMap, boolean forAny) {

			final Map<ElementType, DetailElementType> map =
					newMap(ElementType.class, forAny);
			for (ElementType baseType : baseMap.keySet()) {
				map.put(
						baseType,
						new DetailElementType(baseMap.get(baseType)));
			}

			setUpPrecisionMaps(map, forAny);
			setUpObjectMappedTypes(map);
			setUpNames(map);

			return map;
		}

		private static Map<GSType, DetailElementType> makeFullTypeMap(
				Map<ElementType, DetailElementType> normalTypeMap,
				Map<ElementType, DetailElementType> arrayTypeMap) {
			final Map<GSType, DetailElementType> map =
					new EnumMap<GSType, DetailElementType>(GSType.class);
			for (boolean forArray : new boolean[] { false, true }) {
				for (ElementType baseType : TYPE_CONSTANTS) {
					final DetailElementType type = (forArray ?
							arrayTypeMap : normalTypeMap).get(baseType);
					if (type == null) {
						continue;
					}
					final int precision = type.precisionMap.firstKey();
					map.put(type.fullType, type.precisionMap.get(precision));
				}
			}
			return map;
		}

		private static Map<Class<?>, ElementType> makeObjectTypeMap(
				Map<ElementType, DetailElementType> typeMap,
				boolean inheritable) {
			final Map<Class<?>, ElementType> map = (inheritable ?
					new LinkedHashMap<Class<?>, ElementType>() :
					new HashMap<Class<?>, ElementType>());

			final Map<Class<?>, SortedMap<Integer, DetailElementType>> baseMap =
					new HashMap<
							Class<?>, SortedMap<Integer, DetailElementType>>();
			for (ElementType elemType : typeMap.keySet()) {
				final DetailElementType type = typeMap.get(elemType);
				if (type != type.objectMappedType) {
					continue;
				}

				if (!inheritable) {
					for (Class<?> objectType : listObjectType(type)) {
						map.put(objectType, type.baseType);
					}
					continue;
				}

				if (Modifier.isFinal(type.objectType.getModifiers())) {
					continue;
				}

				Class<?> topObjectType = type.objectType;
				int level = 0;
				if (!Modifier.isInterface(type.objectType.getModifiers())) {
					for (;;) {
						final Class<?> next = topObjectType.getSuperclass();
						if (next == null || next == Object.class) {
							break;
						}
						topObjectType = next;
						level--;
					}
				}

				SortedMap<Integer, DetailElementType> subMap =
						baseMap.get(topObjectType);
				if (subMap == null) {
					subMap = new TreeMap<Integer, DetailElementType>();
					baseMap.put(topObjectType, subMap);
				}
				subMap.put(level, type);
			}

			for (Class<?> objectType : baseMap.keySet()) {
				for (DetailElementType type :
						baseMap.get(objectType).values()) {
					map.put(type.objectType, type.baseType);
				}
			}
			return map;
		}

		private static Map<TimeUnit, Integer> makeTimePrecisionMap(
				Map<ElementType, DetailElementType> baseMap) {
			final Map<TimeUnit, Integer> map =
					new EnumMap<TimeUnit, Integer>(TimeUnit.class);
			for (DetailElementType type : baseMap.values()) {
				final TimeUnit timePrecision = type.timePrecision;
				if (timePrecision == null) {
					continue;
				}
				map.put(timePrecision, type.precisionLevel);
			}
			return map;
		}

		private static void setUpPrecisionMaps(
				Map<ElementType, DetailElementType> map, boolean forAny) {
			final Map<GSType, SortedMap<Integer, DetailElementType>> totalMap =
					newMap(GSType.class, true);
			for (ElementType elemType : map.keySet()) {
				final DetailElementType type = map.get(elemType);
				SortedMap<Integer, DetailElementType> precisionMap =
						totalMap.get(type.fullType);
				if (precisionMap == null) {
					precisionMap = new TreeMap<Integer, DetailElementType>();
					totalMap.put(type.fullType, precisionMap);
				}
				precisionMap.put(type.precisionLevel, type);
			}

			for (ElementType elemType : map.keySet()) {
				final DetailElementType type = map.get(elemType);
				type.precisionMap = totalMap.get(type.fullType);
			}
		}

		private static void setUpObjectMappedTypes(
				Map<ElementType, DetailElementType> map) {
			final Map<Class<?>, DetailElementType> objectTypeMap =
					new HashMap<Class<?>, DetailElementType>();
			for (ElementType elemType : map.keySet()) {
				final DetailElementType type = map.get(elemType);
				for (Class<?> objectType : listObjectType(type)) {
					final DetailElementType mappedType =
							objectTypeMap.get(objectType);
					if (mappedType != null) {
						final boolean precisionLower = (
								type.precisionLevel <
								map.get(mappedType.base()).precisionLevel);
						if (precisionLower) {
							continue;
						}
					}
					objectTypeMap.put(objectType, type);
				}
			}

			for (ElementType elemType : map.keySet()) {
				final DetailElementType type = map.get(elemType);
				for (Class<?> objectType : listObjectType(type)) {
					type.objectMappedType = objectTypeMap.get(objectType);
				}
			}
		}

		private static void setUpNames(
				Map<ElementType, DetailElementType> map) {
			for (DetailElementType type : map.values()) {
				type.name = makeName(
						type.fullType, type.precisionLevel, type.precisionMap);
			}
		}

		private static String makeName(
				GSType fullType, int precisionLevel,
				SortedMap<Integer, DetailElementType> precisionMap) {
			final StringBuilder builder = new StringBuilder();
			builder.append((fullType == null ? "ANY" : fullType.name()));

			if (precisionMap.size() > 1 &&
					precisionLevel != precisionMap.firstKey()) {
				builder.append("(");
				builder.append(precisionLevel);
				builder.append(")");
			}

			return builder.toString();
		}

		private static List<Class<?>> listObjectType(DetailElementType type) {
			if (type.objectPrimitiveType == null) {
				return Arrays.<Class<?>>asList(type.objectType);
			}
			else {
				return Arrays.<Class<?>>asList(
						type.objectType, type.objectPrimitiveType);
			}
		}

		private static <K extends Enum<K>, V> Map<K, V> newMap(
				Class<K> keyClass, boolean forAny) {
			if (forAny) {
				return new HashMap<K, V>();
			}
			else {
				return new EnumMap<K, V>(keyClass);
			}
		}

		static DetailElementType of(ElementType base, boolean forArray) {
			final DetailElementType type =
					(forArray ? ARRAY_TYPE_MAP : NORMAL_TYPE_MAP).get(base);
			if (type == null) {
				if (forArray) {
					throw new IllegalArgumentException();
				}
				return ANY;
			}
			return type;
		}

		static DetailElementType of(
				GSType type, int precisionLevel, boolean anyTypeAllowed)
				throws GSException {
			DetailElementType detailType = FULL_TYPE_MAP.get(type);
			if (detailType == null) {
				if (type != null) {
					throw new IllegalArgumentException();
				}
				if (!anyTypeAllowed) {
					throw new GSException(
							GSErrorCode.EMPTY_PARAMETER,
							"Column type not specified");
				}
				detailType = ANY;
			}

			return detailType.withPrecision(precisionLevel, false);
		}

		static DetailElementType resolve(ElementType base, boolean forArray)
				throws GSException {
			try {
				return of(base, forArray);
			}
			catch (IllegalArgumentException e) {
				throw new GSException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by illegal option for column type", e);
			}
		}

		static DetailElementType resolve(
				Class<?> objectType, boolean subClassAllowed, boolean always)
				throws GSException {
			final boolean forArray = objectType.isArray();

			final Class<?> objectElementType;
			if (forArray) {
				objectElementType = objectType.getComponentType();
			}
			else {
				objectElementType = objectType;
			}

			ElementType elementType = OBJECT_TYPE_MAP.get(objectElementType);
			if (elementType == null && subClassAllowed && !forArray) {
				for (Map.Entry<Class<?>, ElementType> entry :
						INHERITABLE_OBJECT_TYPE_MAP.entrySet()) {
					if (entry.getKey().isAssignableFrom(objectElementType)) {
						elementType = entry.getValue();
						break;
					}
				}
			}

			final DetailElementType type;
			if (forArray) {
				final DetailElementType baseType =
						ARRAY_TYPE_MAP.get(elementType);
				if (baseType != null && baseType.objectPrimitiveType != null &&
						objectElementType != baseType.objectPrimitiveType) {
					elementType = null;
					type = null;
				}
				else if (baseType == null || !listObjectType(baseType).contains(
						objectElementType)) {
					type = null;
				}
				else {
					type = baseType;
				}
			}
			else {
				type = NORMAL_TYPE_MAP.get(elementType);
			}

			if (type != null) {
				return type;
			}
			else if (!always && elementType == null) {
				return null;
			}
			else {
				throw new GSException(
						GSErrorCode.UNSUPPORTED_FIELD_TYPE,
						"Unsupported field type (" +
						"className=" + objectType.getName() +
						", elementClassName=" + objectElementType.getName() +
						", forArray=" + forArray + ")");
			}
		}

		static DetailElementType resolveByObject(
				Object object, boolean subClassAllowed, boolean always)
				throws GSException {
			final DetailElementType type =
					resolve(object.getClass(), subClassAllowed, always);
			if (type == NANO_TIMESTAMP || type == MICRO_TIMESTAMP) {
				if (((Timestamp) object).getNanos() == 0) {
					return TIMESTAMP;
				}
			}
			return type;
		}

		static int resolvePrecisionLevel(TimePrecision timePrecisionAnnotation)
				throws GSException {
			final TimeUnit timePrecision = (timePrecisionAnnotation == null ?
					null : timePrecisionAnnotation.value());
			return resolvePrecisionLevel(timePrecision);
		}

		static int resolvePrecisionLevel(ColumnInfo info) throws GSException {
			return resolvePrecisionLevel(info.getTimePrecision());
		}

		static int resolvePrecisionLevel(TimeUnit timePrecision)
				throws GSException {
			final Integer level = TIME_PRECISION_MAP.get(timePrecision);
			if (level == null) {
				if (timePrecision == null) {
					return 0;
				}
				throw new GSException(
						GSErrorCode.ILLEGAL_SCHEMA,
						"Unsupported time precision (value=" + timePrecision + ")");
			}
			return level;
		}

		static List<DetailElementType> values() {
			final List<DetailElementType> values =
					new ArrayList<DetailElementType>();
			values.add(ANY);
			values.addAll(NORMAL_TYPE_MAP.values());
			values.addAll(ARRAY_TYPE_MAP.values());
			return values;
		}

		void put(BasicBuffer out) throws GSException {
			out.prepare(1);
			putPrepared(out);
		}

		void putPrepared(BasicBuffer out) throws GSException {
			if (isForArray()) {
				throw new GSException(GSErrorCode.INTERNAL_ERROR, "");
			}

			if (isAny()) {
				out.base().put(ANY_NULL_TYPE);
			}
			else {
				out.putByteEnumPrepared(base());
			}
		}

		static DetailElementType get(BasicBuffer in) throws GSException {
			final byte rawType = in.base().get();

			if (rawType == ANY_NULL_TYPE) {
				return DetailElementType.ANY;
			}
			else {
				final ElementType baseType;
				try {
					baseType = TYPE_CONSTANTS[rawType & 0xff];
				}
				catch (IndexOutOfBoundsException e) {
					throw new GSException(
							GSErrorCode.MESSAGE_CORRUPTED,
							"Protocol error by illegal element type", e);
				}
				return DetailElementType.of(baseType, false);
			}
		}

		ElementType base() {
			return baseType;
		}

		boolean isForArray() {
			return attributes.contains(ElementTypeAttribute.FOR_ARRAY);
		}

		boolean isForKey() {
			return attributes.contains(ElementTypeAttribute.FOR_ROW_KEY);
		}

		boolean isAny() {
			return (base() == null);
		}

		GSType toFullType() {
			return fullType;
		}

		boolean match(DetailElementType another, boolean forObject) {
			if (this == another) {
				return true;
			}
			else if (!forObject) {
				return false;
			}
			else {
				return (objectMappedType == another.objectMappedType);
			}
		}

		TimeUnit getTimePrecision() {
			return timePrecision;
		}

		int getPrecisionLevel() {
			return precisionLevel;
		}

		Set<Integer> getPrecisionLevelSet() {
			return Collections.unmodifiableSet(precisionMap.keySet());
		}

		DetailElementType withPrecision(int precisionLevel, boolean forObject)
				throws GSException {
			final DetailElementType dest;
			if (precisionLevel == 0) {
				dest = this;
			}
			else {
				dest = precisionMap.get(precisionLevel);
				if (dest == null) {
					throw new GSException(
							GSErrorCode.ILLEGAL_SCHEMA,
							"Unsupported precision option (type=" + this + ")");
				}
				else if (forObject && dest.objectMappedType != objectMappedType) {
					throw new GSException(
							GSErrorCode.ILLEGAL_SCHEMA,
							"Unacceptable object type for the precision " +
							"option (specifiedType=" + objectType.getName() +
							", expectedType=" + dest.objectType.getName() +
							")");
				}
			}
			return dest;
		}

		int getFixedEncodedSize(MappingMode mode) {
			if (isAny()) {
				return ((Byte.SIZE + Long.SIZE) / Byte.SIZE);
			}
			else if (isForArray() || baseFixedSize <= 0) {
				if (mode == MappingMode.ROWWISE_SEPARATED_V2) {
					return 0;
				}
				else {
					return Long.SIZE / Byte.SIZE;
				}
			}
			else {
				return baseFixedSize;
			}
		}

		int getBaseFixedSize() {
			return baseFixedSize;
		}

		boolean hasVarDataPart() {
			return (isAny() || isForArray() || baseFixedSize <= 0);
		}

		Class<?> getObjectType() {
			return objectType;
		}

		EmptyObjectAccessor getEmptyObjectAccessor() {
			return emptyObjectAccessor;
		}

		SchemaFeatureLevel getFeatureLevel() {
			if (attributes().contains(ElementTypeAttribute.FEATURE_LEVEL2)) {
				return SchemaFeatureLevel.LEVEL2;
			}
			return SchemaFeatureLevel.LEVEL1;
		}

		Set<ElementTypeAttribute> attributes() {
			return attributes;
		}

		String name() {
			return name;
		}

		@Override
		public String toString() {
			return name();
		}

		@Override
		public int hashCode() {
			return super.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			return super.equals(obj);
		}

	}

	public static class TypeUtils {

		public static Set<GSType> getKeyTypeSet() {
			final Set<GSType> typeSet = EnumSet.noneOf(GSType.class);
			for (DetailElementType detailType : DetailElementType.values()) {
				if (detailType.isForKey()) {
					typeSet.add(detailType.toFullType());
				}
			}
			return typeSet;
		}

		public static Set<Integer> getPrecisionLevelSet(GSType type) {
			final DetailElementType detailType;
			try {
				detailType = DetailElementType.of(type, 0, false);
			}
			catch (GSException e) {
				throw new IllegalStateException(e);
			}
			return detailType.getPrecisionLevelSet();
		}

		public static int resolvePrecisionLevel(
				GSType type, TimeUnit timePrecision) throws GSException {
			final int base =
					DetailElementType.resolvePrecisionLevel(timePrecision);
			return DetailElementType.of(type, base, false).getPrecisionLevel();
		}

		public static int resolvePrecisionLevel(
				Class<?> type, TimeUnit timePrecision) throws GSException {
			final int base =
					DetailElementType.resolvePrecisionLevel(timePrecision);
			return DetailElementType.resolve(type, false, true).withPrecision(
					base, true).getPrecisionLevel();
		}

		public static TimeUnit resolveTimePrecision(
				GSType type, int precisionLevel) {
			final DetailElementType detailType;
			try {
				detailType = DetailElementType.of(type, precisionLevel, false);
			}
			catch (GSException e) {
				throw new IllegalStateException(e);
			}
			return detailType.getTimePrecision();
		}

		public static void putFullColumnType(BasicBuffer out, ColumnInfo fullType)
				throws GSException {
			getDetailType(fullType).put(out);
		}

		public static ColumnInfo getFullColumnType(BasicBuffer in)
				throws GSException {
			final DetailElementType type = DetailElementType.get(in);
			final ColumnInfo.Builder builder = new ColumnInfo.Builder();

			builder.setType(type.toFullType());
			builder.setTimePrecision(type.getTimePrecision());
			return builder.toInfo();
		}

		public static void putSingleField(
				BasicBuffer out, ColumnInfo fullType, Object value)
				throws GSException {
			RowMapper.putSingleField(
					out, value, getDetailType(fullType),
					MappingMode.ROWWISE_SEPARATED_V2);
		}

		public static Object getSingleField(
				BasicBuffer in, ColumnInfo fullType) throws GSException {
			return RowMapper.getSingleField(in, getDetailType(fullType));
		}

		private static DetailElementType getDetailType(ColumnInfo info)
				throws GSException {
			final GSType fullType = info.getType();
			return DetailElementType.of(
					fullType,
					resolvePrecisionLevel(fullType, info.getTimePrecision()),
					false);
		}

	}

	static class ValueUtils {

		static final long HIGH_MICRO_UNIT = 4;

		static Timestamp duplicatePreciseTimestamp(Timestamp src) {
			final Timestamp dest = new Timestamp(src.getTime());
			dest.setNanos(src.getNanos());
			return dest;
		}

	}

	public static abstract class ValueDuplicator<T> {

		private final Class<T> bindingClass;

		private final RowMapper mapper;

		private final boolean single;

		private final boolean immutable;

		ValueDuplicator(
				Class<T> bindingClass, RowMapper mapper, boolean single,
				boolean immutable) {
			this.bindingClass = bindingClass;
			this.mapper = mapper;
			this.single = single;
			this.immutable = immutable;
		}

		public Class<T> getBindingClass() {
			return bindingClass;
		}

		public Class<?> getValueClass() {
			if (single) {
				return mapper.getEntry(0).getDetailType().getObjectType();
			}
			return mapper.getRowType();
		}

		public RowMapper getMapper() {
			return mapper;
		}

		public boolean isImmutable() {
			return immutable;
		}

		public abstract T duplicate(T src, boolean identical);

		public static ValueDuplicator<Row.Key> createForKey(RowMapper mapper) {
			return new ValueDuplicator<Row.Key>(
					Row.Key.class, mapper, false, false) {
				public Row.Key duplicate(Row.Key src, boolean identical) {
					try {
						if (identical) {
							return createIdenticalRowKey(src);
						}
						else {
							return src.createKey();
						}
					}
					catch (GSException e) {
						throw new IllegalStateException(e);
					}
				}
			};
		}

		public static <T> ValueDuplicator<T> createSingle(
				Class<T> bindingClass, RowMapper mapper) {
			if (mapper.getColumnCount() != 1) {
				throw new IllegalArgumentException();
			}

			final DetailElementType type = mapper.getEntry(0).getDetailType();
			if (type.attributes().contains(ElementTypeAttribute.IMMUTABLE)) {
				return createForImmutable(bindingClass, mapper);
			}

			switch (type.base()) {
			case TIMESTAMP:
				return createForTimestamp(bindingClass, mapper);
			case MICRO_TIMESTAMP:
				return createForPreciseTimestamp(bindingClass, mapper);
			case NANO_TIMESTAMP:
				return createForPreciseTimestamp(bindingClass, mapper);
			default:
				throw new IllegalArgumentException();
			}
		}

		private static <T> ValueDuplicator<T> createForImmutable(
				Class<T> bindingClass, RowMapper mapper) {
			final boolean single = true;
			final boolean immutable = true;
			return new ValueDuplicator<T>(
					bindingClass, mapper, single, immutable) {
				public T duplicate(T src, boolean identical) {
					return src;
				}
			};
		}

		private static <T> ValueDuplicator<T> createForTimestamp(
				Class<T> bindingClass, RowMapper mapper) {
			final boolean single = true;
			final boolean immutable = false;
			return new ValueDuplicator<T>(
					bindingClass, mapper, single, immutable) {
				public T duplicate(T src, boolean identical) {
					return getBindingClass().cast(
							new Date(((Date) src).getTime()));
				}
			};
		}

		private static <T> ValueDuplicator<T> createForPreciseTimestamp(
				Class<T> bindingClass, RowMapper mapper) {
			final boolean single = true;
			final boolean immutable = false;
			return new ValueDuplicator<T>(
					bindingClass, mapper, single, immutable) {
				public T duplicate(T src, boolean identical) {
					return getBindingClass().cast(
							ValueUtils.duplicatePreciseTimestamp(
									(Timestamp) src));
				}
			};
		}

	}

	
	
	
	
	
	
	
	

	static final long VAR_SIZE_1BYTE_THRESHOLD = 1L << (Byte.SIZE - 1);
	static final long VAR_SIZE_4BYTE_THRESHOLD = 1L << (Integer.SIZE - 2);
	static final long VAR_SIZE_8BYTE_THRESHOLD = 1L << (Long.SIZE - 2);

	static final boolean varSizeIs1Byte(byte val) {
		return ((val & 0x01) == 0x01);
	}
	static final boolean varSizeIs4Byte(byte val) {
		return ((val & 0x03) == 0x00);
	}
	static final boolean varSizeIs8Byte(byte val) {
		return ((val & 0x03) == 0x02);
	}

	static final int decode1ByteVarSize(byte byteVal) { 
		int val = byteVal;
		return (val & 0xff) >>> 1; 
	}
	static final int decode4ByteVarSize(int val) { 
		return val >>> 2;
	}
	static final long decode8ByteVarSize(long val) { 
		return val >>> 2;
	}

	static final byte encode1ByteVarSize(byte val) {
		return (byte) ((val << 1) | 0x1);
	}
	static final int encode4ByteVarSize(int val) {
		return val << 2;
	}
	static final long encode8ByteVarSize(long val) {
		return (val << 2) | 0x2;
	}

	static final int getEncodedLength(int val) {
		if (val < VAR_SIZE_1BYTE_THRESHOLD) {
			return 1;
		}
		else if (val < VAR_SIZE_4BYTE_THRESHOLD) {
			return 4;
		}
		else {
			return 8;
		}
	}

	public static void putSize(BasicBuffer out, int value, MappingMode mode) {
		if (mode == MappingMode.ROWWISE_SEPARATED_V2) {
			putVarSize(out, value);
		}
		else {
			out.putInt(value);
		}
	}

	static final void putVarSize(BasicBuffer out, int value) {
		if (value < VAR_SIZE_1BYTE_THRESHOLD) {
			out.put(encode1ByteVarSize((byte) value));
		}
		else if (value < VAR_SIZE_4BYTE_THRESHOLD) {
			out.putInt(encode4ByteVarSize(value));
		}
		else {
			out.putLong(encode8ByteVarSize(value));
		}
	}

	static void putVarSizePrepared(BasicBuffer out, int value) {
		if (value < VAR_SIZE_1BYTE_THRESHOLD) {
			out.base().put(encode1ByteVarSize((byte) value));
		}
		else if (value < VAR_SIZE_4BYTE_THRESHOLD) {
			out.base().putInt(encode4ByteVarSize(value));
		}
		else {
			out.base().putLong(encode8ByteVarSize(value));
		}
	}

	static final int getVarSize(BasicBuffer in) throws GSException {
		final byte first = in.base().get();
		if (varSizeIs1Byte(first)) {
			return decode1ByteVarSize(first);
		}
		else if (varSizeIs4Byte(first)) {
			in.base().position(in.base().position() - Byte.SIZE / Byte.SIZE);
			final int rawSize = in.base().getInt();
			return decode4ByteVarSize(rawSize);
		}
		else {
			in.base().position(in.base().position() - Byte.SIZE / Byte.SIZE);
			final long rawSize = in.base().getLong();
			final long decodedSize = decode8ByteVarSize(rawSize);
			if (decodedSize > Integer.MAX_VALUE) {
				throw new GSException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Decoded size = " + decodedSize);
			}
			else {
				return (int) decodedSize;
			}
		}
	}

	static void putString(BasicBuffer out, String value, boolean varSizeMode)
			throws GSException {
		if (varSizeMode) {
			final byte[] buf = value.getBytes(BasicBuffer.DEFAULT_CHARSET);
			out.prepare(MAX_VAR_SIZE_LENGTH + buf.length);
			putVarSizePrepared(out, buf.length);

			if (STRING_FIELD_ENCODING_STRICT) {
				final int pos = out.base().position();
				final byte[] dest = out.base().array();
				for (int i = 0; i < buf.length; i++) {
					if (buf[i] == 0) {
						throw BasicBuffer.errorNullCharacter();
					}
					dest[pos + i] = buf[i];
				}
				out.base().position(pos + buf.length);
			}
			else {
				out.base().put(buf);
			}
		}
		else {
			out.putString(value);
		}
	}

	static String getString(BasicBuffer in, boolean varSizeMode) throws GSException {
		final int bytesLength =
				varSizeMode ? getVarSize(in) : in.base().getInt();

		final byte[] buf = new byte[bytesLength];
		in.base().get(buf);
		return new String(buf, 0, buf.length, BasicBuffer.DEFAULT_CHARSET);
	}

	static void putMicroTimestamp(BasicBuffer out, Timestamp ts)
			throws GSException {
		out.prepare(DetailElementType.MICRO_TIMESTAMP.getBaseFixedSize());
		putMicroTimestampPrepared(out, ts);
	}

	static void putMicroTimestampPrepared(BasicBuffer out, Timestamp ts)
			throws GSException {
		final long time = ts.getTime();
		if (time < 0 || time > Long.MAX_VALUE / 1000) {
			throw new GSException(
					GSErrorCode.UNSUPPORTED_ROW_MAPPING,
					"Unsupported value range for TIMESTAMP(6)");
		}

		final long micros =
				time * 1000 + (ts.getNanos() / 1000) % 1000;
		out.base().putLong(micros);
	}

	static Timestamp getMicroTimestamp(BasicBuffer in) throws GSException {
		final long micros = in.base().getLong();
		if (micros < 0) {
			throw new GSException(
					GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by value range for TIMESTAMP(6)");
		}
		final Timestamp ts = new Timestamp(micros / (1000 * 1000) * 1000);
		ts.setNanos((int) (micros % (1000 * 1000)) * 1000);
		return ts;
	}

	static void putNanoTimestampPrepared(
			Cursor cursor, BasicBuffer out, Timestamp ts)
			throws GSException {
		if (cursor.isOnAnyData()) {
			cursor.beginVarDataOutput();
			putFixedNanoTimestamp(out, ts);
			cursor.endVarData();
		}
		else {
			putFixedNanoTimestampPrepared(out, ts);
		}
	}

	static Timestamp getNanoTimestamp(Cursor cursor, BasicBuffer in)
			throws GSException {
		final Timestamp ts;
		if (cursor.isOnAnyData()) {
			cursor.beginVarDataInput();
			ts = getFixedNanoTimestamp(in);
			cursor.endVarData();
		}
		else {
			ts = getFixedNanoTimestamp(in);
		}
		return ts;
	}

	static void putFixedNanoTimestamp(BasicBuffer out, Timestamp ts)
			throws GSException {
		out.prepare(DetailElementType.NANO_TIMESTAMP.getBaseFixedSize());
		putFixedNanoTimestampPrepared(out, ts);
	}

	static void putFixedNanoTimestampPrepared(BasicBuffer out, Timestamp ts)
			throws GSException {
		final long unit = ValueUtils.HIGH_MICRO_UNIT;
		final long revUnit = 1000 / unit;
		final long base = ts.getTime();
		final long nanos = ts.getNanos();

		if (base < 0 || base > Long.MAX_VALUE / (1000 * unit)) {
			throw new GSException(
					GSErrorCode.UNSUPPORTED_ROW_MAPPING,
					"Unsupported value range for TIMESTAMP(9)");
		}

		final long high = base / 1000 * (1000 * 1000 * unit) + nanos / revUnit;
		final long low = nanos % revUnit;

		out.base().putLong(high);
		out.base().put((byte) low);
	}

	static Timestamp getFixedNanoTimestamp(BasicBuffer in)
			throws GSException {
		final long unit = ValueUtils.HIGH_MICRO_UNIT;
		final long revUnit = 1000 / unit;
		final long high = in.base().getLong();
		final int low = in.base().get() & 0xff;

		if (high < 0 || low >= revUnit) {
			throw new GSException(
					GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by value range for TIMESTAMP(9)");
		}

		final long base = high / (1000 * unit);
		final long nanos = (int) (high % (1000 * 1000 * unit) * revUnit) + low;

		final Timestamp ts = new Timestamp(base);
		ts.setNanos((int) nanos);
		return ts;
	}

}
