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
import java.sql.Blob;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.toshiba.mwcloud.gs.AggregationResult;
import com.toshiba.mwcloud.gs.ColumnInfo;
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

	private static final GSType[] TYPE_CONSTANTS = GSType.values();

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

	private static final Map<Class<?>, GSType> ELEMENT_TYPE_MAP =
			createElementTypeMap();

	private static final Map<GSType, Object> EMPTY_ELEMENT_VALUES =
			createEmptyValueMap(false);

	private static final Map<GSType, Object> EMPTY_ARRAY_VALUES =
			createEmptyValueMap(true);

	private static final Config BASIC_CONFIG = new Config(false, false, true);

	private static final Config GENERAL_CONFIG = new Config(true, true, true);

	private static final List<Integer> EMPTY_KEY_LIST =
			Collections.emptyList();

	private static final List<Integer> SINGLE_FIRST_KEY_LIST =
			Collections.singletonList(0);

	private final Class<?> rowType;

	private transient final Constructor<?> rowConstructor;

	private transient final Map<String, Entry> entryMap;

	private final List<Entry> entryList;

	private transient Entry keyEntry;

	private final boolean forTimeSeries;

	private final boolean nullableAllowed;

	private transient final int variableEntryCount;

	private transient Object[] emptyFieldArray;

	private RowMapper(
			Class<?> rowType, Constructor<?> rowConstructor,
			Map<String, Entry> entryMap,
			List<Entry> entryList, Entry keyEntry,
			ContainerType containerType, boolean nullableAllowed) {
		this.rowType = rowType;
		this.rowConstructor = rowConstructor;
		this.entryMap = entryMap;
		this.entryList = entryList;
		this.keyEntry = keyEntry;
		this.forTimeSeries = (containerType == ContainerType.TIME_SERIES);
		this.nullableAllowed = nullableAllowed;
		this.variableEntryCount = calculateVariableEntryCount(entryList);
	}

	private RowMapper(
			Class<?> rowType, ContainerType containerType,
			boolean nullableAllowed) throws GSException {
		this.rowType = rowType;
		this.entryMap = new HashMap<String, Entry>();
		this.entryList = new ArrayList<Entry>();
		this.forTimeSeries = (containerType == ContainerType.TIME_SERIES);
		this.nullableAllowed = nullableAllowed;
		this.rowConstructor = getRowConstructor(rowType);

		final Set<String> transientRowFields = new HashSet<String>();
		for (Field field : rowType.getDeclaredFields()) {
			accept(transientRowFields, field);
		}
		for (Method method : rowType.getDeclaredMethods()) {
			accept(transientRowFields, method);
		}

		applyOrder(transientRowFields, restrictKeyOrderFirst);
		applyNullable(nullableAllowed);
		checkKeyType(forTimeSeries);

		this.variableEntryCount = calculateVariableEntryCount(entryList);
	}

	private RowMapper(
			ContainerType containerType, ContainerInfo containerInfo,
			boolean anyTypeAllowed, boolean nullableAllowed)
			throws GSException {
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
		this.nullableAllowed = nullableAllowed;
		this.rowConstructor = null;

		final int columnCount = containerInfo.getColumnCount();
		if (columnCount <= 0 && !anyTypeAllowed) {
			throw new GSException(
					GSErrorCode.ILLEGAL_SCHEMA, "Empty schema");
		}

		final boolean rowKeyAssigned = containerInfo.isRowKeyAssigned();
		for (int i = 0; i < columnCount; i++) {
			accept(
					containerInfo.getColumnInfo(i),
					(i == 0 && rowKeyAssigned),
					anyTypeAllowed, nullableAllowed);
		}

		checkKeyType(forTimeSeries);

		this.variableEntryCount = calculateVariableEntryCount(entryList);
	}

	private static int calculateVariableEntryCount(List<Entry> entryList) {
		int count = 0;
		for (Entry entry : entryList) {
			if (hasVarDataPart(entry.elementType, entry.arrayUsed)) {
				count++;
			}
		}

		return count;
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
				containerType, containerInfo, config.anyTypeAllowed,
				config.nullableAllowed));
	}

	public static RowMapper getInstance(Row row, Config config)
			throws GSException {
		if (row instanceof ArrayRow) {
			return ((ArrayRow) row).mapper;
		}

		return getInstance(null, row.getSchema(), config);
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
			columnInfoList.add(importColumnSchema(in, config));
		}

		keyList = importKeyListEnd(in, config, columnCount, keyList);
		return getInstance(containerType, new ContainerInfo(
				null, containerType, columnInfoList, !keyList.isEmpty()),
				config);
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
			final Entry thisEntry = entryList.get(i);
			final Entry entry = mapper.entryList.get(i);

			if (thisEntry.keyType ^ entry.keyType) {
				throw new GSException(GSErrorCode.ILLEGAL_SCHEMA, "");
			}

			if (thisEntry.elementType != entry.elementType) {
				throw new GSException(GSErrorCode.ILLEGAL_SCHEMA, "");
			}

			if (thisEntry.arrayUsed ^ entry.arrayUsed) {
				throw new GSException(GSErrorCode.ILLEGAL_SCHEMA, "");
			}

			if (!thisEntry.columnName.equals(entry.columnName) &&
					!normalizeSymbolUnchecked(thisEntry.columnName).equals(
							normalizeSymbolUnchecked(entry.columnName))) {
				throw new GSException(GSErrorCode.ILLEGAL_SCHEMA, "");
			}
		}
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

		final Entry newKeyEntry;
		if (keyEntry == null) {
			if (!keyList.isEmpty()) {
				throw new GSException(
						GSErrorCode.ILLEGAL_SCHEMA,
						"Remote schema must not have a key");
			}
			newKeyEntry = null;
		}
		else {
			if (keyList.isEmpty()) {
				throw new GSException(
						GSErrorCode.ILLEGAL_SCHEMA,
						"Remote schema must have a key");
			}
			newKeyEntry = newEntryList.get(keyList.get(0));
			final String normalizedName =
					normalizeSymbolUnchecked(keyEntry.columnName);
			if (!normalizedName.equals(
					normalizeSymbolUnchecked(newKeyEntry.columnName))) {
				throw new GSException(
						GSErrorCode.ILLEGAL_SCHEMA,
						"Inconsistent remote schema (column name)");
			}
			newKeyEntry.keyType = true;
		}

		return CACHE.intern(new RowMapper(
				rowType, rowConstructor, newEntryMap, newEntryList,
				newKeyEntry, getContainerType(), nullableAllowed));
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
			return toKeyList(columnId >= 0);
		}

		return null;
	}

	public static List<Integer> importKeyListEnd(
			BasicBuffer in, Config config, int columnCount,
			List<Integer> lastKeyList) throws GSException {
		if (config.keyExtensible) {
			final int count = in.base().getShort();
			if (!(count == 0 || count == 1)) {
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by illegal row key count (" +
						"count=" + count + ")");
			}

			for (int i = 0; i < count; i++) {
				final int columnId = in.base().getShort();
				if (columnId != 0 || columnId >= columnCount) {
					throw new GSConnectionException(
							GSErrorCode.MESSAGE_CORRUPTED,
							"Protocol error by illegal index of row key " +
							"column (keyColumn=" + columnId + ")");
				}
			}

			return toKeyList(count > 0);
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
			if (count > 1) {
				throw new GSException(GSErrorCode.INTERNAL_ERROR, "");
			}
			out.putShort((short) count);
			for (int keyIndex : keyList) {
				if (keyIndex != 0) {
					throw new GSException(GSErrorCode.INTERNAL_ERROR, "");
				}
				out.putShort((short) keyIndex);
			}
		}
	}

	public static ColumnInfo importColumnSchema(
			BasicBuffer in, Config config) throws GSException {
		final Entry entry = new Entry(null);
		entry.importColumnSchema(in, -1, config.nullableAllowed);
		entry.filterNullable(
				entry.columnNullable, null, config.nullableAllowed);

		if (config.anyTypeAllowed && entry.columnName.isEmpty()) {
			entry.columnName = null;
		}

		return entry.getColumnInfo();
	}

	public static List<Integer> toKeyList(boolean rowKeyAssigned) {
		return rowKeyAssigned ? SINGLE_FIRST_KEY_LIST : EMPTY_KEY_LIST;
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
		return (keyEntry != null);
	}

	public Class<?> getRowType() {
		return rowType;
	}

	public boolean isForTimeSeries() {
		return forTimeSeries;
	}

	public GSType getFieldElementType(int columnId) {
		return entryList.get(columnId).elementType;
	}

	public boolean isArray(int columnId) {
		return entryList.get(columnId).arrayUsed;
	}

	public boolean hasAnyTypeColumn() {
		for (Entry entry : entryList) {
			if (entry.elementType == null) {
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

		final List<ColumnInfo> columnInfoList =
				new ArrayList<ColumnInfo>(entryList.size());
		for (Entry entry : entryList) {
			columnInfoList.add(entry.getColumnInfo());
		}

		
		return new ContainerInfo(null, null, columnInfoList, hasKey());
	}

	public void exportSchema(BasicBuffer out, Config config)
			throws GSException {
		if (rowType == AggregationResult.class) {
			throw new GSException(
					GSErrorCode.INTERNAL_ERROR,
					"Unexpected row type: AggregationResult");
		}

		final List<Integer> keyList = toKeyList(keyEntry != null);

		exportColumnCount(out, entryList.size());
		exportKeyListBegin(out, config, keyList);
		for (Entry entry : entryList) {
			entry.exportColumnSchema(out);
		}
		exportKeyListEnd(out, config, keyList);
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
		return entryList.get(column).getFieldObj(rowObj, isGeneral());
	}

	public Object resolveKey(
			Object keyObj, Object rowObj) throws GSException {
		return resolveKey(keyObj, rowObj, isGeneral());
	}

	public Object resolveKey(
			Object keyObj, Object rowObj, boolean general) throws GSException {
		if (keyObj != null) {
			return keyObj;
		}

		if (keyEntry == null) {
			throw new GSException(
					GSErrorCode.KEY_NOT_FOUND, "Row key does not exist");
		}

		return keyEntry.getFieldObj(rowObj, general);
	}

	public Object resolveKey(String keyString) throws GSException {
		if (keyEntry == null) {
			throw new GSException(
					GSErrorCode.KEY_NOT_FOUND, "Row key does not exist");
		}

		if (keyEntry.arrayUsed) {
			throw new GSException(
					GSErrorCode.UNSUPPORTED_KEY_TYPE, "Unsupported key type");
		}
		try {
			switch (keyEntry.elementType) {
			case STRING:
				return keyString;
			case INTEGER:
				return Integer.parseInt(keyString);
			case LONG:
				return Long.parseLong(keyString);
			case TIMESTAMP:
				return TimestampUtils.getFormat().parse(keyString);
			default:
				throw new GSException(
						GSErrorCode.UNSUPPORTED_KEY_TYPE,
						"Unsupported key type");
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
		return ArrayRow.create(this, true);
	}

	public Object createRow(boolean general) throws GSException {
		if (general || isGeneral()) {
			return createGeneralRow();
		}

		try {
			return rowConstructor.newInstance();
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

	public void encodeKey(
			BasicBuffer buffer, Object keyObj, MappingMode mode) throws GSException {
		if (keyEntry == null) {
			throw new GSException(
					GSErrorCode.KEY_NOT_FOUND, "Row key does not exist");
		}

		if (keyEntry.arrayUsed) {
			throw new GSException(
					GSErrorCode.UNSUPPORTED_KEY_TYPE, "Unsupported key type");
		}
		encodeKey(buffer, keyObj, keyEntry.elementType, mode);
	}

	public static void encodeKey(
			BasicBuffer buffer, Object keyObj, GSType type,
			MappingMode mode) throws GSException {
		switch (type) {
		case STRING:
			putString(buffer, (String) keyObj,
					(mode == MappingMode.ROWWISE_SEPARATED_V2));
			break;
		case INTEGER:
			buffer.putInt((Integer) keyObj);
			break;
		case LONG:
			buffer.putLong((Long) keyObj);
			break;
		case TIMESTAMP:
			buffer.putDate((Date) keyObj);
			break;
		default:
			throw new GSException(
					GSErrorCode.UNSUPPORTED_KEY_TYPE, "Unsupported key type");
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

	public void encode(Cursor cursor,
			Object keyObj, Object rowObj, boolean general) throws GSException {
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

		if (keyObj != null && keyEntry == null) {
			throw new GSException(
					GSErrorCode.KEY_NOT_ACCEPTED, "Key must not be specified");
		}

		if (rowType == Row.class || general) {
			getInstance((Row) rowObj, GENERAL_CONFIG).checkSchemaMatched(this);
		}

		cursor.beginRowOutput();
		if (cursor.getMode() == MappingMode.AGGREGATED) {
			throw new IllegalArgumentException();
		}
		else {
			for (Entry entry : entryList) {
				entry.encode(cursor, keyObj, rowObj, general);
			}
		}
		cursor.endRowOutput();
	}

	public Object decode(Cursor cursor, boolean general) throws GSException {
		final Object rowObj = (general ?
				ArrayRow.createUninitialized(this) : createRow(false));
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
			if (hasVarDataPart(entry.elementType, entry.arrayUsed)) {
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
			if (hasVarDataPart(entry.elementType, entry.arrayUsed)) {
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

	private boolean isGeneral() {
		return (rowType == Row.class);
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
			Set<String> transientRowFields, Field field) throws GSException {
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

		if (resolveElementType(field.getType(), false, false) == null) {
			return;
		}

		final Entry entry = putEntry(orgName, rowField);
		if (entry.rowTypeField != null) {
			throw new GSException("Duplicate field name (" + name + ")");
		}
		entry.rowTypeField = field;
		entry.nameByField = orgName;
		entry.applyAccessibleObject(field);
		entry.setObjectType(field.getType());
		acceptKeyEntry(entry);
	}

	private void accept(
			Set<String> transientRowFields, Method method)
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

		if (resolveElementType(objectType, false, false) == null) {
			return;
		}

		final Entry entry = putEntry(orgName, rowField);
		if (forGetter) {
			if (entry.getterMethod != null) {
				throw new GSException("Duplicate getter name (" + name + ")");
			}
			entry.getterMethod = method;
			entry.nameByGetter = orgName;
		}
		else{
			if (entry.setterMethod != null) {
				throw new GSException("Duplicate setter name (" + name + ")");
			}
			entry.setterMethod = method;
		}
		entry.applyAccessibleObject(method);
		entry.setObjectType(objectType);
		acceptKeyEntry(entry);
	}

	private void accept(
			ColumnInfo columnInfo, boolean keyType, boolean anyTypeAllowed,
			boolean nullableAllowed) throws GSException {
		final String orgName = columnInfo.getName();
		final String normalizedName =
				(orgName == null ? null : normalizeSymbolUnchecked(orgName));
		if (!anyTypeAllowed || orgName != null) {
			RowMapper.checkSymbol(orgName, "column name");
		}

		if (entryMap.containsKey(normalizedName)) {
			throw new GSException("Duplicate column name (" + orgName + ")");
		}

		final Entry entry = new Entry(orgName);
		final GSType type = columnInfo.getType();
		if (!(anyTypeAllowed && type == null)) {
			entry.elementType = toArrayElementType(type);
			if (entry.elementType == null) {
				entry.elementType = columnInfo.getType();
			}
			else {
				entry.arrayUsed = true;
			}
		}
		entry.order = entryList.size();
		entry.keyType = keyType;
		entry.setNullableGeneral(columnInfo.getNullable(), nullableAllowed);
		entry.setInitialValueNull(columnInfo.getDefaultValueNull());

		if (normalizedName != null) {
			entryMap.put(normalizedName, entry);
		}
		entryList.add(entry);

		if (keyType) {
			keyEntry = entry;
		}
	}

	private Entry putEntry(String name, RowField rowField) throws GSException {
		Entry entry = entryMap.get(normalizeSymbolUnchecked(name));
		if (entry == null) {
			entry = new Entry(name);
			entryMap.put(normalizeSymbolUnchecked(name), entry);
			entryList.add(entry);
		}

		if (rowField != null) {
			final int order = rowField.columnNumber();
			if (order >= 0) {
				if (entry.order >= 0) {
					if (entry.order != order) {
						throw new GSException("Illegal column number");
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

	private void acceptKeyEntry(Entry entry) throws GSException {
		if (entry.keyType) {
			if (keyEntry != null && keyEntry != entry) {
				throw new GSException(
						GSErrorCode.MULTIPLE_KEYS_FOUND, "Multiple keys found");
			}
			keyEntry = entry;
		}
	}

	private void applyOrder(
			Set<String> transientRowFields, boolean keyFirst)
			throws GSException {
		boolean specified = false;
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
		}

		if (entryList.isEmpty()) {
			throw new GSException(
					GSErrorCode.ILLEGAL_SCHEMA, "Empty schema");
		}

		if (!specified && !(keyFirst && !entryList.get(0).keyType)) {
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
					throw new GSException("Illegal order");
				}
				entryList.set(order, entry);
				rest--;
			}
		}

		if (rest > 0) {
			ListIterator<Entry> it = entryList.listIterator();
			boolean keyConsumed = false;
			if (keyFirst && keyEntry != null) {
				if (keyEntry.order > 0) {
					throw new GSException("Key must be first column");
				}
				while (it.next() != null) {
				}
				keyEntry.order = it.previousIndex();
				it.set(keyEntry);
				rest--;
				keyConsumed = true;
			}
			for (Entry entry : orgList) {
				if ((entry == keyEntry && keyConsumed) || entry.order >= 0) {
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

		if (keyEntry != null && keyFirst && !entryList.get(0).keyType) {
			throw new GSException("Key must be first column");
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
			entry.setNullableByAccessors(nullableDefault, nullableAllowed);
		}
	}

	private void checkKeyType(boolean forTimeSeries) throws GSException {
		if (keyEntry == null) {
			if (forTimeSeries) {
				throw new GSException("Key must be required for time series");
			}
			return;
		}

		if (keyEntry.arrayUsed) {
			throw new GSException(
					GSErrorCode.UNSUPPORTED_KEY_TYPE,
					"Key type must not be array");
		}

		if (forTimeSeries) {
			if (keyEntry.elementType != GSType.TIMESTAMP) {
				throw new GSException(
						GSErrorCode.UNSUPPORTED_KEY_TYPE,
						"Illegal key type for time series: " + keyEntry.elementType);
			}
		}
		else if (keyEntry.elementType == null) {
			throw new GSException(
					GSErrorCode.UNSUPPORTED_KEY_TYPE,
					"Key must not be any type");
		}
		else {
			switch (keyEntry.elementType) {
			case STRING:
			case INTEGER:
			case LONG:
			case TIMESTAMP:
				break;
			default:
				throw new GSException(
						GSErrorCode.UNSUPPORTED_KEY_TYPE,
						"Illegal key type for collection: " + keyEntry.elementType);
			}
		}
	}

	private void decodeAggregation(
			Cursor cursor, boolean general, Object rowObj) throws GSException {
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

			final GSType type = in.getByteEnum(TYPE_CONSTANTS);
			final Object orgValue = getField(cursor, type, false);
			final Object value;
			switch (type) {
			case BYTE:
				value = (long) (byte) (Byte) orgValue;
				break;
			case SHORT:
				value = (long) (short) (Short) orgValue;
				break;
			case INTEGER:
				value = (long) (int) (Integer) orgValue;
				break;
			case LONG:
				value = orgValue;
				break;
			case FLOAT:
				value = (double) (float) (Float) orgValue;
				break;
			case DOUBLE:
				value = orgValue;
				break;
			case TIMESTAMP:
				value = orgValue;
				break;
			default:
				throw new GSException(
						GSErrorCode.UNSUPPORTED_FIELD_TYPE,
						"Unsupported aggregation result type");
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

		if (!acceptAggregationResultColumnId) {
			throw new GSException(GSErrorCode.UNSUPPORTED_ROW_MAPPING, "");
		}

		cursor.beginRowInput();

		final BasicBuffer in = cursor.getBuffer();
		final int column = in.base().getInt();
		final GSType type = in.getByteEnum(TYPE_CONSTANTS);
		if (column < 0 || column >= entryList.size()) {
			if (column == -1) {
				throw new GSException(
						GSErrorCode.UNSUPPORTED_ROW_MAPPING,
						"Unable to map non columnwise aggregation (ex. COUNT())");
			}
			throw new GSException(
					GSErrorCode.UNSUPPORTED_ROW_MAPPING, "Illegal column ID");
		}
		final Entry entry = entryList.get(column);
		if (type == entry.elementType) {
			entry.decode(cursor, rowObj, general);
		}
		else {
			final Object orgValue = getField(cursor, type, false);
			if (!(orgValue instanceof Long || orgValue instanceof Double)) {
				throw new GSException(
						GSErrorCode.UNSUPPORTED_ROW_MAPPING,
						"Unacceptable result type");
			}
			final Object fieldObj;
			switch (entry.elementType) {
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
			entry.setFieldObj(rowObj, fieldObj, general);
		}

		cursor.endRowInput();
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
			size += getFixedEncodedSize(entry.elementType, entry.arrayUsed, mode);
			if (!hasVarDataPart && hasVarDataPart(entry.elementType, entry.arrayUsed)) {
				hasVarDataPart = true;
			}
		}
		if (mode == MappingMode.ROWWISE_SEPARATED_V2 && hasVarDataPart) {
			
			size += Long.SIZE / Byte.SIZE;
		}
		return size;
	}

	private int getFixedFieldPartSize(int columnId, MappingMode mode) {
		final Entry entry = entryList.get(columnId);
		return getFixedEncodedSize(entry.elementType, entry.arrayUsed, mode);
	}

	private int getColumnCount() {
		return entryList.size();
	}

	private Entry getEntry(int index) {
		return entryList.get(index);
	}

	private void getAllInitialValue(boolean nullable, Object[] dest) {
		Object[] target = emptyFieldArray;
		if (target == null || nullable) {
			final int count = getColumnCount();
			target = (nullable ? dest : new Object[count]);
			for (int i = 0; i < count; i++) {
				target[i] = getEntry(i).getInitialObj(nullable, true);
			}
			if (nullable) {
				return;
			}
			emptyFieldArray = target;
		}
		System.arraycopy(target, 0, dest, 0, target.length);
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

	private static GSType resolveElementType(
			Class<?> objectType, boolean subClassAllowed, boolean validating)
			throws GSException {
		final Class<?> objectElementType;
		if (objectType.isArray()) {
			objectElementType = objectType.getComponentType();
		}
		else {
			objectElementType = objectType;
		}

		final GSType elementType = ELEMENT_TYPE_MAP.get(objectElementType);
		if (elementType == null) {
			if (Date.class.isAssignableFrom(objectElementType)) {
				return GSType.TIMESTAMP;
			}
			if (Geometry.class.isAssignableFrom(objectElementType)) {
				return GSType.GEOMETRY;
			}
			if (Blob.class.isAssignableFrom(objectElementType)) {
				return GSType.BLOB;
			}
		}
		else if (objectType.isArray()) {
			if (objectElementType.isPrimitive()) {
				return elementType;
			}
			else {
				switch (elementType) {
				case STRING:
				case TIMESTAMP:
					return elementType;
				case GEOMETRY:
				case BLOB:
					if (!validating) {
						return elementType;
					}
					break;
				default:
					break;
				}
			}
		}
		else {
			return elementType;
		}

		if (validating) {
			throw new GSException(
					GSErrorCode.UNSUPPORTED_FIELD_TYPE,
					"Unsupported field type (" +
					"className=" + objectType.getName() +
					", elementClassName=" + objectElementType.getName() +
					", arrayType=" + objectType.isArray() + ")");
		}

		return null;
	}

	private static Map<Class<?>, GSType> createElementTypeMap() {
		final Map<Class<?>, GSType> map = new HashMap<Class<?>, GSType>();
		map.put(String.class, GSType.STRING);
		map.put(boolean.class, GSType.BOOL);
		map.put(byte.class, GSType.BYTE);
		map.put(short.class, GSType.SHORT);
		map.put(int.class, GSType.INTEGER);
		map.put(long.class, GSType.LONG);
		map.put(float.class, GSType.FLOAT);
		map.put(double.class, GSType.DOUBLE);
		map.put(Date.class, GSType.TIMESTAMP);
		map.put(Geometry.class, GSType.GEOMETRY);
		map.put(Blob.class, GSType.BLOB);
		map.put(Boolean.class, GSType.BOOL);
		map.put(Byte.class, GSType.BYTE);
		map.put(Short.class, GSType.SHORT);
		map.put(Integer.class, GSType.INTEGER);
		map.put(Long.class, GSType.LONG);
		map.put(Float.class, GSType.FLOAT);
		map.put(Double.class, GSType.DOUBLE);
		return map;
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

	private static void putField(
			Cursor cursor, Object fieldObj,
			GSType type, boolean arrayUsed) throws GSException {
		cursor.beginField();
		final BasicBuffer out = cursor.buffer;
		if (arrayUsed) {
			cursor.beginVarDataOutput();
			switch (type) {
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
		else if (type != null) {
			switch (type) {
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
								throw new GSException("Blob size limit exceeded");
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
				putTypePrepared(out, null);
				out.base().putLong(0);
			}
			else {
				final Class<?> fieldClass = fieldObj.getClass();

				final GSType actualElementType =
						resolveElementType(fieldClass, true, true);
				final boolean actualArrayUsed = fieldClass.isArray();
				putTypePrepared(out, toFullType(actualElementType, actualArrayUsed));

				final int lastPos = out.base().position();
				putField(cursor, fieldObj, actualElementType, actualArrayUsed);

				final int fixedGap = lastPos + Long.SIZE / Byte.SIZE -
						out.base().position();
				out.base().put(EMPTY_LONG_BYTES, 0, fixedGap);
			}
		}
	}

	private static Object getField(
			Cursor cursor, GSType type, boolean arrayUsed)
			throws GSException {
		cursor.beginField();
		final BasicBuffer in = cursor.buffer;
		final Object result;
		if (arrayUsed) {
			cursor.beginVarDataInput();
			final int length;
			if (cursor.isVarSizeMode()) {
				getVarSize(in);	
				length = getVarSize(in);
			} else {
				in.base().getInt();	
				length = in.base().getInt();
			}
			switch (type) {
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
		else if (type != null) {
			switch (type) {
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
			final GSType actualType = getType(in);
			if (actualType == null) {
				in.base().getLong();
				return null;
			}
			else {
				final GSType actualElementType =
						toArrayElementType(actualType);
				final boolean actualArrayUsed = (actualElementType != null);

				final int lastPos = in.base().position();
				final Object value = getField(cursor, actualArrayUsed ?
						actualElementType : actualType, actualArrayUsed);

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

	private static int getFixedEncodedSize(
			GSType type, boolean arrayUsed, MappingMode mode) {
		if (arrayUsed) {
			if (mode == MappingMode.ROWWISE_SEPARATED_V2) {
				return 0;
			} else {
				return Long.SIZE / Byte.SIZE;
			}
		}
		else if (type != null) {
			switch (type) {
			case STRING:
				if (mode == MappingMode.ROWWISE_SEPARATED_V2) {
					return 0;
				} else {
					return Long.SIZE / Byte.SIZE;
				}
			case BOOL:
				return Byte.SIZE / Byte.SIZE;
			case BYTE:
				return Byte.SIZE / Byte.SIZE;
			case SHORT:
				return Short.SIZE / Byte.SIZE;
			case INTEGER:
				return Integer.SIZE / Byte.SIZE;
			case LONG:
				return Long.SIZE / Byte.SIZE;
			case FLOAT:
				return Float.SIZE / Byte.SIZE;
			case DOUBLE:
				return Double.SIZE / Byte.SIZE;
			case TIMESTAMP:
				return Long.SIZE / Byte.SIZE;
			case GEOMETRY:
				if (mode == MappingMode.ROWWISE_SEPARATED_V2) {
					return 0;
				} else {
					return Long.SIZE / Byte.SIZE;
				}
			case BLOB:
				if (mode == MappingMode.ROWWISE_SEPARATED_V2) {
					return 0;
				} else {
					return Long.SIZE / Byte.SIZE;
				}
			default:
				throw new Error();
			}
		}
		else {
			return ((Byte.SIZE + Long.SIZE) / Byte.SIZE);
		}
	}

	private static void putTypePrepared(BasicBuffer out, GSType type) {
		if (type == null) {
			out.base().put(ANY_NULL_TYPE);
		}
		else {
			out.putByteEnumPrepared(type);
		}
	}

	private static GSType getType(BasicBuffer in) {
		final byte rawType = in.base().get();

		if (rawType == ANY_NULL_TYPE) {
			return null;
		}
		else {
			try {
				return TYPE_CONSTANTS[rawType & 0xff];
			}
			catch (IndexOutOfBoundsException e) {
				throw new IllegalStateException(e);
			}
		}
	}

	private static boolean hasVarDataPart(GSType type, boolean arrayUsed) {
		if (arrayUsed) {
			return true;
		}
		else if (type != null) {
			switch (type) {
			case STRING:
				return true;
			case GEOMETRY:
				return true;
			case BLOB:
				return true;
			default:
				return false;
			}
		}
		else {
			return true;
		}
	}

	private static Map<GSType, Object> createEmptyValueMap(boolean arrayUsed) {
		final Map<GSType, Object> map =
				new EnumMap<GSType, Object>(GSType.class);
		if (arrayUsed) {
			map.put(GSType.STRING, new String[0]);
			map.put(GSType.BOOL, new boolean[0]);
			map.put(GSType.BYTE, new byte[0]);
			map.put(GSType.SHORT, new short[0]);
			map.put(GSType.INTEGER, new int[0]);
			map.put(GSType.LONG, new long[0]);
			map.put(GSType.FLOAT, new float[0]);
			map.put(GSType.DOUBLE, new double[0]);
			map.put(GSType.TIMESTAMP, new Date[0]);
		}
		else {
			map.put(GSType.STRING, "");
			map.put(GSType.BOOL, false);
			map.put(GSType.BYTE, (byte) 0);
			map.put(GSType.SHORT, (short) 0);
			map.put(GSType.INTEGER, (int) 0);
			map.put(GSType.LONG, (long) 0);
			map.put(GSType.FLOAT, (float) 0);
			map.put(GSType.DOUBLE, (double) 0);
			map.put(GSType.TIMESTAMP, new Date(0));
			map.put(GSType.GEOMETRY, Geometry.valueOf("POINT(EMPTY)"));
			map.put(GSType.BLOB, new BlobImpl());
		}
		return map;
	}

	public static GSType toFullType(GSType elementType, boolean arrayUsed) {
		if (arrayUsed) {
			switch (elementType) {
			case STRING:
				return GSType.STRING_ARRAY;
			case BOOL:
				return GSType.BOOL_ARRAY;
			case BYTE:
				return GSType.BYTE_ARRAY;
			case SHORT:
				return GSType.SHORT_ARRAY;
			case INTEGER:
				return GSType.INTEGER_ARRAY;
			case LONG:
				return GSType.LONG_ARRAY;
			case FLOAT:
				return GSType.FLOAT_ARRAY;
			case DOUBLE:
				return GSType.DOUBLE_ARRAY;
			case TIMESTAMP:
				return GSType.TIMESTAMP_ARRAY;
			default:
				throw new Error();
			}
		}
		else {
			return elementType;
		}
	}

	public static GSType toArrayElementType(GSType type) {
		switch (type) {
		case STRING_ARRAY:
			return GSType.STRING;
		case BOOL_ARRAY:
			return GSType.BOOL;
		case BYTE_ARRAY:
			return GSType.BYTE;
		case SHORT_ARRAY:
			return GSType.SHORT;
		case INTEGER_ARRAY:
			return GSType.INTEGER;
		case LONG_ARRAY:
			return GSType.LONG;
		case FLOAT_ARRAY:
			return GSType.FLOAT;
		case DOUBLE_ARRAY:
			return GSType.DOUBLE;
		case TIMESTAMP_ARRAY:
			return GSType.TIMESTAMP;
		default:
			return null;
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

		public Config(
				boolean anyTypeAllowed,
				boolean nullableAllowed,
				boolean keyExtensible) {
			this.anyTypeAllowed = anyTypeAllowed;
			this.nullableAllowed = nullableAllowed;
			this.keyExtensible = keyExtensible;
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

		GSType elementType;
		boolean arrayUsed;
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
					throw new GSException("Inconsistent annotation");
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

		void setNullableByAccessors(
				Boolean nullableDefault, boolean nullableAllowed)
				throws GSException {
			Boolean nullable = null;
			AccessibleObject lastFoundTarget = null;
			for (int i = 0; i < 3; i++) {
				final AccessibleObject target = (i == 0 ? rowTypeField :
						(i == 1 ? getterMethod : setterMethod));
				if (target == null) {
					continue;
				}
				final Boolean nextNullable = findNullableFromAccessor(
						target, lastFoundTarget, nullable, this);
				if (nullable == null && nextNullable != null) {
					lastFoundTarget = target;
					nullable = nextNullable;
				}
			}

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

		void setObjectType(Class<?> objectType) throws GSException {
			elementType = resolveElementType(objectType, false, false);
			if (objectType.isArray()) {
				switch (elementType) {
				case BLOB:
				case GEOMETRY:
					throw new GSException(
							GSErrorCode.UNSUPPORTED_FIELD_TYPE,
							"BLOB or GEOMETRY must not be an element of array");
				}
				arrayUsed = true;
			}
			else {
				arrayUsed = false;
			}
			objectNullable = !objectType.isPrimitive();
		}

		GSType getFullType() {
			return toFullType(elementType, arrayUsed);
		}

		Boolean getInitialValueNull() {
			if (initialValueSpecified) {
				return initialValueNull;
			}
			return null;
		}

		ColumnInfo getColumnInfo() {
			return new ColumnInfo(
					columnName, getFullType(), columnNullable,
					getInitialValueNull(), null);
		}

		void exportColumnSchema(BasicBuffer out) {
			out.putString((columnName == null ? "" : columnName));
			out.prepare(1);
			putTypePrepared(out, elementType);

			byte flags = 0;
			flags |= (arrayUsed ? COLUMN_FLAG_ARRAY : 0);
			flags |= (columnNullable ? 0 : COLUMN_FLAG_NOT_NULL);
			out.put(flags);
		}

		void importColumnSchema(
				BasicBuffer in, int order, boolean nullableAllowed)
				throws GSException {
			columnName = in.getString();
			elementType = getType(in);

			final byte flags = in.base().get();
			arrayUsed = (flags & COLUMN_FLAG_ARRAY) != 0;
			columnNullable = (nullableAllowed &&
					(flags & COLUMN_FLAG_NOT_NULL) == 0);

			this.order = order;
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
					elementType != orgEntry.elementType ||
					arrayUsed != orgEntry.arrayUsed ||
					columnNullable != orgEntry.columnNullable) {
				throw new GSException(
						GSErrorCode.ILLEGAL_SCHEMA,
						"Inconsistent remote column");
			}

			rowTypeField = orgEntry.rowTypeField;
			getterMethod = orgEntry.getterMethod;
			setterMethod = orgEntry.setterMethod;
			objectNullable = orgEntry.objectNullable;
		}

		void encode(
				Cursor cursor, Object keyObj, Object rowObj,
				boolean general) throws GSException {
			Object fieldObj = (!keyType || keyObj == null ?
					getFieldObj(rowObj, general) : keyObj);

			if (fieldObj == null && elementType != null) {
				if (!columnNullable) {
					throw new NullPointerException(
							"Null field (columnName=" + columnName +
							", type=" + getFullType() + ")");
				}
				cursor.setNull(order);
				fieldObj = getInitialObj(false, general);
			}

			putField(cursor, fieldObj, elementType, arrayUsed);
		}

		void decode(
				Cursor cursor, Object rowObj,
				boolean general) throws GSException {
			Object fieldObj = getField(cursor, elementType, arrayUsed);
			if (cursor.isNull(order)) {
				if (objectNullable && columnNullable) {
					fieldObj = null;
				}
				else {
					fieldObj = getInitialObj(false, general);
				}
			}
			setFieldObj(rowObj, fieldObj, general);
		}

		void decodeNoNull(
				Cursor cursor, Object rowObj,
				boolean general) throws GSException {
			final Object fieldObj = getField(cursor, elementType, arrayUsed);
			setFieldObj(rowObj, fieldObj, general);
		}

		Object getFieldObj(Object rowObj, boolean general) throws GSException {
			if (general) {
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

		void setFieldObj(Object rowObj,
				Object fieldObj, boolean general) throws GSException {
			if (general) {
				if (rowObj.getClass() == ArrayRow.class) {
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

		Object getInitialObj(boolean nullable, boolean general) {
			if (nullable && (general || objectNullable) && columnNullable &&
					initialValueNull) {
				return null;
			}

			if (elementType == null) {
				return null;
			}

			final Object emptyObj;
			if (arrayUsed) {
				emptyObj = EMPTY_ARRAY_VALUES.get(elementType);
			}
			else {
				emptyObj = EMPTY_ELEMENT_VALUES.get(elementType);
			}

			if (emptyObj == null) {
				throw new Error();
			}

			return emptyObj;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (arrayUsed ? 1231 : 1237);
			result = prime * result +
					((columnName == null) ? 0 : columnName.hashCode());
			result = prime * result + (columnNullable ? 1231 : 1237);
			result = prime * result + (initialValueNull ? 1231 : 1237);
			result = prime * result + (initialValueSpecified ? 1231 : 1237);
			result = prime * result +
					((elementType == null) ? 0 : elementType.hashCode());
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
			if (arrayUsed != other.arrayUsed)
				return false;
			if (columnName == null) {
				if (other.columnName != null)
					return false;
			}
			else if (!columnName.equals(other.columnName))
				return false;
			if (columnNullable != other.columnNullable)
				return false;
			if (initialValueNull != other.initialValueNull)
				return false;
			if (initialValueSpecified != other.initialValueSpecified)
				return false;
			if (elementType != other.elementType)
				return false;
			if (keyType != other.keyType)
				return false;
			if (objectNullable != other.objectNullable)
				return false;
			return true;
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
			if (mode == MappingMode.AGGREGATED) {
				decodeAggregation(this, general, rowObj);
			}
			else {
				if (rowType == AggregationResult.class) {
					throw new GSException(
							GSErrorCode.INTERNAL_ERROR,
							"Unexpected row type: AggregationResult");
				}

				beginRowInput();
				if (nullFound) {
					for (Entry entry : entryList) {
						entry.decode(this, rowObj, general);
					}
				}
				else {
					for (Entry entry : entryList) {
						entry.decodeNoNull(this, rowObj, general);
					}
				}
				endRowInput();
			}
		}

		public int getRowIndex() {
			return rowIndex;
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

		private boolean isNull(int ordinal) {
			return (nullsBytes[ordinal / Byte.SIZE] &
					(1 << (ordinal % Byte.SIZE))) != 0;
		}

		private void setNull(int ordinal) {
			nullsBytes[ordinal / Byte.SIZE] |= (1 << (ordinal % Byte.SIZE));
			nullFound = true;
		}

	}

	private static class ArrayRow implements Row {

		private final RowMapper mapper;

		private final Object[] fieldArray;

		private ArrayRow(
				RowMapper mapper, Object[] fieldArray) throws GSException {
			if (mapper.getContainerType() == null) {
				throw new GSException(GSErrorCode.UNSUPPORTED_OPERATION, "");
			}

			this.mapper = mapper;
			this.fieldArray = fieldArray;
		}

		static ArrayRow create(
				RowMapper mapper, boolean nullable) throws GSException {
			final Object[] fieldArray = new Object[mapper.getColumnCount()];
			mapper.getAllInitialValue(nullable, fieldArray);
			return new ArrayRow(mapper, fieldArray);
		}

		static ArrayRow createUninitialized(
				RowMapper mapper) throws GSException {
			final Object[] fieldArray = new Object[mapper.getColumnCount()];
			return new ArrayRow(mapper, fieldArray);
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

		private void setPrimitiveNull(
				int column, GSType elementType) throws GSException {
			final Entry entry = getEntry(column);
			checkNullable(entry);
			checkType(entry, elementType, false);
			setAnyValueDirect(column, null);
		}

		private void setArrayNull(
				int column, GSType elementType) throws GSException {
			final Entry entry = getEntry(column);
			checkNullable(entry);
			checkType(entry, elementType, true);
			setAnyValueDirect(column, null);
		}

		private void checkPrimitiveType(
				int column, GSType elementType) throws GSException {
			final Entry entry = getEntry(column);
			checkType(entry, elementType, false);
		}

		private void checkArrayType(
				int column, GSType elementType) throws GSException {
			final Entry entry = getEntry(column);
			checkType(entry, elementType, true);
		}

		private void checkNullable(Entry entry) throws GSException {
			if (!entry.columnNullable && entry.elementType != null) {
				throw errorNull(entry.order, null);
			}
		}

		private void checkType(
				Entry entry, GSType elementType, boolean arrayUsed)
				throws GSException {
			if (entry.arrayUsed != arrayUsed ||
					elementType != entry.elementType) {
				final GSType expectedType = toFullType(elementType, arrayUsed);
				throw errorTypeUnmatch(entry.order, expectedType, null);
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
				int column, GSType specifiedType, ClassCastException cause) {
			return errorTypeUnmatch(column, specifiedType, null, cause);
		}

		private GSException errorTypeUnmatch(
				int column, GSType specifiedType, Object specifiedValue,
				ClassCastException cause) {
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
							"" : "actualType=" + entry.getFullType() + ", ") +
					(entry == null ?
							"" : "column=" + entry.columnName + ", ") +
					"columnNumber=" + column + ")",
					cause);
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
			try {
				if (entry.arrayUsed) {
					switch (entry.elementType) {
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
				else if (entry.elementType != null) {
					switch (entry.elementType) {
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
					default:
						throw new Error();
					}
				}
				else {
					resolveElementType(fieldValue.getClass(), true, true);
					setAnyValueDirect(column, fieldValue);
				}
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(column, null, fieldValue, e);
			}
		}

		@Override
		public Object getValue(int column) throws GSException {
			final Entry entry = getEntry(column);
			if (entry.arrayUsed) {
				switch (entry.elementType) {
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
			else if (entry.elementType != null) {
				switch (entry.elementType) {
				case TIMESTAMP:
					return getTimestamp(column);
				case BLOB:
					return getBlob(column);
				default:
					break;
				}
			}
			return getAnyValue(column);
		}

		@Override
		public void setString(int column, String fieldValue)
				throws GSException {
			if (fieldValue == null) {
				setPrimitiveNull(column, GSType.STRING);
				return;
			}
			checkPrimitiveType(column, GSType.STRING);
			setAnyValueDirect(column, fieldValue);
		}

		@Override
		public String getString(int column) throws GSException {
			final String src;
			try {
				src = (String) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(column, GSType.STRING, e);
			}
			return (src == null ? null : src);
		}

		@Override
		public void setBool(int column, boolean fieldValue)
				throws GSException {
			checkPrimitiveType(column, GSType.BOOL);
			setAnyValueDirect(column, fieldValue);
		}

		@Override
		public boolean getBool(int column) throws GSException {
			final Boolean src;
			try {
				src = (Boolean) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(column, GSType.BOOL, e);
			}
			return (src == null ? false : src);
		}

		@Override
		public void setByte(int column, byte fieldValue)
				throws GSException {
			checkPrimitiveType(column, GSType.BYTE);
			setAnyValueDirect(column, fieldValue);
		}

		@Override
		public byte getByte(int column) throws GSException {
			final Byte src;
			try {
				src = (Byte) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(column, GSType.BYTE, e);
			}
			return (src == null ? 0 : src);
		}

		@Override
		public void setShort(int column, short fieldValue)
				throws GSException {
			checkPrimitiveType(column, GSType.SHORT);
			setAnyValueDirect(column, fieldValue);
		}

		@Override
		public short getShort(int column) throws GSException {
			final Short src;
			try {
				src = (Short) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(column, GSType.SHORT, e);
			}
			return (src == null ? 0 : src);
		}

		@Override
		public void setInteger(int column, int fieldValue)
				throws GSException {
			checkPrimitiveType(column, GSType.INTEGER);
			setAnyValueDirect(column, fieldValue);
		}

		@Override
		public int getInteger(int column) throws GSException {
			final Integer src;
			try {
				src = (Integer) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(column, GSType.INTEGER, e);
			}
			return (src == null ? 0 : src);
		}

		@Override
		public void setLong(int column, long fieldValue)
				throws GSException {
			checkPrimitiveType(column, GSType.LONG);
			setAnyValueDirect(column, fieldValue);
		}

		@Override
		public long getLong(int column) throws GSException {
			final Long src;
			try {
				src = (Long) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(column, GSType.LONG, e);
			}
			return (src == null ? 0 : src);
		}

		@Override
		public void setFloat(int column, float fieldValue)
				throws GSException {
			checkPrimitiveType(column, GSType.FLOAT);
			setAnyValueDirect(column, fieldValue);
		}

		@Override
		public float getFloat(int column) throws GSException {
			final Float src;
			try {
				src = (Float) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(column, GSType.FLOAT, e);
			}
			return (src == null ? 0 : src);
		}

		@Override
		public void setDouble(int column, double fieldValue)
				throws GSException {
			checkPrimitiveType(column, GSType.DOUBLE);
			setAnyValueDirect(column, fieldValue);
		}

		@Override
		public double getDouble(int column) throws GSException {
			final Double src;
			try {
				src = (Double) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(column, GSType.DOUBLE, e);
			}
			return (src == null ? 0 : src);
		}

		@Override
		public void setTimestamp(int column, Date fieldValue)
				throws GSException {
			if (fieldValue == null) {
				setPrimitiveNull(column, GSType.TIMESTAMP);
				return;
			}
			checkPrimitiveType(column, GSType.TIMESTAMP);
			setAnyValueDirect(column, fieldValue);
		}

		@Override
		public Date getTimestamp(int column) throws GSException {
			final Date src;
			try {
				src = (Date) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(column, GSType.TIMESTAMP, e);
			}
			return (src == null ? null : new Date(src.getTime()));
		}

		@Override
		public void setGeometry(int column, Geometry fieldValue)
				throws GSException {
			if (fieldValue == null) {
				setPrimitiveNull(column, GSType.GEOMETRY);
				return;
			}
			checkPrimitiveType(column, GSType.GEOMETRY);
			setAnyValueDirect(column, fieldValue);
		}

		@Override
		public Geometry getGeometry(int column) throws GSException {
			final Geometry src;
			try {
				src = (Geometry) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(column, GSType.GEOMETRY, e);
			}
			return (src == null ? null : src);
		}

		@Override
		public void setBlob(int column, Blob fieldValue) throws GSException {
			if (fieldValue == null) {
				setPrimitiveNull(column, GSType.BLOB);
				return;
			}
			checkPrimitiveType(column, GSType.BLOB);
			setAnyValueDirect(column, shareBlob(fieldValue));
		}

		@Override
		public Blob getBlob(int column) throws GSException {
			final BlobImpl src;
			try {
				src = (BlobImpl) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(column, GSType.BLOB, e);
			}
			if (src == null) {
				return null;
			}
			return BlobImpl.shareDirect(src);
		}

		@Override
		public void setStringArray(int column, String[] fieldValue)
				throws GSException {
			if (fieldValue == null) {
				setArrayNull(column, GSType.STRING);
				return;
			}
			checkArrayType(column, GSType.STRING);
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
				throw errorTypeUnmatch(column, GSType.STRING_ARRAY, e);
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
			if (fieldValue == null) {
				setArrayNull(column, GSType.BOOL);
				return;
			}
			checkArrayType(column, GSType.BOOL);
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
				throw errorTypeUnmatch(column, GSType.BOOL_ARRAY, e);
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
			if (fieldValue == null) {
				setArrayNull(column, GSType.BYTE);
				return;
			}
			checkArrayType(column, GSType.BYTE);
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
				throw errorTypeUnmatch(column, GSType.BYTE_ARRAY, e);
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
			if (fieldValue == null) {
				setArrayNull(column, GSType.SHORT);
				return;
			}
			checkArrayType(column, GSType.SHORT);
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
				throw errorTypeUnmatch(column, GSType.SHORT_ARRAY, e);
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
			if (fieldValue == null) {
				setArrayNull(column, GSType.INTEGER);
				return;
			}
			checkArrayType(column, GSType.INTEGER);
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
				throw errorTypeUnmatch(column, GSType.INTEGER_ARRAY, e);
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
			if (fieldValue == null) {
				setArrayNull(column, GSType.LONG);
				return;
			}
			checkArrayType(column, GSType.LONG);
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
				throw errorTypeUnmatch(column, GSType.LONG_ARRAY, e);
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
			if (fieldValue == null) {
				setArrayNull(column, GSType.FLOAT);
				return;
			}
			checkArrayType(column, GSType.FLOAT);
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
				throw errorTypeUnmatch(column, GSType.FLOAT_ARRAY, e);
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
			if (fieldValue == null) {
				setArrayNull(column, GSType.DOUBLE);
				return;
			}
			checkArrayType(column, GSType.DOUBLE);
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
				throw errorTypeUnmatch(column, GSType.DOUBLE_ARRAY, e);
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
			if (fieldValue == null) {
				setArrayNull(column, GSType.TIMESTAMP);
				return;
			}
			checkArrayType(column, GSType.TIMESTAMP);
			final Date[] dest = new Date[fieldValue.length];
			System.arraycopy(fieldValue, 0, dest, 0, fieldValue.length);
			checkObjectArrayElements(column, dest);
			setAnyValueDirect(column, dest);
		}

		@Override
		public Date[] getTimestampArray(int column) throws GSException {
			final Date[] src;
			try {
				src = (Date[]) getAnyValue(column);
			}
			catch (ClassCastException e) {
				throw errorTypeUnmatch(column, GSType.TIMESTAMP_ARRAY, e);
			}
			if (src == null) {
				return null;
			}
			final Date[] dest = new Date[src.length];
			System.arraycopy(src, 0, dest, 0, src.length);
			return dest;
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
				mapper = new RowMapper(
						rowType, containerType, config.nullableAllowed);
				map.put(rowType, mapper);
				internPool.intern(mapper);
			}

			return mapper;
		}

		synchronized RowMapper intern(RowMapper mapper) {
			return internPool.intern(mapper);
		}

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
			return mapper.getEntry(index).elementType;
		}

		public static boolean isArrayColumn(RowMapper mapper, int index) {
			return mapper.getEntry(index).arrayUsed;
		}

		public static boolean isColumnNullable(RowMapper mapper, int index) {
			return mapper.getEntry(index).columnNullable;
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
			return mapper.getEntry(index).getFieldObj(rowObj, mapper.isGeneral());
		}

		public static void setFieldObj(
				RowMapper mapper, int index, Object rowObj, Object fieldObj)
				throws GSException {
			mapper.getEntry(index).setFieldObj(rowObj, fieldObj, mapper.isGeneral());
		}

		public static Blob createBlob(byte[] bytes) throws GSException {
			return DIRECT_BLOB_FACTORY.createBlob(bytes);
		}

		/*
		 * For NewSQL
		 */

		public static GSType getAnyValueType(
				Row generatedRow, int column) throws GSException {
			final Object value;
			if (!(generatedRow instanceof ArrayRow)) {
				value = generatedRow.getValue(column);
			}
			else {
				value = ((ArrayRow) generatedRow).getAnyValue(column);
			}

			if (value == null) {
				return null;
			}

			return resolveElementType(value.getClass(), true, false);
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
						throw errorNullCharacter();
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

	private static GSException errorNullCharacter() {
		return new GSException(
				GSErrorCode.ILLEGAL_VALUE_FORMAT,
				"Illegal '\\0' character found");
	}

}
