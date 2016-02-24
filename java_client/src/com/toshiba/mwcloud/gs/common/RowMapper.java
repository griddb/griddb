/*
   Copyright (c) 2012 TOSHIBA CORPORATION.

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
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Blob;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
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

import javax.sql.rowset.serial.SerialBlob;

import com.toshiba.mwcloud.gs.AggregationResult;
import com.toshiba.mwcloud.gs.ColumnInfo;
import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.ContainerType;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GSType;
import com.toshiba.mwcloud.gs.Geometry;
import com.toshiba.mwcloud.gs.QueryAnalysisEntry;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowField;
import com.toshiba.mwcloud.gs.RowKey;
import com.toshiba.mwcloud.gs.TimestampUtils;
import com.toshiba.mwcloud.gs.TransientRowField;

public class RowMapper {

	protected static boolean acceptAggregationResultColumnId = false;

	protected static boolean restrictKeyOrderFirst = true;

	private static final RowMapper AGGREGATION_RESULT_MAPPER;

	static {
		try {
			AGGREGATION_RESULT_MAPPER = new RowMapper(
					AggregationResult.class,
					AggregationResultImpl.class.getConstructor(),
					Collections.<String, Entry>emptyMap(),
					Collections.<Entry>emptyList(),
					null,
					false);
		}
		catch (NoSuchMethodException e) {
			throw new Error(e);
		}
	}

	private static final byte[] EMPTY_LONG_BYTES =
			new byte[Long.SIZE / Byte.SIZE];

	private static final int MAX_VAR_SIZE_LENGTH = 4;

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

	private static final BlobFactory SERIAL_BLOB_FACTORY =
			new BlobFactory() {
				@Override
				public Blob createBlob(byte[] data) throws GSException {
					try {
						return new SerialBlob(data);
					}
					catch (SQLException e) {
						throw new Error(e);
					}
				}
			};

	private static final Pattern METHOD_PATTERN =
			Pattern.compile("^((get)|(set)|(is))(.+)$");

	private static final Cache CACHE = new Cache();

	private static final byte ANY_NULL_TYPE = -1;

	private Class<?> rowType;

	private Constructor<?> rowConstructor;

	private final Map<String, Entry> entryMap;

	private final List<Entry> entryList;

	private Entry keyEntry;

	private final boolean forTimeSeries;

	private final Set<String> transientRowFilelds = new HashSet<String>();

	private final int variableEntryCount;

	private RowMapper(
			Class<?> rowType, Constructor<?> rowConstructor,
			Map<String, Entry> entryMap,
			List<Entry> entryList, Entry keyEntry,
			boolean forTimeSeries) {
		this.rowType = rowType;
		this.rowConstructor = rowConstructor;
		this.entryMap = entryMap;
		this.entryList = entryList;
		this.keyEntry = keyEntry;
		this.forTimeSeries = forTimeSeries;
		this.variableEntryCount = calculateVariableEntryCount(entryList);
	}

	private RowMapper(
			Class<?> rowType, boolean forTimeSeries)
			throws GSException, SecurityException {
		this.rowType = rowType;
		this.entryMap = new HashMap<String, Entry>();
		this.entryList = new ArrayList<Entry>();
		this.forTimeSeries = forTimeSeries;
		this.rowConstructor = getRowConstructor(rowType);

		for (Field field : rowType.getDeclaredFields()) {
			accept(field);
		}
		for (Method method : rowType.getDeclaredMethods()) {
			accept(method);
		}

		applyOrder(restrictKeyOrderFirst);
		checkKeyType(forTimeSeries);

		this.variableEntryCount = calculateVariableEntryCount(entryList);
	}

	private RowMapper(
			ContainerType containerType, ContainerInfo containerInfo,
			boolean anyTypeAllowed) throws GSException {
		final ContainerType anotherContainerType = containerInfo.getType();
		if (anotherContainerType != null &&
				anotherContainerType != containerType) {
			throw new GSException(
					GSErrorCode.INTERNAL_ERROR, "Inconsistent container type");
		}

		this.rowType = Row.class;
		this.entryMap = new HashMap<String, Entry>();
		this.entryList = new ArrayList<Entry>();
		this.forTimeSeries =
				(containerType == ContainerType.TIME_SERIES);
		this.rowConstructor = null;

		final int columnCount = containerInfo.getColumnCount();
		if (columnCount <= 0 && !anyTypeAllowed) {
			throw new GSException(
					GSErrorCode.ILLEGAL_SCHEMA, "Empty schema");
		}

		for (int i = 0; i < columnCount; i++) {
			accept(containerInfo.getColumnInfo(i), anyTypeAllowed);
		}

		if (containerInfo.isRowKeyAssigned()) {
			this.keyEntry = entryList.get(0);
			this.keyEntry.keyType = true;
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
			Class<?> rowType, boolean forTimeSeries)
			throws GSException, SecurityException {
		if (rowType == AggregationResult.class) {
			throw new GSException(
					GSErrorCode.ILLEGAL_PARAMETER, "Illegal row type");
		}

		return CACHE.getInstance(rowType, forTimeSeries);
	}

	public static RowMapper getInstance(
			ContainerType containerType, ContainerInfo containerInfo)
			throws GSException {
		return CACHE.intern(
				new RowMapper(containerType, containerInfo, false));
	}

	public static RowMapper getInstance(
			ContainerType containerType, ContainerInfo containerInfo,
			boolean anyTypeAllowed) throws GSException {
		return CACHE.intern(
				new RowMapper(containerType, containerInfo, anyTypeAllowed));
	}

	public static RowMapper getInstance(Row row) throws GSException {
		if (row instanceof ArrayRow) {
			return ((ArrayRow) row).mapper;
		}

		return getInstance(null, row.getSchema());
	}

	public static RowMapper getAggregationResultMapper() {
		return AGGREGATION_RESULT_MAPPER;
	}

	public static RowMapper getInstance(
			BasicBuffer in, boolean forTimeSeries) throws GSException {
		return getInstance(in, forTimeSeries, false);
	}

	public static RowMapper getInstance(
			BasicBuffer in, boolean forTimeSeries, boolean anyTypeAllowed)
			throws GSException {
		final ContainerType containerType = (forTimeSeries ?
				ContainerType.TIME_SERIES : ContainerType.COLLECTION);
		final int columnCount = in.base().getInt();
		final int keyIndex = in.base().getInt();

		final List<ColumnInfo> columnInfoList = new ArrayList<ColumnInfo>();
		for (int i = 0; i < columnCount; i++) {
			final String columnName = in.getString();
			final GSType elementType = getType(in);
			final boolean arrayUsed = in.getBoolean();

			columnInfoList.add(new ColumnInfo(
					(columnName.isEmpty() && anyTypeAllowed ? null : columnName),
					toFullType(elementType, arrayUsed)));
		}

		return getInstance(containerType, new ContainerInfo(
				null, containerType, columnInfoList, keyIndex == 0),
				anyTypeAllowed);
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
					!normalizeSymbol(thisEntry.columnName).equals(
							normalizeSymbol(entry.columnName))) {
				throw new GSException(GSErrorCode.ILLEGAL_SCHEMA, "");
			}
		}
	}

	public RowMapper reorderBySchema(
			BasicBuffer in, boolean columnOrderIgnorable) throws GSException {
		final int size = in.base().getInt();
		if (size != entryList.size()) {
			throw new GSException(
					GSErrorCode.ILLEGAL_SCHEMA,
					"Inconsistent remote schema (column count)");
		}

		final int newKeyIndex = in.base().getInt();

		final Map<String, Entry> newEntryMap = new HashMap<String, Entry>();
		final List<Entry> newEntryList = new ArrayList<Entry>(size);
		for (int i = 0; i < size; i++) {
			newEntryList.add(null);
		}

		for (int i = 0; i < size; i++) {
			final Entry newEntry = new Entry(null);
			newEntry.importColumnSchema(in, i);

			final String normalizedName = normalizeSymbol(newEntry.columnName);
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

		final Entry newKeyEntry;
		if (keyEntry == null) {
			if (newKeyIndex >= 0) {
				throw new GSException(
						GSErrorCode.ILLEGAL_SCHEMA,
						"Remote schema must not have a key");
			}
			newKeyEntry = null;
		}
		else {
			if (newKeyIndex < 0) {
				throw new GSException(
						GSErrorCode.ILLEGAL_SCHEMA,
						"Remote schema must have a key");
			}
			newKeyEntry = newEntryList.get(newKeyIndex);
			final String normalizedName = normalizeSymbol(keyEntry.columnName);
			if (!normalizedName.equals(
					normalizeSymbol(newKeyEntry.columnName))) {
				throw new GSException(
						GSErrorCode.ILLEGAL_SCHEMA,
						"Inconsistent remote schema (column name)");
			}
			newKeyEntry.keyType = true;
		}

		return CACHE.intern(new RowMapper(
				rowType, rowConstructor, newEntryMap, newEntryList, newKeyEntry,
				forTimeSeries));
	}

	public RowMapper applyResultType(Class<?> rowType) throws GSException {
		if (getRowType() == rowType) {
			return this;
		}
		else if (rowType == AggregationResult.class) {
			return AGGREGATION_RESULT_MAPPER;
		}
		else if (rowType == QueryAnalysisEntry.class) {
			return getInstance(QueryAnalysisEntry.class, false);
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

	public void exportSchema(BasicBuffer out) throws GSException {
		if (rowType == AggregationResult.class) {
			throw new GSException(
					GSErrorCode.INTERNAL_ERROR,
					"Unexpected row type: AggregationResult");
		}

		out.putInt(entryList.size());
		out.putInt(keyEntry == null ? -1 : keyEntry.order);
		for (Entry entry : entryList) {
			entry.exportColumnSchema(out);
		}
	}

	public int resolveColumnId(String name) throws GSException {
		final Entry entry = entryMap.get(normalizeSymbol(name));
		if (entry == null) {
			throw new GSException(
					GSErrorCode.UNKNOWN_COLUMN_NAME,
					"Unknown column: \"" + name + "\"");
		}
		return entry.order;
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
		return new ArrayRow(this);
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
			getInstance((Row) rowObj).checkSchemaMatched(this);
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
		cursor.endRow();
	}

	public Object decode(Cursor cursor, boolean general) throws GSException {
		final Object rowObj = createRow(general);
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

	private int scanVarDataStartOffset(Cursor cursor) {
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
			cursor.endRow();

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
		cursor.endRow();

		if (startOffset == Integer.MAX_VALUE) {
			startOffset = 0;
		}

		return startOffset;
	}

	private int scanVarDataEndOffset(Cursor cursor) {
		final BasicBuffer in = cursor.buffer;

		if (cursor.mode == MappingMode.ROWWISE_SEPARATED_V2) {
			cursor.beginRowInput();
			for (int i = 0; i < entryList.size(); i++) {
				in.base().position(in.base().position() +
						getFixedFieldPartSize(i, cursor.mode));
			}
			cursor.endRow();

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
		cursor.endRow();

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
					"Default construtor not found or specified class is non-static");
		}
		defaultConstructor.setAccessible(true);
		return defaultConstructor;
	}

	private void accept(Field field) throws GSException {
		final int modifier = field.getModifiers();
		if (Modifier.isFinal(modifier) || Modifier.isPrivate(modifier) ||
				Modifier.isStatic(modifier) || Modifier.isTransient(modifier)) {
			return;
		}

		final RowField rowField = field.getAnnotation(RowField.class);
		final String orgName = (rowField == null || rowField.name().isEmpty() ?
				field.getName() : rowField.name());
		final String name = normalizeSymbol(orgName);
		if (field.getAnnotation(TransientRowField.class) != null) {
			transientRowFilelds.add(name);
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

	private void accept(Method method) throws GSException {
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
		final String name = normalizeSymbol(orgName);
		if (method.getAnnotation(TransientRowField.class) != null) {
			transientRowFilelds.add(name);
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
			ColumnInfo columnInfo, boolean anyTypeAllowed) throws GSException {
		final String orgName = columnInfo.getName();
		final String normalizedName =
				(anyTypeAllowed && orgName == null ?
						orgName : normalizeSymbol(orgName));

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

		if (normalizedName != null) {
			entryMap.put(normalizedName, entry);
		}
		entryList.add(entry);
	}

	private Entry putEntry(String name, RowField rowField) throws GSException {
		Entry entry = entryMap.get(normalizeSymbol(name));
		if (entry == null) {
			entry = new Entry(name);
			entryMap.put(normalizeSymbol(name), entry);
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

	private void applyOrder(boolean keyFirst) throws GSException {
		boolean specified = false;
		for (Iterator<Entry> i = entryList.iterator(); i.hasNext();) {
			final Entry entry = i.next();
			final String normalizedName = normalizeSymbol(entry.columnName);
			if (!entry.reduceByAccessors() ||
					transientRowFilelds.contains(normalizedName)) {
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
			throw new InternalError();
		}

		if (keyEntry != null && keyFirst && !entryList.get(0).keyType) {
			throw new GSException("Key must be first column");
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

		cursor.endRow();
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result +
				((entryList == null) ? 0 : entryList.hashCode());
		result = prime * result +
				((entryMap == null) ? 0 : entryMap.hashCode());
		result = prime * result + (forTimeSeries ? 1231 : 1237);
		result = prime * result +
				((keyEntry == null) ? 0 : keyEntry.hashCode());
		result = prime * result +
				((rowConstructor == null) ? 0 : rowConstructor.hashCode());
		result = prime * result + ((rowType == null) ? 0 : rowType.hashCode());
		result = prime *
				result +
				((transientRowFilelds == null) ? 0 : transientRowFilelds
						.hashCode());
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
		} else if (!entryList.equals(other.entryList))
			return false;
		if (entryMap == null) {
			if (other.entryMap != null)
				return false;
		} else if (!entryMap.equals(other.entryMap))
			return false;
		if (forTimeSeries != other.forTimeSeries)
			return false;
		if (keyEntry == null) {
			if (other.keyEntry != null)
				return false;
		} else if (!keyEntry.equals(other.keyEntry))
			return false;
		if (rowConstructor == null) {
			if (other.rowConstructor != null)
				return false;
		} else if (!rowConstructor.equals(other.rowConstructor))
			return false;
		if (rowType == null) {
			if (other.rowType != null)
				return false;
		} else if (!rowType.equals(other.rowType))
			return false;
		if (transientRowFilelds == null) {
			if (other.transientRowFilelds != null)
				return false;
		} else if (!transientRowFilelds.equals(other.transientRowFilelds))
			return false;
		return true;
	}

	private static GSType resolveElementType(
			Class<?> type, boolean subClassAllowed, boolean validating)
			throws GSException {
		final Class<?> elementTypeBase;
		if (type.isArray()) {
			elementTypeBase = type.getComponentType();
		}
		else {
			elementTypeBase = type;
		}

		do {
			if (elementTypeBase == boolean.class) {
				return GSType.BOOL;
			}
			else if (elementTypeBase == byte.class) {
				return GSType.BYTE;
			}
			else if (elementTypeBase == short.class) {
				return GSType.SHORT;
			}
			else if (elementTypeBase == int.class) {
				return GSType.INTEGER;
			}
			else if (elementTypeBase == long.class) {
				return GSType.LONG;
			}
			else if (elementTypeBase == float.class) {
				return GSType.FLOAT;
			}
			else if (elementTypeBase == double.class) {
				return GSType.DOUBLE;
			}
			else if (elementTypeBase == String.class) {
				return GSType.STRING;
			}
			else if (elementTypeBase == Date.class) {
				return GSType.TIMESTAMP;
			}
			else if (elementTypeBase == Geometry.class) {
				if (validating && type.isArray()) {
					break;
				}
				return GSType.GEOMETRY;
			}
			else if (elementTypeBase == Blob.class) {
				if (validating && type.isArray()) {
					break;
				}
				return GSType.BLOB;
			}
			else if (type.isArray()) {
				break;
			}
			else if (elementTypeBase == Boolean.class) {
				return GSType.BOOL;
			}
			else if (elementTypeBase == Byte.class) {
				return GSType.BYTE;
			}
			else if (elementTypeBase == Short.class) {
				return GSType.SHORT;
			}
			else if (elementTypeBase == Integer.class) {
				return GSType.INTEGER;
			}
			else if (elementTypeBase == Long.class) {
				return GSType.LONG;
			}
			else if (elementTypeBase == Float.class) {
				return GSType.FLOAT;
			}
			else if (elementTypeBase == Double.class) {
				return GSType.DOUBLE;
			}
			else if (!subClassAllowed) {
				break;
			}
			else if (Date.class.isAssignableFrom(elementTypeBase)) {
				return GSType.TIMESTAMP;
			}
			else if (Geometry.class.isAssignableFrom(elementTypeBase)) {
				return GSType.GEOMETRY;
			}
			else if (Blob.class.isAssignableFrom(elementTypeBase)) {
				return GSType.BLOB;
			}
		}
		while (false);

		if (validating) {
			throw new GSException(
					GSErrorCode.UNSUPPORTED_FIELD_TYPE,
					"Unsupported field type (" +
					"className=" + type.getName() +
					", elementClassName=" + elementTypeBase +
					", arrayType=" + type.isArray() + ")");
		}
		return null;
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
				throw new InternalError();
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
				throw new InternalError();
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
				throw new InternalError();
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
					blob = SERIAL_BLOB_FACTORY.createBlob(rawArray);
				}
				else {
					blob = cursor.blobFactory.createBlob(rawArray);
				}
				cursor.endVarData();
				return blob;
			}
			default:
				throw new InternalError();
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
				throw new InternalError();
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
				throw new InternalError();
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

	public static String normalizeSymbol(String value) throws GSException {

		return normalizeExtendedSymbol(value, false);
	}

	public static String normalizeExtendedSymbol(String value, boolean extend)
			throws GSException {
		final String result = value.toLowerCase(Locale.US);
		if (result.isEmpty()) {
			throw new GSException(
					GSErrorCode.EMPTY_PARAMETER,
					"Empty symbol (value=\"" + value + "\")");
		}

		final char charArray[] = result.toCharArray();
		for (int i = 0; i < charArray.length; i++) {
			final char ch = charArray[i];
			if (!('a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' ||
					'0' <= ch && ch <= '9' || ch == '_')) {
				if (!extend || !(ch == '@' || ch == '/' || ch == '#')) {
					throw new GSException(
						GSErrorCode.ILLEGAL_SYMBOL_CHARACTER,
						"Illegal char in symbol (value=\"" + value + "\")");
				}
			}
			if (i == 0 && '0' <= ch && ch <= '9') {
				throw new GSException(
						GSErrorCode.ILLEGAL_SYMBOL_CHARACTER,
						"Illegal char in symbol (value=\"" + value + "\")");
			}
		}
		return result;
	}

	private static class Entry implements Cloneable {
		String columnName;
		String nameByField;
		String nameByGetter;
		Field rowTypeField;
		Method getterMethod;
		Method setterMethod;

		GSType elementType;
		boolean arrayUsed;
		boolean keyType;
		int order = -1;
		boolean orderSpecified;

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
						setterMethod.getAnnotation(RowField.class) != null ||
						setterMethod == null &&
						getterMethod.getAnnotation(RowField.class) != null) {
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
		}

		ColumnInfo getColumnInfo() {
			return new ColumnInfo(columnName, toFullType(elementType, arrayUsed));
		}

		void exportColumnSchema(BasicBuffer out) {
			out.putString(columnName);
			out.prepare(1);
			putTypePrepared(out, elementType);
			out.putBoolean(arrayUsed);
		}

		void importColumnSchema(BasicBuffer in, int order) throws GSException {
			columnName = in.getString();
			elementType = getType(in);
			arrayUsed = in.getBoolean();
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

			if (!normalizeSymbol(columnName).equals(
					normalizeSymbol(orgEntry.columnName)) ||
					elementType != orgEntry.elementType ||
					arrayUsed != orgEntry.arrayUsed) {
				throw new GSException(
						GSErrorCode.ILLEGAL_SCHEMA,
						"Inconsistent remote column");
			}

			rowTypeField = orgEntry.rowTypeField;
			getterMethod = orgEntry.getterMethod;
			setterMethod = orgEntry.setterMethod;
		}

		void encode(Cursor cursor,
				Object keyObj, Object rowObj, boolean general) throws GSException {
			final Object fieldObj = (!keyType || keyObj == null ?
					getFieldObj(rowObj, general) : keyObj);

			if (fieldObj == null && elementType != null) {
				throw new NullPointerException(
						"Null field (columnName=" + columnName +
						", type=" + toFullType(elementType, arrayUsed) + ")");
			}

			putField(cursor, fieldObj, elementType, arrayUsed);
		}

		void decode(Cursor cursor,
				Object rowObj, boolean general) throws GSException {
			setFieldObj(rowObj,
					getField(cursor, elementType, arrayUsed),
					general);
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
					((ArrayRow) rowObj).setValueDirect(order, fieldObj);
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

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (arrayUsed ? 1231 : 1237);
			result = prime * result +
					((columnName == null) ? 0 : columnName.hashCode());
			result = prime * result +
					((elementType == null) ? 0 : elementType.hashCode());
			result = prime * result +
					((getterMethod == null) ? 0 : getterMethod.hashCode());
			result = prime * result + (keyType ? 1231 : 1237);
			result = prime * result +
					((nameByField == null) ? 0 : nameByField.hashCode());
			result = prime * result +
					((nameByGetter == null) ? 0 : nameByGetter.hashCode());
			result = prime * result + order;
			result = prime * result + (orderSpecified ? 1231 : 1237);
			result = prime * result +
					((rowTypeField == null) ? 0 : rowTypeField.hashCode());
			result = prime * result +
					((setterMethod == null) ? 0 : setterMethod.hashCode());
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
			} else if (!columnName.equals(other.columnName))
				return false;
			if (elementType != other.elementType)
				return false;
			if (getterMethod == null) {
				if (other.getterMethod != null)
					return false;
			} else if (!getterMethod.equals(other.getterMethod))
				return false;
			if (keyType != other.keyType)
				return false;
			if (nameByField == null) {
				if (other.nameByField != null)
					return false;
			} else if (!nameByField.equals(other.nameByField))
				return false;
			if (nameByGetter == null) {
				if (other.nameByGetter != null)
					return false;
			} else if (!nameByGetter.equals(other.nameByGetter))
				return false;
			if (order != other.order)
				return false;
			if (orderSpecified != other.orderSpecified)
				return false;
			if (rowTypeField == null) {
				if (other.rowTypeField != null)
					return false;
			} else if (!rowTypeField.equals(other.rowTypeField))
				return false;
			if (setterMethod == null) {
				if (other.setterMethod != null)
					return false;
			} else if (!setterMethod.equals(other.setterMethod))
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

		private final int topPos;

		private final int varDataTop;

		private int partialVarDataOffset;

		private int varDataLast;

		private int pendingPos = -1;

		private long lastRowId = -1;

		private long varDataBaseOffset;

		private int nullsByteSize;

		private Cursor(BasicBuffer buffer, MappingMode mode, int rowCount,
				boolean rowIdIncluded, BlobFactory blobFactory) {
			this.buffer = buffer;
			this.mode = mode;
			this.rowCount = rowCount;
			this.rowIdIncluded = rowIdIncluded;
			this.blobFactory = blobFactory;

			this.topPos = buffer.base().position();
			switch (mode) {
			case ROWWISE_SEPARATED:
				varDataTop =
						topPos + getFixedRowPartSize(rowIdIncluded, mode) * rowCount;
				break;
			case ROWWISE_SEPARATED_V2:
				varDataTop =
						topPos + getFixedRowPartSize(rowIdIncluded, mode) * rowCount;
				nullsByteSize = getNullsByteSize(entryList.size());
				break;
			case COLUMNWISE_SEPARATED:
				if (rowIdIncluded) {
					throw new IllegalArgumentException();
				}
				varDataTop = topPos + getFixedRowPartSize(false, mode) * rowCount;
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
			if (varDataTop >= 0) {
				buffer.base().limit(varDataTop);
			}
			varDataLast = varDataTop;
			partialVarDataOffset = 0;
			pendingPos = -1;
			lastRowId = -1;
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
				for (Entry entry : entryList) {
					entry.decode(this, rowObj, general);
				}
				endRow();
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
			buffer.base().position(topPos +
					getFixedRowPartSize(rowIdIncluded, mode) * (rowIndex + 1));
		}

		private void beginRowInput() {
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
					final int elemCount = getVarSize(buffer);
					assert(elemCount == getVariableEntryCount());
					varDataLast = buffer.base().position();
					buffer.base().position(savePos);
				}
				
				buffer.base().position(buffer.base().position() + nullsByteSize);
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
					buffer.base().putLong(varOffset);
					
					final int savePos = buffer.base().position();
					buffer.base().position(varDataLast);
					putVarSize(buffer, getVariableEntryCount());
					varDataLast = buffer.base().position();
					buffer.base().position(savePos);
				}
				
				for (int i = nullsByteSize; i > 0; --i) {
					final byte nullByte = 0;
					buffer.base().put(nullByte);
				}
			}
			rowIndex++;
			fieldIndex = -1;
		}

		private void endRow() {
			if (varDataLast >= 0 && rowIndex + 1 >= rowCount) {
				buffer.base().position(varDataLast);
			}
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

	}

	private static class ArrayRow implements Row {

		private final RowMapper mapper;

		private final Object[] fieldArray;

		ArrayRow(RowMapper mapper) throws GSException {
			if (mapper.getContainerType() == null) {
				throw new GSException(GSErrorCode.UNSUPPORTED_OPERATION, "");
			}

			this.mapper = mapper;
			this.fieldArray = new Object[mapper.getColumnCount()];
		}

		private Object getInitialValue(int column) throws GSException {
			final Entry entry = mapper.entryList.get(column);
			if (entry.arrayUsed) {
				switch (entry.elementType) {
				case STRING:
					return new String[0];
				case BOOL:
					return new boolean[0];
				case BYTE:
					return new byte[0];
				case SHORT:
					return new short[0];
				case INTEGER:
					return new int[0];
				case LONG:
					return new long[0];
				case FLOAT:
					return new float[0];
				case DOUBLE:
					return new double[0];
				case TIMESTAMP:
					return new Date[0];
				default:
					throw new InternalError();
				}
			}
			else if (entry.elementType != null) {
				switch (entry.elementType) {
				case STRING:
					return "";
				case BOOL:
					return false;
				case BYTE:
					return (byte) 0;
				case SHORT:
					return (short) 0;
				case INTEGER:
					return (int) 0;
				case LONG:
					return (long) 0;
				case FLOAT:
					return (float) 0;
				case DOUBLE:
					return (double) 0;
				case TIMESTAMP:
					return new Date(0);
				case GEOMETRY:
					return Geometry.valueOf("POINT(EMPTY)");
				case BLOB:
					return SERIAL_BLOB_FACTORY.createBlob(new byte[0]);
				default:
					throw new InternalError();
				}
			}
			else {
				return null;
			}
		}

		private void checkPrimitiveType(
				int column, GSType elementType) throws GSException {
			final Entry entry;
			try {
				entry = mapper.entryList.get(column);
			}
			catch (IndexOutOfBoundsException e) {
				throw new GSException(GSErrorCode.ILLEGAL_PARAMETER, "", e);
			}

			if (entry.arrayUsed || elementType != entry.elementType) {
				throw new GSException(GSErrorCode.ILLEGAL_PARAMETER, "");
			}
		}

		private void checkArrayType(
				int column, GSType elementType) throws GSException {
			final Entry entry;
			try {
				entry = mapper.entryList.get(column);
			}
			catch (IndexOutOfBoundsException e) {
				throw new GSException(GSErrorCode.ILLEGAL_PARAMETER, "", e);
			}

			if (!entry.arrayUsed || elementType != entry.elementType) {
				throw new GSException(GSErrorCode.ILLEGAL_PARAMETER, "");
			}
		}

		private void setValueDirect(int column, Object fieldValue)
				throws GSException {
			try {
				if (fieldValue == null) {
					if (mapper.entryList.get(column).elementType != null) {
						throw new GSException(GSErrorCode.EMPTY_PARAMETER, "");
					}
				}

				fieldArray[column] = fieldValue;
			}
			catch (IndexOutOfBoundsException e) {
				throw new GSException(GSErrorCode.ILLEGAL_PARAMETER, e);
			}
		}

		private Object getValueDirect(int column) throws GSException {
			final Object fieldValue;
			try {
				fieldValue = fieldArray[column];
			}
			catch (IndexOutOfBoundsException e) {
				throw new GSException(GSErrorCode.ILLEGAL_PARAMETER, e);
			}

			if (fieldValue == null) {
				return getInitialValue(column);
			}

			return fieldValue;
		}

		private void setNonNullValueDirect(int column, Object fieldValue)
				throws GSException {
			try {
				fieldArray[column] = fieldValue;
			}
			catch (IndexOutOfBoundsException e) {
				throw new GSException(GSErrorCode.ILLEGAL_PARAMETER, e);
			}
		}

		private void checkObjectArrayElements(
				Object[] arrayElements) throws GSException {
			for (Object element : arrayElements) {
				if (element == null) {
					throw new GSException(GSErrorCode.EMPTY_PARAMETER, "");
				}
			}
		}

		private Blob tryCopyBlob(Blob src) throws GSException {
			if (src == null) {
				return null;
			}

			try {
				if (src.length() == 0) {
					return SERIAL_BLOB_FACTORY.createBlob(new byte[0]);
				}
				return new SerialBlob(src);
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
			final Entry entry;
			try {
				entry = mapper.entryList.get(column);
			}
			catch (IndexOutOfBoundsException e) {
				throw new GSException(GSErrorCode.ILLEGAL_PARAMETER, "", e);
			}

			final boolean typeMatched;
			if (entry.arrayUsed) {
				switch (entry.elementType) {
				case STRING:
					if (fieldValue instanceof String[]) {
						setStringArray(column, (String[]) fieldValue);
						return;
					}
					break;
				case BOOL:
					if (fieldValue instanceof boolean[]) {
						setBoolArray(column, (boolean[]) fieldValue);
						return;
					}
					break;
				case BYTE:
					if (fieldValue instanceof byte[]) {
						setByteArray(column, (byte[]) fieldValue);
						return;
					}
					break;
				case SHORT:
					if (fieldValue instanceof short[]) {
						setShortArray(column, (short[]) fieldValue);
						return;
					}
					break;
				case INTEGER:
					if (fieldValue instanceof int[]) {
						setIntegerArray(column, (int[]) fieldValue);
						return;
					}
					break;
				case LONG:
					if (fieldValue instanceof long[]) {
						setLongArray(column, (long[]) fieldValue);
						return;
					}
					break;
				case FLOAT:
					if (fieldValue instanceof float[]) {
						setFloatArray(column, (float[]) fieldValue);
						return;
					}
					break;
				case DOUBLE:
					if (fieldValue instanceof double[]) {
						setDoubleArray(column, (double[]) fieldValue);
						return;
					}
					break;
				case TIMESTAMP:
					if (fieldValue instanceof Date[]) {
						setTimestampArray(column, (Date[]) fieldValue);
						return;
					}
					break;
				default:
					throw new InternalError();
				}
				typeMatched = false;
			}
			else if (entry.elementType != null) {
				switch (entry.elementType) {
				case STRING:
					typeMatched = (fieldValue instanceof String);
					break;
				case BOOL:
					typeMatched = (fieldValue instanceof Boolean);
					break;
				case BYTE:
					typeMatched = (fieldValue instanceof Byte);
					break;
				case SHORT:
					typeMatched = (fieldValue instanceof Short);
					break;
				case INTEGER:
					typeMatched = (fieldValue instanceof Integer);
					break;
				case LONG:
					typeMatched = (fieldValue instanceof Long);
					break;
				case FLOAT:
					typeMatched = (fieldValue instanceof Float);
					break;
				case DOUBLE:
					typeMatched = (fieldValue instanceof Double);
					break;
				case TIMESTAMP:
					typeMatched = (fieldValue instanceof Date);
					break;
				case GEOMETRY:
					typeMatched = (fieldValue instanceof Geometry);
					break;
				case BLOB:
					if (fieldValue instanceof Blob) {
						setBlob(column, (Blob) fieldValue);
						return;
					}
					typeMatched = false;
					break;
				default:
					throw new InternalError();
				}
			}
			else {
				if (fieldValue != null) {
					resolveElementType(fieldValue.getClass(), true, true);
				}
				typeMatched = true;
			}

			if (!typeMatched) {
				if (fieldValue == null) {
					throw new GSException(GSErrorCode.EMPTY_PARAMETER, "");
				}
				else {
					throw new GSException(GSErrorCode.ILLEGAL_PARAMETER, "");
				}
			}
			setValueDirect(column, fieldValue);
		}

		@Override
		public Object getValue(int column) throws GSException {
			final Entry entry;
			try {
				entry = mapper.entryList.get(column);
			}
			catch (IndexOutOfBoundsException e) {
				throw new GSException(GSErrorCode.ILLEGAL_PARAMETER, "", e);
			}

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
					throw new InternalError();
				}
			}
			else {
				if (entry.elementType == GSType.BLOB) {
					return getBlob(column);
				}
				return getValueDirect(column);
			}
		}

		@Override
		public void setString(int column, String fieldValue)
				throws GSException {
			checkPrimitiveType(column, GSType.STRING);
			setValueDirect(column, fieldValue);
		}

		@Override
		public String getString(int column) throws GSException {
			checkPrimitiveType(column, GSType.STRING);
			return (String) getValueDirect(column);
		}

		@Override
		public void setBool(int column, boolean fieldValue)
				throws GSException {
			checkPrimitiveType(column, GSType.BOOL);
			setValueDirect(column, fieldValue);
		}

		@Override
		public boolean getBool(int column) throws GSException {
			checkPrimitiveType(column, GSType.BOOL);
			return (Boolean) getValueDirect(column);
		}

		@Override
		public void setByte(int column, byte fieldValue)
				throws GSException {
			checkPrimitiveType(column, GSType.BYTE);
			setValueDirect(column, fieldValue);
		}

		@Override
		public byte getByte(int column) throws GSException {
			checkPrimitiveType(column, GSType.BYTE);
			return (Byte) getValueDirect(column);
		}

		@Override
		public void setShort(int column, short fieldValue)
				throws GSException {
			checkPrimitiveType(column, GSType.SHORT);
			setValueDirect(column, fieldValue);
		}

		@Override
		public short getShort(int column) throws GSException {
			checkPrimitiveType(column, GSType.SHORT);
			return (Short) getValueDirect(column);
		}

		@Override
		public void setInteger(int column, int fieldValue)
				throws GSException {
			checkPrimitiveType(column, GSType.INTEGER);
			setValueDirect(column, fieldValue);
		}

		@Override
		public int getInteger(int column) throws GSException {
			checkPrimitiveType(column, GSType.INTEGER);
			return (Integer) getValueDirect(column);
		}

		@Override
		public void setLong(int column, long fieldValue)
				throws GSException {
			checkPrimitiveType(column, GSType.LONG);
			setValueDirect(column, fieldValue);
		}

		@Override
		public long getLong(int column) throws GSException {
			checkPrimitiveType(column, GSType.LONG);
			return (Long) getValueDirect(column);
		}

		@Override
		public void setFloat(int column, float fieldValue)
				throws GSException {
			checkPrimitiveType(column, GSType.FLOAT);
			setValueDirect(column, fieldValue);
		}

		@Override
		public float getFloat(int column) throws GSException {
			checkPrimitiveType(column, GSType.FLOAT);
			return (Float) getValueDirect(column);
		}

		@Override
		public void setDouble(int column, double fieldValue)
				throws GSException {
			checkPrimitiveType(column, GSType.DOUBLE);
			setValueDirect(column, fieldValue);
		}

		@Override
		public double getDouble(int column) throws GSException {
			checkPrimitiveType(column, GSType.DOUBLE);
			return (Double) getValueDirect(column);
		}

		@Override
		public void setTimestamp(int column, Date fieldValue)
				throws GSException {
			checkPrimitiveType(column, GSType.TIMESTAMP);
			setValueDirect(column, fieldValue);
		}

		@Override
		public Date getTimestamp(int column) throws GSException {
			checkPrimitiveType(column, GSType.TIMESTAMP);
			return (Date) getValueDirect(column);
		}

		@Override
		public void setGeometry(int column, Geometry fieldValue)
				throws GSException {
			checkPrimitiveType(column, GSType.GEOMETRY);
			setValueDirect(column, fieldValue);
		}

		@Override
		public Geometry getGeometry(int column) throws GSException {
			checkPrimitiveType(column, GSType.GEOMETRY);
			return (Geometry) getValueDirect(column);
		}

		@Override
		public void setBlob(int column, Blob fieldValue)
				throws GSException {
			checkPrimitiveType(column, GSType.BLOB);
			setValueDirect(column, tryCopyBlob(fieldValue));
		}

		@Override
		public Blob getBlob(int column) throws GSException {
			checkPrimitiveType(column, GSType.BLOB);
			if (fieldArray[column] == null) {
				return (Blob) getInitialValue(column);
			}
			return tryCopyBlob((Blob) getValueDirect(column));
		}

		@Override
		public void setStringArray(int column, String[] fieldValue)
				throws GSException {
			checkArrayType(column, GSType.STRING);
			if (fieldValue == null) {
				setValueDirect(column, null);
				return;
			}
			checkObjectArrayElements(fieldValue);
			final String[] dest = new String[fieldValue.length];
			System.arraycopy(fieldValue, 0, dest, 0, fieldValue.length);
			setNonNullValueDirect(column, dest);
		}

		@Override
		public String[] getStringArray(int column) throws GSException {
			checkArrayType(column, GSType.STRING);
			final String[] src = (String[]) getValueDirect(column);
			final String[] dest = new String[src.length];
			System.arraycopy(src, 0, dest, 0, src.length);
			return dest;
		}

		@Override
		public void setBoolArray(int column, boolean[] fieldValue)
				throws GSException {
			checkArrayType(column, GSType.BOOL);
			if (fieldValue == null) {
				setValueDirect(column, null);
				return;
			}
			final boolean[] dest = new boolean[fieldValue.length];
			System.arraycopy(fieldValue, 0, dest, 0, fieldValue.length);
			setNonNullValueDirect(column, dest);
		}

		@Override
		public boolean[] getBoolArray(int column) throws GSException {
			checkArrayType(column, GSType.BOOL);
			final boolean[] src = (boolean[]) getValueDirect(column);
			final boolean[] dest = new boolean[src.length];
			System.arraycopy(src, 0, dest, 0, src.length);
			return dest;
		}

		@Override
		public void setByteArray(int column, byte[] fieldValue)
				throws GSException {
			checkArrayType(column, GSType.BYTE);
			if (fieldValue == null) {
				setValueDirect(column, null);
				return;
			}
			final byte[] dest = new byte[fieldValue.length];
			System.arraycopy(fieldValue, 0, dest, 0, fieldValue.length);
			setNonNullValueDirect(column, dest);
		}

		@Override
		public byte[] getByteArray(int column) throws GSException {
			checkArrayType(column, GSType.BYTE);
			final byte[] src = (byte[]) getValueDirect(column);
			final byte[] dest = new byte[src.length];
			System.arraycopy(src, 0, dest, 0, src.length);
			return dest;
		}

		@Override
		public void setShortArray(int column, short[] fieldValue)
				throws GSException {
			checkArrayType(column, GSType.SHORT);
			if (fieldValue == null) {
				setValueDirect(column, null);
				return;
			}
			final short[] dest = new short[fieldValue.length];
			System.arraycopy(fieldValue, 0, dest, 0, fieldValue.length);
			setNonNullValueDirect(column, dest);
		}

		@Override
		public short[] getShortArray(int column) throws GSException {
			checkArrayType(column, GSType.SHORT);
			final short[] src = (short[]) getValueDirect(column);
			final short[] dest = new short[src.length];
			System.arraycopy(src, 0, dest, 0, src.length);
			return dest;
		}

		@Override
		public void setIntegerArray(int column, int[] fieldValue)
				throws GSException {
			checkArrayType(column, GSType.INTEGER);
			if (fieldValue == null) {
				setValueDirect(column, null);
				return;
			}
			final int[] dest = new int[fieldValue.length];
			System.arraycopy(fieldValue, 0, dest, 0, fieldValue.length);
			setNonNullValueDirect(column, dest);
		}

		@Override
		public int[] getIntegerArray(int column) throws GSException {
			checkArrayType(column, GSType.INTEGER);
			final int[] src = (int[]) getValueDirect(column);
			final int[] dest = new int[src.length];
			System.arraycopy(src, 0, dest, 0, src.length);
			return dest;
		}

		@Override
		public void setLongArray(int column, long[] fieldValue)
				throws GSException {
			checkArrayType(column, GSType.LONG);
			if (fieldValue == null) {
				setValueDirect(column, null);
				return;
			}
			final long[] dest = new long[fieldValue.length];
			System.arraycopy(fieldValue, 0, dest, 0, fieldValue.length);
			setNonNullValueDirect(column, dest);
		}

		@Override
		public long[] getLongArray(int column) throws GSException {
			checkArrayType(column, GSType.LONG);
			final long[] src = (long[]) getValueDirect(column);
			final long[] dest = new long[src.length];
			System.arraycopy(src, 0, dest, 0, src.length);
			return dest;
		}

		@Override
		public void setFloatArray(int column, float[] fieldValue)
				throws GSException {
			checkArrayType(column, GSType.FLOAT);
			if (fieldValue == null) {
				setValueDirect(column, null);
				return;
			}
			final float[] dest = new float[fieldValue.length];
			System.arraycopy(fieldValue, 0, dest, 0, fieldValue.length);
			setNonNullValueDirect(column, dest);
		}

		@Override
		public float[] getFloatArray(int column) throws GSException {
			checkArrayType(column, GSType.FLOAT);
			final float[] src = (float[]) getValueDirect(column);
			final float[] dest = new float[src.length];
			System.arraycopy(src, 0, dest, 0, src.length);
			return dest;
		}

		@Override
		public void setDoubleArray(int column, double[] fieldValue)
				throws GSException {
			checkArrayType(column, GSType.DOUBLE);
			if (fieldValue == null) {
				setValueDirect(column, null);
				return;
			}
			final double[] dest = new double[fieldValue.length];
			System.arraycopy(fieldValue, 0, dest, 0, fieldValue.length);
			setNonNullValueDirect(column, dest);
		}

		@Override
		public double[] getDoubleArray(int column) throws GSException {
			checkArrayType(column, GSType.DOUBLE);
			final double[] src = (double[]) getValueDirect(column);
			final double[] dest = new double[src.length];
			System.arraycopy(src, 0, dest, 0, src.length);
			return dest;
		}

		@Override
		public void setTimestampArray(int column, Date[] fieldValue)
				throws GSException {
			checkArrayType(column, GSType.TIMESTAMP);
			if (fieldValue == null) {
				setValueDirect(column, null);
				return;
			}
			checkObjectArrayElements(fieldValue);
			final Date[] dest = new Date[fieldValue.length];
			System.arraycopy(fieldValue, 0, dest, 0, fieldValue.length);
			setNonNullValueDirect(column, dest);
		}

		@Override
		public Date[] getTimestampArray(int column) throws GSException {
			checkArrayType(column, GSType.TIMESTAMP);
			final Date[] src = (Date[]) getValueDirect(column);
			final Date[] dest = new Date[src.length];
			System.arraycopy(src, 0, dest, 0, src.length);
			return dest;
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
				Class<?> rowType, boolean forTimeSeries)
				throws GSException {
			final Map<Class<?>, RowMapper> map =
					(forTimeSeries ? timeSeriesMap : collectionMap);
			RowMapper mapper = map.get(rowType);
			if (mapper == null) {
				mapper = new RowMapper(rowType, forTimeSeries);
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
			return getInstance(containerSchema.getType(), containerSchema);
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
				value = ((ArrayRow) generatedRow).getValueDirect(column);
			}

			if (value == null) {
				return null;
			}

			return resolveElementType(value.getClass(), true, false);
		}

	}

	
	
	
	
	
	
	
	
	
	static final int VAR_SIZE_1BYTE_THRESHOLD = 128;

	static final boolean varSizeIs1Byte(int val) {
		return ((val & 0x01) == 0x01);
	}
	static final boolean varSizeIs4Byte(int val) {
		return ((val & 0x03) == 0x00);
	}
	static final boolean varSizeIsOId(int val) {
		return ((val & 0x03) == 0x02);
	}

	static final boolean varSizeIs1Byte(byte val) {
		return ((val & 0x01) == 0x01);
	}
	static final boolean varSizeIs4Byte(byte val) {
		return ((val & 0x03) == 0x00);
	}
	static final boolean varSizeIsOId(byte val) {
		return ((val & 0x03) == 0x02);
	}

	static final int decode1ByteVarSize(int val) { 
		return (val & 0xff) >>> 1; 
	}
	static final int decode4ByteVarSize(int val) { 
		return val >>> 2;
	}
	static final long decode8ByteVarSize(long val) { 
		return val >>> 2;
	}

	static final int encode1ByteVarSize(int val) {
		return (val << 1) | 0x1;
	}
	static final int encode4ByteVarSize(int val) {
		return val << 2;
	}
	static final long encode8ByteVarSize(long val) {
		return (val << 2) | 0x2;
	}

	static final int encodeVarSize(int val) {
		assert(val < 0x40000000L);
		if (val < VAR_SIZE_1BYTE_THRESHOLD) {
			return ((val << 1L) | 0x01);
		} else {
			return (val << 2L);
		}
	}

	static final long encodeVarSizeOId(long val) {
		
		return (val << 2L) | 0x02;
	}

	static final int getEncodedLength(int val) {
		if (val < VAR_SIZE_1BYTE_THRESHOLD) {
			return 1;
		} else {
			return 4;
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
			out.put((byte)(encode1ByteVarSize(value)));
		} else {
			out.putInt(encode4ByteVarSize(value));
		}
	}

	static void putVarSizePrepared(BasicBuffer out, int value) {
		if (value < VAR_SIZE_1BYTE_THRESHOLD) {
			out.base().put((byte)(encode1ByteVarSize(value)));
		} else {
			out.base().putInt(encode4ByteVarSize(value));
		}
	}

	static final int getVarSize(BasicBuffer in) {
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
			throw new RuntimeException();
		}
	}

	static void putString(BasicBuffer out, String value, boolean varSizeMode) {
		if (varSizeMode) {
			final byte[] buf = value.getBytes(BasicBuffer.DEFAULT_CHARSET);
			out.prepare(MAX_VAR_SIZE_LENGTH + buf.length);
			putVarSizePrepared(out, buf.length);
			out.base().put(buf);
		}
		else {
			out.putString(value);
		}
	}

	static String getString(BasicBuffer in, boolean varSizeMode) {
		final int bytesLength =
				varSizeMode ? getVarSize(in) : in.base().getInt();

		final byte[] buf = new byte[bytesLength];
		in.base().get(buf);
		return new String(buf, 0, buf.length, BasicBuffer.DEFAULT_CHARSET);
	}

}
