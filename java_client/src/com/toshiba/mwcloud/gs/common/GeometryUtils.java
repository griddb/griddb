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

import java.nio.BufferUnderflowException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.Geometry;

public class GeometryUtils {

	private enum GeometryType {
		POINT,
		LINESTRING,
		LINE,	
		LINEARRING,	
		POLYGON,
		POINTGEOM,	
		MULTIPOINT,	
		MULTISTRING,	
		MULTIPOLYGON,	
		POLYHEDRALSURFACE,
		QUADRATICSURFACE
	}

	
	private static boolean strictMode = false;

	private static final int SR_ID_DEFAULT = -1;

	private static final Pattern SR_ID_BASE_PATTERN =
			Pattern.compile("[^\\s,();]+");

	private static final String SR_ID_PATTERN_STR =
			"(;\\s*" + SR_ID_BASE_PATTERN.pattern() + "\\s*)?";

	private static final short PV3KEY_NONE = 0;

	private static boolean specialPointValueEnabled = false;

	private static final String EMPTY_CONTENT = "EMPTY";

	private static final String NAN_STRING = "NAN";

	private static final String POSITIVE_INFINITY_STRING = "INF";

	private static final String NEGATIVE_INFINITY_STRING = "-INF";

	private static final Pattern GEOMETRY_MAIN_PETTERN = Pattern.compile(
			"^(\\w+)\\s*" +
			"(\\(\\s*" + EMPTY_CONTENT + "\\s*\\)\\s*|(.*))" +
			"$");

	private static final Pattern POINT_PETTERN =
			Pattern.compile("^([^\\s,();]+)\\s+([^\\s,();]+)(\\s+([^\\s,();]+))?$");

	private static final int[] LOWER_PATTERN_LEVELS = { -1, 0, 0, 2, 3 };

	private static final int[] COUNT_BIASES = { 0, 0, 0, 1, 0 };

	private static final Pattern[] POINTS_PATTERN_ARRAY;

	static {
		final Pattern base =
				Pattern.compile("[^\\s,();][^,();]*\\s[^,();]*[^\\s,();]");
		final Pattern[] array = new Pattern[5];

		array[0] = base;
		
		array[1] = parenthesisePattern(base);
		
		array[2] = parenthesisePattern(repeatPattern(base));
		
		array[3] = parenthesisePattern(repeatPattern(array[2]));
		
		array[4] = parenthesisePattern(repeatPattern(array[3]));

		POINTS_PATTERN_ARRAY = array;
	}

	private static Pattern parenthesisePattern(Pattern base) {
		return Pattern.compile(
				"\\(\\s*" + base.pattern() + "\\s*" + SR_ID_PATTERN_STR + "\\)");
	}

	private static Pattern repeatPattern(Pattern base) {
		return Pattern.compile(
				base.pattern() + "(?:\\s*,\\s*" + base.pattern() + ")*");
	}

	private static final Pattern QUADRATIC_SURFACE = Pattern.compile(
			"(\\(\\s*" +
			
			"([^\\s,();]+)\\s+([^\\s,();]+)\\s+([^\\s,();]+)\\s+" +
			
			"([^\\s,();]+)\\s+([^\\s,();]+)\\s+([^\\s,();]+)\\s+" +
			
			"([^\\s,();]+)\\s+([^\\s,();]+)\\s+([^\\s,();]+)\\s+" +
			
			"([^\\s,();]+)\\s+([^\\s,();]+)\\s+([^\\s,();]+)\\s+" +
			
			"([^\\s,();]+)\\s+" +
			
			"([^\\s,();]+)\\s+([^\\s,();]+)\\s+([^\\s,();]+)\\s*" +
			SR_ID_PATTERN_STR + "\\))");

	private static final int QUADRATICSURFACE_ELEMENT_COUNT = 16;

	public static byte[] encodeGeometry(String geometryStr) throws GSException {
		final Matcher typeMatcher =
				GEOMETRY_MAIN_PETTERN.matcher(geometryStr);
		if (!typeMatcher.matches()) {
			throw new GSException(
					GSErrorCode.ILLEGAL_VALUE_FORMAT,
					"Illegal geometry string: " + geometryStr);
		}

		final String geometryTypeStr = typeMatcher.group(1);
		final String geometryValuePart = typeMatcher.group(3);

		final BasicBuffer buffer = new BasicBuffer(256);
		final GeometryType geometryType;
		try {
			try {
				geometryType = GeometryType.valueOf(geometryTypeStr);
			}
			catch (IllegalArgumentException e) {
				throw new GSException(
						GSErrorCode.ILLEGAL_VALUE_FORMAT,
						"Unsupported geometry type: " + geometryTypeStr);
			}

			buffer.putShort((short) geometryType.ordinal());
			final int srIdPos = buffer.base().position();
			final int srId;
			buffer.putInt(SR_ID_DEFAULT);
			switch (geometryType) {
			case POINT:
				srId = encodePoints(buffer, geometryValuePart, 1);
				break;
			case LINESTRING:
				srId = encodePoints(buffer, geometryValuePart, 2);
				break;
			case POLYGON:
				srId = encodePoints(buffer, geometryValuePart, 3);
				break;
			case POLYHEDRALSURFACE:
				srId = encodePoints(buffer, geometryValuePart, 4);
				break;
			case QUADRATICSURFACE:
				srId = encodeQuadraticSurface(buffer, geometryValuePart);
				break;
			default:
				throw new GSException(
						GSErrorCode.INTERNAL_ERROR,
						"Unsupported geometry type: " + geometryTypeStr);
			}

			if (srId != SR_ID_DEFAULT) {
				final int endPos = buffer.base().position();
				buffer.base().position(srIdPos);
				buffer.putInt(srId);
				buffer.base().position(endPos);
			}

			buffer.base().flip();
			final byte[] result = new byte[buffer.base().limit()];
			buffer.base().get(result);

			return result;
		}
		catch (GSException e) {
			throw new GSException(
					GSErrorCode.ILLEGAL_VALUE_FORMAT,
					"Illegal geometry string: " + geometryStr, e);
		}
	}

	private static int encodePoints(
			BasicBuffer buffer, String str, int level) throws GSException {
		if (str == null) {
			final byte dimension = 0;
			buffer.put(dimension);

			return SR_ID_DEFAULT;
		}
		else {
			final Matcher matcher =
					POINTS_PATTERN_ARRAY[level].matcher(str);
			if (!matcher.matches()) {
				throw new GSException(
						GSErrorCode.ILLEGAL_VALUE_FORMAT,
						"Illegal format: " + str);
			}

			final int srId = parseSrId(matcher.group(matcher.groupCount()));
			final int startPos = buffer.base().position();

			final byte dummyDimension = 0;
			buffer.put(dummyDimension);

			final int dimension = encodePointsContent(buffer, str, level);
			final int endPos = buffer.base().position();

			buffer.base().position(startPos);
			buffer.put((byte) dimension);
			buffer.base().position(endPos);

			return srId;
		}
	}

	private static int encodePointsContent(
			BasicBuffer buffer, String str, int level)
			throws GSException {
		if (level > 0) {
			final int startPos = buffer.base().position();
			final short dummyCount = 0;
			if (level > 1) {
				buffer.putShort(dummyCount);
			}

			final int lowerLevel = LOWER_PATTERN_LEVELS[level];
			final Matcher matcher =
					POINTS_PATTERN_ARRAY[lowerLevel].matcher(str);
			int count = 0;
			int dimension = -1;
			while (matcher.find()) {
				if (lowerLevel > 0 &&
						matcher.group(matcher.groupCount()) != null) {
					throw new GSException(
							GSErrorCode.ILLEGAL_PARAMETER,
							"Illegal SR_ID: " + str);
				}
				final int lastDimension = encodePointsContent(
						buffer, matcher.group(), lowerLevel);
				if (count > 0 && dimension != lastDimension) {
					throw new GSException(
							GSErrorCode.ILLEGAL_PARAMETER,
							"Inconsistent dimension: " + str);
				}
				dimension = lastDimension;
				count++;
			}

			if (count > Short.MAX_VALUE) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"Too many points: " + str);
			}

			if (level > 1) {
				final int endPos = buffer.base().position();
				buffer.base().position(startPos);
				buffer.putShort((short) (count - COUNT_BIASES[level]));
				buffer.base().position(endPos);
			}

			return dimension;
		}
		else {
			final Matcher matcher = POINT_PETTERN.matcher(str);
			if (!matcher.find()) {
				throw new GSException(
						GSErrorCode.ILLEGAL_VALUE_FORMAT,
						"Illegal format: " + str);
			}

			encodeDoubleValue(buffer, matcher.group(1));
			encodeDoubleValue(buffer, matcher.group(2));
			final String lastElementStr = matcher.group(4);
			if (lastElementStr != null) {
				encodeDoubleValue(buffer, lastElementStr);
				return 3;
			}
			else {
				buffer.putDouble(Double.NaN);
				return 2;
			}
		}
	}

	private static int encodeQuadraticSurface(BasicBuffer buffer, String str)
			throws GSException {
		if (str == null) {
			final byte dimension = 0;
			buffer.put(dimension);

			return SR_ID_DEFAULT;
		}
		else {
			final byte dimension = 3;
			buffer.put(dimension);

			buffer.putShort(PV3KEY_NONE);
			final Matcher matcher = QUADRATIC_SURFACE.matcher(str);
			if (!matcher.find()) {
				throw new GSException(
						GSErrorCode.ILLEGAL_VALUE_FORMAT,
						"Illegal format: " + str);
			}

			final int srId = parseSrId(matcher.group(matcher.groupCount()));

			for (int i = 2; i < matcher.groupCount(); i++) {
				encodeDoubleValue(buffer, matcher.group(i));
			}

			return srId;
		}
	}

	private static void encodeDoubleValue(BasicBuffer buffer, String valueStr)
			throws GSException {
		final double value;
		try {
			final boolean	special;
			if (valueStr.equals(NAN_STRING)) {
				special = true;
				value = Double.NaN;
			}
			else if (valueStr.equals(POSITIVE_INFINITY_STRING)) {
				special = true;
				value = Double.POSITIVE_INFINITY;
			}
			else if (valueStr.equals(NEGATIVE_INFINITY_STRING)) {
				special = true;
				value = Double.NEGATIVE_INFINITY;
			}
			else {
				special = false;
				value = Double.parseDouble(valueStr);
			}

			if (special && !specialPointValueEnabled) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"Illegal value: " + value);
			}
		}
		catch (NumberFormatException e) {
			throw new GSException(
					GSErrorCode.ILLEGAL_VALUE_FORMAT,
					"Illegal point element: " + valueStr, e);
		}
		buffer.putDouble(value);
	}

	private static int parseSrId(String valueStr) throws GSException {
		if (valueStr == null) {
			return SR_ID_DEFAULT;
		}

		try {
			final Matcher matcher = SR_ID_BASE_PATTERN.matcher(valueStr);
			matcher.find();
			return Integer.parseInt(matcher.group());
		}
		catch (NumberFormatException e) {
			throw new GSException(
					GSErrorCode.ILLEGAL_VALUE_FORMAT,
					"Illegal SR_ID: " + valueStr, e);
		}
	}

	public static String decodeGeometry(byte[] bytesData)
			throws GSException {
		final BasicBuffer in = new BasicBuffer(bytesData.length);
		in.base().put(bytesData);
		in.base().flip();

		final StringBuilder out = new StringBuilder();
		try {
			final GeometryType geometryType = decodeGeometryType(in, out);
			final int srId = in.base().getInt();

			switch (geometryType) {
			case POINT:
				decodePoints(in, out, 1, srId);
				break;
			case LINESTRING:
				decodePoints(in, out, 2, srId);
				break;
			case POLYGON:
				decodePoints(in, out, 3, srId);
				break;
			case POLYHEDRALSURFACE:
				decodePoints(in, out, 4, srId);
				break;
			case QUADRATICSURFACE:
				decodeQuadraticSurface(in, out, srId);
				break;
			default:
				throw new GSException(
						GSErrorCode.INTERNAL_ERROR,
						"Unsupported geometry type: " + geometryType);
			}
		}
		catch (BufferUnderflowException e) {
			throw new GSException(
					GSErrorCode.ILLEGAL_PARAMETER,
					"Illegal input", e);
		}

		if (strictMode) {
			final int remaining = in.base().remaining();
			if (remaining > 0) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"Unknown part exists (size=" + remaining + ")");
			}
		}

		return out.toString();
	}

	private static GeometryType decodeGeometryType(
			BasicBuffer in, StringBuilder out) throws GSException {
		final GeometryType[] typeArray = GeometryType.class.getEnumConstants();

		final short typeNumber = in.base().getShort();
		if (typeNumber < 0 || typeNumber >= typeArray.length) {
			throw new GSException(
					GSErrorCode.ILLEGAL_PARAMETER,
					"Unsupported geometry type: " + typeNumber);
		}

		final GeometryType type = typeArray[typeNumber];
		out.append(type);

		return type;
	}

	private static void decodePoints(
			BasicBuffer in, StringBuilder out, int level, int srId) throws GSException {
		final int dimension = in.base().get();
		switch (dimension) {
		case 0:
			out.append("(");
			out.append(EMPTY_CONTENT);
			out.append(")");
			return;
		case 2:
			decodePointsContent(in, out, dimension, level, srId);
			return;
		case 3:
			decodePointsContent(in, out, dimension, level, srId);
			return;
		default:
			throw new GSException(
					GSErrorCode.ILLEGAL_PARAMETER,
					"Unsupported dimension: " + dimension);
		}
	}

	private static void decodePointsContent(
			BasicBuffer in, StringBuilder out,
			int dimension, int level, int srId) throws GSException {
		if (level > 0) {
			final int count =
					(level > 1 ? in.base().getShort() : 1) + COUNT_BIASES[level];
			out.append("(");
			if (count <= 0) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"Illegal points count: " + count);
			}
			else {
				for (int i = 0; i < count; i++) {
					if (i > 0) {
						out.append(",");
					}
					decodePointsContent(
							in, out, dimension, LOWER_PATTERN_LEVELS[level],
							SR_ID_DEFAULT);
				}
			}
			decodeSrId(out, srId);
			out.append(")");
		}
		else {
			final double x = in.base().getDouble();
			final double y = in.base().getDouble();
			final double z = in.base().getDouble();
			decodeDoubleValue(out, x);
			out.append(" ");
			decodeDoubleValue(out, y);
			if (dimension > 2) {
				out.append(" ");
				decodeDoubleValue(out, z);
			}
		}
	}

	private static void decodeQuadraticSurface(
			BasicBuffer in, StringBuilder out, int srId) throws GSException {
		out.append("(");
		final int dimension = in.base().get();
		if (dimension == 0) {
			out.append(EMPTY_CONTENT);
		}
		else {
			final short pv3key = in.base().getShort();
			if (pv3key != PV3KEY_NONE) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PARAMETER,
						"Illegal pv3key: " + pv3key);
			}
			for (int i = 0; i < QUADRATICSURFACE_ELEMENT_COUNT; i++) {
				if (i > 0) {
					out.append(" ");
				}
				decodeDoubleValue(out, in.base().getDouble());
			}
			decodeSrId(out, srId);
		}
		out.append(")");
	}

	private static void decodeDoubleValue(
			StringBuilder out, double value) throws GSException {
		String valueStr;
		final boolean special;
		if (Double.isInfinite(value)) {
			special = true;
			if (value < 0) {
				valueStr = NEGATIVE_INFINITY_STRING;
			}
			else {
				valueStr = POSITIVE_INFINITY_STRING;
			}
		}
		else if (Double.isNaN(value)) {
			special = true;
			valueStr = NAN_STRING;
		}
		else {
			special = false;
			valueStr = Double.toString(value);
			if (valueStr.contains("E")) {
				valueStr = valueStr.replace("E", "e").replace(".0e", "e");
			}
			else if (valueStr.endsWith("0") && valueStr.contains(".")) {
				int trimingLength = 1;
				if (valueStr.endsWith(".0")) {
					trimingLength += 1;
				}
				valueStr = valueStr.substring(0, valueStr.length() - trimingLength);
			}
		}

		if (special && !specialPointValueEnabled) {
			throw new GSException(
					GSErrorCode.ILLEGAL_PARAMETER, "Illegal value: " + value);
		}

		out.append(valueStr);
	}

	private static void decodeSrId(StringBuilder out, int srId) {
		if (srId != SR_ID_DEFAULT) {
			out.append(";");
			out.append(srId);
		}
	}

	private static final AtomicReference<DirectAccessor> ACCESSOR_REF =
			new AtomicReference<DirectAccessor>();

	public static void setDirectAccessor(DirectAccessor accessor) {
		ACCESSOR_REF.set(accessor);
	}

	public static DirectAccessor getDirectAccessor() {
		for (int i = 0; i < 10; i++) {
			final DirectAccessor accessor = ACCESSOR_REF.get();

			if (accessor != null) {
				return accessor;
			}

			try {
				Class.forName(Geometry.class.getName());
			}
			catch (ClassNotFoundException e) {
				throw new Error(e);
			}

			if (i > 0) {
				Thread.yield();
			}
		}

		throw new Error();
	}

	public interface DirectAccessor {

		public int getBytesLength(Geometry geometry);

		public void putGeometry(BasicBuffer out, Geometry geometry);

		public Geometry getGeometry(BasicBuffer in, int size) throws GSException;

	}

	public static int getBytesLength(Geometry geometry) {
		final GeometryUtils.DirectAccessor accessor =
				GeometryUtils.getDirectAccessor();
		return accessor.getBytesLength(geometry);
	}

	public static void putGeometry(BasicBuffer out, Geometry geometry) {
		final GeometryUtils.DirectAccessor accessor =
				GeometryUtils.getDirectAccessor();
		accessor.putGeometry(out, geometry);
	}

	public static Geometry getGeometry(BasicBuffer in, int size) throws GSException {
		return GeometryUtils.getDirectAccessor().getGeometry(
				in, size);
	}

	public static boolean isSpecialPointValueEnabled() {
		return specialPointValueEnabled;
	}

	public static void setStrictMode(boolean strictMode) {
		GeometryUtils.strictMode = strictMode;
	}

}
