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

import static com.toshiba.mwcloud.gs.common.ContainerKeyConverter.ASCIIUtils.isDigit;
import static com.toshiba.mwcloud.gs.common.ContainerKeyConverter.ASCIIUtils.isLower;
import static com.toshiba.mwcloud.gs.common.ContainerKeyConverter.ASCIIUtils.isUpper;
import static com.toshiba.mwcloud.gs.common.ContainerKeyConverter.ASCIIUtils.isZero;
import static com.toshiba.mwcloud.gs.common.ContainerKeyConverter.ASCIIUtils.shiftToUpper;
import static com.toshiba.mwcloud.gs.common.ContainerKeyConverter.ASCIIUtils.toLower;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.Locale;

import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.common.BasicBuffer.BufferUtils;

public class ContainerKeyConverter {

	public static class ContainerKey {

		private static final int OFF_NO_STR = -1;

		private static final int OFF_COMPAT_NO_SENSITIVE = -2;

		private static final int OFF_COMPAT_SENSITIVE = -3;

		private final byte[] bytes;

		private final int caseOffset;

		ContainerKey(
				byte[] bytes, int caseOffset, boolean caseSensitive,
				boolean compatible) {
			this.bytes = bytes;
			if (compatible) {
				this.caseOffset = (caseSensitive ?
						OFF_COMPAT_SENSITIVE : OFF_COMPAT_NO_SENSITIVE);
			}
			else if (caseSensitive) {
				this.caseOffset = bytes.length;
			}
			else {
				if (caseOffset < 0) {
					throw new IllegalArgumentException();
				}
				if (bytes.length == caseOffset) {
					this.caseOffset = OFF_NO_STR;
				}
				else {
					this.caseOffset = caseOffset;
				}
			}
		}

		public boolean isCaseSensitive() {
			if (caseOffset >= 0) {
				return (caseOffset == bytes.length);
			}
			else {
				return (caseOffset == OFF_COMPAT_SENSITIVE);
			}
		}

		public ContainerKey toCaseSensitive(boolean caseSensitive) {
			final boolean thisCaseSensitive = isCaseSensitive();
			if (thisCaseSensitive == caseSensitive) {
				return this;
			}

			final boolean compatible = isCompatible();
			if (compatible || caseSensitive) {
				return new ContainerKey(
						bytes, -1, caseSensitive, compatible);
			}
			else {
				final ContainerKeyConverter converter =
						getInstance(LATEST_VERSION, true);
				final int[] caseOffsetRef = new int[1];
				try {
					converter.decomposeInternal(
							BasicBuffer.wrapBase(bytes), null, caseOffsetRef,
							false, true, bytes.length);
				}
				catch (GSException e) {
					throw new IllegalStateException(e);
				}
				return new ContainerKey(
						bytes, caseOffsetRef[0], caseSensitive, false);
			}
		}

		@Override
		public int hashCode() {
			int hash = 1;
			final int comparisonEnd = getComparisonEnd();
			final boolean normalizing = isComparisonNormalizing();
			for (int i = 0; i < comparisonEnd; i++) {
				byte b = bytes[i];
				if (normalizing) {
					b = (byte) toLower(b);
				}
				hash = hash * 31 + b;
			}
			return hash;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (!(obj instanceof ContainerKey)) {
				return false;
			}

			final ContainerKey key = (ContainerKey) obj;
			if (bytes == key.bytes) {
				return true;
			}
			if (bytes.length != key.bytes.length) {
				return false;
			}

			final int comparisonEnd;
			final boolean normalizing;
			if (isCaseSensitive() || key.isCaseSensitive()) {
				comparisonEnd = bytes.length;
				normalizing = false;
			}
			else {
				if (isCompatible() != key.isCompatible()) {
					return false;
				}
				if (caseOffset != key.caseOffset) {
					return false;
				}
				comparisonEnd = getComparisonEnd();
				normalizing = isComparisonNormalizing();
			}

			for (int i = 0; i < comparisonEnd; i++) {
				final byte b1 = bytes[i];
				final byte b2 = key.bytes[i];
				if (b1 == b2) {
					continue;
				}
				if (normalizing && toLower(b1) == toLower(b2)) {
					continue;
				}
				return false;
			}
			return true;
		}

		@Override
		public String toString() {
			final int version = isCompatible() ?
					LATEST_COMPATIBLE_VERSION : LATEST_VERSION;
			final ContainerKeyConverter converter =
					new ContainerKeyConverter(version, true);

			final StringBuilder builder = new StringBuilder();
			try {
				converter.format(this, builder);
			}
			catch (GSException e) {
				builder.append("(Container key with problem (cause=");
				builder.append(e.getMessage());
				builder.append("))");
			}

			return builder.toString();
		}

		private int getComparisonEnd() {
			return (caseOffset >= 0 ? caseOffset : bytes.length);
		}

		private boolean isComparisonNormalizing() {
			return (caseOffset == OFF_COMPAT_NO_SENSITIVE);
		}

		private boolean isCompatible() {
			return (caseOffset == OFF_COMPAT_NO_SENSITIVE ||
					caseOffset == OFF_COMPAT_SENSITIVE);
		}

	}

	public static class Components {

		public long databaseId = PUBLIC_DATABASE_ID;

		public String base;

		public long largeId = -1;

		public long affinityNum = -1;

		public String affinityStr;

		public long systemNum = -1;

		public String systemStr;

		public int subCount = -1;

	}

	static class ValidationSource {

		String str;

		int index;

		ValidationSource(String str) {
			this(str, -1);
		}

		ValidationSource(String str, int index) {
			this.str = str;
			this.index = index;
		}

		ValidationSource at(int index) {
			final ValidationSource dest = duplicate(this);
			dest.index = index;
			return dest;
		}

		ValidationSource merge(int relativeIndex) {
			final ValidationSource dest = duplicate(this);
			if (dest.index < 0) {
				dest.index = relativeIndex;
			}
			else {
				dest.index += relativeIndex;
			}
			return dest;
		}

		static ValidationSource duplicate(ValidationSource src) {
			if (src == null) {
				return new ValidationSource(null);
			}
			return new ValidationSource(src.str, src.index);
		}

		static boolean isStringSpecified(ValidationSource src) {
			return (src != null && src.str != null);
		}

	}

	static class ASCIIUtils {

		static boolean isLower(int ch) {
			return ch <= 'z' && ch >= 'a';
		}

		static boolean isUpper(int ch) {
			return ch <= 'Z' && ch >= 'A';
		}

		static boolean isDigit(int ch) {
			return ch <= '9' && ch >= '0';
		}

		static boolean isZero(int ch) {
			return ch == '0';
		}

		static int shiftToLower(int ch) {
			return ch - 'A' + 'a';
		}

		static int shiftToUpper(int ch) {
			return ch - 'a' + 'A';
		}

		static int toLower(int ch) {
			return isUpper(ch) ? shiftToLower(ch) : ch;
		}

		static int toUpper(int ch) {
			return isLower(ch) ? shiftToUpper(ch) : ch;
		}

	}

	private static final int PUBLIC_DATABASE_ID = 0;

	private static final int LATEST_VERSION = 14;

	private static final int LATEST_COMPATIBLE_VERSION = 13;

	private static final int DEFAULT_VERSION = LATEST_COMPATIBLE_VERSION;

	private static final int DATABASE_ID_FLAG = 1 << 0;

	private static final int LARGE_ID_FLAG = 1 << 1;

	private static final int AFFINITY_NUM_FLAG = 1 << 2;

	private static final int AFFINITY_STR_FLAG = 1 << 3;

	private static final int SYSTEM_NUM_FLAG = 1 << 4;

	private static final int SYSTEM_STR_FLAG = 1 << 5;

	private static final Charset DEFAULT_CHARSET = BasicBuffer.DEFAULT_CHARSET;

	private static final long VAR_INT_MAX_VALUE =
			RowMapper.VAR_SIZE_8BYTE_THRESHOLD - 1;

	private static final int COMPATIBLE_MAX_AFFINITY_DIGITS = 9;

	private static final int COMPATIBLE_MAX_AFFINITY_NUM = 999999999;

	private static final char SYSTEM_SEP = '#';

	private static final char NON_SYSTEM_SEP = '@';

	private static final char SUB_SEP = '/';

	private static final char SUB_COUNT_SEP = '_';

	private static final String BASIC_SYMBOLS = "_";

	private static final String EXTRA_SYMBOLS1 = ".-/=";

	private static final ContainerKeyConverter DEFAULT_INSTANCE =
			new ContainerKeyConverter(DEFAULT_VERSION, false);

	private static final ContainerKeyConverter DEFAULT_INTERNAL_INSTANCE =
			new ContainerKeyConverter(DEFAULT_VERSION, true);

	private int version;

	private boolean internalMode;

	public ContainerKeyConverter(int version, boolean internalMode) {
		this.version = version;
		this.internalMode = internalMode;
	}

	public static ContainerKeyConverter getInstance(
			int version, boolean internalMode) {
		if (version <= 0 || version == DEFAULT_VERSION) {
			if (internalMode) {
				return DEFAULT_INTERNAL_INSTANCE;
			}
			else {
				return DEFAULT_INSTANCE;
			}
		}

		return new ContainerKeyConverter(version, internalMode);
	}

	public static long getPublicDatabaseId() {
		return PUBLIC_DATABASE_ID;
	}

	public ContainerKey get(
			BasicBuffer buf, boolean caseSensitive, boolean validating)
			throws GSException {
		final byte[] bytes;
		final Components components = (validating ? new Components() : null);
		final int[] caseOffsetRef = new int[1];
		final ByteBuffer base = buf.base();
		try {
			final int expectedBodySize =
					(since14() ? getIntVarSize(base) : base.getInt());
			bytes = decomposeInternal(
					base, components, caseOffsetRef, true, false,
					expectedBodySize);
		}
		catch (BufferUnderflowException e) {
			throw errorValidation(
					GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by illegal container key image", null, e);
		}
		if (validating) {
			validateInternal(components, null);
		}
		return new ContainerKey(
				bytes, caseOffsetRef[0], caseSensitive, !since14());
	}

	public void put(ContainerKey key, long databaseId, BasicBuffer buf)
			throws GSException {
		final byte[] bytes = key.bytes;

		if (!since14()) {
			buf.putInt(bytes.length);
			buf.prepare(bytes.length);
			buf.base().put(bytes);
			return;
		}

		int bodySize = bytes.length;
		byte flags = bytes[0];
		int databaseIdSize = 0;
		final boolean modifying =
				(databaseId != PUBLIC_DATABASE_ID &&
				(flags & DATABASE_ID_FLAG) == 0);
		if (modifying) {
			flags |= DATABASE_ID_FLAG;
			databaseIdSize = Long.SIZE / Byte.SIZE;
			bodySize += databaseIdSize;
		}

		putIntVarSize(buf, bodySize);
		buf.prepare(bodySize);
		buf.base().put(flags);
		if (modifying) {
			buf.base().putLong(databaseId);
		}
		buf.base().put(bytes, 1, bytes.length - 1);
	}

	public ContainerKey compose(
			Components components, boolean caseSensitive) throws GSException {
		final BasicBuffer buf = new BasicBuffer(64);
		compose(components, buf);
		buf.base().flip();

		final int[] caseOffsetRef = new int[1];
		final byte[] bytes = decomposeInternal(
				buf.base(), null, caseOffsetRef, true, true,
				buf.base().remaining());
		return new ContainerKey(
				bytes, caseOffsetRef[0], caseSensitive, !since14());
	}

	public void compose(
			Components components, BasicBuffer buf) throws GSException {
		if (!since14()) {
			final StringBuilder builder = new StringBuilder();
			format(components, builder);

			final byte[] bytes = builder.toString().getBytes(DEFAULT_CHARSET);
			buf.prepare(bytes.length);
			buf.base().put(bytes);

			return;
		}

		final int startPos = buf.base().position();

		int flags = 0;
		buf.put((byte) flags);

		if (components.databaseId != PUBLIC_DATABASE_ID) {
			flags |= DATABASE_ID_FLAG;
			buf.putLong(components.databaseId);
		}

		final byte[] baseBytes = (components.base == null ? new byte[0] :
				components.base.getBytes(DEFAULT_CHARSET));
		putIntVarSize(buf, baseBytes.length);
		final int basePos = buf.base().position();
		buf.prepare(baseBytes.length);
		buf.base().position(basePos + baseBytes.length);

		if (components.largeId >= 0) {
			flags |= LARGE_ID_FLAG;
			putLongVarSize(buf, components.largeId);
		}

		byte[] affinityBytes = null;
		int affinityStrPos = -1;
		if (components.affinityNum >= 0) {
			flags |= AFFINITY_NUM_FLAG;
			putLongVarSize(buf, components.affinityNum);
		}
		else if (components.affinityStr != null) {
			flags |= AFFINITY_STR_FLAG;
			affinityBytes = components.affinityStr.getBytes(DEFAULT_CHARSET);
			putIntVarSize(buf, affinityBytes.length);
			affinityStrPos = buf.base().position();
			buf.prepare(affinityBytes.length);
			buf.base().position(affinityStrPos + affinityBytes.length);
		}

		byte[] systemBytes = null;
		int systemStrPos = -1;
		if (components.systemNum >= 0) {
			flags |= SYSTEM_NUM_FLAG;
			putLongVarSize(buf, components.systemNum);
		}
		else if (components.systemStr != null) {
			flags |= SYSTEM_STR_FLAG;
			systemBytes = components.systemStr.getBytes(DEFAULT_CHARSET);
			putIntVarSize(buf, systemBytes.length);
			systemStrPos = buf.base().position();
			buf.prepare(systemBytes.length);
			buf.base().position(systemStrPos + systemBytes.length);
		}

		int caseBitPos = 0;
		int caseBits = 0;
		for (int i = 0; i < 3; i++) {
			final byte[] bytes = (i == 0 ? baseBytes :
					(i == 1 ? affinityBytes : systemBytes));
			if (bytes == null) {
				continue;
			}
			for (int j = 0; j < bytes.length; j++) {
				if (caseBitPos >= Byte.SIZE) {
					buf.put((byte) caseBits);
					caseBitPos = 0;
					caseBits = 0;
				}
				final int ch = bytes[j];
				if (isUpper(ch)) {
					bytes[j] = (byte) toLower(ch);
					caseBits |= (1 << caseBitPos);
				}
				caseBitPos++;
			}
		}
		if (caseBitPos != 0) {
			buf.put((byte) caseBits);
		}

		final int endPos = buf.base().position();
		buf.base().position(startPos);
		buf.base().put((byte) flags);

		{
			buf.base().position(basePos);
			buf.base().put(baseBytes);
		}
		if (affinityBytes != null) {
			buf.base().position(affinityStrPos);
			buf.base().put(affinityBytes);
		}
		if (systemBytes != null) {
			buf.base().position(systemStrPos);
			buf.base().put(systemBytes);
		}

		buf.base().position(endPos);
	}

	public void decompose(
			ContainerKey key, Components components) throws GSException {
		final ByteBuffer buf = BasicBuffer.wrapBase(key.bytes);
		decomposeInternal(
				buf, components, null, false, true, key.bytes.length);
		if (buf.remaining() > 0) {
			throw errorValidation(
					GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by remaining bytes in container key",
					null, null);
		}
	}

	public void format(
			ContainerKey key, StringBuilder builder) throws GSException {
		final Components components = new Components();
		decompose(key, components);
		format(components, builder);
	}

	public void format(Components components, StringBuilder builder) {
		if (components.base != null) {
			escape(components.base, builder);
		}

		if (components.systemStr != null || components.systemNum >= 0) {
			builder.append(SYSTEM_SEP);
			if (components.systemStr != null) {
				escape(components.systemStr, builder);
			}
			else {
				builder.append(components.systemNum);
			}
		}

		if (components.largeId >= 0) {
			builder.append((since14() ? NON_SYSTEM_SEP : SUB_SEP));
			builder.append(components.largeId);
		}

		if (components.subCount >= 0) {
			builder.append(SUB_COUNT_SEP);
			builder.append(components.subCount);
		}

		if (components.affinityStr != null || components.affinityNum >= 0) {
			builder.append(NON_SYSTEM_SEP);
			if (components.affinityStr != null) {
				escape(components.affinityStr, builder);
			}
			else {
				builder.append(components.affinityNum);
			}
		}
	}

	public ContainerKey parse(
			String str, boolean caseSensitive) throws GSException {
		final Components components = new Components();
		parse(str, components, true);

		final BasicBuffer buf = new BasicBuffer(str.length() * 2);
		compose(components, buf);
		buf.base().flip();

		final int[] caseOffsetRef = new int[1];
		final byte[] bytes = decomposeInternal(
				buf.base(), null, caseOffsetRef, true, false,
				buf.base().remaining());
		return new ContainerKey(
				bytes, caseOffsetRef[0], caseSensitive, !since14());
	}

	public void parse(
			String str, Components components, boolean validating)
			throws GSException {
		final ValidationSource source = new ValidationSource(str);
		String baseStr = null;
		boolean systemFound = false;
		boolean largeIdFound = false;

		final int len = str.length();
		for (int off = 0; off < len;) {
			final char ch = str.charAt(off);
			boolean separated = false;
			int endOff;
			switch (ch) {
			case NON_SYSTEM_SEP:
				if (!largeIdFound && internalMode && since14() &&
						(endOff = findSeparator(
								str, NON_SYSTEM_SEP, off + 1)) >= 0) {
					final String subStr = str.substring(off + 1, endOff);
					final Long num = tryParseLongVar(
							subStr, true, source.at(off + 1), "largeId");
					components.largeId = num;
					largeIdFound = true;
				}
				else {
					final String subStr = str.substring(off + 1);
					final Long num;
					if (!since14() && subStr.length() >
							COMPATIBLE_MAX_AFFINITY_DIGITS) {
						num = null;
					}
					else {
						num = tryParseLongVar(subStr, false, null, null);
					}
					if (num == null) {
						components.affinityStr = unescape(
								subStr, source.at(off + 1), "affinity");
					}
					else {
						components.affinityNum = num;
					}
					endOff = len;
				}
				separated = true;
				break;
			case SUB_SEP:
				if (!since14() && !largeIdFound && internalMode) {
					int subEndOff = findSeparator(str, SUB_COUNT_SEP, off + 1);
					endOff = findSeparator(
							str, NON_SYSTEM_SEP,
							(subEndOff >= 0 ? subEndOff : off) + 1);
					if (endOff < 0) {
						endOff = len;
					}
					if (subEndOff >= 0) {
						components.subCount = parseInt(
								str.substring(subEndOff + 1, endOff),
								source.at(subEndOff + 1), "subCount");
					}
					components.largeId = parseInt(str.substring(
							off + 1, (subEndOff >= 0 ? subEndOff : endOff)),
							source.at(off + 1), "largeId");
					largeIdFound = true;
					separated = true;
					break;
				}
				endOff = off + 1;
				break;
			case SYSTEM_SEP:
				if (!systemFound && since14()) {
					if (!internalMode) {
						throw errorCharacter(ch, source.at(off), null, null);
					}
					endOff = findSeparator(str, NON_SYSTEM_SEP, off + 1);
					if (endOff < 0) {
						endOff = len;
					}
					final String subStr = str.substring(off + 1, endOff);
					final Long num =
							tryParseLongVar(subStr, false, null, null);
					if (num == null) {
						components.systemStr = unescape(
								subStr, source.at(off + 1), "system");
					}
					else {
						components.systemNum = num;
					}
					systemFound = true;
					separated = true;
					break;
				}
				endOff = off + 1;
				break;
			default:
				endOff = off + 1;
				break;
			}
			if (separated && baseStr == null) {
				baseStr = str.substring(0, off);
			}
			off = endOff;
		}

		if (baseStr == null) {
			baseStr = str;
		}
		components.base = unescape(baseStr, source.at(0), "base");

		if (validating) {
			validateInternal(components, source.at(-1));
		}
	}

	public void validate(Components components) throws GSException {
		validateInternal(components, null);
	}

	private void validateInternal(
			Components components, ValidationSource source)
			throws GSException {
		boolean affinityFound = false;
		boolean systemFound = false;

		for (int i = 0; i < 2; i++) {
			final String componentName = (i == 0 ? "affinity" : "system");
			final String str =
					(i == 0 ? components.affinityStr : components.systemStr);
			final long num =
					(i == 0 ? components.affinityNum : components.systemNum);

			boolean found = false;
			if (str != null) {
				validateStr(str, false, false, source, componentName);
				found = true;
			}

			if (num != -1) {
				validateLongVar(num, source, componentName);
				found = true;
			}

			if (str != null && num >= 0) {
				throw errorValidation(
						GSErrorCode.ILLEGAL_VALUE_FORMAT,
						"Both string and number specified", source, null,
						"component", componentName);
			}

			if (found) {
				if (i == 0) {
					affinityFound = true;
				}
				else {
					systemFound = true;
				}
			}
		}

		if (components.databaseId < 0) {
			throw errorValidation(
					GSErrorCode.ILLEGAL_VALUE_FORMAT,
					"Database ID must not be negative",
					source, null, "databaseId", components.databaseId);
		}

		if (components.largeId != -1) {
			validateLongVar(components.largeId, source, "largeId");
			if (since6()) {
				if (!affinityFound && since13()) {
					throw errorValidation(
							GSErrorCode.ILLEGAL_VALUE_FORMAT,
							"Affinity must be specified on largeId specified",
							source, null, "largeId", components.largeId);
				}
			}
			else {
				throw errorComponent(source, "largeId");
			}
		}

		if (components.subCount != -1) {
			validateLongVar(components.subCount, source, "subCount");
			if (since13() || !since11()) {
				throw errorComponent(source, "subCount");
			}
			if (components.largeId >= components.subCount) {
				throw errorValidation(
						GSErrorCode.ILLEGAL_VALUE_FORMAT,
						"LargeId must be less than subCount", source, null,
						"largeId", components.largeId,
						"subCount", components.subCount);
			}
		}

		if (affinityFound) {
			if (components.affinityNum >= 0) {
				if (!since14() && components.affinityNum >
						COMPATIBLE_MAX_AFFINITY_NUM) {
					throw errorValidation(
							GSErrorCode.ILLEGAL_VALUE_FORMAT,
							"Affinity number out of range", source, null,
							"affinity", components.affinityNum);
				}
			}
			if (!since6()) {
				throw errorComponent(source, "affinity");
			}
		}

		if (systemFound) {
			if (!since14()) {
				throw errorComponent(source, "system");
			}
		}

		if (components.base == null) {
			throw errorValidation(
					GSErrorCode.ILLEGAL_VALUE_FORMAT,
					"Base name not specified", source, null);
		}
		validateStr(components.base, systemFound, true, source, "base");
	}

	private byte[] decomposeInternal(
			ByteBuffer buf, Components components, int[] caseOffsetRef,
			boolean withBytes, boolean withDatabase, int expectedBodySize)
			throws GSException, BufferUnderflowException {
		if (!since14()) {
			if (caseOffsetRef != null) {
				caseOffsetRef[0] = -1;
			}

			final byte[] bytes = new byte[expectedBodySize];
			buf.get(bytes);

			if (components != null) {
				final String str = new String(bytes, DEFAULT_CHARSET);
				parse(str, components, true);
			}

			return (withBytes ? bytes : null);
		}

		final int bodyPos = buf.position();
		boolean databaseIdFound = false;

		final int baseStrSize;
		final int baseStrPos;
		int affinityStrSize = -1;
		int affinityStrPos = -1;
		int systemStrSize = -1;
		int systemStrPos = -1;

		do {
			int flags = buf.get();

			do {
				if (flags == 0) {
					break;
				}

				flags &= 0xff;
				if ((flags & DATABASE_ID_FLAG) == 0) {
					break;
				}

				final long value = buf.getLong();
				if (value == PUBLIC_DATABASE_ID) {
					throw errorValidation(
							GSErrorCode.MESSAGE_CORRUPTED,
							"Protocol error by illegal database ID " +
							"in container key", null, null);
				}
				if (components != null && withDatabase) {
					components.databaseId = value;
				}
				flags &= ~DATABASE_ID_FLAG;
				databaseIdFound = true;
			}
			while (false);

			baseStrSize = getIntVarSize(buf);
			baseStrPos = buf.position();
			BufferUtils.skipForward(buf, baseStrSize);

			if (flags == 0) {
				break;
			}

			if ((flags & LARGE_ID_FLAG) != 0) {
				final long value = getLongVarSize(buf);
				if (components != null) {
					components.largeId = value;
				}
				if ((flags &= ~LARGE_ID_FLAG) == 0) {
					break;
				}
			}

			if ((flags & AFFINITY_NUM_FLAG) != 0) {
				final long value = getLongVarSize(buf);
				if (components != null) {
					components.affinityNum = value;
				}
				if ((flags &= ~AFFINITY_NUM_FLAG) == 0) {
					break;
				}
			}
			else if ((flags & AFFINITY_STR_FLAG) != 0) {
				affinityStrSize = getIntVarSize(buf);
				affinityStrPos = buf.position();
				BufferUtils.skipForward(buf, affinityStrSize);
				if ((flags &= ~AFFINITY_STR_FLAG) == 0) {
					break;
				}
			}

			if ((flags & SYSTEM_NUM_FLAG) != 0) {
				final long value = getLongVarSize(buf);
				if (components != null) {
					components.systemNum = value;
				}
				if ((flags &= ~SYSTEM_NUM_FLAG) == 0) {
					break;
				}
			}
			else if ((flags & SYSTEM_STR_FLAG) != 0) {
				systemStrSize = getIntVarSize(buf);
				systemStrPos = buf.position();
				BufferUtils.skipForward(buf, systemStrSize);
				if ((flags &= ~SYSTEM_STR_FLAG) == 0) {
					break;
				}
			}

			if (flags != 0) {
				throw errorValidation(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by unknown container key flags",
						null, null, "flags", flags);
			}
		}
		while (false);

		final int totalStrSize = baseStrSize +
				Math.max(affinityStrSize, 0) +
				Math.max(systemStrSize, 0);
		final int caseSize = (totalStrSize + Byte.SIZE - 1) / Byte.SIZE;

		final int bodySize = (buf.position() - bodyPos) + caseSize;
		if (expectedBodySize != bodySize) {
			throw errorValidation(
					GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by invalid container key size", null, null,
					"expected", expectedBodySize, "actual", bodySize);
		}

		final int reducingDatabaseIdSize = (databaseIdFound && !withDatabase ?
				Long.SIZE / Byte.SIZE : 0);

		if (caseOffsetRef != null) {
			caseOffsetRef[0] = bodySize - caseSize - reducingDatabaseIdSize;
		}

		if (components != null) {
			final byte[] bytes = new byte[totalStrSize + caseSize];

			{
				int off = 0;
				buf.position(baseStrPos);
				buf.get(bytes, off, baseStrSize);
				off += baseStrSize;

				if (affinityStrSize >= 0) {
					buf.position(affinityStrPos);
					buf.get(bytes, off, affinityStrSize);
					off += affinityStrSize;
				}

				if (systemStrSize >= 0) {
					buf.position(systemStrPos);
					buf.get(bytes, off, systemStrSize);
					off += systemStrSize;
				}

				buf.position(bodyPos + bodySize - caseSize);
				buf.get(bytes, off, caseSize);
			}

			{
				int off = 0;
				int caseOff = totalStrSize;
				int caseBitPos = Byte.SIZE;
				int caseBits = 0;
				for (; off < totalStrSize;) {
					if (caseBitPos >= Byte.SIZE) {
						caseBits = bytes[caseOff++] & 0xff;
						if (caseBits == 0) {
							off += Byte.SIZE;
							continue;
						}
						caseBitPos = 0;
					}
					final int ch = bytes[off] & 0xff;
					if ((caseBits & (1 << caseBitPos)) != 0) {
						if (!isLower(ch)) {
							throw errorValidation(
									GSErrorCode.MESSAGE_CORRUPTED,
									"Protocol error by illegal container key " +
									"image", null, null, "characterByte", ch);
						}
						bytes[off] = (byte) shiftToUpper(ch);
					}
					off++;
					caseBitPos++;
				}
			}

			{
				int off = 0;
				components.base = new String(
						bytes, off, baseStrSize, DEFAULT_CHARSET);
				off += baseStrSize;

				if (affinityStrSize >= 0) {
					components.affinityStr = new String(
							bytes, off, affinityStrSize, DEFAULT_CHARSET);
					off += affinityStrSize;
				}

				if (systemStrSize >= 0) {
					components.systemStr = new String(
							bytes, off, systemStrSize, DEFAULT_CHARSET);
				}
			}
		}

		if (withBytes) {
			final byte[] bytes = new byte[bodySize - reducingDatabaseIdSize];

			buf.position(bodyPos);
			bytes[0] = buf.get();
			if (!withDatabase) {
				bytes[0] = (byte) (bytes[0] & ~DATABASE_ID_FLAG);
			}
			buf.position(buf.position() + reducingDatabaseIdSize);
			buf.get(bytes, 1, bytes.length - 1);

			return bytes;
		}

		return null;
	}

	private void validateLongVar(
			long value, ValidationSource source, String componentName)
			throws GSException {
		if (value < 0 || value > VAR_INT_MAX_VALUE) {
			throw errorValidation(
					GSErrorCode.ILLEGAL_VALUE_FORMAT,
					"Number out of range", source, null, null, value,
					"component", componentName);
		}

		if (!since14() && value > Integer.MAX_VALUE) {
			throw errorValidation(
					GSErrorCode.ILLEGAL_VALUE_FORMAT,
					"Number out of range for integer",
					source, null, null, value,
					"component", componentName);
		}
	}

	private void validateStr(
			String str, boolean withEmpty, boolean withExtra,
			ValidationSource source, String componentName) throws GSException {
		final int len = str.length();
		if (len == 0 && !withEmpty) {
			throw errorValidation(
					GSErrorCode.ILLEGAL_VALUE_FORMAT,
					"Empty component of container name", source, null,
					"component", componentName);
		}
		for (int off = 0; off < len; off++) {
			final char ch = str.charAt(off);
			if (isUpper(ch) || isLower(ch) || BASIC_SYMBOLS.indexOf(ch) >= 0) {
				continue;
			}
			else if (isDigit(ch)) {
				if (off > 0 || !withExtra || since14()) {
					continue;
				}
			}

			if (since14()) {
				if (EXTRA_SYMBOLS1.indexOf(ch) >= 0) {
					continue;
				}
			}
			else {
				if (withExtra && ch == SYSTEM_SEP && internalMode) {
					continue;
				}
			}
			throw errorCharacter(
					ch, ValidationSource.duplicate(source).merge(off),
					componentName, str);
		}
	}

	private static void escape(String str, StringBuilder builder) {
		builder.append(str);
	}

	private String unescape(
			String str, ValidationSource source, String componentName)
			throws GSException {
		final int len = str.length();
		for (int off = 0; off < len; off++) {
			final char ch = str.charAt(off);
			if (ch == NON_SYSTEM_SEP ||
					(ch == SYSTEM_SEP && since14()) ||
					(ch == SUB_SEP && !since14())) {
				throw errorCharacter(
						ch, ValidationSource.duplicate(source).merge(off),
						componentName, str);
			}
		}
		return str;
	}

	private static int findSeparator(
			String str, char separatorChar, int offset) {
		return str.indexOf(separatorChar, offset);
	}

	private static int parseInt(
			String str, ValidationSource source, String componentName)
			throws GSException {
		try {
			return Integer.parseInt(str);
		}
		catch (NumberFormatException e) {
			throw errorValidation(
					GSErrorCode.ILLEGAL_VALUE_FORMAT,
					"Illegal integer format", source, e, null, str);
		}
	}

	private static Long tryParseLongVar(
			String str, boolean force,
			ValidationSource source, String componentName) throws GSException {
		if (str.isEmpty()) {
			if (!force) {
				return null;
			}
			throw errorValidation(
					GSErrorCode.ILLEGAL_VALUE_FORMAT,
					"Empty number string", source, null,
					"component", componentName);
		}

		final char ch = str.charAt(0);
		if (!isDigit(ch) || (isZero(ch) && str.length() > 1)) {
			if (!force) {
				return null;
			}
			throw errorValidation(
					GSErrorCode.ILLEGAL_VALUE_FORMAT,
					"Illegal number format", source, null, null, str,
					"component", componentName);
		}

		final NumberFormat format =
				NumberFormat.getIntegerInstance(Locale.ROOT);
		format.setParseIntegerOnly(true);

		final ParsePosition pos = new ParsePosition(0);
		final Object rawValue = format.parse(str, pos);
		if (!(rawValue instanceof Long) || str.length() != pos.getIndex()) {
			if (!force) {
				return null;
			}
			throw errorValidation(
					GSErrorCode.ILLEGAL_VALUE_FORMAT,
					"Illegal number format", source, null, null, str,
					"component", componentName);
		}
		final Long value = (Long) rawValue;

		if (value < 0 || value > VAR_INT_MAX_VALUE) {
			if (!force) {
				return null;
			}
			throw errorValidation(
					GSErrorCode.ILLEGAL_VALUE_FORMAT,
					"Number out of range", source, null, null, str,
					"component", componentName);
		}

		return value;
	}

	private static int getIntVarSize(ByteBuffer buf) throws GSException {
		final byte first = buf.get();
		if (RowMapper.varSizeIs1Byte(first)) {
			return RowMapper.decode1ByteVarSize(first);
		}
		else if (RowMapper.varSizeIs4Byte(first)) {
			buf.position(buf.position() - Byte.SIZE / Byte.SIZE);
			final int rawSize = buf.getInt();
			return RowMapper.decode4ByteVarSize(rawSize);
		}
		else {
			throw errorValidation(
					GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by illegal var size", null, null);
		}
	}

	private static long getLongVarSize(ByteBuffer buf) {
		final byte first = buf.get();
		if (RowMapper.varSizeIs1Byte(first)) {
			return RowMapper.decode1ByteVarSize(first);
		}
		else if (RowMapper.varSizeIs4Byte(first)) {
			buf.position(buf.position() - Byte.SIZE / Byte.SIZE);
			final int rawSize = buf.getInt();
			return RowMapper.decode4ByteVarSize(rawSize);
		}
		else {
			buf.position(buf.position() - Byte.SIZE / Byte.SIZE);
			final long rawSize = buf.getLong();
			return RowMapper.decode8ByteVarSize(rawSize);
		}
	}

	private static void putIntVarSize(
			BasicBuffer buf, int value) throws GSException {
		if (value < RowMapper.VAR_SIZE_1BYTE_THRESHOLD) {
			buf.put(RowMapper.encode1ByteVarSize((byte) value));
		}
		else if (value < RowMapper.VAR_SIZE_4BYTE_THRESHOLD) {
			buf.putInt(RowMapper.encode4ByteVarSize((int) value));
		}
		else {
			throw errorValidation(
					GSErrorCode.INTERNAL_ERROR,
					"Internal error by illegal var size",
					null, null, null, value);
		}
	}

	private static void putLongVarSize(
			BasicBuffer buf, long value) throws GSException {
		if (value < RowMapper.VAR_SIZE_1BYTE_THRESHOLD) {
			buf.put(RowMapper.encode1ByteVarSize((byte) value));
		}
		else if (value < RowMapper.VAR_SIZE_4BYTE_THRESHOLD) {
			buf.putInt(RowMapper.encode4ByteVarSize((int) value));
		}
		else if (value < RowMapper.VAR_SIZE_8BYTE_THRESHOLD) {
			buf.putLong(RowMapper.encode8ByteVarSize(value));
		}
		else {
			throw errorValidation(
					GSErrorCode.INTERNAL_ERROR,
					"Internal error by illegal var size",
					null, null, null, value);
		}
	}

	private static GSException errorComponent(
			ValidationSource source, String componentName) {
		return errorValidation(
				GSErrorCode.ILLEGAL_VALUE_FORMAT,
				"Unacceptable component specified", source, null,
				"component", componentName);
	}

	private static GSException errorCharacter(
			char ch, ValidationSource source, String componentName,
			String componentStr) {
		final StringBuilder builder = new StringBuilder();

		if (!Character.isISOControl(ch) && Character.isDefined(ch)) {
			builder.append("\"");
			builder.append(ch);
			builder.append("\"");
		}

		if (ch < 0 || ch > 0x7f || ch == ' ' || Character.isISOControl(ch)) {
			builder.append("(\\u");
			builder.append(String.format(Locale.ROOT, "%04x", (int) ch));
			builder.append(")");
		}

		final String errorStr = (ValidationSource.isStringSpecified(source) ?
				null : componentStr);

		return errorValidation(
				GSErrorCode.ILLEGAL_SYMBOL_CHARACTER,
				"Illegal character found", source, null,
				"character", builder.toString(),
				"component", componentName,
				null, errorStr);
	}

	private static GSException errorValidation(
			int errorCode, String message, ValidationSource source,
			Throwable cause, Object ...paramNameAndValues) {
		final StringBuilder builder = new StringBuilder();

		builder.append(message);

		boolean paramFound = false;
		for (Object p : paramNameAndValues) {
			if (p != null) {
				paramFound = true;
			}
		}

		final boolean extra = (source != null || cause != null || paramFound);
		boolean extraStarted = false;

		if (extra) {
			if (message != null && !message.isEmpty()) {
				builder.append(' ');
			}
			builder.append('(');
		}

		if (paramFound) {
			for (int i = 0; i < paramNameAndValues.length; i += 2) {
				final Object name = paramNameAndValues[i];
				final Object value = (i + 1 < paramNameAndValues.length ?
						paramNameAndValues[i + 1] : null);

				if (value == null) {
					continue;
				}

				if (extraStarted) {
					builder.append(", ");
				}
				extraStarted = true;

				builder.append((name == null ? "value" : name));
				builder.append('=');
				builder.append(value);
			}
		}

		if (source != null) {
			if (source.str != null) {
				if (extraStarted) {
					builder.append(", ");
				}
				extraStarted = true;

				builder.append("containerName=");
				builder.append("\"");
				builder.append(source.str);
				builder.append("\"");
			}

			if (source.index >= 0) {
				if (extraStarted) {
					builder.append(", ");
				}
				extraStarted = true;

				builder.append("index=");
				builder.append(source.index);
			}
		}

		if (cause != null) {
			if (extraStarted) {
				builder.append(", ");
			}
			extraStarted = true;

			builder.append("reason=");
			builder.append(cause.getMessage());
		}

		if (extra) {
			builder.append(')');
		}

		return new GSException(errorCode, builder.toString(), cause);
	}

	
	private boolean since6() {
		return (version >= 6);
	}

	
	private boolean since11() {
		return (version >= 11);
	}

	
	private boolean since13() {
		return (version >= 13);
	}

	
	private boolean since14() {
		return (version >= 14);
	}

}
