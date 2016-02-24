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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.UUID;

public class BasicBuffer {

	public static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

	private static final int HUGE_BUFFER_GROW_UNIT = 1024 * 1024;

	private ByteBuffer base;

	public BasicBuffer(int initialSize) {
		base = ByteBuffer.allocate(initialSize);
		base.order(ByteOrder.LITTLE_ENDIAN);
	}

	public ByteBuffer base() {
		return base;
	}

	public void prepare(int size) {
		if ( size <= base.remaining() ) {
			return;
		}

		final long requiredCapacity = (long) base.position() + size;
		long newCapacity =
				Long.highestOneBit(requiredCapacity) << 1;

		if (newCapacity > Integer.MAX_VALUE) {
			if (requiredCapacity <= Integer.MAX_VALUE) {
				final long unit = HUGE_BUFFER_GROW_UNIT;
				newCapacity = Math.min(Integer.MAX_VALUE,
						(requiredCapacity + unit - 1) / unit * unit);
			}
			else {
				throw new IllegalArgumentException(
						"Too large size (size=" + size +
						", requiredCapacity=" + requiredCapacity + ")");
			}
		}

		final ByteBuffer newBuffer = ByteBuffer.allocate((int) newCapacity);

		newBuffer.order( base.order() );
		base.flip();
		newBuffer.put(base);
		base = newBuffer;
	}

	public void putBoolean(boolean value) {
		prepare(Byte.SIZE / Byte.SIZE);
		base.put((byte) (value ? 1 : 0));
	}

	public void putBooleanPrepared(boolean value) {
		base.put((byte) (value ? 1 : 0));
	}

	public void put(byte value) {
		prepare(Byte.SIZE / Byte.SIZE);
		base.put(value);
	}

	public void putShort(short value) {
		prepare(Short.SIZE / Byte.SIZE);
		base.putShort(value);
	}

	public void putInt(int value) {
		prepare(Integer.SIZE / Byte.SIZE);
		base.putInt(value);
	}

	public void putLong(long value) {
		prepare(Long.SIZE / Byte.SIZE);
		base.putLong(value);
	}

	public void putFloat(float value) {
		prepare(Float.SIZE / Byte.SIZE);
		base.putFloat(value);
	}

	public void putDouble(double value) {
		prepare(Double.SIZE / Byte.SIZE);
		base.putDouble(value);
	}

	public void putChar(char value) {
		prepare(Character.SIZE / Byte.SIZE);
		base.putChar(value);
	}

	public void putString(String value) {
		final byte[] buf = value.getBytes(DEFAULT_CHARSET);
		prepare(Integer.SIZE / Byte.SIZE + buf.length);
		base.putInt(buf.length);
		base.put(buf);
	}

	public void putDate(Date value) {
		putLong(value.getTime());
	}

	public void putDatePrepared(Date value) {
		base.putLong(value.getTime());
	}

	public void putEnum(Enum<?> value) {
		prepare(Integer.SIZE / Byte.SIZE);
		base.putInt(value.ordinal());
	}

	public void putEnumPrepared(Enum<?> value) {
		base.putInt(value.ordinal());
	}

	public void putByteEnum(Enum<?> value) {
		prepare(Byte.SIZE / Byte.SIZE);
		base.put((byte) (value.ordinal() & 0xff));
	}

	public void putByteEnumPrepared(Enum<?> value) {
		base.put((byte) (value.ordinal() & 0xff));
	}

	public void putUUID(UUID uuid) {
		prepare(Long.SIZE * 2 / Byte.SIZE);

		final ByteOrder orgOrder = base.order();
		try {
			base.order(ByteOrder.BIG_ENDIAN);

			final long mostSigBits = uuid.getMostSignificantBits();
			final long leastSigBits = uuid.getLeastSignificantBits();

			base.putLong(mostSigBits);
			base.putLong(leastSigBits);
		}
		finally {
			base.order(orgOrder);
		}
	}

	public boolean getBoolean() {
		return (base.get() != 0);
	}

	public String getString() {
		final byte[] buf = new byte[base.getInt()];
		base().get(buf);
		return new String(buf, 0, buf.length, DEFAULT_CHARSET);
	}

	public Date getDate() {
		return new Date(base.getLong());
	}

	public <E extends Enum<E>> E getEnum(Class<E> type) {
		try {
			return type.getEnumConstants()[base.getInt()];
		}
		catch (IndexOutOfBoundsException e) {
			throw new IllegalStateException(e);
		}
	}

	public <E extends Enum<E>> E getByteEnum(Class<E> type) {
		try {
			return type.getEnumConstants()[base.get() & 0xff];
		}
		catch (IndexOutOfBoundsException e) {
			throw new IllegalStateException(e);
		}
	}

	public <E extends Enum<E>> E getByteEnum(E[] enumConstants) {
		try {
			return enumConstants[base.get() & 0xff];
		}
		catch (IndexOutOfBoundsException e) {
			throw new IllegalStateException(e);
		}
	}

	public UUID getUUID() {
		final ByteOrder orgOrder = base.order();
		try {
			base.order(ByteOrder.BIG_ENDIAN);

			final long mostSigBits = base.getLong();
			final long leastSigBits = base.getLong();

			return new UUID(mostSigBits, leastSigBits);
		}
		finally {
			base.order(orgOrder);
		}
	}

	public void clear() {
		base.clear();
	}

}
