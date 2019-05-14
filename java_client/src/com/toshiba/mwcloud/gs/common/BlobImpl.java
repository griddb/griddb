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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;

public class BlobImpl implements Blob {

	private final BlobImpl chain;

	private byte[] data;

	private ByteArrayOutputStream streamOutput;

	private long streamOutputPos;

	private boolean shared;

	private boolean closed;

	public BlobImpl() {
		this(null);
	}

	public BlobImpl(BlobImpl chain) {
		this.chain = chain;
	}

	public static BlobImpl share(Blob src) throws SQLException {
		if (src instanceof BlobImpl) {
			return share((BlobImpl) src);
		}

		final long length = src.length();
		if (length == 0) {
			return new BlobImpl();
		}
		else if (length < 0 || length > Integer.MAX_VALUE) {
			throw new SQLException(
					"Too long or unacceptable blob length " +
					"(length=" + length + ")");
		}
		else {
			final BlobImpl dest = new BlobImpl();
			dest.data = src.getBytes(1, (int) length);
			return dest;
		}
	}

	public static BlobImpl share(BlobImpl src) throws SQLException {
		src.checkAvailable();
		src.prepareData();

		final BlobImpl dest = new BlobImpl();
		dest.data = src.data;

		src.shared = true;
		dest.shared = true;

		return dest;
	}

	public static BlobImpl shareDirect(BlobImpl src) {
		final BlobImpl dest = new BlobImpl();
		dest.data = src.data;

		src.shared = true;
		dest.shared = true;

		return dest;
	}

	public byte[] getDataDirect() throws SQLException {
		prepareData();
		return data;
	}

	public void setDataDirect(byte[] data) {
		this.data = data;
		streamOutput = null;
		streamOutputPos = 0;
		shared = false;
	}

	public void close() {
		if (chain != null) {
			chain.close();
		}

		if (!closed) {
			free();
			closed = true;
		}
	}

	private void checkAvailable() throws SQLException {
		if (closed) {
			throw new SQLException();
		}
	}

	private void prepareData() throws SQLException {
		if (streamOutput != null) {
			final byte[] outBytes = streamOutput.toByteArray();
			setBytesInternal(
					streamOutputPos, outBytes, true, 0, outBytes.length);
			streamOutput = null;
			streamOutputPos = 0;
		}

		if (data == null) {
			data = new byte[0];
		}
	}

	@Override
	public long length() throws SQLException {
		checkAvailable();
		prepareData();

		if (data != null) {
			return data.length;
		}
		else if (streamOutput != null) {
			return streamOutput.size();
		}

		return 0;
	}

	@Override
	public byte[] getBytes(long pos, int length) throws SQLException {
		checkAvailable();
		prepareData();

		if (pos <= 0 || pos + length >= Integer.MAX_VALUE ||
				length < 0 || (int) (pos - 1 + length) > data.length) {
			throw new SQLException();
		}

		return Arrays.copyOfRange(data, (int) pos - 1, (int) pos - 1 + length);
	}

	@Override
	public InputStream getBinaryStream() throws SQLException {
		checkAvailable();
		prepareData();
		return new ByteArrayInputStream(data);
	}

	@Override
	public long position(byte[] pattern, long start) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public long position(Blob pattern, long start) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int setBytes(long pos, byte[] bytes) throws SQLException {
		return setBytes(pos, bytes, 0, bytes.length);
	}

	@Override
	public int setBytes(long pos, byte[] bytes, int offset, int len)
			throws SQLException {
		checkAvailable();
		prepareData();
		setBytesInternal(pos, bytes, false, offset, len);
		return len;
	}

	private void setBytesInternal(
			long pos, byte[] bytes, boolean internalArray, int offset, int len)
			throws SQLException {
		if (pos <= 0 || offset < 0 || len < 0 ||
				pos + len >= Integer.MAX_VALUE) {
			throw new SQLException();
		}

		if (len == 0) {
			return;
		}

		final int orgLength = (data == null ? 0 : data.length);
		final int minLength = (int) pos - 1 + len;
		final byte[] newData;
		if (internalArray && orgLength == 0 && orgLength == minLength) {
			data = bytes;
			shared = false;
			return;
		}
		else if (!shared && minLength <= orgLength) {
			newData = data;
		}
		else {
			newData = new byte[Math.max(orgLength, minLength)];
			if (orgLength > 0) {
				System.arraycopy(data, 0, newData, 0, orgLength);
			}
			shared = false;
		}

		System.arraycopy(bytes, offset, newData, (int) pos - 1, len);
		data = newData;
	}

	@Override
	public OutputStream setBinaryStream(long pos) throws SQLException {
		checkAvailable();
		prepareData();

		if (pos <= 0 || pos > Integer.MAX_VALUE) {
			throw new SQLException();
		}

		streamOutputPos = pos;
		return (streamOutput = new ByteArrayOutputStream());
	}

	@Override
	public void truncate(long len) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void free() {
		data = null;
		streamOutput = null;
		streamOutputPos = 0;
		shared = false;
	}

	@Override
	public InputStream getBinaryStream(long pos, long length)
			throws SQLException {
		checkAvailable();
		final int dataLen = (data == null ? 0 : data.length);
		if (pos <= 0 || length <= 0 || pos + length > dataLen) {
			throw new SQLException();
		}

		return new ByteArrayInputStream(data, (int) pos - 1, (int) length);
	}

}
