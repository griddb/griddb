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
package com.toshiba.mwcloud.gs;

import java.util.Arrays;

import com.toshiba.mwcloud.gs.common.BasicBuffer;
import com.toshiba.mwcloud.gs.common.GSErrorCode;
import com.toshiba.mwcloud.gs.common.GeometryUtils;
import com.toshiba.mwcloud.gs.common.GeometryUtils.DirectAccessor;

/**
 * Not supported
 */
public class Geometry {

	static {
		GeometryUtils.setDirectAccessor(new DirectAccessor() {

			@Override
			public int getBytesLength(Geometry geometry) {
				return geometry.bytesData.length;
			}

			@Override
			public void putGeometry(BasicBuffer out, Geometry geometry) {
				out.prepare(geometry.bytesData.length);
				out.base().put(geometry.bytesData);
			}

			@Override
			public Geometry getGeometry(BasicBuffer in, int size) {
				final byte[] bytesData = new byte[size];
				in.base().get(bytesData);
				return new Geometry(bytesData);
			}

		});
	}

	private final byte[] bytesData;

	private Geometry(byte[] bytesData) {
		this.bytesData = bytesData;
	}

	/**
	 * Not supported
	 */
	public static Geometry valueOf(String value)
			throws IllegalArgumentException {
		try {
			return new Geometry(GeometryUtils.encodeGeometry(value));
		}
		catch (GSException e) {
			throw new IllegalArgumentException(
					"Illegal geometry format (value=\"" + value + "\", " +
					"reason=" + e.getMessage() + ")", e);
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(value, "value", e);
		}
	}

	/**
	 * Not supported
	 */
	@Override
	public String toString() {
		try {
			return GeometryUtils.decodeGeometry(bytesData);
		}
		catch (GSException e) {
			throw new Error(
					"Internal error or decode error of remote data (reason=" +
					e.getMessage() + ")", e);
		}
	}

	/**
	 * Not supported
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(bytesData);
		return result;
	}

	/**
	 * Not supported
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Geometry other = (Geometry) obj;
		if (!Arrays.equals(bytesData, other.bytesData))
			return false;
		return true;
	}

}
