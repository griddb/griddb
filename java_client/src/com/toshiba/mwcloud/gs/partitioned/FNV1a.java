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
package com.toshiba.mwcloud.gs.partitioned;

public class FNV1a {

	private static final int OFFSET_BASIS32 = (int) 2166136261L;

	private static final int PRIME32 = 16777619;

	private FNV1a() {
	}

	public static int init32() {
		return OFFSET_BASIS32;
	}

	public static int update(int base, byte data) {
		return ((base ^ (data & 0xff)) * PRIME32);
	}

	public static int update(int base, byte[] data, int len) {
		int hash = base;
		for (int i = 0; i < len; i++) {
			hash = update(hash, data[i]);
		}
		return hash;
	}

}
