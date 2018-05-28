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

import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.WeakHashMap;

public class InternPool<T> {

	private final Map<T, WeakReference<T>> refMap =
			new WeakHashMap<T, WeakReference<T>>();

	public T intern(T obj) {
		final WeakReference<T> ref = refMap.get(obj);

		if (ref != null) {
			final T cachedObj = ref.get();
			if (cachedObj != null) {
				return cachedObj;
			}
		}

		refMap.put(obj, new WeakReference<T>(obj));
		return obj;
	}

	public int size() {
		return refMap.size();
	}

	public void clear() {
		refMap.clear();
	}

}
