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

import java.util.Date;

import com.toshiba.mwcloud.gs.AggregationResult;

public class AggregationResultImpl implements AggregationResult {

	private Object value;

	void setValue(Object value) {
		this.value = value;
	}

	@Override
	public Double getDouble() {
		if (value instanceof Double) {
			return (Double) value;
		}
		else if (value instanceof Long) {
			return (double) (Long) value;
		}
		else {
			return null;
		}
	}

	@Override
	public Long getLong() {
		if (value instanceof Long) {
			return (Long) value;
		}
		else if (value instanceof Double) {
			return (long) (double) (Double) value;
		}
		else {
			return null;
		}
	}

	@Override
	public Date getTimestamp() {
		if (value instanceof Date) {
			return (Date) value;
		}
		else {
			return null;
		}
	}

}
