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

import java.util.Map;

import com.toshiba.mwcloud.gs.GSException;

public class GSConnectionException extends GSException {

	private static final long serialVersionUID = -5146627981460469737L;

	public GSConnectionException(
			int errorCode, String errorName, String description,
			Map<String, String> parameters, Throwable cause) {
		super(errorCode, errorName, description, parameters, cause);
	}

	public GSConnectionException(int errorCode,
			String errorName, String description, Throwable cause) {
		super(errorCode, errorName, description, cause);
	}

	public GSConnectionException(
			int errorCode, String description, Throwable cause) {
		super(errorCode, description, cause);
	}

	public GSConnectionException(int errorCode, String description) {
		super(errorCode, description);
	}

	public GSConnectionException(int errorCode, Throwable cause) {
		super(errorCode, cause);
	}

	public GSConnectionException(String message, Throwable cause) {
		super(message, cause);
	}

	public GSConnectionException(Throwable cause) {
		super(cause);
	}

}
