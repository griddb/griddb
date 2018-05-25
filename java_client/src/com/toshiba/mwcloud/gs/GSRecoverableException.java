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

import java.util.Map;

/**
 * @deprecated
 */
@Deprecated
public class GSRecoverableException extends GSException {

	private static final long serialVersionUID = 1241771194878438360L;

	/**
	 * Build a non-descriptive exception.
	 *
	 * @see GSException#GSException()
	 */
	public GSRecoverableException() {
		super();
	}

	public GSRecoverableException(
			int errorCode, String errorName, String description,
			Map<String, String> parameters, Throwable cause) {
		super(errorCode, errorName, description, parameters, cause);
	}

	/**
	 * Specify the error number, error name, description, and cause,
	 * then build an exception.
	 *
	 * @param errorCode Error number
	 * @param errorName Error name or {@code null}
	 * @param description Description or {@code null}
	 * @param cause Cause or {@code null}
	 *
	 * @see GSException#GSException(int, String, String, Throwable)
	 */
	public GSRecoverableException(int errorCode,
			String errorName, String description, Throwable cause) {
		super(errorCode, errorName, description, cause);
	}

	/**
	 * Specify the error number, description and cause, then build an exception.
	 *
	 * @param errorCode Error number
	 * @param description Description or {@code null}
	 * @param cause Cause or {@code null}
	 *
	 * @see GSException#GSException(int, String, Throwable)
	 */
	public GSRecoverableException(int errorCode, String description,
			Throwable cause) {
		super(errorCode, description, cause);
	}

	/**
	 * Specify the error number and description, then build an exception.
	 *
	 * @param errorCode Error number
	 * @param description Description or {@code null}
	 *
	 * @see GSException#GSException(int, String)
	 */
	public GSRecoverableException(int errorCode, String description) {
		super(errorCode, description);
	}

	/**
	 * Specify the error number and description, then build an exception.
	 *
	 * @param errorCode Error number
	 * @param cause Cause or {@code null}
	 *
	 * @see GSException#GSException(int, Throwable)
	 */
	public GSRecoverableException(int errorCode, Throwable cause) {
		super(errorCode, cause);
	}

	/**
	 * Specify the description and cause, then build an exception.
	 *
	 * @param message Description or {@code null}
	 * @param cause Cause or {@code null}
	 *
	 * @see GSException#GSException(String, Throwable)
	 */
	public GSRecoverableException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * Specify the description, then build an exception.
	 *
	 * @param message Description or {@code null}
	 *
	 * @see GSException#GSException(String)
	 */
	public GSRecoverableException(String message) {
		super(message);
	}

	/**
	 * Specify the error, then build an exception.
	 *
	 * @param cause Cause or {@code null}
	 *
	 * @see GSException#GSException(Throwable)
	 */
	public GSRecoverableException(Throwable cause) {
		super(cause);
	}

}
