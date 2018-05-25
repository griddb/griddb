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
 * Represents the exceptions indicating that the requested operation
 * operation did not finish within a normal time.
 */
public class GSTimeoutException extends GSException {

	private static final long serialVersionUID = -2321647495394140580L;

	/**
	 * Build a non-descriptive exception.
	 *
	 * @see GSException#GSException()
	 */
	public GSTimeoutException() {
		super();
	}

	/**
	 * Specify the error number, error name, description, map of parameters,
	 * and cause, then build an exception.
	 *
	 * @param errorCode Error number
	 * @param errorName Error name or {@code null}
	 * @param description Description or {@code null}
	 * @param parameters Map of paramaters or {@code null}
	 * @param cause Cause or {@code null}
	 *
	 * @see GSException#GSException(int, String, String, Map, Throwable)
	 */
	public GSTimeoutException(
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
	public GSTimeoutException(int errorCode,
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
	public GSTimeoutException(int errorCode, String description,
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
	public GSTimeoutException(int errorCode, String description) {
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
	public GSTimeoutException(int errorCode, Throwable cause) {
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
	public GSTimeoutException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * Specify the description, then build an exception.
	 *
	 * @param message Description or {@code null}
	 *
	 * @see GSException#GSException(String)
	 */
	public GSTimeoutException(String message) {
		super(message);
	}

	/**
	 * Specify the error, then build an exception.
	 *
	 * @param cause Cause or {@code null}
	 *
	 * @see GSException#GSException(Throwable)
	 */
	public GSTimeoutException(Throwable cause) {
		super(cause);
	}

}
