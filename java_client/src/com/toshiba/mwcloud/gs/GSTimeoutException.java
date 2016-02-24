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
package com.toshiba.mwcloud.gs;

/**
 * Represents the exceptions indicating that the requested operation
 * operation did not finish within a nomal time.
 */
public class GSTimeoutException extends GSException {

	private static final long serialVersionUID = -2321647495394140580L;

	public GSTimeoutException() {
		super();
	}

	public GSTimeoutException(int errorCode,
			String errorName, String description, Throwable cause) {
		super(errorCode, errorName, description, cause);
	}

	public GSTimeoutException(int errorCode, String description,
			Throwable cause) {
		super(errorCode, description, cause);
	}

	public GSTimeoutException(int errorCode, String description) {
		super(errorCode, description);
	}

	public GSTimeoutException(int errorCode, Throwable cause) {
		super(errorCode, cause);
	}

	public GSTimeoutException(String message, Throwable cause) {
		super(message, cause);
	}

	public GSTimeoutException(String message) {
		super(message);
	}

	public GSTimeoutException(Throwable cause) {
		super(cause);
	}

}
