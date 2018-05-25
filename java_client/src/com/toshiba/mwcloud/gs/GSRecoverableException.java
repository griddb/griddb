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

/**
 * @deprecated
 */
@Deprecated
public class GSRecoverableException extends GSException {

	private static final long serialVersionUID = 1241771194878438360L;

	public GSRecoverableException() {
		super();
	}

	public GSRecoverableException(int errorCode,
			String errorName, String description, Throwable cause) {
		super(errorCode, errorName, description, cause);
	}

	public GSRecoverableException(int errorCode, String description,
			Throwable cause) {
		super(errorCode, description, cause);
	}

	public GSRecoverableException(int errorCode, String description) {
		super(errorCode, description);
	}

	public GSRecoverableException(int errorCode, Throwable cause) {
		super(errorCode, cause);
	}

	public GSRecoverableException(String message, Throwable cause) {
		super(message, cause);
	}

	public GSRecoverableException(String message) {
		super(message);
	}

	public GSRecoverableException(Throwable cause) {
		super(cause);
	}

}
