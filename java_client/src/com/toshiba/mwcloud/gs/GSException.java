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

import java.io.IOException;

import com.toshiba.mwcloud.gs.common.GSErrorCode;

/**
 * Represents the exceptions occurring during a process of a GridDB function. 
 */
public class GSException extends IOException {

	private static final long serialVersionUID = -7261622831192521426L;

	private final int errorCode;

	private final String errorName;

	private final String description;

	public GSException() {
		errorCode = 0;
		errorName = null;
		description = null;
	}

	public GSException(String message, Throwable cause) {
		super(cause);
		errorCode = resolveErrorCode(0, cause);
		errorName = resolveErrorName(0, null, cause);
		description = resolveDescription(message, cause);
	}

	public GSException(String message) {
		errorCode = 0;
		errorName = null;
		description = message;
	}

	public GSException(Throwable cause) {
		super(cause);
		errorCode = resolveErrorCode(0, cause);
		errorName = resolveErrorName(0, null, cause);
		description = resolveDescription(null, cause);
	}

	public GSException(int errorCode, String description, Throwable cause) {
		super(cause);
		this.errorCode = resolveErrorCode(errorCode, cause);
		errorName = resolveErrorName(errorCode, null, cause);
		this.description = resolveDescription(description, cause);
	}

	public GSException(int errorCode, String description) {
		this.errorCode = resolveErrorCode(errorCode, null);
		errorName = resolveErrorName(errorCode, null, null);
		this.description = resolveDescription(description, null);
	}

	public GSException(int errorCode, Throwable cause) {
		super(cause);
		this.errorCode = resolveErrorCode(errorCode, cause);
		errorName = resolveErrorName(errorCode, null, null);
		this.description = resolveDescription(null, cause);
	}

	public GSException(int errorCode,
			String errorName, String description, Throwable cause) {
		super(cause);
		this.errorCode = resolveErrorCode(errorCode, cause);
		this.errorName = resolveErrorName(errorCode, errorName, cause);
		this.description = resolveDescription(description, cause);
	}

	private static int resolveErrorCode(int errorCode, Throwable cause) {
		if (errorCode != 0) {
			return errorCode;
		}

		if (cause instanceof GSException) {
			return ((GSException) cause).getErrorCode();
		}

		return 0;
	}

	private static String resolveErrorName(
			int errorCode, String errorName, Throwable cause) {
		if (errorCode != 0 && errorName != null) {
			return errorName;
		}

		if (cause instanceof GSException) {
			final GSException gsCause = ((GSException) cause);
			if (errorCode == 0 || errorCode == gsCause.getErrorCode()) {
				return gsCause.errorName;
			}
		}

		return null;
	}

	private static String resolveDescription(
			String description, Throwable cause) {
		if (description != null) {
			return description;
		}

		if (cause instanceof GSException) {
			return ((GSException) cause).description;
		}

		if (cause != null) {
			return cause.getMessage(); 
		}

		return null;
	}

	/**
	 * Returns an error number. 
	 *
	 * <p>It returns {@code 0} if no corresponding number is found.</p>
	 */
	public int getErrorCode() {
		return errorCode;
	}

	@Override
	public String getMessage() {
		if (errorCode == 0 && description == null) {
			return super.getMessage();
		}

		if (errorCode == 0) {
			return description;
		}

		final String resolvedErrorName;
		if (errorName == null) {
			resolvedErrorName = GSErrorCode.getName(errorCode);
		}
		else {
			resolvedErrorName = errorName;
		}

		final StringBuilder builder = new StringBuilder();

		if (resolvedErrorName == null) {
			builder.append("[Code:").append(errorCode).append("]");
		}
		else {
			builder.append("[").append(errorCode).append(":");
			builder.append(resolvedErrorName).append("]");
		}

		if (description != null) {
			builder.append(" ").append(description);
		}

		return builder.toString();
	}

}
