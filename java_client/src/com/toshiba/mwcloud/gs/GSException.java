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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import com.toshiba.mwcloud.gs.common.GSErrorCode;

/**
 * Represents the exceptions occurring during a process of a GridDB function.
 */
public class GSException extends IOException {

	private static final long serialVersionUID = -7261622831192521426L;

	private static final Map<String, String> EMPTY_PARAMETERS =
			Collections.emptyMap();

	private final int errorCode;

	private final String errorName;

	private final String description;

	private final Map<String, String> parameters;

	static {
		GSErrorCode.setExceptionAccessor(new GSErrorCode.ExceptionAccessor() {
			@Override
			public String getDescription(GSException e) {
				return e.description;
			}
		});
	}

	/**
	 * Build a non-descriptive exception.
	 *
	 * @see Exception#Exception()
	 */
	public GSException() {
		errorCode = 0;
		errorName = null;
		description = null;
		parameters = EMPTY_PARAMETERS;
	}

	/**
	 * Specify the description and cause, then build an exception.
	 *
	 * @param message Description or {@code null}
	 * @param cause Cause or {@code null}
	 *
	 * @see Exception#Exception(String, Throwable)
	 */
	public GSException(String message, Throwable cause) {
		super(cause);
		errorCode = resolveErrorCode(0, cause);
		errorName = resolveErrorName(0, null, cause);
		description = resolveDescription(message, cause);
		parameters = EMPTY_PARAMETERS;
	}

	/**
	 * Specify the description, then build an exception.
	 *
	 * @param message Description or {@code null}
	 *
	 * @see Exception#Exception(String)
	 */
	public GSException(String message) {
		errorCode = 0;
		errorName = null;
		description = message;
		parameters = EMPTY_PARAMETERS;
	}

	/**
	 * Specify the error, then build an exception.
	 *
	 * @param cause Cause or {@code null}
	 *
	 * @see Exception#Exception(Throwable)
	 */
	public GSException(Throwable cause) {
		super(cause);
		errorCode = resolveErrorCode(0, cause);
		errorName = resolveErrorName(0, null, cause);
		description = resolveDescription(null, cause);
		parameters = resolveParameters(null, cause);
	}

	/**
	 * Specify the error number, description and cause, then build an exception.
	 *
	 * @param errorCode Error number
	 * @param description Description or {@code null}
	 * @param cause Cause or {@code null}
	 *
	 * @see Exception#Exception(String, Throwable)
	 */
	public GSException(int errorCode, String description, Throwable cause) {
		super(cause);
		this.errorCode = resolveErrorCode(errorCode, cause);
		errorName = resolveErrorName(errorCode, null, cause);
		this.description = resolveDescription(description, cause);
		parameters = EMPTY_PARAMETERS;
	}

	/**
	 * Specify the error number and description, then build an exception.
	 *
	 * @param errorCode Error number
	 * @param description Description or {@code null}
	 *
	 * @see Exception#Exception(String)
	 */
	public GSException(int errorCode, String description) {
		this.errorCode = resolveErrorCode(errorCode, null);
		errorName = resolveErrorName(errorCode, null, null);
		this.description = resolveDescription(description, null);
		parameters = EMPTY_PARAMETERS;
	}

	/**
	 * Specify the error number and description, then build an exception.
	 *
	 * @param errorCode Error number
	 * @param cause Cause or {@code null}
	 *
	 * @see Exception#Exception(Throwable)
	 */
	public GSException(int errorCode, Throwable cause) {
		super(cause);
		this.errorCode = resolveErrorCode(errorCode, cause);
		errorName = resolveErrorName(errorCode, null, null);
		this.description = resolveDescription(null, cause);
		parameters = EMPTY_PARAMETERS;
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
	 * @see Exception#Exception(String, Throwable)
	 */
	public GSException(int errorCode,
			String errorName, String description, Throwable cause) {
		super(cause);
		this.errorCode = resolveErrorCode(errorCode, cause);
		this.errorName = resolveErrorName(errorCode, errorName, cause);
		this.description = resolveDescription(description, cause);
		parameters = EMPTY_PARAMETERS;
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
	 * @see Exception#Exception(String, Throwable)
	 */
	public GSException(
			int errorCode, String errorName, String description,
			Map<String, String> parameters, Throwable cause) {
		super(cause);
		this.errorCode = resolveErrorCode(errorCode, cause);
		this.errorName = resolveErrorName(errorCode, errorName, cause);
		this.description = resolveDescription(description, cause);
		this.parameters = resolveParameters(parameters, null);
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

	private static Map<String, String> resolveParameters(
			Map<String, String> parameters, Throwable cause) {
		do {
			if (parameters != null) {
				if (parameters.isEmpty()) {
					break;
				}
				return Collections.unmodifiableMap(
						GSErrorCode.newParameters(parameters));
			}

			if (cause instanceof GSException) {
				return ((GSException) cause).parameters;
			}
		}
		while (false);

		return EMPTY_PARAMETERS;
	}

	/**
	 * Returns an error number.
	 *
	 * <p>It returns {@code 0} if no corresponding number is found.</p>
	 */
	public int getErrorCode() {
		return errorCode;
	}

	/**
	 * This method gets a map of parameters related to the error.
	 *
	 * <p>It is used to extract particular information about the
	 * error.  Returned map is a set which each entry consists of
	 * a parameter name and a parameter value.  For the parameters
	 * included in this map, see the definition of interfaces
	 * which may output this exception or the definition of
	 * related interfaces.</p>
	 *
	 * <p>The information in the map is also included in the message
	 * string returned by {@link #getMessage()} in principle.  But
	 * by a fixed parsing rule, it may not be able to extract
	 * the particular information from this message.  Even if the
	 * right information can be extracted from a context
	 * in a version, for other conditions, incorrect information
	 * may be acquired or nothing may be acquired.  By using this
	 * map, a part of information specified in the definition of
	 * the interfaces can be acquired without parsing.</p>
	 *
	 * <p>When recording only the content of the returned map and
	 * not recording other exception information such as the message
	 * text, it may become difficult to identify the reason for
	 * the error.</p>
	 *
	 */
	public Map<String, String> getParameters() {
		return parameters;
	}

	/**
	 * {@inheritDoc}
	 */
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
