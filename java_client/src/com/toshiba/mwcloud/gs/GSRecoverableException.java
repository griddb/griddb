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
 * <div lang="ja">
 * 一時的な障害を原因とし、一定の手続きにより回復できる可能性のある例外です。
 *
 * <p>現バージョンでは、非公開オプションである{@link FetchOption#SIZE}を指定して
 * 取得した{@link RowSet}に対し、{@link RowSet#next()}を実行した場合のみ
 * 発生しうる例外です。</p>
 *
 * @deprecated
 *
 * @since 1.5
 * </div><div lang="en">
 * @deprecated
 * </div>
 */
@Deprecated
public class GSRecoverableException extends GSException {

	private static final long serialVersionUID = 1241771194878438360L;

	/**
	 * <div lang="ja">
	 * 詳細メッセージを持たない例外を構築します。
	 *
	 * @see GSException#GSException()
	 * </div><div lang="en">
	 * Build a non-descriptive exception.
	 *
	 * @see GSException#GSException()
	 * </div>
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
	 * <div lang="ja">
	 * エラー番号、エラー名、詳細メッセージ、および原因を指定して、例外を
	 * 構築します。
	 *
	 * @param errorCode エラー番号
	 * @param errorName エラー名または{@code null}
	 * @param description 詳細メッセージまたは{@code null}
	 * @param cause 原因または{@code null}
	 *
	 * @see GSException#GSException(int, String, String, Throwable)
	 * </div><div lang="en">
	 * Specify the error number, error name, description, and cause,
	 * then build an exception.
	 *
	 * @param errorCode Error number
	 * @param errorName Error name or {@code null}
	 * @param description Description or {@code null}
	 * @param cause Cause or {@code null}
	 *
	 * @see GSException#GSException(int, String, String, Throwable)
	 * </div>
	 */
	public GSRecoverableException(int errorCode,
			String errorName, String description, Throwable cause) {
		super(errorCode, errorName, description, cause);
	}

	/**
	 * <div lang="ja">
	 * エラー番号、詳細メッセージ、および原因を指定して、例外を構築します。
	 *
	 * @param errorCode エラー番号
	 * @param description 詳細メッセージまたは{@code null}
	 * @param cause 原因または{@code null}
	 *
	 * @see GSException#GSException(int, String, Throwable)
	 * </div><div lang="en">
	 * Specify the error number, description and cause, then build an exception.
	 *
	 * @param errorCode Error number
	 * @param description Description or {@code null}
	 * @param cause Cause or {@code null}
	 *
	 * @see GSException#GSException(int, String, Throwable)
	 * </div>
	 */
	public GSRecoverableException(int errorCode, String description,
			Throwable cause) {
		super(errorCode, description, cause);
	}

	/**
	 * <div lang="ja">
	 * エラー番号および詳細メッセージを指定して、例外を構築します。
	 *
	 * @param errorCode エラー番号
	 * @param description 詳細メッセージまたは{@code null}
	 *
	 * @see GSException#GSException(int, String)
	 * </div><div lang="en">
	 * Specify the error number and description, then build an exception.
	 *
	 * @param errorCode Error number
	 * @param description Description or {@code null}
	 *
	 * @see GSException#GSException(int, String)
	 * </div>
	 */
	public GSRecoverableException(int errorCode, String description) {
		super(errorCode, description);
	}

	/**
	 * <div lang="ja">
	 * エラー番号および原因を指定して、例外を構築します。
	 *
	 * @param errorCode エラー番号
	 * @param cause 原因または{@code null}
	 *
	 * @see GSException#GSException(int, Throwable)
	 * </div><div lang="en">
	 * Specify the error number and description, then build an exception.
	 *
	 * @param errorCode Error number
	 * @param cause Cause or {@code null}
	 *
	 * @see GSException#GSException(int, Throwable)
	 * </div>
	 */
	public GSRecoverableException(int errorCode, Throwable cause) {
		super(errorCode, cause);
	}

	/**
	 * <div lang="ja">
	 * 詳細メッセージおよび原因を指定して、例外を構築します。
	 *
	 * @param message 詳細メッセージまたは{@code null}
	 * @param cause 原因または{@code null}
	 *
	 * @see GSException#GSException(String, Throwable)
	 * </div><div lang="en">
	 * Specify the description and cause, then build an exception.
	 *
	 * @param message Description or {@code null}
	 * @param cause Cause or {@code null}
	 *
	 * @see GSException#GSException(String, Throwable)
	 * </div>
	 */
	public GSRecoverableException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * <div lang="ja">
	 * 詳細メッセージを指定して、例外を構築します。
	 *
	 * @param message 詳細メッセージまたは{@code null}
	 *
	 * @see GSException#GSException(String)
	 * </div><div lang="en">
	 * Specify the description, then build an exception.
	 *
	 * @param message Description or {@code null}
	 *
	 * @see GSException#GSException(String)
	 * </div>
	 */
	public GSRecoverableException(String message) {
		super(message);
	}

	/**
	 * <div lang="ja">
	 * 原因を指定して、例外を構築します。
	 *
	 * @param cause 原因または{@code null}
	 *
	 * @see GSException#GSException(Throwable)
	 * </div><div lang="en">
	 * Specify the error, then build an exception.
	 *
	 * @param cause Cause or {@code null}
	 *
	 * @see GSException#GSException(Throwable)
	 * </div>
	 */
	public GSRecoverableException(Throwable cause) {
		super(cause);
	}

}
