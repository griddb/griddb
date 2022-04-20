/*
	Copyright (c) 2017 TOSHIBA Digital Solutions Corporation
	
	This program is free software: you can redistribute it and/or modify
	it under the terms of the GNU Affero General Public License as
	published by the Free Software Foundation, either version 3 of the
	License, or (at your option) any later version.

	This program is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU Affero General Public License for more details.

	You should have received a copy of the GNU Affero General Public License
	along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
/*!
	@file nosql_client_session.cpp
	@brief nosql client session
*/

#include "nosql_client_session.h"
#include "nosql_command.h"

#define CLIENT_SESSION_THROW_INTERNAL() \
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "")
#define CLIENT_SESSION_ILLEGAL_COMMIT() \
	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, \
			"Internal error by illegal commit mode")
#define CLIENT_SESSION_RETHROW(cause, message) \
	GS_RETHROW_USER_ERROR(cause, message)

struct ClientSession::Statement {
	enum Id {
		CONNECT = 100,
		DISCONNECT,
		LOGIN,
		LOGOUT,
		GET_PARTITION_ADDRESS,
		GET_CONTAINER,
		GET_TIME_SERIES,		
		PUT_CONTAINER,
		PUT_TIME_SERIES,		
		DROP_COLLECTION,		
		DROP_TIME_SERIES,		
		CREATE_SESSION,
		CLOSE_SESSION,
		CREATE_INDEX,
		DROP_INDEX,
		CREATE_EVENT_NOTIFICATION,
		DROP_EVENT_NOTIFICATION,
		FLUSH_LOG,
		COMMIT_TRANSACTION,
		ABORT_TRANSACTION,
		GET_ROW,
		QUERY_TQL,
		QUERY_COLLECTION_GEOMETRY_RELATED,
		QUERY_COLLECTION_GEOMETRY_WITH_EXCLUSION,
		PUT_ROW,
		PUT_MULTIPLE_ROWS,
		UPDATE_ROW_BY_ID,
		DELETE_ROW,
		DELETE_ROW_BY_ID,
		APPEND_TIME_SERIES_ROW,
		GET_TIME_SERIES_ROW,		
		GET_TIME_SERIES_ROW_RELATED,
		INTERPOLATE_TIME_SERIES_ROW,
		AGGREGATE_TIME_SERIES,
		QUERY_TIME_SERIES_TQL,		
		QUERY_TIME_SERIES_RANGE,
		QUERY_TIME_SERIES_SAMPLING,
		PUT_TIME_SERIES_ROW,		
		PUT_TIME_SERIES_MULTIPLE_ROWS,		
		DELETE_TIME_SERIES_ROW,		
		GET_CONTAINER_PROPERTIES,
		GET_MULTIPLE_ROWS,
		GET_TIME_SERIES_MULTIPLE_ROWS,		


		STATEMTNT_ID_MAX
	};
};

const ClientSession::StatementInfo ClientSession::STATEMENT_INFO_LIST[] = {
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, true },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, true },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_POST, TRANSACTION_INFO_DEFAULT, true },
		{ STATEMENT_FAMILY_POST, TRANSACTION_INFO_DEFAULT, true },
		{ STATEMENT_FAMILY_QUERY, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_QUERY, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_QUERY, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_QUERY, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_UPDATE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_UPDATE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_UPDATE, TRANSACTION_INFO_SKIP_COMMIT_MODE, false },
		{ STATEMENT_FAMILY_UPDATE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_UPDATE, TRANSACTION_INFO_SKIP_COMMIT_MODE, false },
		{ STATEMENT_FAMILY_POST, TRANSACTION_INFO_DEFAULT, false },

		{ STATEMENT_FAMILY_QUERY, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_QUERY, TRANSACTION_INFO_SKIP_COMMIT_MODE, false },
		{ STATEMENT_FAMILY_QUERY, TRANSACTION_INFO_SKIP_COMMIT_MODE, false },
		{ STATEMENT_FAMILY_QUERY, TRANSACTION_INFO_SKIP_COMMIT_MODE, false },
		{ STATEMENT_FAMILY_QUERY, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_QUERY, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_QUERY, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_UPDATE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_UPDATE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_UPDATE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_QUERY, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_QUERY, TRANSACTION_INFO_DEFAULT, false },

		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },

		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },

		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
		{ STATEMENT_FAMILY_NONE, TRANSACTION_INFO_DEFAULT, false },
};

const int32_t ClientSession::SESSION_NOT_FOUND_ERROR_CODE = 110003;
const int32_t ClientSession::UUID_UNMATCHED_ERROR_CODE = 110016;
const int32_t ClientSession::MAX_SESSION_REPAIR_COUNT = 2;


/*!
	@brief 1つのコンテナについてのセッション状態を管理する
	@note 状態の保持とエンコード処理は行うが、通信は行わない
*/
ClientSession::ClientSession() : sessionId_(0), transactionId_(0),
		statementId_(0), sessionPrepared_(false), transactionStarted_(false),
		autoCommit_(true) {
}

/*!
	@brief 指定のコミットモードに変更するために、コミット処理が必要か
			どうかを返す
	@note コミットしておかないと、コミットモードを変更できないことがある
	@param autoCommit 新たに変更予定のコミットモード
*/
bool ClientSession::isCommitRequired(bool autoCommit) {
	return (!autoCommit_ && autoCommit);
}

/*!
	@brief コミットモードを変更する
	@note isCommitRequiredに従い、必要に応じてコミットし終えておく必要がある
	@param autoCommit 新たなコミットモード
*/
void ClientSession::acceptAutoCommit(bool autoCommit) {
	if (autoCommit_ && !autoCommit) {
		transactionStarted_ = false;
	}
	autoCommit_ = autoCommit;
}

/*!
	@brief 新たなステートメント実行の前準備を行う
	@note ステートメント実行途中に新たなステートメント実行を行えるのは、
			セッション生成・クローズの場合に限られる。
			ただしセッションクローズの際はこのメンバ関数は使用しない。
			ステートメントを実行し終えた後は、acceptStatementResultを呼び出す
	@return 新たなステートメント実行の前に、セッション生成が必要かどうか
*/
bool ClientSession::setUpStatement(Builder &builder) {
	bool sessionRequired;
	bool requiredImmediately;
	if (builder.isSessionIdGeneratorEnabled()) {
		switch (builder.statementInfo_.family_) {
		case STATEMENT_FAMILY_QUERY:
			sessionRequired = (!autoCommit_ && transactionStarted_);
			requiredImmediately = false;
			break;
		case STATEMENT_FAMILY_LOCK:
			if (autoCommit_) {
				CLIENT_SESSION_ILLEGAL_COMMIT();
			}
			sessionRequired = true;
			requiredImmediately = false;
			break;
		case STATEMENT_FAMILY_UPDATE:
			sessionRequired = (!autoCommit_ || !builder.env_->hasRowKey());
			requiredImmediately =
					(sessionRequired && !builder.env_->hasRowKey());
			break;
		case STATEMENT_FAMILY_POST:
			sessionRequired = true;
			requiredImmediately = true;
			break;
		case STATEMENT_FAMILY_NONE:
			sessionRequired = false;
			requiredImmediately = false;
			break;
		default:
			assert(false);
			CLIENT_SESSION_THROW_INTERNAL();
		}

		if (!sessionPrepared_) {
			if (requiredImmediately || sessionRequired) {
				sessionId_ = builder.env_->generateSessionId();
			}
		}
	}
	else {
		sessionRequired =
				(builder.statementInfo_.family_ != STATEMENT_FAMILY_QUERY ||
				(!autoCommit_ && sessionId_ != 0));
		requiredImmediately = (sessionRequired && sessionId_ == 0);

		if (requiredImmediately) {
			sessionId_ = builder.env_->generateSessionId();
		}
	}

	builder.sessionRequired_ = sessionRequired;

	if (sessionRequired) {
		if (builder.statementInfo_.family_ != STATEMENT_FAMILY_QUERY) {
			while (++statementId_ == 0) {
			}
		}
	}
	else {
		statementId_ = 0;
	}

	return requiredImmediately;
}

/*!
	@brief 新たなステートメントIDをセットする
*/
void ClientSession::setUpStatementId(Builder &builder) {
	if (!builder.env_->isTransactionMode()) {
		return;
	}
	if (builder.statementInfo_.family_ != STATEMENT_FAMILY_QUERY
		&& builder.statementInfo_.family_  != STATEMENT_FAMILY_NONE) {
		++statementId_;
		builder.env_->setStatementId(statementId_);
	}
	else {
		builder.env_->setStatementId(0);
	}
}

/*!
	@brief ステートメント実行の後処理を行う
	@param cause キャッチブロック内の例外。キャッチブロック外ではNULL
	@return セッション再生成によるリトライが可能かどうか。リトライ可能な場合は、
			セッションをクローズ・生成し、再度setUpStatementを実行する。
	@throw util::Exception リトライできなかった場合。セッションがあればクローズする
*/
bool ClientSession::acceptStatementResult(
		Builder &builder, const util::Exception *cause) {
	if (cause != NULL) {
		if (!builder.sessionRequired_ || !isInitialSessionLost(builder, *cause)) {
			throw;
		}
		else if (builder.trialCount_ >= MAX_SESSION_REPAIR_COUNT) {
			CLIENT_SESSION_RETHROW(*cause,
					"Failed to repair session (trialCount=" <<
					builder.trialCount_ << ")");
		}

		builder.trialCount_++;
		return (sessionPrepared_ &&
				builder.statementType_ != Statement::CLOSE_SESSION);
	}

	if (builder.sessionRequired_) {
		sessionPrepared_ = true;

		if (!autoCommit_) {
			transactionStarted_ = (
					builder.statementType_ != Statement::COMMIT_TRANSACTION &&
					builder.statementType_ != Statement::ABORT_TRANSACTION);
		}
	}

	if (builder.statementType_ == Statement::CREATE_SESSION) {
		sessionPrepared_ = true;
	}
	else if (builder.statementType_ == Statement::CLOSE_SESSION) {
		sessionId_ = 0;
		sessionPrepared_ = false;
		transactionStarted_ = false;
	}

	return false;
}


/*!
	@brief セッション情報を出力する
	@note セッション情報はUUID、セッションIDからなる
	@param reqOut リクエスト出力ストリーム
	@param old isSessionCloseRequiredが示す古いセッションIDを
			出力するかどうか。それ以外の場合、作成済みか新たに作成を要求する
			セッションID、またはセッションなし(ID:0)が出力される
*/
void ClientSession::writeSessionInfo(ByteOutStream &reqOut, Builder &builder) {
	reqOut << sessionId_;
	if (builder.isInitialSessionRetrialEnabled()) {
		SessionUUID sessionUUID;
		builder.env_->getSessionUUID(sessionUUID);
		reqOut.writeAll(&sessionUUID, sizeof(SessionUUID));
	}
}

/*!
	@brief トランザクション情報を出力する
	@note トランザクション情報はスキーマバージョンID、セッションモード、
			トランザクションモードからなる
	@param reqOut リクエスト出力ストリーム
*/
void ClientSession::writeTransactionInfo(
		ByteOutStream &reqOut, Builder &builder) {
	const bool sessionRequired = builder.sessionRequired_;
	if (sessionRequired && sessionId_ == 0) {
		assert(false);
		CLIENT_SESSION_THROW_INTERNAL();
	}

	reqOut << (sessionRequired ? sessionId_ : 0);

	if (builder.statementInfo_.infoType_ != TRANSACTION_INFO_NO_UUID &&
			builder.isInitialSessionRetrialEnabled()) {
		SessionUUID sessionUUID;
		builder.env_->getSessionUUID(sessionUUID);
		reqOut.writeAll(&sessionUUID, sizeof(SessionUUID));
	}

	const bool generatorEnabled = builder.isSessionIdGeneratorEnabled();
	if (!generatorEnabled) {
		if (builder.forUpdate_ != NULL) {
			reqOut << toGSBool(*builder.forUpdate_);
		}

		if (builder.statementInfo_.infoType_ !=
				TRANSACTION_INFO_SKIP_COMMIT_MODE) {
			reqOut << toGSBool(autoCommit_);
		}
	}

	reqOut << builder.env_->getSchemaVersionId();

	if (generatorEnabled) {
		if (sessionRequired) {
			reqOut << static_cast<int8_t>(sessionPrepared_ ?
					SESSION_MODE_GET : SESSION_MODE_CREATE);
		}
		else {
			reqOut << static_cast<int8_t>(SESSION_MODE_AUTO);
		}

		if (autoCommit_ || !sessionRequired) {
			reqOut << static_cast<int8_t>(TRANSACTION_MODE_AUTO);
		}
		else {
			reqOut << static_cast<int8_t>(transactionStarted_ ?
					TRANSACTION_MODE_CONTINUE :
					TRANSACTION_MODE_BEGIN);
		}
	}
}

/*!
	@brief 初期セッションがロストしたか
*/
bool ClientSession::isInitialSessionLost(
		Builder &builder, const util::Exception &cause) {
	if (builder.statementInfo_.sessionModeFixed_) {
		return false;
	}

	const int32_t errorCode = cause.getErrorCode();
	if (errorCode != SESSION_NOT_FOUND_ERROR_CODE &&
			errorCode != UUID_UNMATCHED_ERROR_CODE) {
		return false;
	}

	if (!builder.isInitialSessionRetrialEnabled()) {
		return false;
	}

	if (builder.isSessionIdGeneratorEnabled()) {
		if (transactionStarted_) {
			return false;
		}
	}
	else {
		if (statementId_ != 1) {
			return false;
		}
	}
	return true;
}

/*!
	@brief Bool値で取得
*/
int8_t ClientSession::toGSBool(bool value) {
	return static_cast<int8_t>(value);
}

/*!
	@brief セッションビルダコンストラクタ
*/
ClientSession::Builder::Builder(
		Environment *env, int32_t statementType, const bool *forUpdate) :
		env_(env),
		statementType_(statementType),
		trialCount_(0),
		forUpdate_(forUpdate),
		statementInfo_(getStatementInfo(statementType)),
		sessionRequired_(false) {
}

const ClientSession::StatementInfo& ClientSession::Builder::getStatementInfo(
		int32_t statementType) {
	if (statementType >= Statement::STATEMTNT_ID_MAX) {
		return STATEMENT_INFO_LIST[0];
	}
	else if (statementType < Statement::CONNECT) {
		assert(false);
		CLIENT_SESSION_THROW_INTERNAL();
	}

	return STATEMENT_INFO_LIST[statementType - Statement::CONNECT];
}

/*!
	@brief セッション管理対応済みサーバか
*/
bool ClientSession::Builder::isSessionIdGeneratorEnabled() {
	return (env_->getProtocolVersion() >= 3);
}

/*!
	@brief 初期セッションをリトライするかどうか
*/
bool ClientSession::Builder::isInitialSessionRetrialEnabled() {
	return (env_->getProtocolVersion() >= 2);
}
