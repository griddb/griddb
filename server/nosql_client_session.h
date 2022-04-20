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
	@file
	@brief Definition of nosql client session
*/

#ifndef NOSQL_CLIENT_SESSION_H_
#define NOSQL_CLIENT_SESSION_H_

#include "event_engine.h"
#include "uuid_utils.h"

class NoSQLContainer;

typedef UUIDValue SessionUUID;

/*!
	@brief 1つのコンテナについてのセッション状態を管理する
	@note 状態の保持とエンコード処理は行うが、通信は行わない
 */
struct ClientSession {
	struct Builder;

	typedef NoSQLContainer Environment;
	typedef EventByteOutStream ByteOutStream;


	/*!
		@brief 1つのコンテナについてのセッション状態を管理する
		@note 状態の保持とエンコード処理は行うが、通信は行わない
	*/
	ClientSession();


	/*!
		@brief 指定のコミットモードに変更するために、コミット処理が必要か
				どうかを返す
		@note コミットしておかないと、コミットモードを変更できないことがある
		@param autoCommit 新たに変更予定のコミットモード
	*/
	bool isCommitRequired(bool autoCommit);

	/*!
		@brief コミットモードを変更する
		@note isCommitRequiredに従い、必要に応じてコミットし終えておく必要がある
		@param autoCommit 新たなコミットモード
	*/
	void acceptAutoCommit(bool autoCommit);


	/*!
		@brief 新たなステートメント実行の前準備を行う
		@note ステートメント実行途中に新たなステートメント実行を行えるのは、
				セッション生成・クローズの場合に限られる。
				ただしセッションクローズの際はこのメンバ関数は使用しない。
				ステートメントを実行し終えた後は、acceptStatementResultを呼び出す
		@return 新たなステートメント実行の前に、セッション生成が必要かどうか
	*/
	bool setUpStatement(Builder &builder);

	/*!
		@brief 新たなステートメントIDをセットする
	*/
	void setUpStatementId(Builder &builder);

	/*!
		@brief ステートメント実行の後処理を行う
		@param cause キャッチブロック内の例外。キャッチブロック外ではNULL
		@return セッション再生成によるリトライが可能かどうか。リトライ可能な場合は、
		セッションをクローズ・生成し、再度setUpStatementを実行する。
		@throw util::Exception リトライできなかった場合。セッションがあればクローズする
	*/
	bool acceptStatementResult(Builder &builder, const util::Exception *cause);


	/*!
		@brief ステートメントIDを取得する
		@note セッション生成
	*/
	int64_t getStatementId() const { return statementId_; }

	/*!
		@brief セッションが準備済みか
	*/
	bool isSessionPrepared() const { return sessionPrepared_; }

	/*!
		@brief セッション情報を出力する
		@note セッション情報はUUID、セッションIDからなる
		@param reqOut リクエスト出力ストリーム
		@param old isSessionCloseRequiredが示す古いセッションIDを
				出力するかどうか。それ以外の場合、作成済みか新たに作成を要求する
				セッションID、またはセッションなし(ID:0)が出力される
	*/
	void writeSessionInfo(ByteOutStream &reqOut, Builder &builder);

	/*!
		@brief トランザクション情報を出力する
		@note トランザクション情報はスキーマバージョンID、セッションモード、
				トランザクションモードからなる
		@param reqOut リクエスト出力ストリーム
	*/
	void writeTransactionInfo(ByteOutStream &reqOut, Builder &builder);

private:

	struct StatementInfo;
	struct Statement;

	enum StatementFamily {
		STATEMENT_FAMILY_QUERY,
		STATEMENT_FAMILY_LOCK,
		STATEMENT_FAMILY_UPDATE,
		STATEMENT_FAMILY_POST,
		STATEMENT_FAMILY_NONE
	};

	enum TransactionInfoType {
		TRANSACTION_INFO_DEFAULT,
		TRANSACTION_INFO_NO_UUID,
		TRANSACTION_INFO_SKIP_COMMIT_MODE
	};

	enum TransactionMode {
		TRANSACTION_MODE_AUTO,
		TRANSACTION_MODE_BEGIN,
		TRANSACTION_MODE_CONTINUE
	};

	enum SessionMode {
		SESSION_MODE_AUTO,
		SESSION_MODE_CREATE,
		SESSION_MODE_GET
	};

	static const StatementInfo STATEMENT_INFO_LIST[];

	static const int32_t SESSION_NOT_FOUND_ERROR_CODE;
	static const int32_t UUID_UNMATCHED_ERROR_CODE;
	static const int32_t MAX_SESSION_REPAIR_COUNT;

	bool isInitialSessionLost(Builder &builder, const util::Exception &cause);
	static int8_t toGSBool(bool value);

	int64_t sessionId_;
	int64_t transactionId_;
	int64_t statementId_;
	bool sessionPrepared_;
	bool transactionStarted_;
	bool autoCommit_;
};

struct ClientSession::StatementInfo {
	StatementFamily family_;
	TransactionInfoType infoType_;
	bool sessionModeFixed_;
};

struct ClientSession::Builder {
public:
	Builder(
			Environment *env, int32_t statementType,
			const bool *forUpdate = NULL);

	int32_t getStatementType() const { return statementType_; };

private:
	friend struct ClientSession;

	static const StatementInfo& getStatementInfo(int32_t statementType);

	bool isInitialSessionRetrialEnabled();
	bool isSessionIdGeneratorEnabled();

	Environment *env_;
	int32_t statementType_;
	int32_t trialCount_;
	const bool *forUpdate_;
	const StatementInfo &statementInfo_;
	bool sessionRequired_;
};

#endif
