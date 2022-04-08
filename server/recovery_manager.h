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
	@brief Definition of RecoveryManager
*/
#ifndef RECOVERY_MANAGER_H_
#define RECOVERY_MANAGER_H_
#include "util/allocator.h"
#include "util/container.h"
#include "config_table.h"
#include "data_store_common.h"
#include "event_engine.h"
#include "sha2.h"
#include "container_key.h"
#include "partition.h"

class TransactionContext;
class ConfigTable;
class PartitionTable;
class ClusterService;
class SystemService;
class TransactionManager;
class BaseContainer;
struct ManagerSet;
class NoLocker;
template <class L> class LogManager;
class PartitionList;

/*!
	@brief Recovers database.
*/
class RecoveryManager {
public:
	static const std::string BACKUP_INFO_FILE_NAME;
	static const std::string BACKUP_INFO_DIGEST_FILE_NAME;
	static const uint32_t BACKUP_INFO_FILE_VERSION = 0x1;  

	static const ChunkCategoryId CHUNK_CATEGORY_NON_READY_PARTITION =
		UNDEF_CHUNK_CATEGORY_ID;  

	RecoveryManager(
			ConfigTable &configTable, bool releaseUnusedFileBlocks);
	~RecoveryManager();

	/*!
		@brief 初期化
	*/
	void initialize(ManagerSet &resourceSet);

	/*
		@brief 与えられたファイル名がバックアップ情報ファイルかどうかを判定する
		@param [in] name 判定したいファイル名
	*/
	static bool checkBackupInfoFileName(const std::string &name);

	/*!
		@brief 与えられたファイル名がバックアップ情報ダイジェストファイルかどうかを判定する
		@param [in] name 判定したいファイル名
	*/
	static bool checkBackupInfoDigestFileName(const std::string &name);

	/*!
		@brief 起動時の既存ファイルチェック処理の実行
		@param [in] configTable
		@param [out] createFrag 新規作成ならtrue
		@note 各種Manager初期化前にmain関数からコールされる
		@note 条件を満たさなければ、起動失敗させる
	*/
	static void checkExistingFiles1(ConfigTable &configTable, bool &createFrag,
		bool &existBackupFile, bool forceRecoveryFromExistingFiles);
	
	static void scanExistingFiles(
			ConfigTable &param,
			bool &existBackupInfoFile1, bool &existBackupInfoFile2);

	/*!
		@brief データベースディレクトリに存在するバックアップ情報ファイルを読み込みパースする
		@param [out] isIncrementalBackup 増分バックアップからの起動ならtrue
		@note バックアップからのリカバリ+CP完了後に呼び出される
	*/
	bool readBackupInfoFile();
	bool readBackupInfoFile(const std::string &path);

	/*!
		@brief データベースディレクトリに存在するバックアップ情報ファイルを削除する
		@note バックアップからのリカバリ+CP完了後に呼び出される
	*/
	void removeBackupInfoFile();

	/*!
		@brief 起動時のリカバリ処理の実行
		@note main関数からコールされる
		@note 一つでもパーティション状態がシステムエラーになれば、起動失敗させる
	*/
	void recovery(util::StackAllocator &alloc, bool existBackup,
		bool forceRecoveryFromExistingFiles,
		const std::string &recoveryTargetPartition, bool forLongArchive);

	/*!
		@brief リカバリ進捗率取得
		@note 内部は0-100(%)。返すのはdouble(0.00 - 1.00)
	*/
	static double getProgressRate() {
		return (progressPercent_ / 100.0);
	}

	/*!
		@brief リカバリ進捗率設定
		@note 0-100(%)
	*/
	static void setProgressPercent(uint32_t percent) {
		progressPercent_ = percent;
	}

	/*!
		@brief バックアップ情報ファイル
		@note 出力時はチェックポイントサービススレッドでのみアクセスを想定(排他無し)
	*/
	class BackupInfo {
	public:
		enum IncrementalBackupMode {
			INCREMENTAL_BACKUP_NONE = 0,  
			INCREMENTAL_BACKUP_LV0,		  
			INCREMENTAL_BACKUP_LV1_CUMULATIVE,   
			INCREMENTAL_BACKUP_LV1_DIFFERENTIAL  
		};

		BackupInfo();
		~BackupInfo();

		void setConfigValue(uint32_t partitionNum, uint32_t partitionGroupNum);

		void startBackup(const std::string &backupPath, bool skipBaseLineFlag);  
		void startIncrementalBackup(const std::string &backupPath,
			const std::string &lastBackupPath, int32_t mode);  

		void
		endBackup();  

		void errorBackup();  

		void startCpFileCopy(PartitionId pId, CheckpointId cpId);
		void endCpFileCopy(PartitionId pId, uint64_t fileSize);
		void startIncrementalBlockCopy(PartitionId pId, int32_t mode);
		void endIncrementalBlockCopy(PartitionId pId, CheckpointId cpId);
		void startLogFileCopy();
		void endLogFileCopy(PartitionId pId, uint64_t fileSize);

		void readBackupInfoFile();
		void readBackupInfoFile(const std::string &path);

		void removeBackupInfoFile(PartitionList &ptList);

		void setBackupPath(const std::string &backupPath);  
		void setDataStorePath(const std::string &dataStorePath);  
		void setGSVersion(const std::string &gsVersion);  

		CheckpointId getBackupCheckpointId(PartitionId pId);
		uint64_t getBackupCheckpointFileSize(PartitionId pId);
		uint64_t getBackupLogFileSize(PartitionId pId);
		const std::string& getDataStorePath();

		IncrementalBackupMode getIncrementalBackupMode();
		void setIncrementalBackupMode(IncrementalBackupMode mode);
		bool isIncrementalBackup();
		std::vector<CheckpointId> &getIncrementalBackupCpIdList(
			PartitionId pId);
		bool getCheckCpFileSizeFlag();
		void setCheckCpFileSizeFlag(bool flag);

	private:
		void reset();

		void writeBackupInfoFile();

		uint32_t partitionNum_;
		uint32_t partitionGroupNum_;

		uint32_t currentPartitionId_;

		std::string backupPath_;
		std::string dataStorePath_;
		std::string status_;
		std::string gsVersion_;

		int64_t backupStartTime_;
		int64_t backupEndTime_;
		int64_t cpFileCopyStartTime_;
		int64_t cpFileCopyEndTime_;
		int64_t logFileCopyStartTime_;
		int64_t logFileCopyEndTime_;

		std::vector<CheckpointId> cpIdList_;

		std::vector<uint64_t> cpFileSizeList_;
		std::vector<uint64_t> logFileSizeList_;

		std::vector<double> cpFileCopyTimeList_;
		std::vector<double> logFileCopyTimeList_;

		std::vector<std::vector<CheckpointId> > incrementalBackupCpIdList_;
		IncrementalBackupMode incrementalBackupMode_;
		bool checkCpFileSizeFlag_;
		bool skipBaselineFlag_;

		char digestData_[SHA256_DIGEST_STRING_LENGTH + 1];
	};

private:
	/*!
		@brief Check restore files.
	*/
	void checkRestoreFile();

	/*!
		@brief Handler to set up Stat.
	*/
	static class StatSetUpHandler : public StatTable::SetUpHandler {
		virtual void operator()(StatTable &stat);
	} statSetUpHandler_;

	/*!
		@brief Updates Stat.
	*/
	class StatUpdator : public StatTable::StatUpdator {
		virtual bool operator()(StatTable &stat);

	public:
		RecoveryManager *manager_;
	} statUpdator_;

	static const uint64_t ADJUST_MEMORY_TIME_INTERVAL_ = 30 * 1000 * 1000;

	PartitionGroupConfig pgConfig_;
	const uint8_t CHUNK_EXP_SIZE_;
	const int32_t CHUNK_SIZE_;

	bool recoveryOnlyCheckChunk_;	
	std::string recoveryDownPoint_;  
	int32_t recoveryDownCount_;		 

	BackupInfo backupInfo_;

	static util::Atomic<uint32_t> progressPercent_;  

	std::vector<PartitionId> recoveryPartitionId_;
	bool recoveryTargetPartitionMode_;

	bool releaseUnusedFileBlocks_;

	std::vector<PartitionId> syncChunkPId_;
	std::vector<int64_t> syncChunkNum_;
	std::vector<int64_t> syncChunkCount_;

	PartitionTable *pt_;
	ClusterService *clsSvc_;
	SystemService *sysSvc_;
	TransactionManager *txnMgr_;
	PartitionList *partitionList_;
};

#endif 
