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
	@brief Implementation of KeyDataStore
*/
#include "key_data_store.h"
#include "btree_map.h"


/** **
	@brief コンストラクタ
	@param [in] stAlloc util::StackAllocator
	@param [in] resultSetPool resultSet用のメモリプール
	@param [in] configTable ConfigTable
	@param [in] txnMgr TransactionManager
	@param [in] chunkmanager ChunkManager
	@param [in] logmanager LogManager
	@param [in] keyStore KeyDataStoreへのポインタ(KeyDataStoreは未使用)
** **/
KeyDataStore::KeyDataStore(
		util::StackAllocator* stAlloc,
		util::FixedSizeAllocator<util::Mutex>* resultSetPool,
		ConfigTable* configTable, TransactionManager* txnMgr,
		ChunkManager* chunkmanager, LogManager<NoLocker>* logmanager,
		KeyDataStore* keyStore, const StatsSet &stats) :
		DataStoreBase(
				stAlloc, resultSetPool, configTable, txnMgr, chunkmanager,
				logmanager, keyStore),
		headerOId_(UNDEF_OID) {
	try {
		objectManager_ = UTIL_NEW ObjectManagerV4(
				*configTable, chunkmanager, stats.objMgrStats_);
		allocateStrategy_.set(META_GROUP_ID, objectManager_);

		if (objectManager_->isActive(allocateStrategy_.getGroupId())) {
			headerOId_ = getHeadOId(allocateStrategy_.getGroupId());
		}
	}
	catch (std::exception& e) {
		delete objectManager_;
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/** **
	@brief デストラクタ
** **/
KeyDataStore::~KeyDataStore() {

	delete objectManager_;
}


/** **
	@brief DataStoreがサポートしている機能かを判定
	@param [IN] type Support機能タイプ
	@return
	@note 現時点では未使用
** **/
bool KeyDataStore::support(Support type) {
	bool isSupport = false;
	switch (type) {
	case Support::TRIGGER:
		isSupport = false;
		break;
	default:
		break;
	}
	return isSupport;
}

/** **
	@brief DataStoreBase::exec実行前に必要な前処理を実行
	@param [in] txn TransactionContext
	@param [in] clsService ClusterService(エラー通知用)
	@note DataStoreBase::Scope()内でpostProcessとペアで呼ばれる
** **/
void KeyDataStore::preProcess(TransactionContext* txn, ClusterService* clsService) {
	ObjectManagerV4& objectManager = *(getObjectManager());
	objectManager.checkDirtyFlag();
	const double HOT_MODE_RATE = 1.0;
	objectManager.setStoreMemoryAgingSwapRate(HOT_MODE_RATE);
}

/** **
	@brief DataStoreBase::exec実行後に必要な後処理を実行
	@param [in] txn TransactionContext
	@note DataStoreBase::Scope()内でpreProcessとペアで呼ばれる
	@note チャンクの参照カウンターのリセット等を実施
** **/
void KeyDataStore::postProcess(TransactionContext* txn) {
	ObjectManagerV4& objectManager = *(getObjectManager());
	objectManager.checkDirtyFlag();
	objectManager.resetRefCounter();
	objectManager.freeLastLatchPhaseMemory();
	objectManager.setSwapOutCounter(0);
}

/** **
	@brief 各データストアのタイプをキー、ヘッダ領域をバリューとするマップに登録
	@param [in] txn TransactionContext
	@param [in] storeType データストアタイプ
	@param [in] allocateSize 対象データストアのヘッダ領域のサイズ
	@return ヘッダ領域のOId
	@note ヘッダ領域はここで確保して返り値でOIdを返却
	@attention このメソッドを呼び出す前にオブジェクトを作成してはならない
** **/
OId KeyDataStore::put(TransactionContext& txn, StoreType storeType, DSObjectSize allocateSize) {
	try {
		DataStorePartitionHeaderObject partitionHeaderObject(*getObjectManager(), allocateStrategy_);
		if (!isActive()) {
			initializeHeader(txn);
		}
		partitionHeaderObject.load(headerOId_, true);

		OId oId = UNDEF_OID;
		BaseObject storeObject(*getObjectManager(), allocateStrategy_);
		uint8_t* data = storeObject.allocate<uint8_t>(allocateSize, oId, OBJECT_TYPE_UNKNOWN);
		memset(data, 0, allocateSize);

		BtreeMap storeMap(txn, *getObjectManager(),
			partitionHeaderObject.getStoreMapOId(), allocateStrategy_, NULL);
		util::XArray<OId> list(txn.getDefaultAllocator());
		TermCondition cond(COLUMN_TYPE_INT, COLUMN_TYPE_INT,
			DSExpression::EQ, UNDEF_COLUMNID, &storeType, sizeof(storeType));
		BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, 1);
		storeMap.search(txn, sc, list);
		int32_t status;
		bool isCaseSensitive = true;
		if (list.empty()) {
			status = storeMap.insert<StoreType, OId>(txn, storeType, oId, isCaseSensitive);
		}
		else {
			status =
				storeMap.update<StoreType, OId>(txn, storeType, list[0], oId, isCaseSensitive);
		}
		if ((status & BtreeMap::ROOT_UPDATE) != 0) {
			partitionHeaderObject.setStoreMapOId(storeMap.getBaseOId());
		}
		return oId;
	}
	catch (std::exception& e) {
		handleUpdateError(e, GS_ERROR_DS_DS_GET_COLLECTION_FAILED);
		return UNDEF_OID;
	}
}

/** **
	@brief 各データストアのタイプをキーとしてデータストアのヘッダ領域へのアドレスを取得
	@param [in] txn TransactionContext
	@param [in] storeType データストアタイプ
	@return ヘッダ領域のOId
** **/
OId KeyDataStore::get(TransactionContext& txn, StoreType storeType) {
	if (!isActive()) {
		return UNDEF_OID;
	}
	DataStorePartitionHeaderObject partitionHeaderObject(*getObjectManager(), allocateStrategy_, headerOId_);
	BtreeMap storeMap(txn, *getObjectManager(),
		partitionHeaderObject.getStoreMapOId(), allocateStrategy_, NULL);
	util::XArray<OId> list(txn.getDefaultAllocator());
	TermCondition cond(COLUMN_TYPE_INT, COLUMN_TYPE_INT,
		DSExpression::EQ, UNDEF_COLUMNID, &storeType, sizeof(storeType));
	BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, 1);
	storeMap.search(txn, sc, list);
	if (list.empty()) {
		return UNDEF_OID;
	}
	else {
		return list[0];
	}
}

/** **
	@brief コンテナIDをキーとして該当するデータストアタイプ及びコンテナのアドレスを取得
	@param [in] txn TransactionContext
	@param [in] id ContainerId
	@return コンテナの情報
** **/
KeyDataStoreValue KeyDataStore::get(util::StackAllocator& alloc, ContainerId id) {
	try {
		KeyDataStoreValue val = containerIdTable_.get(id);
		if (val.oId_ == UNDEF_OID) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_CONTAINER_UNEXPECTEDLY_REMOVED, "");
		}
		return val;
	}
	catch (std::exception& e) {
		handleSearchError(e, GS_ERROR_DS_DS_GET_COLLECTION_FAILED);
		return KeyDataStoreValue();
	}
}

/** **
	@brief コンテナキーをキーとして該当するデータストアタイプ及びコンテナのアドレスを取得
	@param [in] txn TransactionContext
	@param [in] containerKey コンテナキー
	@param [in] isCaseSensitive 大小同一視指定
	@return コンテナの情報
** **/
KeyDataStoreValue KeyDataStore::get(TransactionContext& txn,
	const FullContainerKey& containerKey, bool isCaseSensitive) {
	util::StackAllocator& alloc = txn.getDefaultAllocator();
	try {
		KeyDataStoreValue ret = KeyDataStoreValue();
		if (!isActive()) {
			return ret;
		}
		DataStorePartitionHeaderObject partitionHeaderObject(
			*getObjectManager(), allocateStrategy_, headerOId_);
		BtreeMap keyMap(txn, *getObjectManager(),
			partitionHeaderObject.getKeyMapOId(), allocateStrategy_, NULL);

		FullContainerKeyCursor keyCursor(const_cast<FullContainerKey*>(&containerKey));
		keyMap.search<FullContainerKeyCursor, KeyDataStoreValue, KeyDataStoreValue>(
			txn, keyCursor, ret, isCaseSensitive);
		return ret;
	}
	catch (std::exception& e) {
		handleSearchError(e, GS_ERROR_DS_DS_GET_COLLECTION_FAILED);
		return KeyDataStoreValue();
	}
}

/** **
	@brief コンテナキーをキー、コンテナの情報をバリューとしてマップに登録
	@param [in] txn TransactionContext
	@param [in] keyOId コンテナキーのOId
	@param [in] newValue コンテナの情報
	@return 実行結果のステータス
** **/
PutStatus KeyDataStore::put(TransactionContext& txn,
	OId keyOId, KeyDataStoreValue& newValue) {
	util::StackAllocator& alloc = txn.getDefaultAllocator();
	PutStatus putStatus = PutStatus::CREATE;
	try {
		DataStorePartitionHeaderObject partitionHeaderObject(*getObjectManager(), allocateStrategy_);
		if (!isActive()) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR,
				"must call 'put(TransactionContext&, StoreType, Size_t)', at first");
		}
		else {
			partitionHeaderObject.load(headerOId_, false);
		}
		BtreeMap keyMap(txn, *getObjectManager(),
			partitionHeaderObject.getKeyMapOId(), allocateStrategy_, NULL);

		bool isCaseSensitive = false;
		KeyDataStoreValue value;
		FullContainerKeyCursor keyCursor(
			*getObjectManager(), allocateStrategy_, keyOId);
		keyMap.search<FullContainerKeyCursor, KeyDataStoreValue, KeyDataStoreValue>(
			txn, keyCursor, value, isCaseSensitive);
		if (value.oId_ != UNDEF_OID) {
			int32_t status = keyMap.remove<FullContainerKeyCursor, KeyDataStoreValue>(
				txn, keyCursor, value, isCaseSensitive);
			if ((status & BtreeMap::ROOT_UPDATE) != 0) {
				partitionHeaderObject.setKeyMapOId(keyMap.getBaseOId());
			}

			containerIdTable_.remove(newValue.containerId_);
			putStatus = PutStatus::UPDATE;
		}
		{
			int32_t status = keyMap.insert<FullContainerKeyCursor, KeyDataStoreValue>(
				txn, keyCursor, newValue, isCaseSensitive);
			if ((status & BtreeMap::ROOT_UPDATE) != 0) {
				partitionHeaderObject.setKeyMapOId(keyMap.getBaseOId());
			}
		}

		containerIdTable_.set(newValue.containerId_, newValue.oId_, keyOId,
			keyCursor.getKey().getComponents(txn.getDefaultAllocator()).dbId_, 
			newValue.storeType_, newValue.attribute_);

		return putStatus;
	}
	catch (std::exception& e) {
		handleUpdateError(e, GS_ERROR_CM_INTERNAL_ERROR);
		return putStatus;
	}
}

/** **
	@brief コンテナキーをキーとして、コンテナ管理マップから削除
	@param [in] txn TransactionContext
	@param [in] keyOId コンテナキーのOId
	@return 指定したコンテナキーの存在の有無
** **/
bool KeyDataStore::remove(TransactionContext& txn, OId keyOId) {
	util::StackAllocator& alloc = txn.getDefaultAllocator();
	PutStatus status = PutStatus::CREATE;
	try {
		DataStorePartitionHeaderObject partitionHeaderObject(*getObjectManager(), allocateStrategy_);
		if (!isActive()) {
			return false;
		}
		else {
			partitionHeaderObject.load(headerOId_, false);
		}
		BtreeMap keyMap(txn, *getObjectManager(),
			partitionHeaderObject.getKeyMapOId(), allocateStrategy_, NULL);

		bool isCaseSensitive = false;
		KeyDataStoreValue value;
		FullContainerKeyCursor keyCursor(
			*getObjectManager(), allocateStrategy_, keyOId);
		keyMap.search<FullContainerKeyCursor, KeyDataStoreValue, KeyDataStoreValue>(
			txn, keyCursor, value, isCaseSensitive);
		if (value.oId_ != UNDEF_OID) {
			int32_t status = keyMap.remove<FullContainerKeyCursor, KeyDataStoreValue>(
				txn, keyCursor, value, isCaseSensitive);
			if ((status & BtreeMap::ROOT_UPDATE) != 0) {
				partitionHeaderObject.setKeyMapOId(keyMap.getBaseOId());
			}
			containerIdTable_.remove(value.containerId_);
		}
		return value.oId_ != UNDEF_OID;
	}
	catch (std::exception& e) {
		handleUpdateError(e, GS_ERROR_DS_DS_DROP_COLLECTION_FAILED);
		return false;
	}
}

/** **
	@brief データストの汎用実行I/F
	@param [in] txn TransactionContext
	@param [in] storeValue コンテナ情報
	@param [in] message 汎用入力メッセージ
	@return 汎用出力メッセージ
	@note KeyDataStoreは内部向けデータストアであり性能重視のため汎用I/Fは利用しない
** **/
Serializable* KeyDataStore::exec(TransactionContext* txn, KeyDataStoreValue* storeValue, Serializable* message) {
	assert(false);
	return NULL;
}

/*!
	@brief Handle Exception of update phase
*/
/** **
	@brief 更新操作で例外が発生した場合の処理
	@param [in] errorCode エラーコード
	@attention 更新操作におけるメモリ確保・上限エラーの場合はUserErrorからSystemErrorに変更する
** **/
void KeyDataStore::handleUpdateError(std::exception&, ErrorCode) {
	try {
		throw;
	}
	catch (SystemException& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
	catch (UserException& e) {
		if (e.getErrorCode() == GS_ERROR_CM_NO_MEMORY ||
			e.getErrorCode() == GS_ERROR_CM_MEMORY_LIMIT_EXCEEDED ||
			e.getErrorCode() == GS_ERROR_CM_SIZE_LIMIT_EXCEEDED) {
			GS_RETHROW_SYSTEM_ERROR(e, "");
		}
		else {
			GS_RETHROW_USER_ERROR(e, "");
		}
	}
	catch (LockConflictException& e) {
		DS_RETHROW_LOCK_CONFLICT_ERROR(e, "");
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Handle Exception of search phase
*/
/** **
	@brief 検索操作で例外が発生した場合の処理
	@param [in] errorCode エラーコード
** **/
void KeyDataStore::handleSearchError(std::exception&, ErrorCode) {
	try {
		throw;
	}
	catch (SystemException& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
	catch (UserException& e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
	catch (LockConflictException& e) {
		DS_RETHROW_LOCK_CONFLICT_ERROR(e, "");
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/** **
	@brief KeyDataStoreのヘッダ領域作成
	@param [in] txn TransactionContext
	@note Partitionで必ず最初にアロケートするオブジェクト
** **/
void KeyDataStore::initializeHeader(TransactionContext& txn) {
	assert(!objectManager_->isActive(allocateStrategy_.getGroupId()));
	DataStorePartitionHeaderObject partitionHeaderObject(
		*getObjectManager(), allocateStrategy_);
	partitionHeaderObject.initialize(txn, allocateStrategy_);
	headerOId_ = getHeadOId(allocateStrategy_.getGroupId());
	if (partitionHeaderObject.getBaseOId() != headerOId_) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_DS_CHUNK_OFFSET_INVALID, "must be first object");
	}
}

/** **
	@brief コンテナIDを新規割当
	@return 新規割当コンテナID
** **/
ContainerId KeyDataStore::allocateContainerId() {
	DataStorePartitionHeaderObject partitionHeaderObject(*getObjectManager(), allocateStrategy_);
	if (!isActive()) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	else {
		partitionHeaderObject.load(headerOId_, false);
	}
	ContainerId containerId = partitionHeaderObject.allocateContainerId();
	return containerId;
}

/** **
	@brief チャンクグループIDを新規割当
	@param [in] num 新規に必要なID数
	@return 新規に割り当てた先頭のチャンクグループID
** **/
DSGroupId KeyDataStore::allocateGroupId(int32_t num) {
	DataStorePartitionHeaderObject partitionHeaderObject(*getObjectManager(), allocateStrategy_);
	if (!isActive()) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	else {
		partitionHeaderObject.load(headerOId_, false);
	}
	DSGroupId groupId = partitionHeaderObject.allocateGroupId(num);
	return groupId;
}



/** **
	@brief calculate checkSum
	@param [in] alloc アロケータ
	@note 試験用。永続化データのチェックサムを計算
	@note V4と比較することを考えてチェックサム対象はキーとContainerIdのみ
** **/
/*!
	@brief Allocate DataStorePartitionHeader Object and BtreeMap Objects for
   DataStores and Containers
*/
/** **
	@brief データストアのヘッダ領域の初期化、コンテナマップ、データストアマップの初期化
	@param [in] txn TransactionContext
	@param [in] allocateStrategy Object割当戦略
** **/
void KeyDataStore::DataStorePartitionHeaderObject::initialize(
	TransactionContext& txn, AllocateStrategy& allocateStrategy) {
	BaseObject::allocate<DataStorePartitionHeader>(
		sizeof(DataStorePartitionHeader), getBaseOId(),
		OBJECT_TYPE_CONTAINER_ID);
	memset(get(), 0, sizeof(DataStorePartitionHeader));

	BtreeMap keyMap(txn, *getObjectManager(), allocateStrategy, NULL);
	keyMap.initialize<FullContainerKeyCursor, KeyDataStoreValue>(
		txn, COLUMN_TYPE_STRING, true, BtreeMap::TYPE_SINGLE_KEY);

	setKeyMapOId(keyMap.getBaseOId());

	BtreeMap storeMap(txn, *getObjectManager(), allocateStrategy, NULL);
	storeMap.initialize<StoreType, OId>(
		txn, COLUMN_TYPE_INT, true, BtreeMap::TYPE_SINGLE_KEY);

	setStoreMapOId(storeMap.getBaseOId());

	get()->maxContainerId_ = 0;
	get()->groupIdCounter_ = 1; 
}

/*!
	@brief Free DataStorePartitionHeader Object and BtreeMap Objects for
   DataStores and Containers
*/
/** **
	@brief データストアのヘッダ領域の終了処理
	@param [in] txn TransactionContextオブジェクト
	@param [in] allocateStrategy Object割当戦略
** **/
void KeyDataStore::DataStorePartitionHeaderObject::finalize(
	TransactionContext& txn, AllocateStrategy& allocateStrategy) {
	BtreeMap keyMap(
		txn, *getObjectManager(), getKeyMapOId(), allocateStrategy, NULL);
	keyMap.finalize(txn);
	BtreeMap storeMap(
		txn, *getObjectManager(), getStoreMapOId(), allocateStrategy, NULL);
	storeMap.finalize(txn);
}

/*!
	@brief Get Container Information by ContainerId
*/
/** **
	@brief コンテナIDをキーとしてコンテナ情報を取得
	@param [in] containerId ContainerId
	@return コンテナ情報
** **/
KeyDataStoreValue KeyDataStore::ContainerIdTable::get(ContainerId containerId) {
	ContainerIdMap::const_iterator itr = containerIdMap_.find(containerId);
	if (itr != containerIdMap_.end()) {
		return KeyDataStoreValue(itr->first, itr->second.containerOId_, itr->second.storeType_, itr->second.attribute_);
	}
	else {
		return KeyDataStoreValue();
	}
}

/*!
	@brief Get ContainerKey OId by ContainerId
*/
/** **
	@brief コンテナIDをキーとしてコンテナキーのOIdを取得
	@param [in] containerId ContainerId
	@return コンテナキーのOId
** **/
OId KeyDataStore::ContainerIdTable::getKey(ContainerId containerId) {
	ContainerIdMap::const_iterator itr = containerIdMap_.find(containerId);
	if (itr != containerIdMap_.end()) {
		return itr->second.keyOId_;
	}
	else {
		return UNDEF_OID;
	}
}

/** **
	@brief DataStoreの処理を受付可能な状態にする
	@param [in] txn TransactionContext
	@param [in] clusterService ClusterService
	@note メモリに保持する必要のあるデータを復元
** **/
void KeyDataStore::activate(
	TransactionContext& txn, ClusterService* clusterService) {
	restoreContainerIdTable(txn, clusterService);
}

/*!
	@brief Restore ContainerIdTable in the partition
*/
/** **
	@brief メモリ上に保持しているContainerIdテーブルをChunkから復元
	@param [in] txn TransactionContext
	@param [in] clsService ClusterService(エラー通知用)
** **/
void KeyDataStore::restoreContainerIdTable(
	TransactionContext& txn, ClusterService* clusterService) {
	util::StackAllocator& alloc = txn.getDefaultAllocator();

	const DataStoreBase::Scope dsScope(&txn, this, clusterService);
	if (!isActive()) {
		return;
	}

	DataStorePartitionHeaderObject partitionHeaderObject(
		*getObjectManager(), allocateStrategy_, headerOId_);
	BtreeMap keyMap(txn, *getObjectManager(),
		partitionHeaderObject.getKeyMapOId(), allocateStrategy_, NULL);

	size_t containerListSize = 0;
	BtreeMap::BtreeCursor btreeCursor;
	while (1) {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		typedef std::pair<FullContainerKeyAddr, KeyDataStoreValue> KeyValue;
		util::XArray<KeyValue> idList(txn.getDefaultAllocator());
		util::XArray<KeyValue>::iterator itr;
		int32_t getAllStatus =
			keyMap.getAll<FullContainerKeyAddr, KeyDataStoreValue>(
				txn, PARTIAL_RESULT_SIZE, idList, btreeCursor);
		for (itr = idList.begin(); itr != idList.end(); itr++) {
			FullContainerKeyCursor keyCursor(*getObjectManager(),
				allocateStrategy_, itr->first.oId_);
			const FullContainerKey& containerKey = keyCursor.getKey();
			FullContainerKeyComponents keyComponents = containerKey.getComponents(txn.getDefaultAllocator());

			KeyDataStoreValue& value = itr->second;
			const DatabaseId databaseVersionId =
				containerKey.getComponents(txn.getDefaultAllocator()).dbId_;
			containerIdTable_.set(
				value.containerId_, value.oId_, itr->first.oId_, databaseVersionId, value.storeType_, value.attribute_);
			containerListSize++;
		}
		if (getAllStatus == GS_SUCCESS) {
			break;
		}
	}

	GS_TRACE_INFO(KEY_DATA_STORE, GS_TRACE_DS_DS_CONTAINER_ID_TABLE_STATUS,
		"Restore container (pId=" << txn.getPartitionId() << ", count="
		<< containerListSize << ")");
}

/*!
	@brief Returns names of Container to meet a given condition in the partition
*/
/** **
	@brief 指定した条件を満たすコンテナ名のリストを返す
	@param [in] txn TransactionContext
	@param [in] start 取得開始件数
	@param [in] limit 取得上限件数
	@param [in] dbId DatabaseId
	@param [in] condition ContainerCondition
	@param [out] nameList 条件を満たすコンテナ名のリスト
** **/
void KeyDataStore::getContainerNameList(TransactionContext& txn,
	int64_t start, ResultSize limit, const DatabaseId dbId,
	ContainerCondition& condition, util::XArray<FullContainerKey>& nameList) {
	nameList.clear();
	if (start < 0) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_GET_CONTAINER_LIST_FAILED,
			"Illeagal parameter. start < 0");
	}
	try {
		ContainerIdTable::ContainerIdList list(txn.getDefaultAllocator());
		containerIdTable_.getList(0, INT64_MAX, list);
		std::sort(list.begin(), list.end(), containerIdMapAsc());
		const StoreType currentStoreType = condition.getStoreType();
		const int64_t currentDatabaseVersionId = dbId;

		int64_t count = 0;
		nameList.clear();
		for (size_t i = 0; i < list.size() && nameList.size() < limit; i++) {
			const ContainerAttribute attribute = list[i].second.attribute_;
			const StoreType storeType = list[i].second.storeType_;
			bool isStoreMatch = (currentStoreType == UNDEF_STORE || storeType == currentStoreType);
			const int64_t databaseVersionId = list[i].second.databaseVersionId_;
			bool isDbMatch = (currentDatabaseVersionId == UNDEF_DBID || databaseVersionId == currentDatabaseVersionId);
			const util::Set<ContainerAttribute>& conditionAttributes =
				condition.getAttributes();
			bool isAttributeMatch = conditionAttributes.find(attribute) !=
				conditionAttributes.end();
			if (isStoreMatch && isDbMatch && isAttributeMatch) {
				if (count >= start) {
					FullContainerKeyCursor keyCursor(*getObjectManager(), allocateStrategy_, 
						list[i].second.keyOId_);
					util::StackAllocator& alloc = txn.getDefaultAllocator();
					const void* srcBody;
					size_t bodySize = 0;
					keyCursor.getKey().toBinary(srcBody, bodySize);
					void* destBody = alloc.allocate(bodySize);
					memcpy(destBody, srcBody, bodySize);
					nameList.push_back(
						FullContainerKey(alloc,
							KeyConstraint::getNoLimitKeyConstraint(), destBody, bodySize));
				}
				count++;
			}
		}
	}
	catch (std::exception& e) {
		handleSearchError(e, GS_ERROR_DS_DS_GET_CONTAINER_LIST_FAILED);
	}
}

/*!
	@brief Returns number of Container in the partition
*/
/** **
	@brief 指定した条件を満たすコンテナ数を返す
	@param [in] txn TransactionContext
	@param [in] dbId DatabaseId
	@param [in] condition ContainerCondition
	@return コンテナ数
** **/
uint64_t KeyDataStore::getContainerCount(TransactionContext& txn,
	const DatabaseId dbId, ContainerCondition& condition) {
	uint64_t count = 0;
	try {
		ContainerIdTable::ContainerIdList list(txn.getDefaultAllocator());
		containerIdTable_.getList(0, INT64_MAX, list);
		const StoreType currentStoreType = condition.getStoreType();
		const int64_t currentDatabaseVersionId = dbId;

		for (size_t i = 0; i < list.size(); i++) {
			const ContainerAttribute attribute = list[i].second.attribute_;
			const StoreType storeType = list[i].second.storeType_;
			bool isStoreMatch = (currentStoreType == UNDEF_STORE || storeType == currentStoreType);
			const int64_t databaseVersionId = list[i].second.databaseVersionId_;
			bool isDbMatch = (currentDatabaseVersionId == UNDEF_DBID || databaseVersionId == currentDatabaseVersionId);

			const util::Set<ContainerAttribute>& conditionAttributes =
				condition.getAttributes();
			bool isAttributeMatch = conditionAttributes.find(attribute) !=
				conditionAttributes.end();
			if (isStoreMatch && isDbMatch && isAttributeMatch) {
				count++;
			}
		}
	}
	catch (std::exception& e) {
		handleSearchError(e, GS_ERROR_DS_DS_GET_CONTAINER_LIST_FAILED);
	}
	return count;
}

/** **
	@brief 指定した条件を満たすコンテナの情報を返す
	@param [in] txn TransactionContext
	@param [in] start 取得開始件数
	@param [in] limit 取得上限件数
	@param [in] dbId DatabaseId
	@param [in] condition ContainerCondition
	@param [out] storeValueList 条件を満たすコンテナ情報のリスト
	@return 全コンテナのスキャンが完了したかを判別
** **/
bool KeyDataStore::scanContainerList(
	TransactionContext& txn, ContainerId startContainerId,
	uint64_t limit, const DatabaseId dbId, ContainerCondition& condition,
	util::XArray< KeyDataStoreValue* >& storeValueList) {

	util::StackAllocator& alloc = txn.getDefaultAllocator();

	typedef ContainerIdTable::ContainerIdRefList ContainerIdRefList;
	ContainerIdRefList list(alloc);

	const bool followingFound = containerIdTable_.getListOrdered(
		startContainerId, limit, dbId, condition, list);

	for (ContainerIdRefList::iterator itr = list.begin();
		itr != list.end(); ++itr) {
		KeyDataStoreValue *storeValue = ALLOC_NEW(alloc) 
			KeyDataStoreValue(itr->first, itr->second->containerOId_,
			itr->second->storeType_, itr->second->attribute_);
		storeValueList.push_back(storeValue);
	}

	return followingFound;
}

/*!
	@brief Get FullContainerKey
*/
/** **
	@brief コンテナIDに対応するコンテナのコンテナキーを取得
	@param [in] alloc util::StackAllocator
	@param [in] id ContainerId
	@return コンテナキー
** **/
FullContainerKey* KeyDataStore::getKey(util::StackAllocator& alloc, ContainerId id) {
	FullContainerKey* returnKey = NULL;
	OId keyOId = containerIdTable_.getKey(id);
	if (keyOId != UNDEF_OID) {
		FullContainerKeyCursor cursor(*getObjectManager(), allocateStrategy_, keyOId);
		FullContainerKey containerKey = cursor.getKey();
		const void* keyData;
		size_t keySize;
		containerKey.toBinary(keyData, keySize);
		uint8_t * destBody = ALLOC_NEW(alloc) uint8_t[keySize];
		memcpy(destBody, keyData, keySize);
		returnKey = ALLOC_NEW(alloc) FullContainerKey(alloc,
				KeyConstraint::getNoLimitKeyConstraint(), destBody, keySize);
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_DS_CONTAINER_UNEXPECTEDLY_REMOVED, "");
	}
	return returnKey;
}

/** **
	@brief コンテナのOIdを指定してコンテナキーを取得
	@param [in] txn TransactionContext
	@param [in] oId コンテナキーのオブジェクトのID
	@return コンテナキー
** **/
FullContainerKey* KeyDataStore::getKey(TransactionContext& txn, OId oId) {
	util::StackAllocator& alloc = txn.getDefaultAllocator();
	FullContainerKeyCursor cursor(*getObjectManager(), allocateStrategy_, oId);
	FullContainerKey containerKey = cursor.getKey();
	const void* keyData;
	size_t keySize;
	containerKey.toBinary(keyData, keySize);
	uint8_t* destBody = ALLOC_NEW(alloc) uint8_t[keySize];
	memcpy(destBody, keyData, keySize);
	FullContainerKey* returnKey = ALLOC_NEW(alloc) FullContainerKey(alloc,
		KeyConstraint::getNoLimitKeyConstraint(), destBody, keySize);
	return returnKey;
}

/** **
	@brief コンテナキーのオブジェクトを作成
	@param [in] txn TransactionContext
	@param [in] key コンテナキー
	@return コンテナキーのオブジェクトのID
** **/
OId KeyDataStore::allocateKey(TransactionContext& txn, const FullContainerKey &key) {
	DataStorePartitionHeaderObject partitionHeaderObject(*getObjectManager(), allocateStrategy_);
	if (!isActive()) {
		initializeHeader(txn);
	}
	FullContainerKeyCursor keyCursor(*getObjectManager(), allocateStrategy_);
	keyCursor.initialize(txn, key);
	return keyCursor.getBaseOId();
}

/** **
	@brief コンテナキーのオブジェクトを削除
	@param [in] txn TransactionContext
	@param [in] oId コンテナキーのオブジェクトのID
** **/
void KeyDataStore::removeKey(OId oId) {
	assert(oId != UNDEF_OID);
	FullContainerKeyCursor keyCursor(*getObjectManager(), allocateStrategy_, oId);
	keyCursor.finalize();
}

/** **
	@brief 指定したチャンクグループIDで最初に割り当てられたオブジェクトを取得
	@param [in] groupId DSGroupId
	@return オブジェクトID
** **/
OId KeyDataStore::getHeadOId(DSGroupId groupId) {
	ChunkId headChunkId = objectManager_->getHeadChunkId(groupId);
	OId partitionHeaderOId = objectManager_->getOId(groupId, headChunkId, FIRST_OBJECT_OFFSET);
	return partitionHeaderOId;
}

/** **
	@brief 指定したコンテナキーが所属するパーティションIDを計算
	@param [in] alloc util::StackAllocator
	@param [in] containerKey コンテナキー
	@param [in] partitionCount パーティション数
	@param [in] hashMode ハッシュ値の計算方法
	@return コンテナキーが所属するパーティションID
** **/
PartitionId KeyDataStore::resolvePartitionId(
	util::StackAllocator& alloc, const FullContainerKey& containerKey,
	PartitionId partitionCount, ContainerHashMode hashMode) {
	assert(partitionCount > 0);

	const FullContainerKeyComponents normalizedComponents =
		containerKey.getComponents(alloc, false);

	if (normalizedComponents.affinityNumber_ != UNDEF_NODE_AFFINITY_NUMBER) {
		return static_cast<PartitionId>(
			normalizedComponents.affinityNumber_ % partitionCount);
	}
	else if (normalizedComponents.affinityStringSize_ > 0) {
		const uint32_t crcValue = util::CRC32::calculate(
			normalizedComponents.affinityString_,
			normalizedComponents.affinityStringSize_);
		return (crcValue % partitionCount);
	}
	else {
		const char8_t* baseContainerName =
			(normalizedComponents.baseNameSize_ == 0 ?
				"" : normalizedComponents.baseName_);
		const uint32_t crcValue = util::CRC32::calculate(
			baseContainerName,
			normalizedComponents.baseNameSize_);
		return (crcValue % partitionCount);
	}
}

/*!
	@brief Set value(ContainerId, ContainerInfoCache)
*/
/** **
	@brief 指定したコンテナのOIdとDBバージョンと属性をテーブルにセット
	@param [in] containerId ContainerId
	@param [in] containerOId コンテナのOId
	@param [in] keyOId コンテナキーのOId
	@param [in] databaseVersionId コンテナが属するDBのバージョン
	@param [in] storeType データストアタイプ
	@param [in] attribute コンテナの属性
	@note databaseVersionIdはDB管理コンテナにてRowIdに該当するためint64_tである
** **/
void KeyDataStore::ContainerIdTable::set(ContainerId containerId,
	OId containerOId, OId keyOId, int64_t databaseVersionId, StoreType storeType, ContainerAttribute attribute) {
	try {
		std::pair<ContainerIdMap::iterator, bool> itr;
		ContainerInfoCache containerInfoCache(
			containerOId, keyOId, databaseVersionId, storeType, attribute);
		itr = containerIdMap_.insert(
			std::make_pair(
				containerId, containerInfoCache));
		if (!itr.second) {
			GS_THROW_SYSTEM_ERROR(
				GS_ERROR_DS_DS_CONTAINER_ID_INVALID, "duplicate container id");
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Remove value by ContainerId key
*/

/** **
	@brief コンテナIDで指定したコンテナの情報をテーブルから削除
	@param [in] containerId ContainerId
** **/
void KeyDataStore::ContainerIdTable::remove(ContainerId containerId) {
	ContainerIdMap::size_type result = containerIdMap_.erase(containerId);
	if (result == 0) {
		GS_TRACE_WARNING(KEY_DATA_STORE, GS_TRACE_DS_DS_CONTAINER_ID_TABLE_STATUS,
			"KeyDataStore::ContainerIdTable::remove: out of bounds");
	}
}

/*!
	@brief Get list of all ContainerId in the map
*/
/** **
	@brief 指定したパーティションのコンテナのコンテナ情報キャッシュのリストを返す
	@param [in] start 取得開始件数
	@param [in] limit 取得上限件数
	@param [out] list 条件を満たすコンテナIdとコンテナの情報のリスト
	@attention limitの指定値はMAX_INT64までOK。ただし、実際に返す個数が
			   MAX_INT32を越える場合はエラーを返す。(EventEngineの制約)
** **/
void KeyDataStore::ContainerIdTable::getList(
	int64_t start, ResultSize limit, ContainerIdList& list) {
	try {
		list.clear();
		if (static_cast<uint64_t>(start) > size()) {
			return;
		}
		int64_t skipCount = 0;
		ResultSize listCount = 0;
		bool inRange = false;
		ContainerIdMap::const_iterator itr;
		for (itr = containerIdMap_.begin();
			itr != containerIdMap_.end(); itr++) {
			++skipCount;
			if (!inRange && skipCount > start) {
				inRange = true;
			}
			if (inRange) {
				if (listCount >= limit) {
					break;
				}
				if (listCount >
					CONTAINER_NAME_LIST_NUM_UPPER_LIMIT) {
					GS_THROW_USER_ERROR(
						GS_ERROR_DS_DS_GET_CONTAINER_LIST_FAILED,
						"Numbers of containers exceed an upper limit level.");
				}
				list.push_back(*itr);
				++listCount;
			}
		}
		return;
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(e, "Failed to list container"));
	}
}

/** **
	@brief 指定したパーティションのコンテナのコンテナ情報キャッシュのリストを昇順で返す
	@param [in] start 取得開始件数
	@param [in] limit 取得上限件数
	@param [in] dbId DatabaseId
	@param [in] condition ContainerCondition
	@param [out] list 条件を満たすコンテナIdとコンテナの情報のリスト
	@attention limitの指定値はMAX_INT64までOK。ただし、実際に返す個数が
			   MAX_INT32を越える場合はエラーを返す。(EventEngineの制約)
** **/
bool KeyDataStore::ContainerIdTable::getListOrdered(
	ContainerId startId, uint64_t limit,
	const DatabaseId dbId, ContainerCondition& condition,
	ContainerIdRefList& list) const {
	list.clear();
	list.reserve(std::min<uint64_t>(containerIdMap_.size(), limit));

	const util::Set<ContainerAttribute>& attributes = condition.getAttributes();
	containerIdMapAsc pred;

	const StoreType storeType = condition.getStoreType();
	bool followingFound = false;
	for (ContainerIdMap::const_iterator itr = containerIdMap_.begin();
		itr != containerIdMap_.end(); ++itr) {

		const ContainerId id = itr->first;
		bool isSkip = (id < startId) || (storeType != UNDEF_STORE && itr->second.storeType_ != storeType) ||
			(dbId != UNDEF_DBID && itr->second.databaseVersionId_ != dbId) || (attributes.find(itr->second.attribute_) == attributes.end());
		if (isSkip) {
			continue;
		}

		const ContainerIdRefList::value_type entry(itr->first, &itr->second);

		if (list.size() >= limit) {
			followingFound = true;

			if (list.empty()) {
				break;
			}

			std::pop_heap(list.begin(), list.end(), pred);
			if (pred(entry, list.back())) {
				list.back() = entry;
			}
		}
		else {
			list.push_back(entry);
		}

		std::push_heap(list.begin(), list.end(), pred);
	}

	std::sort_heap(list.begin(), list.end(), pred);
	return followingFound;
}

bool KeyDataStore::containerIdMapAsc::operator()(
	const std::pair<ContainerId, ContainerInfoCache>& left,
	const std::pair<ContainerId, ContainerInfoCache>& right) const {
	return left.first < right.first;
}

bool KeyDataStore::containerIdMapAsc::operator()(
	const std::pair<ContainerId, const ContainerInfoCache*>& left,
	const std::pair<ContainerId, const ContainerInfoCache*>& right) const {
	return left.first < right.first;
}


