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
	@brief Definition of KeyDataStore
*/
#ifndef KEY_DATA_STORE_H_
#define KEY_DATA_STORE_H_

#include "util/container.h"
#include "util/trace.h"
#include "config_table.h"
#include "data_type.h"
#include "object_manager_v4.h"
#include "container_key.h"
#include "base_object.h"
#include "partition.h"
#include "transaction_context.h"

UTIL_TRACER_DECLARE(KEY_DATA_STORE);

class DataStoreStats;
class KeyDataStoreValue;

/** **
	@brief ContainerKeyとコンテナの位置情報(コンテナID、アドレス、DataStore)を保持する管理用Store
	@note 全てのコンテナはこのStoreを起点に操作される
** **/
class KeyDataStore : public DataStoreBase {
	friend class StoreV5Impl;
public:
	/*!
		@brief Condition of Container search
	*/
	/** **
		@brief コンテナの検索条件
	** **/
	class ContainerCondition {
	public:
		ContainerCondition(util::StackAllocator& alloc) : attributes_(alloc), storeType_() {}
		/*!
			@brief Insert list of ContainerAttribute condition
		*/
		/** **
			@brief コンテナの取得条件として指定する属性を追加します
			@param [in] sourceSet コンテナの属性集合
		** **/
		void setAttributes(const util::Set<ContainerAttribute>& sourceSet) {
			for (util::Set<ContainerAttribute>::const_iterator itr =
				sourceSet.begin();
				itr != sourceSet.end(); itr++) {
				attributes_.insert(*itr);
			}
		}

		/*!
			@brief Insert ContainerAttribute condition
		*/
		/** **
			@brief コンテナの取得条件として指定する属性を追加します
			@param [in] attribute コンテナ
		** **/
		void insertAttribute(ContainerAttribute attribute) {
			attributes_.insert(attribute);
		}

		/*!
			@brief Get list of ContainerAttribute condition
		*/
		/** **
			@brief コンテナ取得条件の属性集合を取得します。
			@return コンテナの属性集合
		** **/
		const util::Set<ContainerAttribute>& getAttributes() {
			return attributes_;
		}

		/** **
			@brief コンテナの取得条件として指定するデータストアタイプを設定します
			@param [in] type ストアタイプ
		** **/
		void setStoreType(StoreType type) {
			storeType_ = type;
		}

		/** **
			@brief コンテナ取得条件のデータストアタイプを取得します。
			@return データストアタイプ
		** **/
		StoreType getStoreType() const {
			return storeType_;
		}
	private:
		util::Set<ContainerAttribute> attributes_;
		StoreType storeType_;
	};
	static const uint32_t MAX_PARTITION_NUM = 10000;

public:
	KeyDataStore(
			util::StackAllocator* stAlloc,
			util::FixedSizeAllocator<util::Mutex>* resultSetPool,
			ConfigTable* configTable, TransactionManager* txnMgr,
			ChunkManager* chunkmanager, LogManager<MutexLocker>* logmanager,
			KeyDataStore* keyStore, const StatsSet &stats);

	~KeyDataStore();
	void initialize(ManagerSet& resourceSet) {
		UNUSED_VARIABLE(resourceSet);
	}
	Serializable* exec(TransactionContext* txn, KeyDataStoreValue* storeValue, Serializable* message);
	void finalize() {};
	void redo(
			util::StackAllocator &alloc, RedoMode mode,
			const util::DateTime &redoStartTime, Timestamp redoStartEmTime,
			Serializable* message) {
		UNUSED_VARIABLE(alloc);
		UNUSED_VARIABLE(mode);
		UNUSED_VARIABLE(redoStartTime);
		UNUSED_VARIABLE(redoStartEmTime);
		UNUSED_VARIABLE(message);
	};
	bool support(Support type);
	void preProcess(TransactionContext* txn, ClusterService* clsService);
	void postProcess(TransactionContext* txn);

	void activate(
		TransactionContext& txn, ClusterService* clusterService);
	StoreType getStoreType() { return KEY_STORE; };

	OId put(TransactionContext& txn, StoreType storeType, DSObjectSize allocateSize);
	OId get(TransactionContext& txn, StoreType storeType);

	KeyDataStoreValue get(util::StackAllocator& alloc, ContainerId id);
	KeyDataStoreValue get(TransactionContext& txn, 
		const FullContainerKey& containerKey, bool isCaseSensitive);
	PutStatus put(TransactionContext& txn, OId keyOId,
		KeyDataStoreValue &newValue);

	bool remove(TransactionContext& txn,
		OId keyOId);

	uint64_t getContainerCount(TransactionContext& txn,
		const DatabaseId dbId, ContainerCondition& condition);
	void getContainerNameList(TransactionContext& txn,
		int64_t start, ResultSize limit, const DatabaseId dbId,
		ContainerCondition& condition, util::XArray<FullContainerKey>& nameList);
	bool scanContainerList(
		TransactionContext& txn, ContainerId startContainerId,
		uint64_t limit, const DatabaseId dbId, ContainerCondition& condition,
		util::XArray< KeyDataStoreValue* >& storeValueList);

	FullContainerKey* getKey(util::StackAllocator& alloc, ContainerId id);
	FullContainerKey* getKey(TransactionContext& txn, OId oId);
	OId allocateKey(TransactionContext& txn, const FullContainerKey &key);
	void removeKey(OId oId);

	ContainerId allocateContainerId();
	DSGroupId allocateGroupId(int32_t num);

	static PartitionId resolvePartitionId(
		util::StackAllocator& alloc, const FullContainerKey& containerKey,
		PartitionId partitionCount, ContainerHashMode hashMode);
private:

	/*!
		@brief DataStore meta data format
	*/
	/** **
		@brief Partition毎のDataStoreヘッダフォーマット
	** **/
	struct DataStorePartitionHeader {
		static const int32_t PADDING_SIZE = 472;
		OId keyMapOId_;
		OId storeMapOId_;
		uint64_t maxContainerId_;
		uint64_t groupIdCounter_;
		uint8_t padding_[PADDING_SIZE];
	};

	/*!
		@brief DataStore meta data
	*/
	/** **
	@brief Partition毎のDataStoreヘッダを管理するオブジェクト
	** **/
	class DataStorePartitionHeaderObject : public BaseObject {
	public:
		DataStorePartitionHeaderObject(
			ObjectManagerV4& objectManager, AllocateStrategy& strategy)
			: BaseObject(objectManager, strategy) {}
		DataStorePartitionHeaderObject(
			ObjectManagerV4& objectManager, AllocateStrategy &strategy, OId oId)
			: BaseObject(objectManager, strategy, oId) {}

		void setKeyMapOId(OId oId) {
			setDirty();
			get()->keyMapOId_ = oId;
		}
		void setStoreMapOId(OId oId) {
			setDirty();
			get()->storeMapOId_ = oId;
		}
		ContainerId allocateContainerId() {
			setDirty();
			ContainerId containerId = get()->maxContainerId_++;
			if (containerId == UNDEF_CONTAINERID) {
				GS_THROW_USER_ERROR(
					GS_ERROR_CM_LIMITS_EXCEEDED, "container num over limit");
			}
			return containerId;
		}
		OId getKeyMapOId() const {
			return get()->keyMapOId_;
		}
		OId getStoreMapOId() const {
			return get()->storeMapOId_;
		}
		OId getMaxContainerId() const {
			return get()->maxContainerId_;
		}
		DSGroupId allocateGroupId(int32_t num) {
			setDirty();
			DSGroupId groupId = get()->groupIdCounter_;
			get()->groupIdCounter_ += num;
			return groupId;
		}
		void initialize(
			TransactionContext& txn, AllocateStrategy& allocateStrategy);
		void finalize(
			TransactionContext& txn, AllocateStrategy& allocateStrategy);
	private:
		DataStorePartitionHeader* get() const {
			return getBaseAddr<DataStorePartitionHeader*>();
		}
	};

	/*!
		@brief Cache of ContainerInfo
	*/
	/** **
		@brief オンメモリに保持するコンテナに関する情報
		@note databaseVersionId_はDB管理コンテナにてRowIdに該当するためint64_tである
	** **/
	struct ContainerInfoCache {
		OId containerOId_;
		OId keyOId_;
		int64_t databaseVersionId_;
		StoreType storeType_;
		ContainerAttribute attribute_;
		ContainerInfoCache(
			OId containerOId, OId keyOId, int64_t databaseVersionId, StoreType storeType, ContainerAttribute attribute)
			: containerOId_(containerOId), keyOId_(keyOId),
			databaseVersionId_(databaseVersionId),
			storeType_(storeType),
			attribute_(attribute) {}
	};

	/*!
		@brief Compare method for ContainerIdTable
	*/
	/** **
		@brief ContainerIdTableの比較関数構造体
	** **/
	struct containerIdMapAsc {
		bool operator()(const std::pair<ContainerId, ContainerInfoCache>& left,
			const std::pair<ContainerId, ContainerInfoCache>& right) const;
		bool operator()(const std::pair<ContainerId, const ContainerInfoCache*>& left,
			const std::pair<ContainerId, const ContainerInfoCache*>& right) const;
	};

#if UTIL_CXX11_SUPPORTED
	typedef std::unordered_map<ContainerId, ContainerInfoCache>
		ContainerIdMap;
#else
	typedef std::map<ContainerId, ContainerInfoCache> ContainerIdMap;
#endif
	/** **
		@brief オンメモリに保持するコンテナIDとそれに紐づけられたコンテナ情報を管理するテーブル
	** **/
	class ContainerIdTable {
	public:
		typedef util::XArray<std::pair<ContainerId, ContainerInfoCache> >
			ContainerIdList;
		typedef util::XArray< std::pair<ContainerId, const ContainerInfoCache*> > ContainerIdRefList;
		ContainerIdTable() {};
		~ContainerIdTable() {};
		void set(ContainerId containerId, OId containerOId, OId keyOId,
			int64_t databaseVersionId, StoreType storeType, ContainerAttribute attribute);
		void remove(ContainerId containerId);
		KeyDataStoreValue get(ContainerId containerId);
		OId getKey(ContainerId containerId);
		/*!
			@brief Returns number of Container in the partition
		*/
		inline uint64_t size() const {  
			return containerIdMap_.size();
		}
		void getList(int64_t start, ResultSize limit,
			ContainerIdList& containerInfoCacheList);
		bool getListOrdered(
			ContainerId startId, uint64_t limit,
			const DatabaseId dbId, ContainerCondition& condition,
			ContainerIdRefList& list) const;
	private:
		ContainerIdMap containerIdMap_;
	};

	static const uint32_t FIRST_OBJECT_OFFSET =
		ObjectManagerV4::CHUNK_HEADER_BLOCK_SIZE * 2 + 4;
	static const ResultSize CONTAINER_NAME_LIST_NUM_UPPER_LIMIT = INT32_MAX;

	OId headerOId_;
	ObjectManagerV4* objectManager_;
	AllocateStrategy allocateStrategy_;
	ContainerIdTable containerIdTable_;
private:
	ObjectManagerV4* getObjectManager() const {
		return objectManager_;
	}
	void initializeHeader(TransactionContext& txn);
	OId getHeadOId(DSGroupId groupId);
	/** **
		@brief パーティションにデータが存在し、activate()を呼び出し済みかの判定
		@return 条件を満たした場合true
	** **/
	bool isActive() const {
		return headerOId_ != UNDEF_OID;
	}

	void restoreContainerIdTable(
		TransactionContext& txn, ClusterService* clusterService);
	void handleUpdateError(std::exception& e, ErrorCode errorCode);
	void handleSearchError(std::exception& e, ErrorCode errorCode);
};

#endif


