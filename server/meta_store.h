﻿/*
	Copyright (c) 2018 TOSHIBA Digital Solutions Corporation

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
	@brief Declaration of metadata store
*/
#ifndef META_STORE_H_
#define META_STORE_H_

#include "meta_type.h"
#include "base_container.h"

class TransactionManager;
class PartitionTable;
struct MetaProcessorSource;

class MetaProcessor {
public:
	typedef util::Vector<Value> ValueList;
	typedef MetaProcessorSource Source;

	struct ValueUtils;
	struct ValueListSource;
	template<typename T> class ValueListBuilder;

	class RowHandler;
	class Context;


	MetaProcessor(
			TransactionContext &txn, MetaContainerId id, bool forCore);

	void scan(
			TransactionContext &txn, const Source &source,
			RowHandler &handler);

	bool isSuspended() const;

	ContainerId getNextContainerId() const;
	void setNextContainerId(ContainerId containerId);

	void setContainerLimit(uint64_t limit);
	void setContainerKey(const FullContainerKey *containerKey);

private:
	typedef util::Vector<bool> ValueCheckList;

	class StoreCoreHandler;
	class RefHandler;

	class ContainerHandler;
	class ColumnHandler;
	class IndexHandler;
	class MetaTriggerHandler;
	class ErasableHandler;

	class KeyRefHandler;

	MetaProcessor(const MetaProcessor &another);
	MetaProcessor& operator=(const MetaProcessor &another);

	template<typename HandlerType>
	void scanCore(TransactionContext &txn, Context &cxt);

	void setState(const MetaProcessor &src);

	const MetaContainerInfo &info_;

	ContainerId nextContainerId_;
	uint64_t containerLimit_;
	const FullContainerKey *containerKey_;
};

struct MetaProcessor::ValueUtils {
	static Value makeNull();
	static Value makeString(util::StackAllocator &alloc, const char8_t *src);
	static Value makeBool(bool src);
	static Value makeShort(int16_t src);
	static Value makeInteger(int32_t src);
	static Value makeLong(int64_t src);
	static Value makeDouble(double src);
	static Value makeTimestamp(Timestamp src);

	static void toUpperString(util::String &str);
};

struct MetaProcessor::ValueListSource {
	ValueListSource(
			ValueList &valueList, ValueCheckList &valueCheckList,
			const MetaContainerInfo &info);

	ValueList &valueList_;
	ValueCheckList &valueCheckList_;
	const MetaContainerInfo &info_;
};

template<typename T>
class MetaProcessor::ValueListBuilder {
public:
	ValueListBuilder(const ValueListSource &source);

	const ValueList& build();

	void set(T column, const Value &value);

private:
	ValueListSource source_;
};

class MetaProcessor::RowHandler {
public:
	virtual void operator()(
			TransactionContext &txn, const ValueList &valueList) = 0;
};

class MetaProcessor::Context {
public:
	Context(
			TransactionContext &txn, MetaProcessor &processor,
			const Source &source, RowHandler &coreRowHandler);

	const Source& getSource() const;
	const ValueListSource& getValueListSource() const;
	RowHandler& getRowHandler() const;

	void stepContainerListing(ContainerId lastContainerId);

private:
	Context(const Context &another);
	Context& operator=(const Context &another);

	MetaProcessor &processor_;
	const Source& source_;

	ValueList valueList_;
	ValueCheckList valueCheckList_;
	const MetaContainerInfo &coreInfo_;
	ValueListSource valueListSource_;

	RowHandler &coreRowHandler_;
};


class MetaProcessor::StoreCoreHandler :
		public DataStore::ContainerListHandler {
public:
	explicit StoreCoreHandler(Context &cxt);

	virtual void operator()(
			TransactionContext &txn, ContainerId id, DatabaseId dbId,
			ContainerAttribute attribute, BaseContainer &container) const;

	virtual void execute(
			TransactionContext &txn, ContainerId id, DatabaseId dbId,
			ContainerAttribute attribute, BaseContainer &container,
			BaseContainer *subContainer) const;

protected:
	Context& getContext() const;

	void getNames(
			TransactionContext &txn, BaseContainer &container,
			const char8_t *&dbName, const char8_t *&containerName) const;

	Context &cxt_;
};

class MetaProcessor::RefHandler : public MetaProcessor::RowHandler {
public:
	RefHandler(
			TransactionContext &txn, const MetaContainerInfo &refInfo,
			RowHandler &rowHandler);

	virtual void operator()(
			TransactionContext &txn, const ValueList &valueList);

protected:
	virtual bool filter(TransactionContext &txn, const ValueList &valueList);

	const MetaContainerInfo &refInfo_;
	ValueList destValueList_;
	RowHandler &rowHandler_;
};

class MetaProcessor::ContainerHandler :
		public MetaProcessor::StoreCoreHandler {
public:
	static const int8_t META_EXPIRATION_TYPE_ROW;

	explicit ContainerHandler(Context &cxt);

	virtual void execute(
			TransactionContext &txn, ContainerId id, DatabaseId dbId,
			ContainerAttribute attribute, BaseContainer &container,
			BaseContainer *subContainer) const;

	static const char8_t* containerTypeToName(ContainerType type);
	static const char8_t* timeUnitToName(TimeUnit unit);
	static const char8_t* compressionToName(int8_t type);
	static const char8_t* expirationTypeToName(int8_t type);
};

class MetaProcessor::ColumnHandler :
		public MetaProcessor::StoreCoreHandler {
public:
	explicit ColumnHandler(Context &cxt);

	virtual void execute(
			TransactionContext &txn, ContainerId id, DatabaseId dbId,
			ContainerAttribute attribute, BaseContainer &container,
			BaseContainer *subContainer) const;
};

class MetaProcessor::IndexHandler :
		public MetaProcessor::StoreCoreHandler {
public:
	explicit IndexHandler(Context &cxt);

	virtual void execute(
			TransactionContext &txn, ContainerId id, DatabaseId dbId,
			ContainerAttribute attribute, BaseContainer &container,
			BaseContainer *subContainer) const;
};

class MetaProcessor::MetaTriggerHandler :
		public MetaProcessor::StoreCoreHandler {
public:
	explicit MetaTriggerHandler(Context &cxt);

	virtual void execute(
			TransactionContext &txn, ContainerId id, DatabaseId dbId,
			ContainerAttribute attribute, BaseContainer &container,
			BaseContainer *subContainer) const;
};

class MetaProcessor::ErasableHandler :
		public MetaProcessor::StoreCoreHandler {
public:
	explicit ErasableHandler(Context &cxt);

	virtual void operator()(
			TransactionContext &txn, ContainerId id, DatabaseId dbId,
			ContainerAttribute attribute, BaseContainer &container) const;

	virtual void execute(
			TransactionContext &txn, ContainerId id, DatabaseId dbId,
			ContainerAttribute attribute, BaseContainer &container,
			BaseContainer *subContainer) const;

	void setErasableTimeLimit(Timestamp limit) {
		limit = erasableTimeLimit_;
	}

private:
	static const char8_t* containerTypeToName(ContainerType type);
	static const char8_t* expirationTypeToName(ExpireType type);

	Timestamp erasableTimeLimit_;
};


class MetaProcessor::KeyRefHandler : public MetaProcessor::RefHandler {
public:
	KeyRefHandler(
			TransactionContext &txn, const MetaContainerInfo &refInfo,
			RowHandler &rowHandler);

protected:
	virtual bool filter(TransactionContext &txn, const ValueList &valueList);
};

struct MetaProcessorSource {
	MetaProcessorSource(DatabaseId dbId, const char8_t *dbName);

	DatabaseId dbId_;
	const char8_t *dbName_;

	DataStore *dataStore_;
	EventContext *eventContext_;
	TransactionManager *transactionManager_;
	PartitionTable *partitionTable_;
};

class MetaContainer : public BaseContainer {
public:
	typedef MetaType::NamingType NamingType;

	MetaContainer(
			TransactionContext &txn, DataStore *dataStore, DatabaseId dbId,
			MetaContainerId id, NamingType containerNamingType,
			NamingType columnNamingType);

	MetaContainerId getMetaContainerId() const;
	NamingType getContainerNamingType() const;
	NamingType getColumnNamingType() const;
	FullContainerKey getContainerKey(TransactionContext &txn);

	const MetaContainerInfo& getMetaContainerInfo() const;

	void getContainerInfo(
			TransactionContext &txn,
			util::XArray<uint8_t> &containerSchema, bool optionIncluded = true, bool internalOptionIncluded = true);
	void getIndexInfoList(
			TransactionContext &txn, util::Vector<IndexInfo> &indexInfoList);

	uint32_t getColumnNum() const;
	void getKeyColumnIdList(util::XArray<ColumnId> &keyColumnIdList);
	void getCommonContainerOptionInfo(
			util::XArray<uint8_t> &containerSchema);
	void getColumnSchema(
			TransactionContext &txn, uint32_t columnId,
			ObjectManager &objectManager, util::XArray<uint8_t> &schema);

	const char8_t* getColumnName(
			TransactionContext &txn, uint32_t columnId,
			ObjectManager &objectManager) const;
	ColumnType getSimpleColumnType(uint32_t columnId) const;
	bool isArrayColumn(uint32_t columnId) const;
	bool isVirtualColumn(uint32_t columnId) const;
	bool isNotNullColumn(uint32_t columnId) const;

	void getTriggerList(
			TransactionContext &txn, util::XArray<const uint8_t*> &triggerList);
	ContainerAttribute getAttribute() const;
	void getNullsStats(util::XArray<uint8_t> &nullsList) const;

	void getColumnInfoList(util::XArray<ColumnInfo> &columnInfoList) const;
	void getColumnInfo(
			TransactionContext &txn, ObjectManager &objectManager,
			const char8_t *name, uint32_t &columnId, ColumnInfo *&columnInfo,
			bool isCaseSensitive) const;
	ColumnInfo& getColumnInfo(uint32_t columnId) const;

	virtual void initialize(TransactionContext &txn) {
		static_cast<void>(txn);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual bool finalize(TransactionContext &txn) {
		static_cast<void>(txn);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	virtual void set(TransactionContext &txn, const FullContainerKey &containerKey,
			ContainerId containerId, OId columnSchemaOId,
			MessageSchema *containerSchema) {
		static_cast<void>(txn);
		static_cast<void>(containerKey);
		static_cast<void>(containerId);
		static_cast<void>(columnSchemaOId);
		static_cast<void>(containerSchema);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	virtual void createIndex(TransactionContext &txn, const IndexInfo &indexInfo,
			IndexCursor& indexCursor,
			bool isIndexNameCaseSensitive = false) {
		static_cast<void>(txn);
		static_cast<void>(indexInfo);
		static_cast<void>(indexCursor);
		static_cast<void>(isIndexNameCaseSensitive);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void continueCreateIndex(TransactionContext& txn, 
			IndexCursor& indexCursor) {
		static_cast<void>(txn);
		static_cast<void>(indexCursor);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	virtual void dropIndex(TransactionContext &txn, IndexInfo &indexInfo,
			bool isIndexNameCaseSensitive = false) {
		static_cast<void>(txn);
		static_cast<void>(indexInfo);
		static_cast<void>(isIndexNameCaseSensitive);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	virtual util::String getBibInfo(TransactionContext &txn, const char* dbName) {
		static_cast<void>(txn);
		static_cast<void>(dbName);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void getErasableList(
			TransactionContext &txn, Timestamp erasableTimeLimit,
			util::XArray<ArchiveInfo> &list) {
		static_cast<void>(txn);
		static_cast<void>(erasableTimeLimit);
		static_cast<void>(list);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual ExpireType getExpireType() const {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	virtual void deleteRow(TransactionContext &txn, uint32_t rowSize,
			const uint8_t *rowKey, RowId &rowId, bool &existing) {
		static_cast<void>(txn);
		static_cast<void>(rowSize);
		static_cast<void>(rowKey);
		static_cast<void>(rowId);
		static_cast<void>(existing);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void deleteRow(
			TransactionContext &txn, RowId rowId, bool &existing) {
		static_cast<void>(txn);
		static_cast<void>(rowId);
		static_cast<void>(existing);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void updateRow(TransactionContext &txn, uint32_t rowSize,
			const uint8_t *rowData, RowId rowId, DataStore::PutStatus &status) {
		static_cast<void>(txn);
		static_cast<void>(rowSize);
		static_cast<void>(rowData);
		static_cast<void>(rowId);
		static_cast<void>(status);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void redoDeleteRow(
			TransactionContext &txn, RowId rowId, bool &existing) {
		static_cast<void>(txn);
		static_cast<void>(rowId);
		static_cast<void>(existing);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void abort(TransactionContext &txn) {
		static_cast<void>(txn);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void commit(TransactionContext &txn) {
		static_cast<void>(txn);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual bool hasUncommitedTransaction(TransactionContext &txn) {
		static_cast<void>(txn);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	virtual void searchRowIdIndex(TransactionContext &txn,
			BtreeMap::SearchContext &sc, util::XArray<OId> &resultList,
			OutputOrder order) {
		static_cast<void>(txn);
		static_cast<void>(sc);
		static_cast<void>(resultList);
		static_cast<void>(order);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void searchRowIdIndex(TransactionContext &txn, uint64_t start,
			uint64_t limit, util::XArray<RowId> &rowIdList,
			util::XArray<OId> &resultList, uint64_t &skipped) {
		static_cast<void>(txn);
		static_cast<void>(start);
		static_cast<void>(limit);
		static_cast<void>(rowIdList);
		static_cast<void>(resultList);
		static_cast<void>(skipped);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void lockRowList(
			TransactionContext &txn, util::XArray<RowId> &rowIdList) {
		static_cast<void>(txn);
		static_cast<void>(rowIdList);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual RowId getMaxRowId(TransactionContext &txn) {
		static_cast<void>(txn);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual AllocateStrategy calcMapAllocateStrategy() const {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual AllocateStrategy calcRowAllocateStrategy() const {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual bool checkRunTime(TransactionContext &txn) {
		static_cast<void>(txn);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual uint32_t getRealColumnNum(TransactionContext &txn) {
		static_cast<void>(txn);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual ColumnInfo* getRealColumnInfoList(TransactionContext &txn) {
		static_cast<void>(txn);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual uint32_t getRealRowSize(TransactionContext &txn) {
		static_cast<void>(txn);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual uint32_t getRealRowFixedDataSize(TransactionContext &txn) {
		static_cast<void>(txn);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual util::String getBibContainerOptionInfo(TransactionContext &txn) {
		static_cast<void>(txn);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void getActiveTxnList(
			TransactionContext &txn, util::Set<TransactionId> &txnList) {
		static_cast<void>(txn);
		static_cast<void>(txnList);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

protected:
	virtual void putRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowData, RowId &rowId, bool rowIdSpecified,
		DataStore::PutStatus &status, PutRowOption putRowOption) {
		static_cast<void>(txn);
		static_cast<void>(rowSize);
		static_cast<void>(rowData);
		static_cast<void>(rowId);
		static_cast<void>(rowIdSpecified);
		static_cast<void>(status);
		static_cast<void>(putRowOption);
		static_cast<void>(rowIdSpecified);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void putRow(TransactionContext &txn,
			InputMessageRowStore *inputMessageRowStore, RowId &rowId,
			bool rowIdSpecified,
			DataStore::PutStatus &status, PutRowOption putRowOption) {
		static_cast<void>(txn);
		static_cast<void>(inputMessageRowStore);
		static_cast<void>(rowId);
		static_cast<void>(status);
		static_cast<void>(putRowOption);
		static_cast<void>(rowIdSpecified);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void getIdList(TransactionContext &txn,
			util::XArray<uint8_t> &serializedRowList,
			util::XArray<RowId> &idList) {
		static_cast<void>(txn);
		static_cast<void>(serializedRowList);
		static_cast<void>(idList);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void lockIdList(TransactionContext &txn, util::XArray<OId> &oIdList,
			util::XArray<RowId> &idList) {
		static_cast<void>(txn);
		static_cast<void>(oIdList);
		static_cast<void>(idList);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void getContainerOptionInfo(
			TransactionContext &txn, util::XArray<uint8_t> &containerSchema);
	virtual void checkContainerOption(MessageSchema *messageSchema,
			util::XArray<uint32_t> &copyColumnMap,
			bool &isCompletelySameSchema) {
		static_cast<void>(messageSchema);
		static_cast<void>(copyColumnMap);
		static_cast<void>(isCompletelySameSchema);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual uint32_t calcRowImageSize(uint32_t rowFixedSize) {
		static_cast<void>(rowFixedSize);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual uint32_t calcRowFixedDataSize() {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	virtual void continueChangeSchema(TransactionContext &txn,
			ContainerCursor &containerCursor) {
		static_cast<void>(txn);
		static_cast<void>(containerCursor);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void setDummyMvccImage(TransactionContext &txn) {
		static_cast<void>(txn);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	virtual void checkExclusive(TransactionContext &txn) {
		static_cast<void>(txn);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	virtual bool getIndexData(TransactionContext &txn, ColumnId columnId,
			MapType mapType, bool withUncommitted, IndexData &indexData) const {
		static_cast<void>(txn);
		static_cast<void>(columnId);
		static_cast<void>(mapType);
		static_cast<void>(withUncommitted);
		static_cast<void>(indexData);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void getIndexList(
			TransactionContext &txn, bool withUncommitted,
			util::XArray<IndexData> &list) const {
		static_cast<void>(txn);
		static_cast<void>(withUncommitted);
		static_cast<void>(list);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void createNullIndexData(TransactionContext &txn, 
			IndexData &indexData) {
		static_cast<void>(txn);
		static_cast<void>(indexData);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void finalizeIndex(TransactionContext &txn) {
		static_cast<void>(txn);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	virtual void incrementRowNum() {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void decrementRowNum() {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	virtual void insertRowIdMap(TransactionContext &txn, BtreeMap *map,
			const void *constKey, OId oId) {
		static_cast<void>(txn);
		static_cast<void>(map);
		static_cast<void>(constKey);
		static_cast<void>(oId);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void insertMvccMap(TransactionContext &txn, BtreeMap *map,
			TransactionId tId, MvccRowImage &mvccImage) {
		static_cast<void>(txn);
		static_cast<void>(map);
		static_cast<void>(tId);
		static_cast<void>(mvccImage);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void insertValueMap(TransactionContext &txn, ValueMap &valueMap,
			const void *constKey, OId oId, bool isNull) {
		static_cast<void>(txn);
		static_cast<void>(valueMap);
		static_cast<void>(constKey);
		static_cast<void>(oId);
		static_cast<void>(isNull);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void updateRowIdMap(TransactionContext &txn, BtreeMap *map,
			const void *constKey, OId oldOId, OId newOId) {
		static_cast<void>(txn);
		static_cast<void>(map);
		static_cast<void>(constKey);
		static_cast<void>(oldOId);
		static_cast<void>(newOId);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void updateMvccMap(TransactionContext &txn, BtreeMap *map,
			TransactionId tId, MvccRowImage &oldMvccImage,
			MvccRowImage &newMvccImage) {
		static_cast<void>(txn);
		static_cast<void>(map);
		static_cast<void>(tId);
		static_cast<void>(oldMvccImage);
		static_cast<void>(newMvccImage);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void updateValueMap(TransactionContext &txn, ValueMap &valueMap,
			const void *constKey, OId oldOId, OId newOId, bool isNull) {
		static_cast<void>(txn);
		static_cast<void>(valueMap);
		static_cast<void>(constKey);
		static_cast<void>(oldOId);
		static_cast<void>(newOId);
		static_cast<void>(isNull);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void removeRowIdMap(TransactionContext &txn, BtreeMap *map,
			const void *constKey, OId oId) {
		static_cast<void>(txn);
		static_cast<void>(map);
		static_cast<void>(constKey);
		static_cast<void>(oId);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void removeMvccMap(TransactionContext &txn, BtreeMap *map,
			TransactionId tId, MvccRowImage &mvccImage) {
		static_cast<void>(txn);
		static_cast<void>(map);
		static_cast<void>(tId);
		static_cast<void>(mvccImage);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void removeValueMap(TransactionContext &txn, ValueMap &valueMap,
			const void *constKey, OId oId, bool isNull) {
		static_cast<void>(txn);
		static_cast<void>(valueMap);
		static_cast<void>(constKey);
		static_cast<void>(oId);
		static_cast<void>(isNull);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	virtual void updateIndexData(
			TransactionContext &txn, const IndexData &indexData) {
		static_cast<void>(txn);
		static_cast<void>(indexData);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	const MetaContainerInfo &info_;
	const NamingType containerNamingType_;
	const NamingType columnNamingType_;
	DatabaseId dbId_;
	util::XArray<ColumnInfo> *columnInfoList_;
};

class MetaStore {
public:
	explicit MetaStore(DataStore &dataStore);

	MetaContainer* getContainer(
			TransactionContext &txn, const FullContainerKey &key,
			MetaType::NamingType defaultNamingType);
	MetaContainer* getContainer(
			TransactionContext &txn, DatabaseId dbId, MetaContainerId id,
			MetaType::NamingType containerNamingType,
			MetaType::NamingType columnNamingType);

private:
	DataStore &dataStore_;
};

#endif
