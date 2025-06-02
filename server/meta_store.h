/*
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
class TransactionService;
class SQLExecutionManager;
class SQLService;

class MetaProcessor {
public:
	typedef util::Vector<Value> ValueList;
	typedef MetaProcessorSource Source;

	struct ValueUtils;
	struct ValueListSource;
	template<typename T> class ValueListBuilder;

	class RowHandler;
	class Context;
	class ScanPosition;
	struct SQLMetaUtils;

	struct RowSuspendedInfo {
		RowSuspendedInfo();

		int64_t rowArrayCount_;
		int64_t columnMismatchCount_;
	};

	struct Stats {
		Stats();

		int64_t scannedRowCount_;
	};

	MetaProcessor(
			TransactionContext &txn, MetaContainerId id, bool forCore);

	void scan(
			TransactionContext &txn, const Source &source,
			RowHandler &handler);

	bool isSuspended() const;

	ScanPosition getNextScanPosition() const;
	void setNextScanPosition(const ScanPosition &pos);

	void setContainerLimit(uint64_t limit);
	void setContainerKey(const FullContainerKey *containerKey);

	void setFullReduced(bool fullReduced);

	Stats getStats() const;

private:
	typedef util::Vector<bool> ValueCheckList;

	class StoreCoreHandler;
	class RefHandler;

	class ContainerHandler;
	class ColumnHandler;
	class IndexHandler;
	class MetaTriggerHandler;
	class ErasableHandler;
	class EventHandler;
	class SocketHandler;
	class ContainerStatsHandler;
	class ClusterPartitionHandler;
	class StatementResHandler;
	class TaskResHandler;

	class PartitionHandler;
	class ViewHandler; 
	class SQLHandler;
	class PartitionStatsHandler;
	class DatabaseStatsHandler;
	class DatabaseHandler;
	class KeyRefHandler;

	class ContainerRefHandler; 
	class ContainerDetailStatsHandler;

	class ReplicationStatsHandler;

	struct ScanPositionData {
		ScanPositionData();

		ContainerId getStartContainerId() const;
		bool isSuspendedAtContainer() const;
		bool isSuspendedAtRow() const;

		void stepContainerListing(ContainerId lastContainerId);

		void suspendAtRow(
				ContainerId lastContainerId, RowId lastRowId,
				const RowSuspendedInfo &info);
		bool resumeAtRow(
				ContainerId lastContainerId, RowId &lastRowId,
				RowSuspendedInfo &info);

		void finishScan();

		ContainerId containerId_;
		RowId rowId_;
		RowSuspendedInfo suspendedInfo_;
		Stats stats_;
	};

	MetaProcessor(const MetaProcessor &another);
	MetaProcessor& operator=(const MetaProcessor &another);

	template<typename HandlerType>
	void scanCore(TransactionContext &txn, Context &cxt);

	void setState(const MetaProcessor &src);

	const MetaContainerInfo &info_;

	ScanPositionData nextScanPos_;
	uint64_t containerLimit_;
	uint32_t limitMillis_;
	const FullContainerKey *containerKey_;
	bool fullReduced_;
};

struct MetaProcessor::ValueUtils {
	static Value makeNull();
	static Value makeString(util::StackAllocator &alloc, const char8_t *src,const uint32_t limit);
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
			const MetaContainerInfo &info, Stats &stats);

	ValueList &valueList_;
	ValueCheckList &valueCheckList_;
	const MetaContainerInfo &info_;
	Stats &stats_;
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

	void suspendAtRow(
			ContainerId lastContainerId, RowId lastRowId,
			const RowSuspendedInfo &info);
	bool resumeAtRow(
			ContainerId lastContainerId, RowId &lastRowId,
			RowSuspendedInfo &info);

	util::Stopwatch& getStopwatch();
	uint32_t getLimitMillis();

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
	util::Stopwatch watch_;
};

struct MetaProcessor::SQLMetaUtils {
public:
	static int32_t toSQLColumnType(ColumnType type);

private:
	enum SQLColumnType {
		TYPE_BIT = -7,
		TYPE_TINYINT = -6,
		TYPE_SMALLINT = 5,
		TYPE_INTEGER = 4,
		TYPE_BIGINT = -5,
		TYPE_FLOAT = 6,
		TYPE_REAL = 7,
		TYPE_DOUBLE = 8,
		TYPE_NUMERIC = 2,
		TYPE_DECIMAL = 3,
		TYPE_CHAR = 1,
		TYPE_VARCHAR = 12,
		TYPE_LONGVARCHAR = -1,
		TYPE_DATE = 91,
		TYPE_TIME = 92,
		TYPE_TIMESTAMP = 93,
		TYPE_BINARY = -2,
		TYPE_VARBINARY = -3,
		TYPE_LONGVARBINARY = -4,
		TYPE_JDBC_NULL = 0,
		TYPE_OTHER = 1111,
		TYPE_JAVA_OBJECT = 2000,
		TYPE_DISTINCT = 2001,
		TYPE_STRUCT = 2002,
		TYPE_ARRAY = 2003,
		TYPE_BLOB = 2004,
		TYPE_CLOB = 2005,
		TYPE_REF = 2006,
		TYPE_DATALINK = 70,
		TYPE_BOOLEAN = 16,
		TYPE_ROWID = -8,
		TYPE_NCHAR = -15,
		TYPE_NVARCHAR = -9,
		TYPE_LONGNVARCHAR = -16,
		TYPE_NCLOB = 2011,
		TYPE_SQLXML = 2009,
		TYPE_REF_CURSOR = 2012,
		TYPE_TIME_WITH_TIMEZONE = 2013,
		TYPE_TIMESTAMP_WITH_TIMEZONE = 2014
	};

	class ColumnTypeTable {
	public:
		typedef std::pair<ColumnType, int32_t> Entry;

		ColumnTypeTable(Entry *entryList, size_t count);
		int32_t get(ColumnType type) const;

	private:
		const Entry *entryList_;
		size_t count_;
	};

	static ColumnTypeTable::Entry COLUMN_TYPE_TABLE_ENTRIES[];
	static const ColumnTypeTable COLUMN_TYPE_TABLE;
};

class MetaProcessor::StoreCoreHandler :
		public DataStoreV4::ContainerListHandler {
public:
	explicit StoreCoreHandler(Context &cxt);

	virtual void operator()(
			TransactionContext &txn, ContainerId id, DatabaseId dbId,
			ContainerAttribute attribute, BaseContainer *container) const;

	virtual void execute(
			TransactionContext &txn, ContainerId id, DatabaseId dbId,
			ContainerAttribute attribute, BaseContainer& container,
			BaseContainer *subContainer) const;

protected:
	Context& getContext() const;
	
	BaseContainer& getBaseContainer(BaseContainer* container) const {
		if (container == NULL) {
			GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Invalid function call(not transaction service)");
		}
		return *container;
	};

	void getNames(
			TransactionContext &txn, BaseContainer &container,
			const char8_t *&dbName, const char8_t *&containerName) const;

	static util::String getBaseContainerName(
			util::StackAllocator &alloc, const FullContainerKey &containerKey);
	static util::String getDataPartitionName(
			util::StackAllocator &alloc, const FullContainerKey &containerKey);
	static util::String getDataPartitionNameDetail(
			util::StackAllocator &alloc, ContainerId largeContainerId,
			NodeAffinityNumber affinityNumber);

	Value makeString(util::StackAllocator &alloc, const char8_t *src) const;

	bool enableAdministratorScan() const;
	template<typename T> static const char* getLimitTime(T time);

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
	static const int8_t META_EXPIRATION_TYPE_PARTITION;

	explicit ContainerHandler(Context &cxt);

	virtual void execute(
			TransactionContext &txn, ContainerId id, DatabaseId dbId,
			ContainerAttribute attribute, BaseContainer &container,
			BaseContainer *subContainer) const;

	static const char8_t* containerTypeToName(ContainerType type);
	static const char8_t* timeUnitToName(TimeUnit unit);
	static const char8_t* partitionTypeToName(uint8_t type);
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
			ContainerAttribute attribute, BaseContainer* container) const;

	virtual void execute(
			TransactionContext &txn, ContainerId id, DatabaseId dbId,
			ContainerAttribute attribute, BaseContainer &container,
			BaseContainer *subContainer) const;

private:
	static const char8_t* containerTypeToName(ContainerType type);
	static const char8_t* expirationTypeToName(ExpireType type);

	Timestamp erasableTimeLimit_;
};

class MetaProcessor::PartitionHandler :
		public MetaProcessor::StoreCoreHandler {
public:
	explicit PartitionHandler(Context &cxt);

	virtual void operator()(
			TransactionContext &txn, ContainerId id, DatabaseId dbId,
			ContainerAttribute attribute, BaseContainer* container) const;
};

class MetaProcessor::ViewHandler :
		public MetaProcessor::StoreCoreHandler { 
public:
	explicit ViewHandler(Context &cxt);

	virtual void operator()(
			TransactionContext &txn, ContainerId id, DatabaseId dbId,
			ContainerAttribute attribute, BaseContainer* container) const;
};

class MetaProcessor::SQLHandler :
		public MetaProcessor::StoreCoreHandler { 
public:
	
	explicit SQLHandler(Context &cxt);

	virtual void operator()(
			TransactionContext &txn, ContainerId id, DatabaseId dbId,
			ContainerAttribute attribute, BaseContainer* container) const;
};

class MetaProcessor::PartitionStatsHandler :
		public MetaProcessor::StoreCoreHandler {
public:
	explicit PartitionStatsHandler(Context &cxt);

	virtual void operator()(
			TransactionContext &txn, ContainerId id, DatabaseId dbId,
			ContainerAttribute attribute, BaseContainer* container) const;
};

class MetaProcessor::DatabaseStatsHandler :
	public MetaProcessor::StoreCoreHandler {
public:
	explicit DatabaseStatsHandler(Context& cxt);

	virtual void operator()(
		TransactionContext& txn, ContainerId id, DatabaseId dbId,
		ContainerAttribute attribute, BaseContainer* container) const;
};

class MetaProcessor::DatabaseHandler :
	public MetaProcessor::StoreCoreHandler {
public:
	explicit DatabaseHandler(Context& cxt);

	virtual void operator()(
		TransactionContext& txn, ContainerId id, DatabaseId dbId,
		ContainerAttribute attribute, BaseContainer* container) const;
};

class MetaProcessor::EventHandler :
		public MetaProcessor::StoreCoreHandler { 
public:
	explicit EventHandler(Context &cxt);

	virtual void operator()(
			TransactionContext &txn, ContainerId id, DatabaseId dbId,
			ContainerAttribute attribute, BaseContainer* container) const;

	void exec(TransactionContext &txn);

};

class MetaProcessor::SocketHandler : public MetaProcessor::StoreCoreHandler {
public:
	explicit SocketHandler(Context &cxt);

	virtual void operator()(
			TransactionContext &txn, ContainerId id, DatabaseId dbId,
			ContainerAttribute attribute, BaseContainer* container) const;

	static bool findApplicationName(
			const NodeDescriptor &nd, util::String &name);

private:
	template<typename T>
	void buildSocketAddress(
			util::StackAllocator &alloc, ValueListBuilder<T> &builder,
			T addressType, T portType,
			const util::SocketAddress &address) const;
};

class MetaProcessor::ContainerStatsHandler :
		public MetaProcessor::StoreCoreHandler { 
public:
	explicit ContainerStatsHandler(Context &cxt);

	virtual void execute(
			TransactionContext &txn, ContainerId id, DatabaseId dbId,
			ContainerAttribute attribute, BaseContainer &container,
			BaseContainer *subContainer) const;
private:
	static const char8_t* containerTypeToName(ContainerType type);
};

class MetaProcessor::ClusterPartitionHandler :
		public MetaProcessor::StoreCoreHandler { 
public:
	explicit ClusterPartitionHandler(Context &cxt);

	virtual void operator()(
			TransactionContext &txn, ContainerId id, DatabaseId dbId,
			ContainerAttribute attribute, BaseContainer* container) const;
private:
	static const char *chunkCategoryList[];
};

class MetaProcessor::StatementResHandler :
		public MetaProcessor::StoreCoreHandler {
public:
	explicit StatementResHandler(Context &cxt);

	virtual void operator()(
			TransactionContext &txn, ContainerId id, DatabaseId dbId,
			ContainerAttribute attribute, BaseContainer* container) const;
};

class MetaProcessor::TaskResHandler : public MetaProcessor::StoreCoreHandler {
public:
	explicit TaskResHandler(Context &cxt);

	virtual void operator()(
			TransactionContext &txn, ContainerId id, DatabaseId dbId,
			ContainerAttribute attribute, BaseContainer* container) const;
};

class MetaProcessor::KeyRefHandler : public MetaProcessor::RefHandler {
public:
	KeyRefHandler(
			TransactionContext &txn, const MetaContainerInfo &refInfo,
			RowHandler &rowHandler);

protected:
	virtual bool filter(TransactionContext &txn, const ValueList &valueList);
};

class MetaProcessor::ContainerRefHandler : public MetaProcessor::RefHandler {
public:
	ContainerRefHandler(
			TransactionContext &txn, const MetaContainerInfo &refInfo,
			RowHandler &rowHandler);

protected:
	virtual bool filter(TransactionContext &txn, const ValueList &valueList);
};

class MetaProcessor::ContainerDetailStatsHandler :
	public MetaProcessor::StoreCoreHandler {
public:
	explicit ContainerDetailStatsHandler(Context& cxt);

	virtual void operator()(
		TransactionContext& txn, ContainerId id, DatabaseId dbId,
		ContainerAttribute attribute, BaseContainer* container) const;
};

class MetaProcessor::ReplicationStatsHandler :
	public MetaProcessor::StoreCoreHandler {
public:
	explicit ReplicationStatsHandler(Context& cxt);

	virtual void operator()(
		TransactionContext& txn, ContainerId id, DatabaseId dbId,
		ContainerAttribute attribute, BaseContainer* container) const;
};

struct MetaProcessor::ScanPosition {
public:
	ScanPosition();
	explicit ScanPosition(const ScanPositionData &data);

	const ScanPositionData& getData() const;

	template<typename In> void decode(In &in);
	template<typename Out> void encode(Out &out) const;

private:
	ScanPositionData data_;
};

struct MetaProcessorSource {
	MetaProcessorSource(DatabaseId dbId, const char8_t *dbName);

	DatabaseId dbId_;
	const char8_t *dbName_;
	bool isAdministrator_;

	DataStoreV4 *dataStore_;
	EventContext *eventContext_;
	TransactionManager *transactionManager_;
	TransactionService *transactionService_;
	PartitionTable *partitionTable_;
	SQLExecutionManager *sqlExecutionManager_;
	SQLService *sqlService_;
};

class MetaContainer : public BaseContainer {
public:
	typedef MetaType::NamingType NamingType;

	MetaContainer(
			TransactionContext &txn, DataStoreV4 *dataStore, DatabaseId dbId,
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

	virtual SchemaFeatureLevel getSchemaFeatureLevel() const;

	uint32_t getColumnNum() const;
	void getKeyColumnIdList(util::Vector<ColumnId> &keyColumnIdList);
	void getCommonContainerOptionInfo(
			util::XArray<uint8_t> &containerSchema);
	void getColumnSchema(
			TransactionContext &txn, uint32_t columnId,
			ObjectManagerV4 &objectManager, util::XArray<uint8_t> &schema);

	const char8_t* getColumnName(
			TransactionContext &txn, uint32_t columnId,
			ObjectManagerV4 &objectManager) const;
	ColumnType getColumnType(uint32_t columnId) const;
	bool isVirtualColumn(uint32_t columnId) const;
	bool isNotNullColumn(uint32_t columnId) const;

	void getTriggerList(
			TransactionContext &txn, util::XArray<const uint8_t*> &triggerList);
	ContainerAttribute getAttribute() const;
	void getNullsStats(util::XArray<uint8_t> &nullsList) const;

	void getColumnInfoList(util::XArray<ColumnInfo> &columnInfoList) const;
	void getColumnInfo(
			TransactionContext &txn, ObjectManagerV4 &objectManager,
			const char8_t *name, uint32_t &columnId, ColumnInfo *&columnInfo,
			bool isCaseSensitive) const;
	ColumnInfo& getColumnInfo(uint32_t columnId) const;

	virtual void initialize(TransactionContext &txn) {
		static_cast<void>(txn);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual bool finalize(TransactionContext &txn, bool isRemoveGroup) {
		static_cast<void>(txn);
		static_cast<void>(isRemoveGroup);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	virtual void set(TransactionContext& txn, const FullContainerKey& containerKey,
		ContainerId containerId, OId columnSchemaOId,
		MessageSchema* containerSchema, DSGroupId groupId) {
		static_cast<void>(txn);
		static_cast<void>(containerKey);
		static_cast<void>(containerId);
		static_cast<void>(columnSchemaOId);
		static_cast<void>(containerSchema);
		static_cast<void>(groupId);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	virtual void createIndex(
			TransactionContext &txn, const IndexInfo &indexInfo,
			IndexCursor& indexCursor,
			bool isIndexNameCaseSensitive = false, 
		    CreateDropIndexMode mode = INDEX_MODE_NOSQL,
			bool *skippedByMode = NULL) {
		static_cast<void>(txn);
		static_cast<void>(indexInfo);
		static_cast<void>(indexCursor);
		static_cast<void>(isIndexNameCaseSensitive);
		static_cast<void>(mode);
		static_cast<void>(skippedByMode);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void continueCreateIndex(TransactionContext& txn,
			IndexCursor& indexCursor) {
		static_cast<void>(txn);
		static_cast<void>(indexCursor);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	virtual void dropIndex(
			TransactionContext &txn, IndexInfo &indexInfo,
			bool isIndexNameCaseSensitive = false,
			CreateDropIndexMode mode = INDEX_MODE_NOSQL,
			bool *skippedByMode = NULL) {
		static_cast<void>(txn);
		static_cast<void>(indexInfo);
		static_cast<void>(isIndexNameCaseSensitive);
		static_cast<void>(mode);
		static_cast<void>(skippedByMode);
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
			const uint8_t *rowData, RowId rowId, PutStatus &status) {
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
	virtual void resetMapAllocateStrategy() const {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual void resetRowAllocateStrategy() const {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual bool checkRunTime(TransactionContext &txn) {
		static_cast<void>(txn);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual ColumnId getRowIdColumnId() {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual ColumnType getRowIdColumnType() {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual util::String getBibContainerOptionInfo(TransactionContext &txn) {
		static_cast<void>(txn);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

protected:
	virtual void putRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowData, RowId &rowId, bool rowIdSpecified,
		PutStatus &status, PutRowOption putRowOption) {
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
	virtual void putRowInternal(TransactionContext &txn,
			InputMessageRowStore *inputMessageRowStore, RowId &rowId,
			bool rowIdSpecified,
			PutStatus &status, PutRowOption putRowOption) {
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

	virtual void setDummyMvccImage(TransactionContext &txn) {
		static_cast<void>(txn);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	virtual void checkExclusive(TransactionContext &txn) {
		static_cast<void>(txn);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual bool getIndexData(
			TransactionContext &txn, const util::Vector<ColumnId> &columnIds,
			MapType mapType, bool withUncommitted, IndexData &indexData,
			bool withPartialMatch = false) const {
		static_cast<void>(txn);
		static_cast<void>(columnIds);
		static_cast<void>(mapType);
		static_cast<void>(withUncommitted);
		static_cast<void>(indexData);
		static_cast<void>(withPartialMatch);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	virtual bool getIndexData(TransactionContext &txn, IndexCursor &indexCursor,
		IndexData &indexData) const {
		static_cast<void>(txn);
		static_cast<void>(indexCursor);
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
	explicit MetaStore(DataStoreV4 &dataStore);

	MetaContainer* getContainer(
			TransactionContext &txn, const FullContainerKey &key,
			MetaType::NamingType defaultNamingType);
	MetaContainer* getContainer(
			TransactionContext &txn, DatabaseId dbId, MetaContainerId id,
			MetaType::NamingType containerNamingType,
			MetaType::NamingType columnNamingType);

private:
	DataStoreV4 &dataStore_;
};


template<typename In>
void MetaProcessor::ScanPosition::decode(In &in) {
	in >> data_.containerId_;
	in >> data_.rowId_;
	in >> data_.suspendedInfo_.rowArrayCount_;
	in >> data_.suspendedInfo_.columnMismatchCount_;
	in >> data_.stats_.scannedRowCount_;
}

template<typename Out>
void MetaProcessor::ScanPosition::encode(Out &out) const {
	out << data_.containerId_;
	out << data_.rowId_;
	out << data_.suspendedInfo_.rowArrayCount_;
	out << data_.suspendedInfo_.columnMismatchCount_;
	out << data_.stats_.scannedRowCount_;
}

#endif
