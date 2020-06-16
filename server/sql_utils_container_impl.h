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
	@brief Definition of SQL container
*/

#ifndef SQL_UTILS_CONTAINER_IMPL_H_
#define SQL_UTILS_CONTAINER_IMPL_H_

#include "sql_utils_container.h"

#include "transaction_service.h"
#include "query_processor.h"
#include "result_set.h"
#include "meta_store.h"

class BoolExpr;
class Expr;


struct SQLContainerImpl {

	typedef SQLType::Id ExpressionType;
	typedef SQLType::IndexType IndexType;

	typedef util::Vector<TupleColumnType> ColumnTypeList;
	typedef TupleValue::VarContext VarContext;

	typedef ::ColumnType ContainerColumnType;

	typedef BtreeMap::SearchContext BtreeSearchContext;
	typedef HashMap::SearchContext HashSearchContext;

	typedef BaseContainer::RowArray RowArray;
	typedef BaseContainer::RowArrayType RowArrayType;
	typedef RowArray::Column ContainerColumn;

	typedef SQLValues::ValueUtils ValueUtils;

	typedef SQLExprs::SyntaxExpr SyntaxExpr;
	typedef SQLExprs::SyntaxExprRefList SyntaxExprRefList;
	typedef SQLExprs::Expression Expression;

	typedef SQLExprs::IndexSelector IndexSelector;
	typedef SQLOps::ContainerLocation ContainerLocation;

	typedef SQLOps::TupleColumnList TupleColumnList;
	typedef SQLOps::OpContext OpContext;
	typedef SQLOps::Projection Projection;

	typedef SQLContainerUtils::ScanCursor::LatchTarget LatchTarget;

	class CursorImpl;

	struct Constants;

	template<RowArrayType RAType>
	struct ScannerHandler;

	struct SearchResult;
	class CursorScope;
	class MetaScanHandler;
	class TupleUpdateRowIdHandler;

	struct ContainerValueUtils;
	struct TQLTool;
};

class SQLContainerImpl::CursorImpl : public SQLContainerUtils::ScanCursor {
public:
	explicit CursorImpl(const Source &source);
	virtual ~CursorImpl();

	virtual void unlatch() throw();
	virtual void close() throw();
	void destroy() throw();

	virtual bool scanFull(OpContext &cxt, const Projection &proj);
	virtual bool scanIndex(
			OpContext &cxt, const Projection &proj,
			const SQLExprs::IndexConditionList &condList);
	virtual bool scanUpdates(OpContext &cxt, const Projection &proj);
	virtual bool scanMeta(
			OpContext &cxt, const Projection &proj, const Expression *pred);

	virtual void getIndexSpec(
			OpContext &cxt, SQLExprs::IndexSelector &selector);
	virtual bool isIndexLost();

	virtual uint32_t getOutputIndex();
	virtual void setOutputIndex(uint32_t index);

	void startScope(OpContext &cxt);
	void finishScope() throw();
	CursorScope& resolveScope();

	ContainerId getNextContainerId() const;
	void setNextContainerId(ContainerId containerId);

	BaseContainer& resolveContainer();
	BaseContainer* getContainer();

	TransactionContext& resolveTransactionContext();
	ResultSet& resolveResultSet();

	template<BaseContainer::RowArrayType RAType>
	void setRow(
			typename BaseContainer::RowArrayImpl<
					BaseContainer, RAType> &rowArrayImpl);

	template<RowArrayType RAType, typename Ret, TupleColumnType T>
	Ret getValueDirect(
			SQLValues::ValueContext &cxt, const ContainerColumn &column);

	template<RowArrayType RAType>
	void writeValue(
			SQLValues::ValueContext &cxt, TupleList::Writer &writer,
			const TupleList::Column &destColumn,
			const ContainerColumn &srcColumn, bool forId);

	template<TupleColumnType T>
	static void writeValueAs(
			TupleList::Writer &writer, const TupleList::Column &column,
			const typename SQLValues::TypeUtils::Traits<T>::ValueType &value);

	ContainerRowScanner* tryCreateScanner(
			OpContext &cxt, const Projection &proj);

	void clearResultSetPreserving(ResultSet &resultSet);
	void activateResultSetPreserving(ResultSet &resultSet);

	static void checkInputInfo(OpContext &cxt, const BaseContainer &container);

	static TupleColumnType resolveColumnType(
			const ColumnInfo &columnInfo, bool used, bool withStats);
	static ContainerColumn resolveColumn(
			BaseContainer &container, bool forId, uint32_t pos);

	void preparePartialSearch(
			OpContext &cxt, BtreeSearchContext &sc,
			const SQLExprs::IndexConditionList *targetCondList);
	bool finishPartialSearch(OpContext &cxt, BtreeSearchContext &sc);

	static void setUpSearchContext(
			OpContext &cxt, const SQLExprs::IndexConditionList &targetCondList,
			BtreeSearchContext &sc, const BaseContainer &container);

	IndexType acceptIndexCond(
			OpContext &cxt, const SQLExprs::IndexConditionList &targetCondList,
			util::Vector<uint32_t> &indexColumnList);

	void acceptSearchResult(
			OpContext &cxt, const SearchResult &result,
			const SQLExprs::IndexConditionList *targetCondList,
			ContainerRowScanner &scanner);

	void setIndexLost(OpContext &cxt);

	template<typename Container>
	static void scanRow(
			TransactionContext &txn, Container &container,
			const SearchResult &result, ContainerRowScanner &scanner);

	void initializeScanRange(
			TransactionContext &txn, BaseContainer &container);

	static void assignIndexInfo(
			TransactionContext &txn, BaseContainer &container,
			IndexSelector &indexSelector, const TupleColumnList &coumnList);
	static void getIndexInfoList(
			TransactionContext &txn, BaseContainer &container,
			util::Vector<IndexInfo> &indexInfoList);

	static TransactionContext& prepareTransactionContext(OpContext &cxt);

	static FullContainerKey* predicateToContainerKey(
			SQLValues::ValueContext &cxt, TransactionContext &txn,
			DataStore &dataStore, const Expression *pred,
			const ContainerLocation &location, util::String &dbNameStr);

	static const MetaContainerInfo& resolveMetaContainerInfo(ContainerId id);

	static DataStore* getDataStore(OpContext &cxt);
	static EventContext* getEventContext(OpContext &cxt);
	static TransactionManager* getTransactionManager(OpContext &cxt);
	static TransactionService* getTransactionService(OpContext &cxt);
	static PartitionTable* getPartitionTable(OpContext &cxt);
	static ClusterService* getClusterService(OpContext &cxt);
	static SQLExecutionManager* getExecutionManager(OpContext &cxt);
	static SQLService* getSQLService(OpContext &cxt);
	static const ResourceSet* getResourceSet(OpContext &cxt);

private:
	friend class CursorScope;

	typedef BaseContainer::RowArrayImpl<
			BaseContainer, BaseContainer::ROW_ARRAY_GENERAL> GeneralRowArray;
	typedef BaseContainer::RowArrayImpl<
			BaseContainer, BaseContainer::ROW_ARRAY_PLAIN> PlainRowArray;

	template<
			TupleColumnType T,
			bool FixedSize = SQLValues::TypeUtils::Traits<T>::SIZE_FIXED>
	struct DirectValueAccessor;

	CursorImpl(const CursorImpl&);
	CursorImpl& operator=(const CursorImpl&);

	template<RowArrayType RAType, typename T>
	const T& getFixedValue(const ContainerColumn &column);

	template<RowArrayType RAType>
	const void* getFixedValueAddr(const ContainerColumn &column);

	template<RowArrayType RAType>
	const void* getVariableArray(const ContainerColumn &column);

	void initializeRowArray(TransactionContext &txn, BaseContainer &container);

	void destroyRowArray();

	RowArray* getRowArray();

	template<RowArrayType RAType>
	typename BaseContainer::RowArrayImpl<
			BaseContainer, RAType>* getRowArrayImpl(bool loaded);

	template<RowArrayType RAType>
	void setUpScannerHandler(
			OpContext &cxt, const Projection &proj,
			ContainerRowScanner::HandlerSet &handlerSet);

	void destroyUpdateRowIdHandler() throw();

	static RowId getMaxRowId(
			TransactionContext& txn, BaseContainer &container);

	static bool isColumnSchemaNullable(const ColumnInfo &info);

	ContainerLocation location_;
	uint32_t outIndex_;

	bool closed_;
	bool resultSetPreserving_;
	bool containerExpired_;
	bool indexLost_;
	bool resultSetLost_;
	ResultSetId resultSetId_;
	ResultSetHolder resultSetHolder_;

	PartitionId lastPartitionId_;
	DataStore *lastDataStore_;
	BaseContainer *container_;
	ContainerType containerType_;
	SchemaVersionId schemaVersionId_;
	ContainerId nextContainerId_;
	RowId maxRowId_;

	util::LocalUniquePtr<RowArray> rowArray_;
	GeneralRowArray *generalRowArray_;
	PlainRowArray *plainRowArray_;
	uint8_t *row_;
	util::LocalUniquePtr<BaseObject> frontFieldCache_;

	util::AllocUniquePtr<CursorScope> scope_;
	LatchTarget latchTarget_;
	util::AllocUniquePtr<TupleUpdateRowIdHandler> updateRowIdHandler_;
};

template<TupleColumnType T, bool VarSize>
struct SQLContainerImpl::CursorImpl::DirectValueAccessor {
	template<RowArrayType RAType, typename Ret>
	Ret access(
			SQLValues::ValueContext &cxt, CursorImpl &cursor,
			const ContainerColumn &column) const;
};

struct SQLContainerImpl::Constants {
	static const uint16_t SCHEMA_INVALID_COLUMN_ID;
};

template<BaseContainer::RowArrayType RAType>
struct SQLContainerImpl::ScannerHandler {
	static const RowArrayType ROW_ARRAY_TYPE = RAType;

	ScannerHandler(OpContext &cxt, CursorImpl &cursor, const Projection &proj);

	template<BaseContainer::RowArrayType InRAType>
	void operator()(
			TransactionContext &txn,
			typename BaseContainer::RowArrayImpl<
					BaseContainer, InRAType> &rowArrayImpl);

	bool update(TransactionContext &txn);

	OpContext &cxt_;
	CursorImpl &cursor_;
	const Projection &proj_;
};

struct SQLContainerImpl::SearchResult {
	explicit SearchResult(util::StackAllocator &alloc);

	util::XArray<OId> oIdList_;
	util::XArray<OId> mvccOIdList_;
	bool asRowArray_;
};

class SQLContainerImpl::CursorScope {
public:
	CursorScope(OpContext &cxt, CursorImpl &cursor);
	~CursorScope();

	TransactionContext& getTransactionContext();
	ResultSet& getResultSet();

private:
	void initialize(OpContext &cxt);
	void clear(bool force);
	void clearResultSet(bool force);

	static const DataStore::Latch* getStoreLatch(
			TransactionContext &txn, OpContext &cxt,
			util::LocalUniquePtr<DataStore::Latch> &latch);
	static BaseContainer* getContainer(
			TransactionContext &txn, OpContext &cxt,
			const DataStore::Latch *latch, const ContainerLocation &location,
			bool expired, ContainerType type);
	static BaseContainer* checkContainer(
			BaseContainer *container, const ContainerLocation &location,
			SchemaVersionId schemaVersionId);
	static ResultSet* prepareResultSet(
			OpContext &cxt, TransactionContext &txn, BaseContainer *container,
			util::LocalUniquePtr<ResultSetGuard> &rsGuard,
			const CursorImpl &cursor);
	static ResultSetId findResultSetId(ResultSet *resultSet);

	CursorImpl &cursor_;
	TransactionContext &txn_;
	TransactionService *txnSvc_;
	util::LocalUniquePtr<DataStore::Latch> latch_;
	StackAllocAutoPtr<BaseContainer> containerAutoPtr_;
	util::LocalUniquePtr<ResultSetGuard> rsGuard_;
	ResultSet *resultSet_;
};

class SQLContainerImpl::MetaScanHandler : public MetaProcessor::RowHandler {
public:
	MetaScanHandler(
			OpContext &cxt, CursorImpl &cursor, const Projection &proj);

	virtual void operator()(
			TransactionContext &txn,
			const MetaProcessor::ValueList &valueList);

private:
	OpContext &cxt_;
	CursorImpl &cursor_;
	const Projection &proj_;
};

class SQLContainerImpl::TupleUpdateRowIdHandler :
		public ResultSet::UpdateRowIdHandler {
public:
	TupleUpdateRowIdHandler(OpContext &cxt, RowId maxRowId);
	~TupleUpdateRowIdHandler();

	void destroy() throw();

	void close();
	void operator()(RowId rowId, uint64_t id, ResultSet::UpdateOperation op);

	bool getRowIdList(util::XArray<RowId> &list, size_t limit);

	TupleUpdateRowIdHandler* share(util::StackAllocator &alloc);

private:
	enum {
		ID_STORE_COLUMN_COUNT = 1
	};

	typedef TupleList::Column IdStoreColumnList[ID_STORE_COLUMN_COUNT];

	struct Shared {
		Shared(OpContext &cxt, RowId maxRowId);

		size_t refCount_;
		util::LocalUniquePtr<OpContext> cxt_;

		IdStoreColumnList columnList_;
		uint32_t extraStoreIndex_;
		uint32_t rowIdStoreIndex_;

		RowId maxRowId_;
	};

	explicit TupleUpdateRowIdHandler(Shared &shared);

	static uint32_t createRowIdStore(
			util::LocalUniquePtr<OpContext> &cxt, const OpContext::Source &cxtSrc,
			IdStoreColumnList &columnList, uint32_t &extraStoreIndex);

	static void copyRowIdStore(
			OpContext &cxt, uint32_t srcIndex, uint32_t destIndex,
			const IdStoreColumnList &columnList);

	static util::ObjectPool<Shared, util::Mutex> sharedPool_;

	Shared *shared_;
};

struct SQLContainerImpl::ContainerValueUtils {
	static void toContainerValue(
			util::StackAllocator &alloc, const TupleValue &src, Value &dest);
	static bool toTupleValue(
			util::StackAllocator &alloc, const Value &src, TupleValue &dest,
			bool failOnUnsupported);

	static TupleColumnType toTupleColumnType(
			ContainerColumnType src, bool nullable);

	static TupleColumnType toRawTupleColumnType(ContainerColumnType src);

	static bool toContainerColumnType(
			TupleColumnType src, ContainerColumnType &dest, bool failOnError);
};

struct SQLContainerImpl::TQLTool {
	static SyntaxExpr genPredExpr(
			util::StackAllocator &alloc, const Query &query);
	static SyntaxExpr genPredExpr(
			util::StackAllocator &alloc, const BoolExpr *src, bool *unresolved);
	static SyntaxExpr genPredExpr(
			util::StackAllocator &alloc, const Expr *src, bool *unresolved);

	static SyntaxExpr filterPredExpr(
			util::StackAllocator &alloc, const SyntaxExpr &src, bool *unresolved);

	static SyntaxExpr genUnresolvedTopExpr(util::StackAllocator &alloc);
	static SyntaxExpr genUnresolvedLeafExpr(
			util::StackAllocator &alloc, bool *unresolved);

	static SyntaxExpr genConstExpr(
			util::StackAllocator &alloc, const TupleValue &value);

	template<typename T>
	static SyntaxExprRefList* genExprList(
			util::StackAllocator &alloc, const util::XArray<T*> *src,
			bool *unresolved, size_t argCount);
	template<typename T>
	static SyntaxExprRefList* genExprList(
			util::StackAllocator &alloc, const util::XArray<T*> *src,
			bool *unresolved, size_t minArgCount, size_t maxArgCount);
};

#endif
