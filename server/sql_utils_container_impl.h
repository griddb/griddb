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
#include "btree_map_ext.h"

#include "sql_expression_core.h"
#include "sql_operator_utils.h" 

class BoolExpr;
class Expr;


struct SQLContainerImpl {
	typedef SQLType::Id ExpressionType;
	typedef SQLType::IndexType IndexType;

	typedef TupleValue::VarContext VarContext;

	typedef ::ColumnType ContainerColumnType;

	typedef BaseIndex::SearchContextPool SearchContextPool;
	typedef BtreeMap::SearchContext BtreeSearchContext;
	typedef BtreeMap::TermConditionUpdator BtreeConditionUpdator;

	typedef BaseContainer::RowArray RowArray;
	typedef BaseContainer::RowArrayType RowArrayType;
	typedef RowArray::Column ContainerColumn;

	typedef SQLValues::TupleColumnList TupleColumnList;
	typedef SQLValues::ValueUtils ValueUtils;

	typedef SQLExprs::SyntaxExpr SyntaxExpr;
	typedef SQLExprs::SyntaxExprRefList SyntaxExprRefList;
	typedef SQLExprs::Expression Expression;

	typedef SQLExprs::IndexSelector IndexSelector;

	typedef SQLOps::TupleListReader TupleListReader;
	typedef SQLOps::TupleListWriter TupleListWriter;
	typedef SQLOps::WritableTuple WritableTuple;

	typedef SQLOps::ContainerLocation ContainerLocation;

	typedef SQLOps::OpStore OpStore;
	typedef SQLOps::OpContext OpContext;
	typedef SQLOps::Projection Projection;
	typedef SQLOps::OpSimulator OpSimulator;

	typedef SQLOps::ExtOpContext ExtOpContext;
	typedef SQLOps::OpProfilerIndexEntry IndexProfiler;

	typedef SQLContainerUtils::IndexScanInfo IndexScanInfo;

	typedef int64_t OIdTableGroupId;
	typedef util::Vector<ColumnId> ColumnIdList;
	typedef util::Vector<IndexInfo> IndexInfoList;

	class CursorImpl;
	class CursorAccessorImpl;

	struct Constants;

	struct ColumnCode;
	template<RowArrayType RAType, typename T, bool Nullable>
	class ColumnEvaluator;

	class ColumnExpression;
	class IdExpression;
	class ComparisonExpressionBase;
	template<typename Func> class ComparisonExpression;

	class GeneralCondEvaluator;
	class VarCondEvaluator;
	template<typename Func, size_t V> class ComparisonEvaluator;
	class EmptyCondEvaluator;
	template<typename Base, RowArrayType RAType>
	class RowIdCondEvaluator;

	class GeneralProjector;
	class CountProjector;

	class CursorRef;

	class ScannerHandlerBase;
	class ScannerHandlerFactory;

	class BaseRegistrar;
	class DefaultRegistrar;

	class SearchResult;
	class IndexConditionUpdator;
	class ExtendedSuspendLimitter;
	class IndexSearchContext;

	class CursorScope;
	class LocalCursorScope;
	class ContainerNameFormatter;

	class MetaScanHandler;
	class TupleUpdateRowIdHandler;

	struct BitUtils;
	class OIdTable;
	class RowIdFilter;

	struct ScanSimulatorUtils;
	struct ContainerValueUtils;
	struct TQLTool;

	typedef void (*UpdatorFunc)(OpContext&, void*);
	typedef std::pair<UpdatorFunc, void*> UpdatorEntry;
};

class SQLContainerImpl::CursorImpl : public SQLContainerUtils::ScanCursor {
public:
	class IndexConditionCursor;

	struct Data;
	class HolderImpl;

	typedef CursorAccessorImpl Accessor;

	CursorImpl(Accessor &accessor, Data &data);
	virtual ~CursorImpl();

	virtual bool scanFull(OpContext &cxt, const Projection &proj);
	virtual bool scanRange(OpContext &cxt, const Projection &proj);
	virtual bool scanIndex(OpContext &cxt, const IndexScanInfo &info);
	virtual bool scanMeta(
			OpContext &cxt, const Projection &proj, const Expression *pred);
	virtual bool scanVisited(OpContext &cxt, const Projection &proj);

	virtual uint32_t getOutputIndex();
	virtual void setOutputIndex(uint32_t index);

	virtual SQLContainerUtils::ScanCursorAccessor& getAccessor();

	void addScannerUpdator(const UpdatorEntry &updator);

	void addContextRef(OpContext **ref);
	void addContextRef(SQLExprs::ExprContext **ref);

	static void setUpConditionColumnTypeList(
			const BaseContainer &container,
			SQLExprs::IndexConditionList::const_iterator condBegin,
			SQLExprs::IndexConditionList::const_iterator condEnd,
			util::Vector<ContainerColumnType> &condTypeList);

	static void setUpSearchContext(
			TransactionContext &txn,
			SQLExprs::IndexConditionList::const_iterator condBegin,
			SQLExprs::IndexConditionList::const_iterator condEnd,
			const util::Vector<ContainerColumnType> &condTypeList,
			BtreeSearchContext &sc);

	static TermCondition makeTermCondition(
			ColumnType columnType, ColumnType valueType,
			DSExpression::Operation opType, uint32_t columnId,
			const void *value, uint32_t valueSize);

private:
	enum RowsTargetType {
		TYPE_NO_ROWS,
		TYPE_COLLECTION_ROWS,
		TYPE_TIME_SERIES_ROWS
	};

	enum IndexTargetType {
		TYPE_NO_INDEX,
		TYPE_TIME_SERIES_KEY_TREE,
		TYPE_TIME_SERIES_VALUE_TREE,
		TYPE_DEFAULT_TREE
	};

	typedef util::AllocVector<UpdatorEntry> UpdatorList;

	typedef util::AllocVector<OpContext**> ContextRefList;
	typedef util::AllocVector<SQLExprs::ExprContext**> ExprContextRefList;

	typedef util::Map<uint64_t, OIdTableGroupId> IndexGroupIdMap;
	typedef util::Map<uint64_t, uint32_t> IndexOrdinalMap;

	RowsTargetType prepareFullScan(
			OpContext &cxt, BtreeSearchContext *&sc, const Projection &proj,
			ContainerRowScanner *&scanner);
	void finishFullScan(OpContext &cxt, BtreeSearchContext *sc, bool &done);

	RowsTargetType prepareRangeScan(
			OpContext &cxt, const Projection &proj,
			ContainerRowScanner *&scanner, SearchResult *&result);
	void finishRangeScan(OpContext &cxt, bool &done);

	IndexTargetType prepareIndexSearch(
			OpContext &cxt, IndexSearchContext *&sc,
			const IndexScanInfo &info, SearchResult *&result);
	bool finishIndexSearch(
			OpContext &cxt, IndexSearchContext *sc, bool &done);

	ContainerId getNextContainerId() const;
	void setNextContainerId(ContainerId containerId);

	ContainerRowScanner* tryCreateScanner(
			OpContext &cxt, const Projection &proj,
			const BaseContainer &container, RowIdFilter *rowIdFilter);
	void updateScanner(OpContext &cxt, bool finished);
	void clearScanner();

	void preparePartialSearch(
			OpContext &cxt, BtreeSearchContext &sc,
			BtreeConditionUpdator *updator,
			const ColumnIdList &indexColumnList,
			const SQLExprs::IndexConditionList *targetCondList,
			util::Vector<ContainerColumnType> *condTypeList, size_t condPos);
	bool finishPartialSearch(
			OpContext &cxt, BtreeSearchContext &sc, bool forIndex);
	void updatePartialExecutionSize(OpContext &cxt, bool done);

	static bool checkUpdatesStart(
			TupleUpdateRowIdHandler &handler, OIdTable &oIdTable);
	RowIdFilter& restartRangeScanWithUpdates(
			OpContext &cxt, TransactionContext &txn, const Projection &proj,
			BaseContainer &container, OIdTable &oIdTable);
	bool detectRangeScanFinish(
			BaseContainer *container, TupleUpdateRowIdHandler *handler,
			OIdTable *oIdTable);
	static void applyRangeScanResultProfile(
			OpContext &cxt, OIdTable *oIdTable);

	IndexTargetType acceptIndexCondition(
			OpContext &cxt, BaseContainer *container, IndexInfoList &infoList,
			const SQLExprs::IndexConditionList &targetCondList, size_t pos,
			ColumnIdList &indexColumnList);
	IndexTargetType detectIndexTargetType(
			IndexType indexType, BaseContainer &container,
			const SQLExprs::IndexConditionList &condList, size_t pos);

	void acceptIndexInfo(
			OpContext &cxt, TransactionContext &txn, BaseContainer &container,
			const SQLExprs::IndexConditionList &targetCondList,
			const IndexInfo &info, size_t pos, size_t condCount);
	static void applyIndexResultProfile(
			IndexSearchContext *sc, IndexProfiler &profiler);

	OIdTableGroupId getIndexGroupId(size_t pos);
	uint32_t getIndexOrdinal(size_t pos);

	IndexSearchContext& prepareIndexSearchContext(
			OpContext &cxt, const IndexScanInfo *info);

	IndexConditionCursor& prepareIndexConditionCursor(
			const SQLExprs::IndexConditionList &condList, bool &initialized);
	BtreeConditionUpdator* prepareIndexConditionUpdator(
			OpContext &cxt, IndexSearchContext &sc, BaseContainer &container,
			const IndexScanInfo &info,
			const SQLExprs::IndexConditionList &condList, size_t pos,
			size_t subPos, size_t &nextPos);
	SQLValues::ValueSetHolder& prepareValueSetHolder(OpContext &cxt);

	void setRowDuplicatable(OpContext &cxt);
	void setIndexLost(OpContext &cxt);
	void setIndexAbortion(OpContext &cxt, bool duplicatableOrLost);

	template<typename Container>
	static void scanRow(
			TransactionContext &txn, Container &container,
			const SearchResult &result, ContainerRowScanner &scanner);

	MetaProcessor* prepareMetaProcessor(
			OpContext &cxt, TransactionContext &txn, ContainerId nextContainerId,
			const Expression *pred, MetaProcessorSource *&source);
	MetaProcessorSource* prepareMetaProcessorSource(
			OpContext &cxt, TransactionContext &txn, const Expression *pred,
			const FullContainerKey *&containerKey);

	static FullContainerKey* predicateToContainerKey(
			SQLValues::ValueContext &cxt, TransactionContext &txn,
			DataStoreV4 &dataStore, const Expression *pred,
			const ContainerLocation &location, util::String &dbNameStr);

	static const MetaContainerInfo& resolveMetaContainerInfo(ContainerId id);

	void updateAllContextRef(OpContext &cxt);
	void clearAllContextRef();

	static OutputOrder getOrder();
	static RowsTargetType getRowsTargetType(BaseContainer &container);

	Collection& resolveCollection();
	TimeSeries& resolveTimeSeries();

	BaseContainer& resolveContainer();

	TransactionContext& resolveTransactionContext();

	RowIdFilter* checkRowIdFilter(
			TransactionContext& txn, BaseContainer &container, bool readOnly);
	void finishRowIdFilter(RowIdFilter *rowIdFilter);

	void setUpOIdTable(
			OpContext &cxt, OIdTable &oIdTable,
			const SQLExprs::IndexConditionList &condList);

	Accessor &accessor_;
	Data &data_;
};

class SQLContainerImpl::CursorImpl::IndexConditionCursor {
public:
	explicit IndexConditionCursor(const SQLExprs::IndexConditionList &condList);

	bool exists();
	void next();
	void close();

	void setNextPosition(size_t pos);
	size_t getCurrentPosition();

	void setCurrentSubPosition(size_t subPos);
	size_t getCurrentSubPosition();

private:
	IndexConditionCursor(const IndexConditionCursor&);
	IndexConditionCursor& operator=(const IndexConditionCursor&);

	size_t currentPos_;
	size_t currentSubPos_;

	size_t nextPos_;
	size_t condCount_;
};

struct SQLContainerImpl::CursorImpl::Data {
	Data(SQLValues::VarAllocator &varAlloc, Accessor &accessor);

	uint32_t outIndex_;
	ContainerId nextContainerId_;

	int64_t totalSearchCount_;
	uint64_t lastPartialExecSize_;

	util::LocalUniquePtr<IndexConditionCursor> condCursor_;

	ContainerRowScanner *scanner_;
	util::LocalUniquePtr<UpdatorList> updatorList_;
	util::AllocUniquePtr<RowIdFilter> rowIdFilter_;

	ContextRefList cxtRefList_;
	ExprContextRefList exprCxtRefList_;

	const IndexGroupIdMap *indexGroupIdMap_;
	const IndexOrdinalMap *indexOrdinalMap_;
	IndexProfiler *lastIndexProfiler_;

	util::LocalUniquePtr<SQLValues::ValueContext> valuesHolderCxt_;
	util::LocalUniquePtr<SQLValues::ValueSetHolder> valuesHolder_;
};

class SQLContainerImpl::CursorImpl::HolderImpl :
		public SQLContainerUtils::ScanCursor::Holder {
public:
	HolderImpl(
			SQLValues::VarAllocator &varAlloc, Accessor &accessor,
			const OpStore::ResourceRef &accessorRef);
	virtual ~HolderImpl();

	virtual util::AllocUniquePtr<ScanCursor>::ReturnType attach();

private:
	Accessor& resolveAccessor();

	SQLValues::VarAllocator &varAlloc_;
	OpStore::ResourceRef accessorRef_;
	void *accessorPtr_;
	Data data_;
};

class SQLContainerImpl::CursorAccessorImpl :
		public SQLContainerUtils::ScanCursorAccessor {
public:
	explicit CursorAccessorImpl(const Source &source);
	virtual ~CursorAccessorImpl();

	virtual util::AllocUniquePtr<
			SQLContainerUtils::ScanCursor::Holder>::ReturnType createCursor(
			const OpStore::ResourceRef &accessorRef);

	virtual void unlatch() throw();
	virtual void close() throw();
	void destroy(bool withResultSet) throw();

	virtual void getIndexSpec(
			ExtOpContext &cxt, const TupleColumnList &inColumnList,
			SQLExprs::IndexSelector &selector);
	virtual bool isIndexLost();
	virtual bool isRowDuplicatable();
	virtual bool isRangeScanFinished();

	virtual void setIndexSelection(const SQLExprs::IndexSelector &selector);
	virtual const SQLExprs::IndexSelector& getIndexSelection();

	virtual void setRowIdFiltering();
	virtual bool isRowIdFiltering();

	const ContainerLocation& getLocation();
	int64_t getIndexLimit();
	int64_t getMemoryLimit();
	const std::pair<uint64_t, uint64_t>& getPartialExecSizeRange();
	bool isPartialExecCountBased();

	void startScope(ExtOpContext &cxt, const TupleColumnList &inColumnList);
	void finishScope() throw();
	CursorScope& resolveScope();

	void handleSearchError(std::exception &e);
	static void handleSearchError(
			TransactionContext *txn, BaseContainer *container,
			std::exception &e);

	BaseContainer& resolveContainer();
	BaseContainer* getContainer();

	TransactionContext& resolveTransactionContext();
	ResultSet& resolveResultSet();

	RowArray* getRowArray();

	bool isValueNull(const ContainerColumn &column);

	template<RowArrayType RAType, typename Ret, TupleColumnType T>
	Ret getValueDirect(
			SQLValues::ValueContext &cxt, const ContainerColumn &column);

	template<TupleColumnType T>
	static void writeValueAs(
			TupleListWriter &writer, const TupleList::Column &column,
			const typename SQLValues::TypeUtils::Traits<T>::ValueType &value);

	void clearResultSetPreserving(ResultSet &resultSet, bool force);
	void activateResultSetPreserving(ResultSet &resultSet);

	static void checkContainer(
			TransactionContext &txn, BaseContainer *container,
			const ContainerLocation &location, SchemaVersionId schemaVersionId,
			const TupleColumnList &inColumnList);
	static void checkContainerValidity(
			TransactionContext &txn, BaseContainer &container,
			const ContainerLocation &location);
	static void checkInputInfo(
			const BaseContainer &container, const TupleColumnList &list);
	static void checkPartitioningVersion(
			TransactionContext &txn, BaseContainer &container,
			const ContainerLocation &location);
	static void checkContainerSize(
			TransactionContext &txn, BaseContainer &container,
			const ContainerLocation &location);

	static TupleColumnType resolveColumnType(
			const ColumnInfo &columnInfo, bool used, bool withStats);
	static ContainerColumn resolveColumn(
			BaseContainer &container, bool forId, uint32_t pos);
	bool isColumnNonNullable(uint32_t pos);
	bool updateNullsStats(
			util::StackAllocator &alloc, const BaseContainer &container);

	TupleUpdateRowIdHandler* findUpdateRowIdHandler();
	TupleUpdateRowIdHandler* prepareUpdateRowIdHandler(
			TransactionContext &txn, BaseContainer &container,
			ResultSet &resultSet);

	void acceptIndexAbortion(bool duplicatableOrLost);
	void setRangeScanFinished();

	void initializeScanRange(
			TransactionContext &txn, BaseContainer &container);
	RowId getMaxRowId(
			TransactionContext& txn, BaseContainer &container);

	static void assignIndexInfo(
			TransactionContext &txn, BaseContainer &container,
			IndexSelector &indexSelector, const TupleColumnList &columnList);
	static void getIndexInfoList(
			TransactionContext &txn, BaseContainer &container,
			IndexInfoList &indexInfoList);

	OIdTable& resolveOIdTable();
	OIdTable& prepareOIdTable(BaseContainer &container);
	OIdTable* findOIdTable();

	RowIdFilter& prepareRowIdFilter(
			TransactionContext &txn, BaseContainer &container);
	RowIdFilter* findRowIdFilter();
	void clearRowIdFilter();

	static TransactionContext& prepareTransactionContext(ExtOpContext &cxt);

	static DataStoreV4* getDataStore(ExtOpContext &cxt);
	static EventContext* getEventContext(ExtOpContext &cxt);
	static TransactionManager* getTransactionManager(ExtOpContext &cxt);
	static TransactionService* getTransactionService(ExtOpContext &cxt);
	static PartitionTable* getPartitionTable(ExtOpContext &cxt);
	static ClusterService* getClusterService(ExtOpContext &cxt);
	static SQLExecutionManager* getExecutionManager(ExtOpContext &cxt);
	static SQLService* getSQLService(ExtOpContext &cxt);
	static const ResourceSet* getResourceSet(ExtOpContext &cxt);

	static DataStoreV4& resolveDataStore(BaseContainer &container);
	static DataStoreV4* resolveDataStoreOnTransaction(ExtOpContext &cxt);
	static ObjectManagerV4& resolveObjectManager(BaseContainer &container);

private:
	friend class CursorScope;

	typedef BaseContainer::RowArrayImpl<
			BaseContainer, BaseContainer::ROW_ARRAY_GENERAL> GeneralRowArray;
	typedef BaseContainer::RowArrayImpl<
			BaseContainer, BaseContainer::ROW_ARRAY_PLAIN> PlainRowArray;

	typedef util::AllocXArray<uint8_t> NullsStats;

	template<
			TupleColumnType T,
			bool VarSize = SQLValues::TypeUtils::Traits<T>::SIZE_VAR>
	struct DirectValueAccessor;

	template<RowArrayType RAType, typename T>
	const T& getFixedValue(const ContainerColumn &column);

	template<RowArrayType RAType>
	const void* getFixedValueAddr(const ContainerColumn &column);

	template<RowArrayType RAType>
	const void* getVariableArray(const ContainerColumn &column);

	void initializeRowArray(TransactionContext &txn, BaseContainer &container);

	void destroyRowArray();

	template<RowArrayType RAType>
	typename BaseContainer::RowArrayImpl<
			BaseContainer, RAType>* getRowArrayImpl(bool loaded);

	void destroyUpdateRowIdHandler() throw();

	static RowId resolveMaxRowId(
			TransactionContext& txn, BaseContainer &container);

	static bool isColumnSchemaNullable(const ColumnInfo &info);

	static void getNullsStats(
			util::StackAllocator &alloc, const BaseContainer &container,
			NullsStats &nullsStats);

	static void initializeBits(NullsStats &bits, size_t bitCount);
	static void setBit(NullsStats &bits, size_t index, bool value);
	static bool getBit(const NullsStats &bits, size_t index);

	SQLValues::VarAllocator &varAlloc_;
	ContainerLocation location_;
	OpContext::Source cxtSrc_;

	bool closed_;
	bool rowIdFiltering_;
	bool resultSetPreserving_;
	bool containerExpired_;
	bool indexLost_;
	bool resultSetLost_;
	bool nullsStatsChanged_;
	bool rowDuplicatable_;
	bool rangeScanFinished_;

	ResultSetId resultSetId_;
	ResultSetHolder resultSetHolder_;

	PartitionId lastPartitionId_;
	ResultSetManager *lastRSManager_;
	BaseContainer *container_;
	ContainerType containerType_;
	SchemaVersionId schemaVersionId_;
	RowId maxRowId_;

	util::LocalUniquePtr<RowArray> rowArray_;
	GeneralRowArray *generalRowArray_;
	PlainRowArray *plainRowArray_;
	uint8_t *row_;
	util::LocalUniquePtr<BaseObject> frontFieldCache_;

	util::AllocUniquePtr<CursorScope> scope_;
	util::AllocUniquePtr<OIdTable> oIdTable_;
	util::AllocUniquePtr<RowIdFilter> rowIdFilter_;
	util::AllocUniquePtr<TupleUpdateRowIdHandler> updateRowIdHandler_;

	NullsStats initialNullsStats_;
	NullsStats lastNullsStats_;
	int64_t indexLimit_;
	int64_t memLimit_;
	std::pair<uint64_t, uint64_t> partialExecSizeRange_;
	bool partialExecCountBased_;

	const SQLExprs::IndexSelector *indexSelection_;
};

template<TupleColumnType T, bool VarSize>
struct SQLContainerImpl::CursorAccessorImpl::DirectValueAccessor {
	template<RowArrayType RAType, typename Ret>
	Ret access(
			SQLValues::ValueContext &cxt, CursorAccessorImpl &base,
			const ContainerColumn &column) const;
};

struct SQLContainerImpl::Constants {
	static const uint16_t SCHEMA_INVALID_COLUMN_ID;
	static const uint32_t ESTIMATED_ROW_ARRAY_ROW_COUNT;
};

struct SQLContainerImpl::ColumnCode {
	typedef SQLExprs::ExprFactoryContext ExprFactoryContext;

	ColumnCode();

	void initialize(
			ExprFactoryContext &cxt, const Expression *expr,
			const size_t *argIndex);

	CursorAccessorImpl *accessor_;
	ContainerColumn column_;
};

template<RowArrayType RAType, typename T, bool Nullable>
class SQLContainerImpl::ColumnEvaluator {
public:
	typedef T ValueTypeTag;

private:
	typedef SQLValues::ValueUtils ValueUtils;
	typedef SQLExprs::ExprContext ExprContext;
	typedef typename ValueTypeTag::ValueType ValueType;

public:
	typedef ColumnEvaluator EvaluatorType;
	typedef ColumnCode CodeType;
	typedef std::pair<ValueType, bool> ResultType;

	static ColumnEvaluator evaluator(const CodeType &code) {
		return ColumnEvaluator(code);
	}

	static bool check(const ResultType &value) {
		return !Nullable || !value.second;
	}

	static const ValueType& get(const ResultType &value) {
		return value.first;
	}

	ResultType eval(ExprContext &cxt) const;

private:
	explicit ColumnEvaluator(const CodeType &code) : code_(code) {
	}

	const CodeType &code_;
};

class SQLContainerImpl::ColumnExpression {
private:
	typedef SQLExprs::ExprCode ExprCode;
	typedef SQLExprs::ExprFactoryContext ExprFactoryContext;

	typedef SQLCoreExprs::ColumnExpression CoreExpr;
	typedef SQLCoreExprs::VariantTypes VariantTypes;

	enum {
		COUNT_FULL_TYPE = CoreExpr::COUNT_FULL_TYPE,
		COUNT_NULLABLE = CoreExpr::COUNT_NULLABLE,
		COUNT_CONTAINER_SOURCE = CoreExpr::COUNT_CONTAINER_SOURCE,

		OFFSET_CONTAINER = CoreExpr::OFFSET_CONTAINER,
		OFFSET_END = CoreExpr::OFFSET_END
	};

public:
	template<size_t I>
	struct RowArrayTypeOf {
		static const BaseContainer::RowArrayType RA_TYPE = (I == 0 ?
				BaseContainer::ROW_ARRAY_GENERAL :
				BaseContainer::ROW_ARRAY_PLAIN);
	};

	struct VariantTraits {
		enum {
			TOTAL_VARIANT_COUNT = OFFSET_END,

			VARIANT_BEGIN = OFFSET_CONTAINER,
			VARIANT_END = OFFSET_END
		};

		static size_t resolveVariant(
				ExprFactoryContext &cxt, const ExprCode &code) {
			return CoreExpr::VariantTraits::resolveVariant(cxt, code);
		}
	};

	template<size_t V, bool = (V <= OFFSET_CONTAINER)>
	struct VariantTraitsBase {
		typedef SQLValues::Types::Any ValueTypeTag;

		typedef SQLCoreExprs::ConstEvaluator<ValueTypeTag> Evaluator;
		typedef typename Evaluator::CodeType Code;
		typedef util::TrueType Checking;
	};

	template<size_t V>
	struct VariantTraitsBase<V, false> {
		enum {
			SUB_OFFSET = (V - OFFSET_CONTAINER - 1),
			SUB_SOURCE =
					(SUB_OFFSET / (COUNT_NULLABLE * COUNT_FULL_TYPE)),
			SUB_NULLABLE =
					(((SUB_OFFSET / COUNT_FULL_TYPE) % COUNT_NULLABLE) != 0),
			SUB_TYPE = (SUB_OFFSET % COUNT_FULL_TYPE)
		};

		static const BaseContainer::RowArrayType SUB_RA_TYPE =
				RowArrayTypeOf<SUB_SOURCE>::RA_TYPE;

		typedef typename VariantTypes::ColumnTypeByFull<
				SUB_TYPE>::ValueTypeTag ValueTypeTag;

		typedef ColumnEvaluator<
				SUB_RA_TYPE, ValueTypeTag, SUB_NULLABLE> Evaluator;
		typedef typename Evaluator::CodeType Code;
		typedef util::FalseType Checking;
	};

	template<size_t V>
	struct VariantAt {
		typedef CoreExpr::VariantBaseAt<V, ColumnExpression> VariantType;
	};
};

class SQLContainerImpl::IdExpression {
private:
	typedef SQLExprs::ExprCode ExprCode;
	typedef SQLExprs::ExprContext ExprContext;
	typedef SQLExprs::ExprFactoryContext ExprFactoryContext;

	typedef SQLCoreExprs::IdExpression CoreExpr;
	typedef SQLCoreExprs::VariantTypes VariantTypes;

public:
	struct VariantTraits {
		enum {
			TOTAL_VARIANT_COUNT = CoreExpr::OFFSET_END,

			VARIANT_BEGIN = CoreExpr::OFFSET_CONTAINER,
			VARIANT_END = CoreExpr::OFFSET_END
		};

		static size_t resolveVariant(
				ExprFactoryContext &cxt, const ExprCode &code) {
			return CoreExpr::VariantTraits::resolveVariant(cxt, code);
		}
	};

	class VariantBase : public SQLExprs::Expression {
	public:
		virtual ~VariantBase() = 0;
		void initializeCustom(ExprFactoryContext &cxt, const ExprCode &code);

	protected:
		ColumnCode columnCode_;
	};

	template<size_t V>
	class VariantAt : public VariantBase {
	public:
		typedef VariantAt VariantType;

		virtual TupleValue eval(ExprContext &cxt) const;

	private:
		enum {
			SUB_OFFSET = (V - CoreExpr::OFFSET_CONTAINER),
			SUB_SOURCE = SUB_OFFSET,
			SUB_NULLABLE = 0
		};

		static const BaseContainer::RowArrayType SUB_RA_TYPE =
				ColumnExpression::template RowArrayTypeOf<
						SUB_SOURCE>::RA_TYPE;

		typedef SQLValues::Types::Long ValueTypeTag;

		typedef ColumnEvaluator<
				SUB_RA_TYPE, ValueTypeTag, SUB_NULLABLE> Evaluator;
	};
};

class SQLContainerImpl::ComparisonExpressionBase {
private:
	typedef SQLExprs::ExprCode ExprCode;
	typedef SQLExprs::ExprFactoryContext ExprFactoryContext;

	typedef SQLCoreExprs::ComparisonExpressionBase CoreExpr;
	typedef SQLCoreExprs::VariantTypes VariantTypes;

	enum {
		COUNT_NULLABLE = CoreExpr::COUNT_NULLABLE,
		COUNT_CONTAINER_SOURCE = CoreExpr::COUNT_CONTAINER_SOURCE,
		COUNT_BASIC_AND_PROMO_TYPE = CoreExpr::COUNT_BASIC_AND_PROMO_TYPE,

		OFFSET_CONTAINER = CoreExpr::OFFSET_CONTAINER,
		OFFSET_END = CoreExpr::OFFSET_END
	};

public:
	struct VariantTraits {
		enum {
			TOTAL_VARIANT_COUNT = OFFSET_END,

			VARIANT_BEGIN = OFFSET_CONTAINER,
			VARIANT_END = OFFSET_END
		};

		static size_t resolveVariant(
				ExprFactoryContext &cxt, const ExprCode &code) {
			return CoreExpr::VariantTraits::resolveVariant(cxt, code);
		}
	};

	template<size_t V>
	struct VariantTraitsBase {
		enum {
			SUB_OFFSET = V - OFFSET_CONTAINER,
			SUB_SOURCE =
					(SUB_OFFSET / (COUNT_NULLABLE * COUNT_BASIC_AND_PROMO_TYPE)),
			SUB_NULLABLE =
					(((SUB_OFFSET / COUNT_BASIC_AND_PROMO_TYPE) %
					COUNT_NULLABLE) != 0),
			SUB_TYPE = (SUB_OFFSET % COUNT_BASIC_AND_PROMO_TYPE)
		};

		static const BaseContainer::RowArrayType SUB_RA_TYPE =
				ColumnExpression::RowArrayTypeOf<SUB_SOURCE>::RA_TYPE;

		typedef typename VariantTypes::ColumnTypeByBasicAndPromo<
				SUB_TYPE>::ValueTypeTag SourceType;
		typedef typename VariantTypes::ColumnTypeByBasicAndPromo<
				SUB_TYPE>::CompTypeTag CompTypeTag;

		typedef ColumnEvaluator<SUB_RA_TYPE, SourceType, SUB_NULLABLE> Arg1;
		typedef SQLCoreExprs::ConstEvaluator<CompTypeTag> Arg2;

		typedef typename Arg1::ValueTypeTag ValueTypeTag1;
		typedef typename Arg2::ValueTypeTag ValueTypeTag2;

		typedef typename Arg1::CodeType Code1;
		typedef typename Arg2::CodeType Code2;

		typedef typename SQLValues::ValueComparator::BasicTypeAt<
				ValueTypeTag1, ValueTypeTag2> TypedComparator;

		typedef util::FalseType Checking;
	};
};

template<typename Func>
class SQLContainerImpl::ComparisonExpression {
	typedef ComparisonExpressionBase Base;
	typedef SQLCoreExprs::ComparisonExpressionBase CoreExpr;
public:
	typedef Base::VariantTraits VariantTraits;

	template<size_t V>
	struct VariantAt {
		typedef CoreExpr::VariantBaseAt<Func, V, Base> VariantType;
	};
};

class SQLContainerImpl::GeneralCondEvaluator {
private:
	typedef SQLExprs::Expression Expression;
	typedef SQLExprs::ExprFactoryContext ExprFactoryContext;

public:
	typedef const Expression &EvaluatorType;
	typedef GeneralCondEvaluator CodeType;
	typedef TupleValue ResultType;

	static EvaluatorType evaluator(const CodeType &code) {
		return *code.expr_;
	}

	static bool check(const ResultType &value) {
		return !ValueUtils::isNull(value);
	}

	static bool get(const ResultType &value) {
		return ValueUtils::getValue<bool>(value);
	}

	void initialize(
			ExprFactoryContext &cxt, const Expression *expr,
			const size_t *argIndex);

private:
	const Expression *expr_;
};

class SQLContainerImpl::VarCondEvaluator {
private:
	typedef SQLExprs::Expression Expression;
	typedef SQLExprs::ExprContext ExprContext;
	typedef SQLExprs::ExprFactoryContext ExprFactoryContext;

public:
	typedef const VarCondEvaluator &EvaluatorType;
	typedef VarCondEvaluator CodeType;
	typedef TupleValue ResultType;

	static EvaluatorType evaluator(const CodeType &code) {
		return code;
	}

	static bool check(const ResultType &value) {
		return !ValueUtils::isNull(value);
	}

	static bool get(const ResultType &value) {
		return ValueUtils::getValue<bool>(value);
	}

	ResultType eval(ExprContext &cxt) const {
		SQLValues::VarContext::Scope varScope(
				cxt.getValueContext().getVarContext());
		assert(expr_ != NULL);
		return expr_->eval(cxt);
	}

	void initialize(
			ExprFactoryContext &cxt, const Expression *expr,
			const size_t *argIndex);

private:
	const Expression *expr_;
};

template<typename Func, size_t V>
class SQLContainerImpl::ComparisonEvaluator {
private:
	typedef SQLExprs::Expression Expression;
	typedef SQLExprs::ExprContext ExprContext;
	typedef SQLExprs::ExprFactoryContext ExprFactoryContext;

	enum {
		RESULT_NULLABLE =
				ComparisonExpressionBase::VariantTraitsBase<V>::SUB_NULLABLE
	};

public:
	typedef const ComparisonEvaluator &EvaluatorType;
	typedef ComparisonEvaluator CodeType;
	typedef typename util::BoolType<RESULT_NULLABLE>::Result NullableType;
	typedef typename util::Conditional<
			RESULT_NULLABLE, TupleValue, bool>::Type ResultType;

	static EvaluatorType evaluator(const CodeType &code) {
		return code;
	}

	static bool check(const bool&) {
		return true;
	}
	static bool check(const TupleValue &value) {
		return !ValueUtils::isNull(value);
	}

	static bool get(const bool &value) {
		return value;
	}
	static bool get(const TupleValue &value) {
		return ValueUtils::getValue<bool>(value);
	}

	ResultType eval(ExprContext &cxt) const {
		return toResult(expr_.evalAsBool(cxt), NullableType());
	}

	void initialize(
			ExprFactoryContext &cxt, const Expression *expr,
			const size_t *argIndex);

private:
	static TupleValue toResult(const TupleValue &src, const util::TrueType&) {
		return src;
	}
	static bool toResult(const TupleValue &src, const util::FalseType&) {
		return ValueUtils::getValue<bool>(src);
	}

	static TupleValue toResult(const bool &src, const util::TrueType&) {
		return TupleValue(src);
	}
	static bool toResult(const bool &src, const util::FalseType&) {
		return src;
	}

	typedef ComparisonExpression<Func> CompExprType;
	typedef typename CompExprType::template VariantAt<
			V>::VariantType CompExprVariantType;

	CompExprVariantType expr_;
};

class SQLContainerImpl::EmptyCondEvaluator {
private:
	typedef SQLExprs::Expression Expression;
	typedef SQLExprs::ExprContext ExprContext;
	typedef SQLExprs::ExprFactoryContext ExprFactoryContext;

public:
	typedef const EmptyCondEvaluator &EvaluatorType;
	typedef EmptyCondEvaluator CodeType;
	typedef bool ResultType;

	static EvaluatorType evaluator(const CodeType &code) {
		return code;
	}

	static bool check(const ResultType &value) {
		static_cast<void>(value);
		return true;
	}

	static bool get(const ResultType &value) {
		static_cast<void>(value);
		return true;
	}

	ResultType eval(ExprContext &cxt) const {
		static_cast<void>(cxt);
		return true;
	}

	void initialize(
			ExprFactoryContext &cxt, const Expression *expr,
			const size_t *argIndex);
};

template<typename Base, BaseContainer::RowArrayType RAType>
class SQLContainerImpl::RowIdCondEvaluator {
private:
	typedef SQLExprs::Expression Expression;
	typedef SQLExprs::ExprContext ExprContext;
	typedef SQLExprs::ExprFactoryContext ExprFactoryContext;

public:
	typedef const RowIdCondEvaluator &EvaluatorType;
	typedef RowIdCondEvaluator CodeType;
	typedef bool ResultType;

	RowIdCondEvaluator();

	static EvaluatorType evaluator(const CodeType &code) {
		return code;
	}

	static bool check(const ResultType &value) {
		static_cast<void>(value);
		return true;
	}

	static bool get(const ResultType &value) {
		return value;
	}

	ResultType eval(ExprContext &cxt) const;

	void initialize(
			ExprFactoryContext &cxt, const Expression *expr,
			const size_t *argIndex);

private:
	typedef typename Base::CodeType BaseCode;

	BaseCode baseCode_;

	RowIdFilter *rowIdFilter_;
	CursorAccessorImpl *accessor_;
	ContainerColumn column_;
};

class SQLContainerImpl::GeneralProjector {
private:
	typedef SQLOps::ProjectionFactoryContext ProjectionFactoryContext;

public:
	GeneralProjector() : proj_(NULL) {
	}

	void project(OpContext &cxt) {
		proj_->project(cxt);
	}

	UpdatorEntry getUpdator() {
		return UpdatorEntry(&updateFunc, this);
	}

	static void updateFunc(OpContext &cxt, void *src) {
		static_cast<GeneralProjector*>(src)->update(cxt);
	}

	bool update(OpContext &cxt);
	void initialize(ProjectionFactoryContext &cxt, const Projection *proj);

private:
	const Projection *proj_;
};

class SQLContainerImpl::CountProjector {
private:
	typedef SQLOps::ProjectionFactoryContext ProjectionFactoryContext;

public:
	CountProjector() : counter_(0) {
	}

	void project(OpContext&) {
		++counter_;
	}

	UpdatorEntry getUpdator() {
		return UpdatorEntry(&updateFunc, this);
	}

	static void updateFunc(OpContext &cxt, void *src) {
		static_cast<CountProjector*>(src)->update(cxt);
	}

	bool update(OpContext &cxt);
	void initialize(ProjectionFactoryContext &cxt, const Projection *proj);

private:
	int64_t counter_;
	SQLValues::SummaryColumn column_;
};

class SQLContainerImpl::CursorRef {
private:
	typedef SQLExprs::ExprContext ExprContext;

public:
	CursorRef();

	CursorImpl& resolveCursor();
	CursorAccessorImpl& resolveAccessor();

	RowIdFilter* findRowIdFilter();
	void setCursor(
			CursorImpl *cursor, CursorAccessorImpl *accessor,
			RowIdFilter *rowIdFilter);

	void addContextRef(OpContext **ref);
	void addContextRef(ExprContext **ref);

	void updateAll(OpContext &cxt);

private:
	CursorImpl *cursor_;
	CursorAccessorImpl *accessor_;
	RowIdFilter *rowIdFilter_;
};

class SQLContainerImpl::ScannerHandlerBase {
private:
	typedef SQLExprs::ExprType ExprType;
	typedef SQLExprs::ExprContext ExprContext;

	typedef SQLCoreExprs::ComparisonExpressionBase CoreCompExpr;

	typedef SQLOps::ProjectionFactoryContext ProjectionFactoryContext;

	enum {
		COUNT_NULLABLE = CoreCompExpr::COUNT_NULLABLE,
		COUNT_BASIC_AND_PROMO_TYPE = CoreCompExpr::COUNT_BASIC_AND_PROMO_TYPE,
		COUNT_CONTAINER_SOURCE = CoreCompExpr::COUNT_CONTAINER_SOURCE,
		COUNT_COMP_BASE = COUNT_NULLABLE * COUNT_BASIC_AND_PROMO_TYPE,

		OFFSET_COMP_CONTAINER = CoreCompExpr::OFFSET_CONTAINER,

		GENERAL_FILTER_NUM = 0,
		EMPTY_FILTER_NUM = 1,
		VAR_FILTER_NUM = 2,

		COUNT_NO_COMP_FILTER_BASE = 3,
		COUNT_ROW_ID_FILTER = 2,
		COUNT_NO_COMP_FILTER = COUNT_NO_COMP_FILTER_BASE * COUNT_ROW_ID_FILTER,

		COUNT_FUNC = 6,
		COUNT_FILTER = COUNT_NO_COMP_FILTER + COUNT_FUNC * COUNT_COMP_BASE,
		COUNT_PROJECTION = 2
	};

public:
	typedef std::pair<
				ProjectionFactoryContext*,
				ContainerRowScanner::HandlerSet*> FactoryContext;

	struct HandlerVariantUtils;

	template<size_t I> struct CompFuncOf;
	template<size_t V> struct VariantTraitsBase;

	struct VariantTraits;
	template<size_t V> struct VariantAt;
};

struct SQLContainerImpl::ScannerHandlerBase::HandlerVariantUtils {
private:
	typedef SQLExprs::ExprCode ExprCode;
	typedef ExprCode::InputSourceType InputSourceType;

public:
	static size_t toInputSourceNumber(InputSourceType type);
	static size_t toCompFuncNumber(ExprType type);

	static void setUpContextRef(
			ProjectionFactoryContext &cxt,
			OpContext **ref, ExprContext **exprRef);
	static void addUpdator(
			ProjectionFactoryContext &cxt, const UpdatorEntry &updator);

	static std::pair<const Expression*, const Projection*>
	getFilterProjectionElements(const Projection &src);
};

template<size_t I>
struct SQLContainerImpl::ScannerHandlerBase::CompFuncOf {
	typedef SQLCoreExprs::Functions Functions;

	typedef
			typename util::Conditional<(I == 0), Functions::Lt,
			typename util::Conditional<(I == 1), Functions::Gt,
			typename util::Conditional<(I == 2), Functions::Le,
			typename util::Conditional<(I == 3), Functions::Ge,
			typename util::Conditional<(I == 4), Functions::Eq,
			typename util::Conditional<(I == 5), Functions::Ne,
			void>::Type>::Type>::Type>::Type>::Type>::Type FuncType;
};

template<size_t V>
struct SQLContainerImpl::ScannerHandlerBase::VariantTraitsBase {
	enum {
		SUB_SOURCE = (V / (COUNT_PROJECTION * COUNT_FILTER)),
		SUB_PROJECTION = ((V / COUNT_FILTER) % COUNT_PROJECTION),
		SUB_FILTER = (V % COUNT_FILTER),

		SUB_COMP_FILTERING = (SUB_FILTER >= COUNT_NO_COMP_FILTER),
		SUB_ROW_ID_FILTERING = (SUB_COMP_FILTERING ?
				false : (SUB_FILTER >= COUNT_NO_COMP_FILTER_BASE)),
		SUB_NO_COMP_BASE = (SUB_COMP_FILTERING ?
				0 : (SUB_FILTER % COUNT_NO_COMP_FILTER_BASE)),

		SUB_EMPTY_FILTERING = (SUB_NO_COMP_BASE == EMPTY_FILTER_NUM),
		SUB_VAR_FILTERING = (SUB_NO_COMP_BASE == VAR_FILTER_NUM),

		SUB_COMP = (SUB_COMP_FILTERING ?
				SUB_FILTER - COUNT_NO_COMP_FILTER : 0),
		SUB_COMP_VARIANT = (OFFSET_COMP_CONTAINER +
				SUB_SOURCE * COUNT_COMP_BASE +
				SUB_COMP % COUNT_COMP_BASE),
		SUB_COMP_FUNC = (SUB_COMP / COUNT_COMP_BASE)
	};

	static const BaseContainer::RowArrayType ROW_ARRAY_TYPE =
			ColumnExpression::RowArrayTypeOf<SUB_SOURCE>::RA_TYPE;

	typedef typename CompFuncOf<SUB_COMP_FUNC>::FuncType CompFuncType;

	typedef ComparisonEvaluator<
			CompFuncType, SUB_COMP_VARIANT> CompEvaluatorType;

	typedef
			typename util::Conditional<
					SUB_EMPTY_FILTERING, EmptyCondEvaluator,
			typename util::Conditional<
					SUB_VAR_FILTERING, VarCondEvaluator,
					GeneralCondEvaluator>::Type>::Type BaseNoCompEvaluatorType;

	typedef typename util::Conditional<
			SUB_ROW_ID_FILTERING,
			RowIdCondEvaluator<BaseNoCompEvaluatorType, ROW_ARRAY_TYPE>,
			BaseNoCompEvaluatorType>::Type NoCompEvaluatorType;

	typedef typename util::Conditional<
			SUB_COMP_FILTERING,
			CompEvaluatorType, NoCompEvaluatorType>::Type EvaluatorType;

	typedef typename util::Conditional<
			(SUB_PROJECTION == 0),
			GeneralProjector, CountProjector>::Type ProjectorType;
};

struct SQLContainerImpl::ScannerHandlerBase::VariantTraits {
	enum {
		TOTAL_VARIANT_COUNT =
				COUNT_CONTAINER_SOURCE * COUNT_PROJECTION * COUNT_FILTER,

		VARIANT_BEGIN = 0,
		VARIANT_END = TOTAL_VARIANT_COUNT
	};

	static size_t resolveVariant(FactoryContext &cxt, const Projection &proj);
};

template<size_t V>
struct SQLContainerImpl::ScannerHandlerBase::VariantAt {
private:
	typedef VariantTraitsBase<V> TraitsBase;

public:
	typedef VariantAt VariantType;

	static const BaseContainer::RowArrayType ROW_ARRAY_TYPE =
			ColumnExpression::RowArrayTypeOf<TraitsBase::SUB_SOURCE>::RA_TYPE;

	VariantAt(ProjectionFactoryContext &cxt, const Projection &proj);

	template<BaseContainer::RowArrayType InRAType>
	void operator()(
			TransactionContext&,
			typename BaseContainer::RowArrayImpl<
					BaseContainer, InRAType>&);

	bool update(TransactionContext&);

private:
	typedef typename TraitsBase::EvaluatorType Evaluator;
	typedef typename TraitsBase::ProjectorType Projector;

	typedef typename Evaluator::CodeType EvaluatorCode;
	typedef typename Evaluator::ResultType ResultType;

	OpContext *cxt_;
	ExprContext *exprCxt_;

	EvaluatorCode condCode_;
	Projector projector_;
};

class SQLContainerImpl::ScannerHandlerFactory {
public:
	typedef ScannerHandlerBase::FactoryContext FactoryContext;

	typedef ContainerRowScanner::HandlerSet& (*FactoryFunc)(
				FactoryContext&, const Projection&);

	static const ScannerHandlerFactory& getInstance();
	static ScannerHandlerFactory& getInstanceForRegistrar();

	ContainerRowScanner& create(
			OpContext &cxt, CursorImpl &cursor, CursorAccessorImpl &accessor,
			const Projection &proj, RowIdFilter *rowIdFilter) const;

	ContainerRowScanner::HandlerSet& create(
				FactoryContext &cxt, const Projection &proj) const;

	void setFactoryFunc(FactoryFunc func);

private:
	ScannerHandlerFactory();

	ScannerHandlerFactory(const ScannerHandlerFactory&);
	ScannerHandlerFactory& operator=(const ScannerHandlerFactory&);

	static const ScannerHandlerFactory &FACTORY_INSTANCE;

	FactoryFunc factoryFunc_;
};

class SQLContainerImpl::BaseRegistrar {
public:
	typedef SQLExprs::ExprRegistrar::VariantTraits ExprVariantTraits;
	struct ScannerVariantTraits;

	explicit BaseRegistrar(const BaseRegistrar &subRegistrar) throw();

	virtual void operator()() const;

protected:
	BaseRegistrar() throw();

	template<SQLExprs::ExprType T, typename E>
	void addExprVariants() const;

	template<typename S>
	void addScannerVariants() const;
	template<typename S, size_t Begin, size_t End>
	void addScannerVariants() const;

private:
	ScannerHandlerFactory *factory_;
};

struct SQLContainerImpl::BaseRegistrar::ScannerVariantTraits {
	typedef ScannerHandlerFactory::FactoryContext ContextType;
	typedef Projection CodeType;
	typedef ContainerRowScanner::HandlerSet ObjectType;

	template<typename V>
	static ObjectType& create(ContextType &cxt, const CodeType &code);
};

class SQLContainerImpl::DefaultRegistrar :
		public SQLContainerImpl::BaseRegistrar {
public:
	virtual void operator()() const;

private:
	static const BaseRegistrar REGISTRAR_INSTANCE;
};

class SQLContainerImpl::SearchResult {
public:
	typedef util::XArray<OId> OIdList;
	typedef std::pair<OIdList*, OIdList*> OIdListRef;

	struct Entry {
	public:
		explicit Entry(util::StackAllocator &alloc);

		bool isEntryEmpty() const;
		size_t getEntrySize() const;

		void clearEntry();

		OIdList& getOIdList(bool forRowArray);
		const OIdList& getOIdList(bool forRowArray) const;

		void swap(Entry &another);

	private:
		OIdList rowOIdList_;
		OIdList rowArrayOIdList_;
	};

	explicit SearchResult(util::StackAllocator &alloc);

	bool isEmpty() const;
	size_t getSize() const;

	void clear();

	Entry& getEntry(bool onMvcc);
	const Entry& getEntry(bool onMvcc) const;

	OIdListRef getOIdListRef(bool forRowArray);

	OIdList& normal();
	OIdList& normalRa();

	OIdList& mvcc();
	OIdList& mvccRa();

private:
	Entry normalEntry_;
	Entry mvccEntry_;
};

class SQLContainerImpl::ExtendedSuspendLimitter :
		public BaseIndex::CustomSuspendLimitter {
public:
	ExtendedSuspendLimitter(
			util::Stopwatch &watch, uint32_t partialExecMaxMillis,
			const std::pair<uint64_t, uint64_t> &partialExecSizeRange);
	virtual ~ExtendedSuspendLimitter();

	virtual bool isLimitReachable(ResultSize &nextLimit);

private:
	util::Stopwatch &watch_;
	uint32_t partialExecMaxMillis_;
	std::pair<uint64_t, uint64_t> partialExecSizeRange_;
};

class SQLContainerImpl::IndexConditionUpdator {
public:
	class ByValue;
	class ByReader;

	IndexConditionUpdator(
			TransactionContext &txn, SQLValues::VarContext &varCxt);

	BtreeConditionUpdator* prepare(
			BaseContainer &container, SQLValues::ValueSetHolder &valuesHolder,
			const SQLExprs::IndexSelector *selector,
			const SQLExprs::IndexConditionList *condList,
			const util::Vector<SQLValues::TupleColumn> *columnList,
			TupleListReader *reader, size_t pos, size_t subPos,
			size_t &nextPos);

	size_t getSubPosition();

private:
	TransactionContext &txn_;
	util::StackAllocator &alloc_;
	SQLValues::ValueContext valueCxt_;

	util::AllocUniquePtr<ByValue> byValue_;
	util::AllocUniquePtr<ByReader> byReader_;

	size_t subPos_;
};

class SQLContainerImpl::IndexConditionUpdator::ByValue :
		public SQLContainerImpl::BtreeConditionUpdator {
public:
	ByValue(TransactionContext &txn, size_t &subPos);
	virtual ~ByValue();

	static bool isAcceptable(
			const SQLExprs::IndexConditionList &condList, size_t pos);

	void assign(
			BaseContainer &container,
			const SQLExprs::IndexConditionList *condList, size_t pos,
			size_t subPos, size_t &nextPos);

	virtual void update(BtreeSearchContext &sc);
	virtual void next();

private:
	void updateKeySet();

	TransactionContext &txn_;
	util::StackAllocator &alloc_;

	const SQLExprs::IndexConditionList *condList_;
	util::Vector<ContainerColumnType> columnTypeList_;
	util::Vector<TermCondition> termCondList_;
	Value::Pool valuePool_;

	size_t &subPos_;
	size_t endPos_;
};

class SQLContainerImpl::IndexConditionUpdator::ByReader :
		public SQLContainerImpl::BtreeConditionUpdator {
private:
	struct Data;
	struct Entry;

public:
	typedef bool (*KeyUpdatorFunc)(ByReader&, Entry&);

	template<typename Compo>
	struct Generator;
	template<typename Promo>
	struct SubGenerator;

	virtual ~ByReader();

	static bool isAcceptable(
			const SQLExprs::IndexConditionList &condList, size_t pos);

	static BtreeConditionUpdator& create(
			TransactionContext &txn, BaseContainer &container,
			SQLValues::ValueSetHolder &valuesHolder,
			const SQLExprs::IndexSelector *selector,
			const SQLExprs::IndexConditionList &condList,
			const util::Vector<SQLValues::TupleColumn> &columnList,
			TupleListReader &reader, size_t pos, size_t &nextPos,
			util::AllocUniquePtr<ByReader> &updator);

	virtual void update(BtreeSearchContext &sc);
	virtual void next() = 0;

protected:
	explicit ByReader(Data &src);

	template<typename SomePromo, typename Compo, typename T>
	void nextAt();

private:
	template<typename SomePromo, typename Compo, typename T>
	class Typed;

	template<typename T>
	struct TypeChecker;

	struct Data {
		Data(
				TransactionContext &txn, SQLValues::ValueSetHolder &valuesHolder,
				const SQLExprs::IndexSelector *selector, TupleListReader &reader);

		TransactionContext &txn_;
		util::StackAllocator &alloc_;

		SQLValues::ValueSetHolder &valuesHolder_;
		const SQLExprs::IndexSelector &selector_;

		util::Vector<Entry> entryList_;
		util::Vector<ContainerColumnType> columnTypeList_;
		util::Vector<TermCondition> termCondList_;

		TupleListReader &reader_;
		bool bindable_;
	};

	static void generateTypedUpdator(
			bool somePromotable, bool composite, TupleColumnType frontKeyType,
			Data &data, util::AllocUniquePtr<ByReader> &updator);
	template<typename SomePromo, typename Compo, typename T>
	static void generateTypedUpdatorAt(
			Data &data, util::AllocUniquePtr<ByReader> &updator);

	static KeyUpdatorFunc generateKeyUpdator(
			bool promotable, TupleColumnType keyType);

	bool updateKeySet();
	template<typename SomePromo, typename Compo, typename T>
	bool updateKeySetAt();

	template<typename SomePromo, typename Compo, typename T>
	bool updateCurrentKeySet();
	template<typename SomePromo>
	void updateFollowingKeySet();

	template<typename Promo, typename T>
	static bool executeKeyUpdator(ByReader &updator, Entry &entry);
	template<typename, typename T>
	bool updateKey(Entry &entry, const util::TrueType&);
	template<typename Promo, typename T>
	bool updateKey(Entry &entry, const util::FalseType&);

	template<typename>
	bool readKey(Entry &entry, const util::TrueType&);
	template<typename T>
	bool readKey(Entry &entry, const util::FalseType&);

	static void swapElements(Data &src, Data &dest);
	static void clearElements(Data &data);

	static size_t setUpEntries(
			const BaseContainer &container,
			const SQLExprs::IndexConditionList &condList,
			const util::Vector<SQLValues::TupleColumn> &columnList,
			size_t pos, util::Vector<ContainerColumnType> &columnTypeList,
			util::Vector<Entry> &entryList, bool &somePromotable);
	static bool tryAppendEntry(
			const SQLExprs::IndexCondition &cond,
			const util::Vector<SQLValues::TupleColumn> &columnList,
			const util::Vector<ContainerColumnType> &columnTypeList,
			util::Vector<Entry> &entryList, bool &promotable);

	void applyKeyConditions(
			BtreeSearchContext &sc, bool withValues, const size_t *keyCount);
	const void* prepareKeyData(
			bool withValues, Entry &entry, uint32_t &keySize);

	static bool isAssignableKey(
			size_t keyPos, const SQLExprs::IndexCondition &cond);
	static TupleColumnType getKeyType(ContainerColumnType src);
	static TupleValue& getKeyRef(Entry &entry);

	static SQLValues::TupleColumn getInputColumn(
			const SQLExprs::IndexCondition &cond,
			const util::Vector<SQLValues::TupleColumn> &columnList);
	static bool findInputColumnPos(
			const SQLExprs::IndexCondition &cond, uint32_t *columnPos);

	static bool isPromotable(
			TupleColumnType keyType, const SQLValues::TupleColumn &inColumn);
	static bool isComposite(const util::Vector<Entry> &entryList);

	Data data_;
};

struct SQLContainerImpl::IndexConditionUpdator::ByReader::Entry {
	Entry(
			size_t keyPos, TupleColumnType keyType,
			const SQLExprs::IndexCondition &subCond,
			const SQLValues::TupleColumn &inColumn, KeyUpdatorFunc func);

	size_t keyPos_;
	TupleColumnType keyType_;
	SQLExprs::IndexCondition subCond_;
	SQLExprs::IndexCondition localSubCond_;
	SQLValues::TupleColumn inColumn_;
	KeyUpdatorFunc keyUpdatorFunc_;
};

template<typename Compo>
struct SQLContainerImpl::IndexConditionUpdator::ByReader::Generator {
	typedef void RetType;
	template<typename T>
	struct TypeAt {
		explicit TypeAt(Generator&) {
		}

		RetType operator()(
				Data &data, util::AllocUniquePtr<ByReader> &updator) const {
			if (!TypeChecker<T>::UPDATOR_ACCEPTABLE) {
				assert(false);
				return;
			}
			typedef typename util::FalseType SomePromo;
			typedef typename TypeChecker<T>::Type Type;
			generateTypedUpdatorAt<SomePromo, Compo, Type>(data, updator);
		}
	};
};

template<typename Promo>
struct SQLContainerImpl::IndexConditionUpdator::ByReader::SubGenerator {
	typedef KeyUpdatorFunc RetType;
	template<typename T>
	struct TypeAt {
		explicit TypeAt(SubGenerator&) {
		}

		RetType operator()() const {
			if (!TypeChecker<T>::UPDATOR_ACCEPTABLE) {
				assert(false);
				return NULL;
			}
			typedef typename TypeChecker<T>::Type Type;
			return &executeKeyUpdator<Promo, Type>;
		}
	};
};

template<typename SomePromo, typename Compo, typename T>
class SQLContainerImpl::IndexConditionUpdator::ByReader::Typed :
		public SQLContainerImpl::IndexConditionUpdator::ByReader {
public:
	explicit Typed(Data &data) : ByReader(data) {
	}

	virtual ~Typed() {
	}

	virtual void next() {
		nextAt<SomePromo, Compo, T>();
	}
};

template<typename T>
struct SQLContainerImpl::IndexConditionUpdator::ByReader::TypeChecker {
	typedef typename SQLValues::TypeUtils::Traits<T::COLUMN_TYPE> TraitsType;

	enum {
		UPDATOR_ACCEPTABLE = (
				TraitsType::FOR_NORMAL_FIXED ||
				TraitsType::SIZE_VAR_OR_LARGE_EXPLICIT)
	};

	typedef SQLValues::Types::Long UnacceptableType;
	typedef typename util::Conditional<
			UPDATOR_ACCEPTABLE, T, UnacceptableType>::Type Type;
};

class SQLContainerImpl::IndexSearchContext {
public:
	IndexSearchContext(
			TransactionContext &txn, SQLValues::VarContext &varCxt,
			SQLOps::OpAllocatorManager &allocManager,
			util::StackAllocator::BaseAllocator &baseAlloc,
			util::Stopwatch &watch, const uint32_t *partialExecMaxMillis,
			const std::pair<uint64_t, uint64_t> &partialExecSizeRange);

	void clear();

	void setInfo(const IndexScanInfo *info);

	const SQLExprs::IndexConditionList& conditionList();
	IndexConditionUpdator& conditionUpdator();

	ColumnIdList& columnList();
	util::Vector<ContainerColumnType>& columnTypeList();
	IndexInfoList& indexInfoList();

	BtreeSearchContext& tree();

	SearchResult& result();

private:
	util::StackAllocator &alloc_;
	SQLOps::OpStore::AllocatorRef settingAllocRef_;

	const SQLExprs::IndexConditionList *condList_;

	ColumnIdList columnList_;
	util::Vector<ContainerColumnType> columnTypeList_;
	IndexInfoList indexInfoList_;

	SearchContextPool scPool_;
	BtreeSearchContext tree_;
	IndexConditionUpdator indexConditionUpdator_;

	SearchResult result_;
	util::LocalUniquePtr<ExtendedSuspendLimitter> suspendLimitter_;
};

class SQLContainerImpl::CursorScope {
public:
	CursorScope(
			ExtOpContext &cxt, const TupleColumnList &inColumnList,
			CursorAccessorImpl &accessor);
	~CursorScope();

	TransactionContext& getTransactionContext();
	ResultSet& getResultSet();

	IndexSearchContext& prepareIndexSearchContext(
			SQLValues::VarContext &varCxt, const IndexScanInfo *info,
			SQLOps::OpAllocatorManager &allocManager,
			util::StackAllocator::BaseAllocator &baseAlloc,
			const uint32_t *partialExecMaxMillis,
			const std::pair<uint64_t, uint64_t> &partialExecSizeRange);
	IndexSearchContext* findIndexSearchContext();

	int64_t getStartPartialExecCount();
	uint64_t getElapsedNanos();

private:
	typedef DataStoreBase::Scope DataStoreScope;

	void initialize(
			ExtOpContext &cxt, BaseContainer *container,
			const TupleColumnList &inColumnList);
	void setUpAccessor(BaseContainer *container);

	void clear(bool force);
	void clearResultSet(bool force);

	static util::Mutex* getPartitionLock(
			TransactionContext &txn, ExtOpContext &cxt);
	static const DataStoreScope* getStoreScope(
			TransactionContext &txn, ExtOpContext &cxt, DataStoreV4 *dataStore,
			util::LocalUniquePtr<DataStoreScope> &scope);
	static BaseContainer* getContainer(
			TransactionContext &txn, DataStoreV4 *dataStore,
			const DataStoreScope *latch, const ContainerLocation &location,
			bool expired, ContainerType type);
	static ResultSet* prepareResultSet(
			ExtOpContext &cxt, TransactionContext &txn, BaseContainer *container,
			util::LocalUniquePtr<ResultSetGuard> &rsGuard,
			const CursorAccessorImpl &accessor);
	static ResultSetId findResultSetId(ResultSet *resultSet);

	CursorAccessorImpl &accessor_;
	TransactionContext &txn_;
	TransactionService *txnSvc_;
	util::DynamicLockGuard<util::Mutex> partitionGuard_;
	DataStoreV4 *dataStore_;
	util::LocalUniquePtr<DataStoreScope> scope_;
	StackAllocAutoPtr<BaseContainer> containerAutoPtr_;
	util::LocalUniquePtr<ResultSetGuard> rsGuard_;
	ResultSet *resultSet_;

	util::LocalUniquePtr<IndexSearchContext> indexSearchContext_;

	int64_t partialExecCount_;
	util::Stopwatch watch_;
};

class SQLContainerImpl::LocalCursorScope {
public:
	LocalCursorScope(OpContext &cxt, CursorAccessorImpl &accessor);
	LocalCursorScope(
			ExtOpContext &extCxt, CursorAccessorImpl &accessor,
			const TupleColumnList &inColumnList);

	~LocalCursorScope();

	void close() throw();
	void closeWithError(std::exception &e);

private:
	enum {
		INPUT_INDEX = 0
	};

	struct Data {
		explicit Data(CursorAccessorImpl &accessor);

		CursorAccessorImpl &accessor_;
		bool closed_;
	};

	LocalCursorScope(const LocalCursorScope&);
	LocalCursorScope& operator=(const LocalCursorScope&);

	Data data_;
};

class SQLContainerImpl::ContainerNameFormatter {
public:
	ContainerNameFormatter(
			TransactionContext &txn, BaseContainer &container,
			const ContainerLocation &location);

	std::ostream& format(std::ostream &os) const throw();

private:
	ContainerNameFormatter(const ContainerNameFormatter&);
	ContainerNameFormatter& operator=(const ContainerNameFormatter&);

	TransactionContext &txn_;
	BaseContainer &container_;
	const ContainerLocation &location_;
};

std::ostream& operator<<(
		std::ostream &os,
		const SQLContainerImpl::ContainerNameFormatter &formatter);

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
private:
	typedef ResultSet::UpdateOperation UpdateOperation;

public:
	struct Shared;

	TupleUpdateRowIdHandler(const OpContext::Source &cxtSrc, RowId maxRowId);
	~TupleUpdateRowIdHandler();

	virtual void close();

	bool isUpdated();
	void setUpdated();

	virtual void operator()(RowId rowId, uint64_t id, UpdateOperation op);

	bool apply(OIdTable &oIdTable, uint64_t limit);

	TupleUpdateRowIdHandler* share(util::StackAllocator &alloc);

private:
	typedef SQLValues::Types::Long IdValueTag;
	typedef SQLValues::Types::Integer OpValueTag;

	enum {
		ID_COLUMN_POS = 0,
		OP_COLUMN_POS = 1,
		ID_STORE_COLUMN_COUNT = 2
	};

	typedef TupleList::Column IdStoreColumnList[ID_STORE_COLUMN_COUNT];

	explicit TupleUpdateRowIdHandler(Shared &shared);

	static uint32_t createRowIdStore(
			util::LocalUniquePtr<OpContext> &cxt, const OpContext::Source &cxtSrc,
			IdStoreColumnList &columnList, uint32_t &extraStoreIndex);

	static void copyRowIdStore(
			OpContext &cxt, uint32_t srcIndex, uint32_t destIndex,
			const IdStoreColumnList &columnList, uint64_t *copiedCountRef);

	static util::ObjectPool<Shared, util::Mutex> sharedPool_;

	Shared *shared_;
};

struct SQLContainerImpl::TupleUpdateRowIdHandler::Shared {
	Shared(const OpContext::Source &cxtSrc, RowId maxRowId);

	util::Mutex mutex_;
	size_t refCount_;
	util::LocalUniquePtr<OpContext> cxt_;

	IdStoreColumnList columnList_;
	uint32_t extraStoreIndex_;
	uint32_t rowIdStoreIndex_;

	RowId maxRowId_;

	uint64_t restInitialUpdateCount_;
	bool updated_;
	bool initialUpdatesAccepted_;
};

struct SQLContainerImpl::BitUtils {
	template<typename V> struct DenseValues;
	template<typename V> struct TupleColumns;
};

template<typename V>
struct SQLContainerImpl::BitUtils::DenseValues {
	typedef V ValueType;

	struct ValueConstants {
		static const uint32_t FULL_BIT_COUNT =
				static_cast<uint32_t>(sizeof(ValueType) * CHAR_BIT);
		static const uint32_t LOW_BIT_COUNT =
				util::ILog2<FULL_BIT_COUNT>::VALUE;
		static const ValueType LOW_BIT_MASK =
				((static_cast<ValueType>(1) << LOW_BIT_COUNT) - 1);
	};

	static ValueType toHighPart(ValueType value);
	static ValueType toLowBit(ValueType value);

	static ValueType extraOfHighPart(ValueType value);

	static bool nextValue(
			uint32_t &nextPos, ValueType highPart, ValueType lowBits,
			ValueType &value);
};

template<typename V>
struct SQLContainerImpl::BitUtils::TupleColumns {
public:
	enum {
		COLUMN_TYPE = SQLValues::TypeUtils::template ValueTraits<
				V>::COLUMN_TYPE_AS_NUMERIC
	};

	typedef typename SQLValues::Types::template Of<COLUMN_TYPE>::Type
			KeyTag;
	typedef KeyTag ValueTag;

	typedef typename KeyTag::ValueType KeyType;
	typedef typename ValueTag::ValueType ValueType;

	static KeyType readCurrentKey(TupleListReader &reader);
	static ValueType readCurrentValue(TupleListReader &reader);

	static void writeKey(WritableTuple &tuple, const KeyType &key);
	static void writeValue(WritableTuple &tuple, const ValueType &value);

	static TupleList::Column getKeyColumn();
	static TupleList::Column getValueColumn();

	static void getColumnTypeList(SQLValues::ColumnTypeList &list);

private:
	enum {
		BITS_COLUMN_COUNT = 2
	};

	struct Constants {
		static const TupleList::Column COLUMN_KEY;
		static const TupleList::Column COLUMN_VALUE;
		static const TupleColumnType COLUMN_TYPES[BITS_COLUMN_COUNT];
	};

	static TupleList::Column resolveColumn(bool forKey);
};

class SQLContainerImpl::OIdTable {
public:
	enum GroupMode {
		MODE_UNION,
		MODE_INTERSECT
	};

	typedef OIdTableGroupId GroupId;

	struct Source;
	struct MergeContext;
	struct MergeElement;
	struct MergeElementRef;
	struct MergeElementGreater;
	template<GroupMode M> struct MergeAction;

	explicit OIdTable(const Source &source);
	~OIdTable();

	void createGroup(GroupId groupId, GroupId parentId, GroupMode mode);

	void addAll(GroupId groupId, SearchResult &result);
	void popAll(
			SearchResult &result, uint64_t limit, uint32_t rowArraySize,
			bool withUpdatesAlways);

	void update(ResultSet::UpdateOperation op, OId oId);

	bool finalizeAddition(uint64_t limit);

	bool isUpdatesAcceptable();
	void acceptUpdates();

	bool isCursorStarted();
	bool isCursorFinished();
	void restartCursor();

	void setSuspendable(bool suspendable);

	bool getModificationProfile(
			IndexProfiler::ModificationType modification, bool onMvcc,
			int64_t &count) const;

private:
	enum {
		GROUP_ID_ROOT = -1,
		GROUP_ID_DELTA_ADDITION = -2,
		GROUP_ID_DELTA_REMOVAL = -3
	};

	enum {
		MOD_TYPE_COUNT = IndexProfiler::END_MOD
	};

	enum EntryType {
		TYPE_CHUNK,
		TYPE_RA_NORMAL,
		TYPE_RA_MVCC,
		TYPE_ROW_NORMAL,
		TYPE_ROW_MVCC,
		END_TYPE
	};

	struct Group;
	struct Slot;
	struct HotSlot;
	struct HotData;
	struct Cursor;
	struct Coders;
	struct CoderConstants;

	typedef BitUtils::TupleColumns<int64_t> BitTupleColumns;
	typedef std::pair<uint64_t, uint64_t> Entry;
	typedef std::pair<int64_t, int64_t> ModificationProfileEntry;

	typedef util::AllocVector<GroupId> GroupIdList;
	typedef util::AllocMap<GroupId, Group*> GroupMap;
	typedef util::AllocVector<Slot> SlotList;
	typedef util::AllocVector<SlotList> SlotStageList;
	typedef util::AllocVector<Entry> EntryList;

	typedef util::XArray<OId> OIdList;
	typedef util::Map<uint64_t, uint64_t> EntryMap;
	typedef util::AllocUniquePtr<EntryMap> EntryMapPtr;
	typedef util::Map<GroupId, HotSlot*> HotSlotMap;

	typedef EntryMap::const_iterator EntryIterator;
	typedef std::pair<EntryIterator, EntryIterator> EntryIteratorPair;


	void checkAdditionWorking();
	void checkAdditionFinalized();
	void checkUpdatesAcceptable();


	Group* findGroup(GroupId groupId);
	Group& getGroup(GroupId groupId);
	Group& createGroupDirect(GroupId groupId, GroupMode mode);
	void clearGroups();

	GroupId getAdditionGroupId(GroupId groupId);

	static GroupId validateGroupId(GroupId groupId, bool withRoot);


	Slot resultToSlot(OpContext &cxt, SearchResult &result);
	Slot oIdListToSlot(OpContext &cxt, EntryType type, OIdList &list);
	Slot entryListToSlot(OpContext &cxt, EntryList &list);

	void resultToEntries(SearchResult &result, EntryList &dest);
	void oIdListToEntries(EntryType type, OIdList &src, EntryList &dest);

	bool slotToResult(
			OpContext &cxt, Slot &slot, SearchResult &result, uint64_t limit,
			uint32_t rowArraySize, HotSlot *removalSlot);
	void resetSlotReader(Slot &slot);

	bool checkRemoved(EntryType type, Entry &entry, HotSlot &removalSlot);
	bool checkRemovedSub(
			EntryType type, Entry &entry, HotSlot &removalSlot);

	bool isLimitReached(
			const SearchResult &result, uint64_t limit, uint32_t rowArraySize);
	static void entryToResult(
			EntryType type, const Entry &entry, SearchResult &result);

	static Entry readEntry(TupleListReader &reader);
	static void writeEntry(TupleListWriter &writer, const Entry &entry);

	static uint64_t readKey(TupleListReader &reader);
	static uint64_t readPositionBits(TupleListReader &reader);


	bool mergeGroup(OpContext &cxt, GroupId groupId, uint64_t &limit);
	bool mergeSlot(OpContext &cxt, Group &group, bool full, uint64_t *limit);

	Slot* updateMergingStage(
			OpContext &cxt, Group &group, bool full, size_t *depth);
	bool mergeStageSlot(
			OpContext &cxt, GroupMode mode, SlotList &slotList, Slot &dest,
			uint64_t *limit);


	Slot* getSingleSlot(Group &group);
	void addSlot(OpContext &cxt, Group &group, Slot &slot, bool withMerge);

	Slot allocateSlot(OpContext &cxt);
	void releaseSlot(OpContext &cxt, Slot &slot);


	HotSlot* findHotSlot(GroupId groupId);
	HotSlot& prepareHotSlot(GroupId groupId);

	HotData& prepareHotData();


	EntryMap* findEntryMap(EntryType type, HotSlot &slot);
	EntryMap& prepareEntryMap(EntryType type, HotSlot &slot);

	void updateEntry(EntryType type, OId oId);
	void removeEntries(EntryType type, OId oId);
	void removeEntriesAt(EntryType type, OId oId, EntryMap &map);
	void addModificationEntry(GroupId groupId, EntryType type, OId oId);

	void popUpdates(SearchResult &result);
	uint64_t popEntries(
			EntryType type, EntryIteratorPair &src, uint32_t rowArraySize,
			OIdList &dest);


	static OIdList* findSingleSearchResultOIdList(
			SearchResult &result, EntryType &type);

	static const OIdList& getSearchResultOIdList(
			EntryType type, const SearchResult &result);
	static OIdList& getSearchResultOIdList(
			EntryType type, SearchResult &result);

	static bool isMvccEntryType(EntryType type);
	static bool isRowArrayEntryType(EntryType type);

	SQLOps::OpAllocatorManager &allocManager_;
	util::StackAllocator::BaseAllocator &baseAlloc_;
	SQLValues::VarAllocator &varAlloc_;
	OpContext::Source cxtSrc_;

	uint32_t stageSizeLimit_;
	bool large_;
	bool suspendable_;

	GroupMap groupMap_;
	SlotList freeSlotList_;
	util::AllocUniquePtr<HotData> hotData_;
	util::AllocUniquePtr<Cursor> cursor_;
	util::LocalUniquePtr<GroupId> mergingGroupId_;

	bool updatesAcceptable_;
	ModificationProfileEntry modificationProfile_[MOD_TYPE_COUNT];
};

struct SQLContainerImpl::OIdTable::Source {
public:
	explicit Source(const OpContext::Source &cxtSrc);

	static Source ofContext(
			OpContext &cxt, BaseContainer &container, int64_t memoryLimit);

	static bool detectLarge(BaseContainer &container);
	static uint32_t resolveStageSizeLimit(
			uint32_t blockSize, int64_t memoryLimit);

	SQLOps::OpAllocatorManager& getAlloactorManager() const;
	util::StackAllocator::BaseAllocator& getBaseAllocator() const;
	SQLValues::VarAllocator& getVarAllocator() const;
	const OpContext::Source& getContextSource() const;
	uint32_t isStageSizeLimit() const;
	bool isLarge() const;

private:
	SQLOps::OpAllocatorManager *allocManager_;
	util::StackAllocator::BaseAllocator *baseAlloc_;
	SQLValues::VarAllocator *varAlloc_;
	OpContext::Source cxtSrc_;
	uint32_t stageSizeLimit_;
	bool large_;
};

struct SQLContainerImpl::OIdTable::MergeContext {
	MergeContext(
			TupleListWriter &writer, GroupMode mode, const uint64_t *limit);

	uint64_t positionBits_;
	TupleListWriter &writer_;

	bool limited_;
	uint64_t limit_;
};

struct SQLContainerImpl::OIdTable::MergeElement {
	explicit MergeElement(TupleListReader &reader);

	TupleListReader &reader_;
};

struct SQLContainerImpl::OIdTable::MergeElementRef {
public:
	explicit MergeElementRef(MergeElement &elem);

	uint64_t key() const;
	uint64_t positionBits() const;

	bool next() const;

private:
	TupleListReader *reader_;
};

struct SQLContainerImpl::OIdTable::MergeElementGreater {
	template<typename>
	struct TypeAt {
		typedef MergeElementGreater TypedOp;
	};

	template<typename Op>
	struct Func {
		typedef typename Op::RetType (*Type)(Op&);
	};

	typedef SQLAlgorithmUtils::HeapElement<MergeElementRef> Element;

	bool operator()(const Element &elem1, const Element &elem2) const;
	bool operator()(
			const MergeElementRef &elem1, const MergeElementRef &elem2) const;

	const MergeElementGreater& getTypeSwitcher() const;

	template<typename Op> typename Func<Op>::Type get() const;
	template<typename Op> typename Func<Op>::Type getWithOptions() const;
	template<typename Op> static typename Op::RetType execute(Op &op);
};

template<SQLContainerImpl::OIdTable::GroupMode M>
struct SQLContainerImpl::OIdTable::MergeAction {
	template<typename>
	struct TypeAt {
		typedef MergeAction TypedOp;
	};

	typedef SQLAlgorithmUtils::HeapElement<MergeElementRef> Element;
	typedef SQLAlgorithmUtils::HeapPredicate<
			MergeElementRef, MergeElementGreater> Predicate;
	typedef void OptionsType;

	explicit MergeAction(MergeContext &cxt);

	bool operator()(const Element &elem, const util::FalseType&);
	void operator()(const Element &elem, const util::TrueType&);
	bool operator()(
			const Element &elem, const util::TrueType&,
			const util::TrueType&);
	bool operator()(
			const Element &elem, const util::TrueType&, const Predicate &pred);

	MergeContext &cxt_;
};

struct SQLContainerImpl::OIdTable::Group {
	Group(SQLValues::VarAllocator &varAlloc, GroupMode mode);

	SlotStageList stageList_;
	GroupId parentId_;
	GroupIdList subIdList_;
	GroupMode mode_;

	util::LocalUniquePtr<uint64_t> mergingStageDepth_;
	util::AllocUniquePtr<Slot> mergingSlot_;
};

struct SQLContainerImpl::OIdTable::Slot {
public:
	static Slot createSlot(
			OpContext &cxt, const SQLValues::ColumnTypeList &typeList);
	static Slot invalidSlot();

	void createTupleList(OpContext &cxt);
	void closeTupleList(OpContext &cxt);

	TupleListReader& getReader(OpContext &cxt);
	void resetReaderPosition(OpContext &cxt);

	TupleListWriter& getWriter(OpContext &cxt);
	void finalizeWriter(OpContext &cxt);

private:
	enum {
		READER_SUB_INDEX_CURRENT = 0,
		READER_SUB_INDEX_INITIAL = 1
	};
	explicit Slot(uint32_t tupleListIndex);

	uint32_t tupleListIndex_;
};

struct SQLContainerImpl::OIdTable::HotSlot {
	explicit HotSlot(util::StackAllocator &alloc);

	EntryMapPtr& mapAt(EntryType type);

	util::StackAllocator &alloc_;
	EntryMapPtr mapList_[END_TYPE];
};

struct SQLContainerImpl::OIdTable::HotData {
	HotData(
			SQLOps::OpAllocatorManager &allocManager,
			util::StackAllocator::BaseAllocator &baseAlloc);

	SQLOps::OpStore::AllocatorRef allocRef_;
	util::StackAllocator &alloc_;
	HotSlotMap hotSlots_;
};

struct SQLContainerImpl::OIdTable::Cursor {
	Cursor();

	uint64_t nextKey_;

	bool started_;
	bool finished_;
};

struct SQLContainerImpl::OIdTable::Coders {
	typedef BitUtils::DenseValues<OId> OIdValues;

	static Entry oIdToEntry(EntryType type, OId oId);
	static uint64_t oIdToKey(EntryType type, OId oId);
	static uint64_t oIdToPositionBit(OId oId);

	static uint64_t chunkOrRaMaskOfKey(EntryType type, bool large);

	static uint64_t chunkBitsOfKey(bool large, uint64_t key);
	static uint64_t chunkMaskOfKey(bool large);

	static uint64_t raBitsOfKey(uint64_t key);
	static uint64_t raMaskOfKey();

	static EntryType typeOfKey(uint64_t key);

	static bool nextOId(uint32_t &nextPos, const Entry &entry, OId &oId);
	static bool tryMergeEntry(const Entry &src, Entry &dest);

	static uint64_t initialPositionBits(GroupMode mode);
	template<GroupMode M> static uint64_t initialPositionBits();

	template<GroupMode M>
	static void mergePositionBits(const uint64_t &src, uint64_t &dest);
};

struct SQLContainerImpl::OIdTable::CoderConstants {


	static const uint32_t MAX_USER_BIT = 10;


	static const uint32_t UNIT_LARGE_CHUNK_SHIFT_BIT = 38;
	static const uint32_t UNIT_SMALL_CHUNK_SHIFT_BIT = 32;


	static const uint64_t OID_USER_MASK =
			((static_cast<uint64_t>(1) << MAX_USER_BIT) - 1);
};

class SQLContainerImpl::RowIdFilter {
public:
	class RowIdCursor;

	RowIdFilter(
			const OpContext::Source &cxtSrc,
			SQLOps::OpAllocatorManager &allocManager,
			util::StackAllocator::BaseAllocator &baseAlloc, RowId maxRowId,
			int64_t memoryLimit);

	bool accept(RowId rowId);

	static bool tryAdjust(RowIdFilter *filter);
	void adjust();
	bool isDuplicatable();

	void setReadOnly();
	bool isReadOnly();

private:
	enum {
		BITS_ENTRY_POS_COUNT = sizeof(int64_t) * CHAR_BIT
	};

	typedef BitUtils::DenseValues<RowId> RowIdValues;
	typedef BitUtils::TupleColumns<int64_t> BitTupleColumns;

	typedef SQLAlgorithmUtils::DenseBitSet<> BitSet;

	TupleListReader& getBitsEntryReader(
			util::LocalUniquePtr<OpContext> &localCxt);
	bool hasCurrentRowId();
	RowId getCurrentRowId();
	void prepareCurrentRowId(TupleListReader &reader, bool forNext);

	void adjustDetail(OpContext &cxt);
	void initializeBits();

	void readBitsEntry(TupleListReader &reader, BitSet::CodedEntry &entry);
	void writeBits(TupleListWriter &writer);

	bool isAllocationLimitReached();
	uint64_t getAllocationUsageSize();

	OpContext::Source cxtSrc_;
	SQLOps::OpAllocatorManager &allocManager_;
	SQLOps::OpStore::AllocatorRef allocRef_;
	util::StackAllocator &alloc_;
	util::LocalUniquePtr<util::StackAllocator::Scope> allocScope_;

	RowId maxRowId_;
	BitSet *rowIdBits_;
	bool readOnly_;
	bool duplicatable_;
	bool cursorStarted_;
	int64_t memLimit_;

	util::LocalUniquePtr<uint32_t> tupleListIndex_;
	BitSet::CodedEntry currentBitsEntry_;
	int32_t currentBitsPos_;
};

class SQLContainerImpl::RowIdFilter::RowIdCursor {
public:
	explicit RowIdCursor(RowIdFilter &filter);

	bool exists();
	RowId get();
	void next();

private:
	RowIdFilter &filter_;
	util::LocalUniquePtr<OpContext> localCxt_;
	TupleListReader &reader_;
};

struct SQLContainerImpl::ScanSimulatorUtils {
	static void trySimulateUpdates(
			OpContext &cxt, CursorAccessorImpl &accessor);
	static void tryAcceptUpdates(
			OpContext &cxt, CursorAccessorImpl &accessor,
			const SearchResult &result);

	static void simulateUpdates(
			TransactionContext &txn, OpSimulator &simulator,
			BaseContainer &container, ResultSet &resultSet);
	static void acceptUpdates(
			TransactionContext &txn, OpSimulator &simulator,
			BaseContainer &container, const SearchResult &result);
};

struct SQLContainerImpl::ContainerValueUtils {
	static Value& toContainerValue(
			Value::Pool &valuePool, const TupleValue &src);
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
