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

#include "sql_expression_core.h"
#include "sql_operator_utils.h" 

class BoolExpr;
class Expr;


struct SQLContainerImpl {

	typedef SQLType::Id ExpressionType;
	typedef SQLType::IndexType IndexType;

	typedef TupleValue::VarContext VarContext;

	typedef ::ColumnType ContainerColumnType;

	typedef BtreeMap::SearchContext BtreeSearchContext;

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

	typedef SQLOps::ExtOpContext ExtOpContext;

	typedef SQLContainerUtils::ScanMergeMode MergeMode;

	class CursorImpl;
	class CursorAccessorImpl;

	struct Constants;

	struct ColumnCode;
	template<RowArrayType RAType, typename T, typename S, bool Nullable>
	class ColumnEvaluator;

	class ColumnExpression;
	class IdExpression;
	class ComparisonExpressionBase;
	template<typename Func> class ComparisonExpression;

	class GeneralCondEvaluator;
	class VarCondEvaluator;
	template<typename Func, size_t V> class ComparisonEvaluator;
	class EmptyCondEvaluator;
	template<typename Base, BaseContainer::RowArrayType RAType>
	class RowIdCondEvaluator;

	class GeneralProjector;
	class CountProjector;

	class CursorRef;

	class ScannerHandlerBase;
	class ScannerHandlerFactory;

	class BaseRegistrar;
	class DefaultRegistrar;

	struct SearchResult;
	class CursorScope;
	class MetaScanHandler;
	class TupleUpdateRowIdHandler;

	class OIdTable;
	class RowIdFilter;

	struct ContainerValueUtils;
	struct TQLTool;

	typedef void (*UpdatorFunc)(OpContext&, void*);
	typedef std::pair<UpdatorFunc, void*> UpdatorEntry;
};

class SQLContainerImpl::CursorImpl : public SQLContainerUtils::ScanCursor {
public:
	struct Data;
	class HolderImpl;

	typedef CursorAccessorImpl Accessor;

	CursorImpl(Accessor &accessor, Data &data);
	virtual ~CursorImpl();

	virtual bool scanFull(OpContext &cxt, const Projection &proj);
	virtual bool scanRange(OpContext &cxt, const Projection &proj);
	virtual bool scanIndex(
			OpContext &cxt, const SQLExprs::IndexConditionList &condList);
	virtual bool scanMeta(
			OpContext &cxt, const Projection &proj, const Expression *pred);

	virtual void finishIndexScan(OpContext &cxt);

	virtual uint32_t getOutputIndex();
	virtual void setOutputIndex(uint32_t index);

	virtual SQLContainerUtils::ScanCursorAccessor& getAccessor();

	void addScannerUpdator(const UpdatorEntry &updator);

	void addContextRef(OpContext **ref);
	void addContextRef(SQLExprs::ExprContext **ref);

private:
	typedef util::AllocVector<UpdatorEntry> UpdatorList;

	typedef util::AllocVector<OpContext**> ContextRefList;
	typedef util::AllocVector<SQLExprs::ExprContext**> ExprContextRefList;

	ContainerId getNextContainerId() const;
	void setNextContainerId(ContainerId containerId);

	ContainerRowScanner* tryCreateScanner(
			OpContext &cxt, const Projection &proj,
			const BaseContainer &container, RowIdFilter *rowIdFilter);
	void updateScanner(OpContext &cxt, bool finished);

	void preparePartialSearch(
			OpContext &cxt, BtreeSearchContext &sc,
			const SQLExprs::IndexConditionList *targetCondList);
	bool finishPartialSearch(
			OpContext &cxt, BtreeSearchContext &sc, bool forIndex);

	bool checkUpdates(
			TransactionContext &txn, BaseContainer &container,
			ResultSet &resultSet);

	static void setUpSearchContext(
			TransactionContext &txn,
			const SQLExprs::IndexConditionList &targetCondList,
			BtreeSearchContext &sc, const BaseContainer &container);

	IndexType acceptIndexCond(
			OpContext &cxt, const SQLExprs::IndexConditionList &targetCondList,
			util::Vector<uint32_t> &indexColumnList);

	void acceptSearchResult(
			OpContext &cxt, const SearchResult &result,
			const SQLExprs::IndexConditionList *targetCondList,
			ContainerRowScanner *scanner, OIdTable *oIdTable);

	static void acceptIndexInfo(
			OpContext &cxt, TransactionContext &txn, BaseContainer &container,
			const SQLExprs::IndexConditionList &targetCondList,
			const IndexInfo &info);

	void setIndexLost(OpContext &cxt);

	template<typename Container>
	static void scanRow(
			TransactionContext &txn, Container &container,
			const SearchResult &result, ContainerRowScanner &scanner);

	static FullContainerKey* predicateToContainerKey(
			SQLValues::ValueContext &cxt, TransactionContext &txn,
			DataStoreV4 &dataStore, const Expression *pred,
			const ContainerLocation &location, util::String &dbNameStr);

	static const MetaContainerInfo& resolveMetaContainerInfo(ContainerId id);

	void updateAllContextRef(OpContext &cxt);
	void clearAllContextRef();

	RowIdFilter* checkRowIdFilter(
			OpContext &cxt, TransactionContext& txn, BaseContainer &container,
			bool readOnly);
	void finishRowIdFilter(RowIdFilter *rowIdFilter);

	OIdTable* findOIdTable();
	OIdTable& resolveOIdTable(OpContext &cxt, MergeMode::Type mode);

	RowIdFilter* findRowIdFilter();
	RowIdFilter& resolveRowIdFilter(OpContext &cxt, RowId maxRowId);

	Accessor &accessor_;
	Data &data_;
};

struct SQLContainerImpl::CursorImpl::Data {
	Data(SQLValues::VarAllocator &varAlloc, Accessor &accessor);

	uint32_t outIndex_;
	ContainerId nextContainerId_;

	int64_t totalSearchCount_;
	uint64_t lastPartialExecSize_;

	ContainerRowScanner *scanner_;
	util::LocalUniquePtr<UpdatorList> updatorList_;

	util::AllocUniquePtr<OIdTable> oIdTable_;
	util::AllocUniquePtr<RowIdFilter> rowIdFilter_;

	ContextRefList cxtRefList_;
	ExprContextRefList exprCxtRefList_;
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

	virtual void setIndexSelection(const SQLExprs::IndexSelector &selector);
	virtual const SQLExprs::IndexSelector& getIndexSelection();

	virtual void setRowIdFiltering();
	virtual bool isRowIdFiltering();

	virtual void setMergeMode(MergeMode::Type mode);

	const ContainerLocation& getLocation();
	MergeMode::Type getMergeMode();
	int64_t getIndexLimit();
	int64_t getMemoryLimit();
	const std::pair<uint64_t, uint64_t>& getPartialExecSizeRange();

	void startScope(ExtOpContext &cxt, const TupleColumnList &inColumnList);
	void finishScope() throw();
	CursorScope& resolveScope();

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

	static void checkInputInfo(
			const TupleColumnList &list, const BaseContainer &container);

	static TupleColumnType resolveColumnType(
			const ColumnInfo &columnInfo, bool used, bool withStats);
	static ContainerColumn resolveColumn(
			BaseContainer &container, bool forId, uint32_t pos);
	bool isColumnNonNullable(uint32_t pos);
	bool updateNullsStats(
			util::StackAllocator &alloc, const BaseContainer &container);

	TupleUpdateRowIdHandler* prepareUpdateRowIdHandler(
			TransactionContext &txn, BaseContainer &container,
			ResultSet &resultSet);

	void acceptIndexLost();

	void initializeScanRange(
			TransactionContext &txn, BaseContainer &container);
	RowId getMaxRowId(
			TransactionContext& txn, BaseContainer &container);

	static void assignIndexInfo(
			TransactionContext &txn, BaseContainer &container,
			IndexSelector &indexSelector, const TupleColumnList &coumnList);
	static void getIndexInfoList(
			TransactionContext &txn, BaseContainer &container,
			util::Vector<IndexInfo> &indexInfoList);

	void restoreRowIdFilter(RowIdFilter &rowIdFilter);
	void finishRowIdFilter(RowIdFilter *rowIdFilter);

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
	MergeMode::Type mergeMode_;

	bool closed_;
	bool rowIdFiltering_;
	bool resultSetPreserving_;
	bool containerExpired_;
	bool indexLost_;
	bool resultSetLost_;
	bool nullsStatsChanged_;

	ResultSetId resultSetId_;
	ResultSetHolder resultSetHolder_;

	PartitionId lastPartitionId_;
	DataStoreV4 *lastDataStore_;
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
	util::AllocUniquePtr<TupleUpdateRowIdHandler> updateRowIdHandler_;

	NullsStats initialNullsStats_;
	NullsStats lastNullsStats_;
	int64_t indexLimit_;
	int64_t memLimit_;
	std::pair<uint64_t, uint64_t> partialExecSizeRange_;

	const SQLExprs::IndexSelector *indexSelection_;
	util::LocalUniquePtr<uint32_t> rowIdFilterId_;
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

template<RowArrayType RAType, typename T, typename S, bool Nullable>
class SQLContainerImpl::ColumnEvaluator {
private:
	typedef SQLValues::ValueUtils ValueUtils;
	typedef SQLExprs::ExprContext ExprContext;

	typedef typename T::ValueType ValueType;
	typedef typename S::ValueType SourceType;

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
		typedef ValueTypeTag ResultTypeTag;

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
		typedef ValueTypeTag ResultTypeTag;

		typedef ColumnEvaluator<
				SUB_RA_TYPE, ResultTypeTag, ValueTypeTag,
				SUB_NULLABLE> Evaluator;
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
		typedef ValueTypeTag ResultTypeTag;

		typedef ColumnEvaluator<
				SUB_RA_TYPE, ResultTypeTag, ValueTypeTag,
				SUB_NULLABLE> Evaluator;
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

		typedef ColumnEvaluator<
				SUB_RA_TYPE, CompTypeTag, SourceType, SUB_NULLABLE> Arg1;
		typedef SQLCoreExprs::ConstEvaluator<CompTypeTag> Arg2;

		typedef typename Arg1::CodeType Code1;
		typedef typename Arg2::CodeType Code2;

		typedef typename SQLValues::ValueBasicComparator::TypeAt<
				CompTypeTag> TypedComparator;

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

struct SQLContainerImpl::SearchResult {
public:
	typedef util::XArray<OId> OIdList;
	typedef std::pair<OIdList*, OIdList*> OIdListRef;

	struct Entry {
	public:
		explicit Entry(util::StackAllocator &alloc);

		bool isEntryEmpty() const;
		size_t getEntrySize() const;

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

	Entry& getEntry(bool onMvcc);
	const Entry& getEntry(bool onMvcc) const;

	OIdListRef getOIdListRef(bool forRowArray);

private:
	Entry normalEntry_;
	Entry mvccEntry_;
};

class SQLContainerImpl::CursorScope {
public:
	CursorScope(
			ExtOpContext &cxt, const TupleColumnList &inColumnList,
			CursorAccessorImpl &accessor);
	~CursorScope();

	TransactionContext& getTransactionContext();
	ResultSet& getResultSet();

	int64_t getStartPartialExecCount();
	uint64_t getElapsedNanos();

private:
	typedef DataStoreBase::Scope DataStoreScope;

	void initialize(ExtOpContext &cxt);
	void clear(bool force);
	void clearResultSet(bool force);

	static util::Mutex* getPartitionLock(
			TransactionContext &txn, ExtOpContext &cxt);
	static const DataStoreScope* getStoreScope(
			TransactionContext &txn, ExtOpContext &cxt,
			util::LocalUniquePtr<DataStoreScope> &scope);
	static BaseContainer* getContainer(
			TransactionContext &txn, ExtOpContext &cxt,
			const DataStoreScope *latch, const ContainerLocation &location,
			bool expired, ContainerType type);
	static BaseContainer* checkContainer(
			const TupleColumnList &inColumnList, BaseContainer *container,
			const ContainerLocation &location,
			SchemaVersionId schemaVersionId);
	static ResultSet* prepareResultSet(
			ExtOpContext &cxt, TransactionContext &txn, BaseContainer *container,
			util::LocalUniquePtr<ResultSetGuard> &rsGuard,
			const CursorAccessorImpl &accessor);
	static ResultSetId findResultSetId(ResultSet *resultSet);

	CursorAccessorImpl &accessor_;
	TransactionContext &txn_;
	TransactionService *txnSvc_;
	util::DynamicLockGuard<util::Mutex> partitionGuard_;
	util::LocalUniquePtr<DataStoreScope> scope_;
	StackAllocAutoPtr<BaseContainer> containerAutoPtr_;
	util::LocalUniquePtr<ResultSetGuard> rsGuard_;
	ResultSet *resultSet_;

	int64_t partialExecCount_;
	util::Stopwatch watch_;
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
			const IdStoreColumnList &columnList);

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
	bool updated_;
};

class SQLContainerImpl::OIdTable {
public:
	struct Source;
	class Reader;
	class Writer;

	typedef SearchResult::OIdList OIdList;
	typedef SearchResult::Entry SearchResultEntry;

	OIdTable(const Source &source);

	bool isEmpty() const;

	void addAll(const SearchResult &result);
	void addAll(const SearchResultEntry &entry, bool onMvcc);

	void popAll(SearchResult &result, uint64_t limit, uint32_t rowArraySize);
	void popAll(
			SearchResultEntry &entry, bool &onMvcc, uint64_t limit,
			uint32_t rowArraySize);

	void update(ResultSet::UpdateOperation op, OId oId);

	static void readAll(
			SearchResult &result, uint64_t limit, bool large, MergeMode::Type mode,
			uint32_t rowArraySize, Reader &reader);

	void load(Reader &reader);
	void save(Writer &writer);

	size_t getTotalAllocationSize();

private:
	enum {
		SUB_TABLE_COUNT = 2,
		ELEMENT_SET_ROWS_CAPACITY = 16
	};

	template<bool Mvcc, bool Large, MergeMode::Type Mode> struct OpFuncTraits;
	template<bool ByMvcc, bool ByMode> struct SwitcherTraits;
	struct SwitcherOption;
	class Switcher;

	class ReadAllOp;
	class LoadOp;
	class SaveOp;

	class AddAllOp;
	class PopAllOp;

	class AddOp;
	class RemoveOp;
	class RemoveChunkOp;

	typedef uint32_t Group;
	typedef uint32_t Element;
	typedef uint64_t Code;
	typedef int64_t SignedCode;

	struct CodeUtils;
	struct OIdConstantsBase;
	template<bool Large> struct OIdConstants;

	typedef util::Vector<Element> ElementList;

	struct ElementSet;

	struct GroupMapHasher {
		size_t operator()(Group key) const { return key; }
	};
	typedef std::equal_to<Group> GroupMapPred;
	typedef SQLAlgorithmUtils::HashMap<
			Group, ElementSet, GroupMapHasher, GroupMapPred> HashGroupMap;
	typedef util::Map<Group, ElementSet> TreeGroupMap;

	struct SubTable {
		SubTable();
		bool isSubTableEmpty() const;

		TreeGroupMap *treeMap_;
		HashGroupMap *hashMap_;
	};

	template<bool Ordering>
	struct MapByOrdering {
		enum {
			MAP_ORDERING = Ordering
		};
		typedef typename util::BoolType<MAP_ORDERING>::Result MapOrderingType;
		typedef typename util::Conditional<
				MAP_ORDERING, TreeGroupMap, HashGroupMap>::Type Type;
	};

	template<MergeMode::Type Mode>
	struct MapByMode {
		typedef MapByOrdering<(Mode != MergeMode::MODE_MIXED)> Base;
		enum {
			MAP_ORDERING = Base::MAP_ORDERING
		};
		typedef typename Base::MapOrderingType MapOrderingType;
		typedef typename Base::Type Type;
	};

	void add(OId oId, bool onMvcc);
	void remove(OId oId, bool onMvcc, bool forRa = false);
	void removeRa(OId oId, bool onMvcc);
	void removeChunk(OId oId, bool onMvcc);

	template<bool Mvcc, bool Large, MergeMode::Type Mode>
	static void readAllDetail(
			SearchResultEntry &entry, uint64_t limit, uint32_t rowArraySize,
			Reader &reader);
	template<bool Mvcc, bool Large, MergeMode::Type Mode>
	void loadDetail(typename MapByMode<Mode>::Type &map, Reader &reader);
	template<bool Mvcc, bool Large, MergeMode::Type Mode>
	void saveDetail(typename MapByMode<Mode>::Type &map, Writer &writer);

	static bool isForRaWithMode(bool forRa, MergeMode::Type mode);
	template<MergeMode::Type Mode>
	static bool isForRaWithMode(bool forRa);
	template<MergeMode::Type Mode>
	static bool isRaMerging(bool forRa);

	template<bool Mvcc, bool Large, MergeMode::Type Mode>
	static bool readTopElement(Reader &reader, bool &forRa, Code &code);
	template<bool Large, MergeMode::Type Mode>
	static bool readNextElement(
			Reader &reader, bool forRa, Code lastMasked, Code &lastCode);

	template<bool Mvcc, bool Large, MergeMode::Type Mode>
	static void writeGroupHead(Writer &writer, bool forRa, Group group);

	template<bool Large, bool Ordering>
	void addAllDetail(
			typename MapByOrdering<Ordering>::Type &map,
			const SearchResultEntry &entry);
	template<bool Large, bool Ordering>
	void addAllDetail(
			typename MapByOrdering<Ordering>::Type &map,
			const OIdList &list, bool forRa);

	template<bool Large, bool Ordering>
	void addDetail(
			typename MapByOrdering<Ordering>::Type &map, OId oId, bool forRa);
	template<bool Large, bool Ordering>
	void addDetail(typename MapByOrdering<Ordering>::Type &map, OId oId);
	template<bool Large, bool Ordering>
	void addRaDetail(typename MapByOrdering<Ordering>::Type &map, OId oId);

	template<bool Large, bool Ordering>
	void popAllDetail(
			typename MapByOrdering<Ordering>::Type &map,
			SearchResultEntry &entry, uint64_t limit, uint32_t rowArraySize);
	template<bool Large, bool Ordering>
	void removeDetail(
			typename MapByOrdering<Ordering>::Type &map, OId oId, bool forRa);
	template<bool Large, bool Ordering>
	void removeChunkDetail(
			typename MapByOrdering<Ordering>::Type &map, OId oId);

	static void elementSetToRa(ElementSet &elemSet);

	template<typename Map>
	ElementSet& insertGroup(Map &map, Group group, bool forRa);

	bool insertElement(ElementSet &elemSet, Element elem);
	static bool eraseElement(ElementSet &elemSet, Element elem);

	void prepareToAdd(const SearchResultEntry &entry);
	void prepareToAdd(OId oId);

	TreeGroupMap& getGroupMap(bool onMvcc, const util::TrueType&);
	HashGroupMap& getGroupMap(bool onMvcc, const util::FalseType&);

	Element resolveCategory();

	SwitcherOption toSwitcherOption(bool onMvcc);

	static uint64_t getEntrySize(
			const SearchResultEntry &entry, uint32_t rowArraySize);
	static uint64_t getEntrySize(
			bool forRa, size_t elemCount, uint32_t rowArraySize);

	SQLOps::OpAllocatorManager &allocManager_;
	SQLOps::OpStore::AllocatorRef allocRef_;
	util::StackAllocator &alloc_;

	bool large_;
	MergeMode::Type mode_;
	int64_t category_;
	SubTable subTables_[SUB_TABLE_COUNT];
};

struct SQLContainerImpl::OIdTable::Source {
	Source();

	static Source ofContext(OpContext &cxt, MergeMode::Type mode);
	static bool detectLarge(DataStoreV4 *dataStore);

	SQLOps::OpAllocatorManager *allocManager_;
	util::StackAllocator::BaseAllocator *baseAlloc_;
	bool large_;
	MergeMode::Type mode_;
};

class SQLContainerImpl::OIdTable::Reader {
public:
	typedef SQLOps::TupleListReader BaseReader;
	typedef SQLOps::TupleColumn TupleColumn;
	typedef SQLOps::TupleColumnList TupleColumnList;

	Reader(BaseReader &base, const TupleColumnList &columnList);

	bool exists();
	void next();
	Code getCode();

private:
	BaseReader &base_;
	const TupleColumn &column_;
};

class SQLContainerImpl::OIdTable::Writer {
public:
	typedef SQLOps::TupleListWriter BaseWriter;
	typedef SQLOps::TupleColumn TupleColumn;
	typedef SQLOps::TupleColumnList TupleColumnList;

	Writer(BaseWriter &base, const TupleColumnList &columnList);

	void writeCode(Code code);

private:
	BaseWriter &base_;
	const TupleColumn &column_;
};

template<bool ByMvcc, bool ByMode>
struct SQLContainerImpl::OIdTable::SwitcherTraits {
	enum {
		MVCC_SPECIFIC = ByMvcc,
		MODE_SPECIFIC = ByMode
	};
	struct MvccFilter {
		enum  {
			OPTION_SPECIFIC = MVCC_SPECIFIC
		};
		template<bool Mvcc> struct At {
			static const bool VALUE = (OPTION_SPECIFIC ? Mvcc : false);
		};
	};
	struct ModeFilter {
		enum  {
			OPTION_SPECIFIC = MODE_SPECIFIC
		};
		template<MergeMode::Type Mode> struct At {
			static const MergeMode::Type PARTIAL_SPECIFIC_VALUE =
					(Mode == MergeMode::MODE_MIXED ?
								Mode : MergeMode::END_MODE);
			static const MergeMode::Type VALUE =
					(OPTION_SPECIFIC ? Mode : PARTIAL_SPECIFIC_VALUE);
		};
	};
};

template<bool Mvcc, bool Large, SQLContainerUtils::ScanMergeMode::Type Mode>
struct SQLContainerImpl::OIdTable::OpFuncTraits {
	static const bool ON_MVCC = Mvcc;
	static const bool ON_LARGE = Large;
	static const MergeMode::Type MERGE_MODE = Mode;

	typedef typename MapByMode<Mode>::MapOrderingType MapOrderingType;
	static const bool MAP_ORDERING = MapByMode<Mode>::MAP_ORDERING;
};

struct SQLContainerImpl::OIdTable::SwitcherOption {
	SwitcherOption();

	OIdTable& getTable() const;

	bool onMvcc_;
	bool onLarge_;
	MergeMode::Type mode_;

	OIdTable *table_;
};

class SQLContainerImpl::OIdTable::Switcher {
public:
	template<typename Op> struct OpTraits {
		typedef void (Op::*FuncType)(const SwitcherOption&) const;
	};

	explicit Switcher(const SwitcherOption &option);

	template<typename Op>
	typename OpTraits<Op>::FuncType resolve() const;

private:
	template<typename Op, bool Mvcc>
	typename OpTraits<Op>::FuncType resolveDetail() const;
	template<typename Op, bool Mvcc, bool Large>
	typename OpTraits<Op>::FuncType resolveDetail() const;
	template<typename Op, bool Mvcc, bool Large, MergeMode::Type Mode>
	typename OpTraits<Op>::FuncType resolveDetail() const;

	SwitcherOption option_;
};

class SQLContainerImpl::OIdTable::ReadAllOp {
public:
	typedef SwitcherTraits<true, true> SwitcherTraitsType;

	ReadAllOp(
			SearchResultEntry &entry, uint64_t limit, uint32_t rowArraySize,
			Reader &reader);
	template<typename Traits> void execute(const SwitcherOption &option) const;

private:
	SearchResultEntry &entry_;
	uint64_t limit_;
	uint32_t rowArraySize_;
	Reader &reader_;
};

class SQLContainerImpl::OIdTable::LoadOp {
public:
	typedef SwitcherTraits<true, true> SwitcherTraitsType;

	explicit LoadOp(Reader &reader);
	template<typename Traits> void execute(const SwitcherOption &option) const;

private:
	Reader &reader_;
};

class SQLContainerImpl::OIdTable::SaveOp {
public:
	typedef SwitcherTraits<true, true> SwitcherTraitsType;

	explicit SaveOp(Writer &writer);
	template<typename Traits> void execute(const SwitcherOption &option) const;

private:
	Writer &writer_;
};

class SQLContainerImpl::OIdTable::AddAllOp {
public:
	typedef SwitcherTraits<false, false> SwitcherTraitsType;

	explicit AddAllOp(const SearchResultEntry &entry);
	template<typename Traits> void execute(const SwitcherOption &option) const;

private:
	const SearchResultEntry &entry_;
};

class SQLContainerImpl::OIdTable::PopAllOp {
public:
	typedef SwitcherTraits<false, false> SwitcherTraitsType;

	PopAllOp(SearchResultEntry &entry, uint64_t limit, uint32_t rowArraySize);
	template<typename Traits> void execute(const SwitcherOption &option) const;

private:
	SearchResultEntry &entry_;
	uint64_t limit_;
	uint32_t rowArraySize_;
};

class SQLContainerImpl::OIdTable::AddOp {
public:
	typedef SwitcherTraits<false, false> SwitcherTraitsType;

	AddOp(OId oId, bool forRa);
	template<typename Traits> void execute(const SwitcherOption &option) const;

private:
	OId oId_;
	bool forRa_;
};

class SQLContainerImpl::OIdTable::RemoveOp {
public:
	typedef SwitcherTraits<false, false> SwitcherTraitsType;

	RemoveOp(OId oId, bool forRa);
	template<typename Traits> void execute(const SwitcherOption &option) const;

private:
	OId oId_;
	bool forRa_;
};

class SQLContainerImpl::OIdTable::RemoveChunkOp {
public:
	typedef SwitcherTraits<false, false> SwitcherTraitsType;

	explicit RemoveChunkOp(OId oId);
	template<typename Traits> void execute(const SwitcherOption &option) const;

private:
	OId oId_;
};

struct SQLContainerImpl::OIdTable::CodeUtils {
	static const uint32_t CODE_MVCC_FLAG_SHIFT_BIT =
			(CHAR_BIT * sizeof(Code) - 1);
	static const Code CODE_MVCC_FLAG =
			(static_cast<Code>(1) << CODE_MVCC_FLAG_SHIFT_BIT);

	static const uint32_t CODE_TO_OID_CHUNK_BIT = 1;

	template<bool Large> static OId toOId(
			Group group, Element elem, Element category);
	template<bool Large> static OId codeToOId(Code code);
	template<bool Large> static Group oIdToGroup(OId oId);
	template<bool Large> static Element oIdToElement(OId oId);
	template<bool Large> static Element oIdToRaElement(OId oId);
	static Element oIdToCategory(OId oId);

	template<bool Mvcc, bool Large> static Code toCode(
			Group group, Element elem, Element category);
	template<bool Large> static Code toMaskedCode(Code code);
	static Code toRaCode(Code code);

	template<bool Mvcc, bool Large, bool ForRa> static Code toGroupHead(Group group);
	template<bool Large> static bool isGroupHead(Code code);
	template<bool Large> static bool isRaHead(Code code);
	static bool isOnMvcc(Code code);

	template<bool Large> static Group toGroup(Code code);
	template<bool Large> static Element toElement(Code code);
	static Element toRaElement(Element elem);
};

struct SQLContainerImpl::OIdTable::OIdConstantsBase {

	static const int32_t MAGIC_NUMBER_EXP_SIZE = 3;
	static const uint64_t MAGIC_NUMBER = UINT64_C(0x000000000000a000);
	static const uint32_t UNIT_OFFSET_SHIFT_BIT = 16;
	static const uint32_t MAX_USER_BIT = 10;



	static const int32_t MAGIC_CATEGORY_EXP_SIZE =
			(UNIT_OFFSET_SHIFT_BIT - MAX_USER_BIT);
	static const int32_t CATEGORY_EXP_SIZE =
			(MAGIC_CATEGORY_EXP_SIZE - MAGIC_NUMBER_EXP_SIZE);

	static const uint32_t CODE_TO_OID_OFFSET_BIT = MAGIC_NUMBER_EXP_SIZE;
	static const uint32_t ELEMENT_TO_CODE_OFFSET_BIT = CATEGORY_EXP_SIZE;
	static const uint32_t ELEMENT_TO_OID_OFFSET_BIT = MAGIC_CATEGORY_EXP_SIZE;

	static const Element USER_MASK =
			~(~static_cast<Element>(0) << MAX_USER_BIT);
	static const Element CATEGORY_MASK =
			(~(~static_cast<Element>(0) << CATEGORY_EXP_SIZE) << MAX_USER_BIT);

	static const Element CATEGORY_USER_MASK = (CATEGORY_MASK | USER_MASK);
	static const Element ELEMENT_NON_USER_MASK = ~USER_MASK;
	static const Code CODE_NON_USER_MASK = ~static_cast<Code>(USER_MASK);
};

template<bool Large>
struct SQLContainerImpl::OIdTable::OIdConstants {

	static const uint32_t UNIT_CHUNK_SHIFT_BIT = (Large ? 38 : 32);


	static const int32_t CHUNK_EXP_SIZE =
			(CHAR_BIT * sizeof(OId) - UNIT_CHUNK_SHIFT_BIT);
	static const int32_t OFFSET_EXP_SIZE = (UNIT_CHUNK_SHIFT_BIT -
			OIdConstantsBase::MAGIC_CATEGORY_EXP_SIZE -
			OIdConstantsBase::MAX_USER_BIT);

	static const uint32_t CODE_BODY_SHIFT_BIT = UNIT_CHUNK_SHIFT_BIT - 2;
	static const uint32_t CODE_RA_SHIFT_BIT = CODE_BODY_SHIFT_BIT - 1;

	static const Code CODE_BODY_FLAG =
			(static_cast<Code>(1) << CODE_BODY_SHIFT_BIT);
	static const Code CODE_RA_FLAG =
			(static_cast<Code>(1) << CODE_RA_SHIFT_BIT);

	static const uint32_t GROUP_TO_OID_CHUNK_BIT = UNIT_CHUNK_SHIFT_BIT;
	static const uint32_t GROUP_TO_CODE_BIT =
			(GROUP_TO_OID_CHUNK_BIT - CodeUtils::CODE_TO_OID_CHUNK_BIT);

	static const Code CODE_GROUP_MASK =
			(~(~static_cast<Code>(0) << CHUNK_EXP_SIZE) <<
			GROUP_TO_CODE_BIT);
	static const Code CODE_OFFSET_MASK =
			(~(~static_cast<Code>(0) << OFFSET_EXP_SIZE) <<
			(OIdConstantsBase::UNIT_OFFSET_SHIFT_BIT -
			OIdConstantsBase::CODE_TO_OID_OFFSET_BIT));

	static const OId OID_OFFSET_MASK =
			(~(~static_cast<OId>(0) << OFFSET_EXP_SIZE) <<
			OIdConstantsBase::UNIT_OFFSET_SHIFT_BIT);

	static const Code CODE_MVCC_GROUP_MASK =
			(CodeUtils::CODE_MVCC_FLAG | CODE_GROUP_MASK);
};


struct SQLContainerImpl::OIdTable::ElementSet {
	ElementSet(util::StackAllocator &alloc, bool forRa);

	void initializeElements();

	bool isForRa() const;
	void setForRa();
	bool acceptRows();

	uint32_t rowsRest_;
	ElementList elems_;

private:
	ElementSet& operator=(const ElementSet&);

};

class SQLContainerImpl::RowIdFilter {
public:
	RowIdFilter(
			SQLOps::OpAllocatorManager &allocManager,
			util::StackAllocator::BaseAllocator &baseAlloc, RowId maxRowId);

	bool accept(RowId rowId);

	void load(OpContext::Source &cxtSrc, uint32_t input);
	uint32_t save(OpContext::Source &cxtSrc);

	void load(TupleListReader &reader);
	void save(TupleListWriter &writer);

	void setReadOnly();

private:
	enum {
		BITS_KEY_COLUMN_POS = 0,
		BITS_VALUE_COLUMN_POS = 1,
		BITS_STORE_COLUMN_COUNT = 2
	};

	typedef SQLValues::Types::Long BitsKeyTag;
	typedef SQLValues::Types::Integer BitsValueTag;

	typedef TupleList::Column BitsStoreColumnList[BITS_STORE_COLUMN_COUNT];
	typedef SQLAlgorithmUtils::DenseBitSet<> BitSet;

	struct Constants {
		static const TupleColumnType COLUMN_TYPES[BITS_STORE_COLUMN_COUNT];
	};

	static void getBitsStoreColumnList(BitsStoreColumnList &columnList);

	SQLOps::OpAllocatorManager &allocManager_;
	SQLOps::OpStore::AllocatorRef allocRef_;
	util::StackAllocator &alloc_;

	RowId maxRowId_;
	BitSet rowIdBits_;
	bool readOnly_;
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
