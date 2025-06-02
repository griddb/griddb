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

#ifndef SQL_PROCESSOR_DQL_H_
#define SQL_PROCESSOR_DQL_H_

#include "sql_processor.h"

#include "sql_operator.h"
#include "sql_operator_utils.h"
#include "sql_compiler.h"
#include "json.h"
#include "picojson.h"

struct DQLProcs {
	typedef SQLProcessor::TupleInfo TupleInfo;
	typedef SQLProcessor::Option ProcInOption;
	typedef SQLProcessor::OutOption ProcOutOption;
	typedef SQLProcessor::Plan ProcPlan;

	typedef SQLExprs::Expression Expression;
	typedef SQLExprs::ExprType ExprType;
	typedef SQLExprs::ExprRewriter ExprRewriter;

	typedef util::Vector<uint32_t> ColumnPosList;

	class ProcRegistrar;
	class ProcRegistrarTable;

	class ProcResourceSet;
	class ExtProcContext;

	class ProcSimulator;

	class OptionInput;
	class OptionOutput;

	class Option;

	class CommonOption;
	class SubOption;
	template<typename T> class BasicSubOption;

	class GroupOption;
	class JoinOption;
	class LimitOption;
	class ScanOption;
	class SelectOption;
	class SortOption;
	class UnionOption;

	struct SortColumn;
	typedef util::Vector<SortColumn> SortColumnList;

	struct CompilerUtils;
};

class DQLProcessor : public SQLProcessor {
public:
	DQLProcessor(Context &cxt, const TypeInfo &typeInfo);
	virtual ~DQLProcessor();

	virtual bool pipe(Context &cxt, InputId inputId, const Block &block);
	virtual bool finish(Context &cxt, InputId inputId);
	virtual bool next(Context &cxt);

	virtual bool applyInfo(
			Context &cxt, const Option &option,
			const TupleInfoList &inputInfo, TupleInfo &outputInfo);

	virtual const Profiler& getProfiler();
	virtual void setProfiler(const Profiler &profiler);

	virtual void exportTo(Context &cxt, const OutOption &option) const;

	static void transcodeProfilerSpecific(
			util::StackAllocator &alloc, const TypeInfo &typeInfo,
			util::AbstractObjectInStream &in,
			util::AbstractObjectOutStream &out);

	static bool isDQL(SQLType::Id type);

	static const DQLProcs::ProcRegistrarTable& getRegistrarTable();

private:
	typedef DQLProcs::ProcRegistrar ProcRegistrar;
	typedef DQLProcs::ProcRegistrarTable ProcRegistrarTable;

	typedef SQLOps::OpStoreId OpStoreId;
	typedef SQLOps::OpCursor OpCursor;
	typedef SQLOps::OpStore OpStore;
	typedef SQLOps::OpCode OpCode;
	typedef SQLOps::OpAllocatorManager OpAllocatorManager;

	typedef util::AllocVector<TupleList::TupleColumnType> AllocTupleInfo;
	typedef util::AllocVector<AllocTupleInfo> AllocTupleInfoList;
	typedef util::AllocXArray<uint8_t> OptionImage;

	struct InfoEntry {
		InfoEntry(const util::StdAllocator<void, void> &alloc);

		util::LocalUniquePtr<TupleList::Group> storeGroup_;
		OptionImage optionImage_;
		AllocTupleInfoList inputInfo_;
		AllocTupleInfo outputInfo_;
	};

	static const ProcRegistrar REGISTRAR_LIST[];
	static const ProcRegistrarTable REGISTRAR_TABLE;

	OpStore::Entry& getInputEntry(Context &cxt, InputId inputId);
	OpStore::Entry* findInputEntry(Context &cxt, InputId inputId);

	OpCursor::Source getCursorSource(Context &cxt);
	OpCursor::Source getCursorSourceForNext(Context &cxt);
	OpCursor::Source getCursorSourceDetail(Context &cxt, bool forNext);

	OpStore& prepareStore(Context &cxt);
	SQLOps::ExtOpContext& getExtOpContext(Context &cxt);

	void loadOption(
			SQLValues::ValueContext &valueCxt, const OptionImage &image,
			DQLProcs::Option &option) const;

	static SQLValues::ValueContext::Source getValueContextSource(
			util::StackAllocator &alloc, SQLValues::VarAllocator &varAlloc,
			TupleList::Group &storeGroup,
			util::LocalUniquePtr<SQLValues::VarContext> &varCxt);

	const OpStoreId& getStoreRootId() const;

	static const SQLOps::OpCode& getRootCode(
			OpStore &store, const OpStoreId &rootId);

	static void setUpConfig(
			OpStore &store, const SQLProcessorConfig *config, bool merged);
	static void setUpRoot(
			OpStore &store, OpStore::Entry &rootEntry,
			const DQLProcs::Option &option,
			const AllocTupleInfoList &inputInfo);
	static void setUpInput(
			const SQLOps::OpCode &code, size_t inputCount,
			InputId &inputIdOffset);

	static SQLOps::OpCode createOpCode(
			util::StackAllocator &alloc, const DQLProcs::Option &option,
			const AllocTupleInfoList &inputInfo);
	static void getOutputInfo(
			const SQLOps::OpCode &code, TupleInfo &outputInfo);

	static bool isProfilerRequested(const Profiler &profiler);
	static bool matchProfilerResultType(const Profiler &profiler, bool asStream);

	util::AllocUniquePtr<InfoEntry> infoEntry_;
	util::LocalUniquePtr<OpAllocatorManager> allocManager_;
	util::LocalUniquePtr<OpStore> store_;
	OpStoreId storeRootId_;
	InputId inputIdOffset_;
	util::AllocUniquePtr<DQLProcs::ExtProcContext> extCxt_;

	Profiler profiler_;
	util::AllocUniquePtr<DQLProcs::ProcSimulator> simulator_;
};

struct SQLOpUtils::AnalysisPartialInfo {
	UTIL_OBJECT_CODER_MEMBERS(globalStatus_);

	SQLProcessorConfig::PartialStatus globalStatus_;
};

class DQLProcs::ProcRegistrar {
public:
	typedef SubOption* (*SubOptionFactoryFunc)(util::StackAllocator&);

	template<typename T> static ProcRegistrar of(SQLType::Id type);

	SQLType::Id getType() const;

	SubOptionFactoryFunc getSubOptionFactory() const;

private:
	ProcRegistrar(SQLType::Id type, SubOptionFactoryFunc subOptionFactory);

	template<typename T> static SubOption* createSubOption(
			util::StackAllocator &alloc);

	SQLProcessor::Factory::Registrar<DQLProcessor> base_;
	SQLType::Id type_;
	SubOptionFactoryFunc subOptionFactory_;
};

class DQLProcs::ProcRegistrarTable {
public:
	ProcRegistrarTable(const ProcRegistrar *list, size_t size);

	const ProcRegistrar& resolve(SQLType::Id type) const;
	const ProcRegistrar* find(SQLType::Id type) const;

private:
	const ProcRegistrar *list_;
	size_t size_;
};

class DQLProcs::ProcResourceSet : public ResourceSet {
public:
	ProcResourceSet();

	void setBase(SQLContext &baseCxt);

	virtual SQLService* getSQLService() const;
	virtual TransactionManager* getTransactionManager() const;
	virtual TransactionService* getTransactionService() const;
	virtual ClusterService* getClusterService() const;
	virtual PartitionTable* getPartitionTable() const;
	virtual PartitionList* getPartitionList() const;
	virtual const DataStoreConfig* getDataStoreConfig() const;

private:
	SQLContext *baseCxt_;
};

class DQLProcs::ExtProcContext : public SQLOps::ExtOpContext {
public:
	ExtProcContext();

	void setBase(SQLContext &baseCxt);

	virtual size_t findMaxStringLength();
	virtual int64_t getCurrentTimeMillis();
	virtual util::TimeZone getTimeZone();

	virtual void checkCancelRequest();
	virtual void transfer(TupleList::Block &block, uint32_t id);
	virtual void finishRootOp();

	virtual const Event* getEvent();
	virtual EventContext* getEventContext();
	virtual SQLExecutionManager* getExecutionManager();

	virtual const ResourceSet* getResourceSet();

	virtual bool isOnTransactionService();
	virtual double getStoreMemoryAgingSwapRate();
	virtual bool isAdministrator();

	virtual uint32_t getTotalWorkerId();
	virtual util::AllocatorLimitter* getAllocatorLimitter();

private:
	SQLContext& getBase();

	SQLContext *baseCxt_;
	ProcResourceSet resourceSet_;
};
class DQLProcs::ProcSimulator {
private:
	typedef SQLProcessorConfig::Manager Manager;
	typedef SQLOps::OpSimulator OpSimulator;

public:
	explicit ProcSimulator(Manager &manager);
	~ProcSimulator();

	SQLOps::OpSimulator* getOpSimulator();

	uint32_t checkPartialMonitor(bool forNext);
	SQLOpUtils::AnalysisPartialInfo* getPartialProfile();

	static SQLOps::OpSimulator* tryCreate(
			SQLProcessor::Context &cxt, Manager &manager,
			util::AllocUniquePtr<ProcSimulator> &simulator,
			SQLType::Id type, const SQLOps::OpPlan &plan);

	static Manager::SimulationEntry toBaseEntry(const OpSimulator::Entry &src);
	static OpSimulator::Entry toOpEntry(const Manager::SimulationEntry &src);

private:
	struct PartialInfo {
		PartialInfo();

		Manager::PartialId id_;
		SQLOpUtils::AnalysisPartialInfo profile_;
		bool pretendSuspendLast_;
	};

	static bool tryCreateOpSimulator(
			SQLProcessor::Context &cxt, Manager &manager,
			util::AllocUniquePtr<ProcSimulator> &simulator,
			SQLType::Id type, const SQLOps::OpPlan &plan);
	static bool tryCreatePartialMonitor(
			SQLProcessor::Context &cxt, Manager &manager,
			util::AllocUniquePtr<ProcSimulator> &simulator,
			SQLType::Id type, const SQLOps::OpPlan &plan);

	util::LocalUniquePtr<SQLOps::OpSimulator> opSimulator_;
	util::LocalUniquePtr<PartialInfo> partialInfo_;
	Manager &manager_;
};

class DQLProcs::OptionInput {
public:
	OptionInput(
			SQLValues::ValueContext &valueCxt, const ProcRegistrarTable &table,
			SQLType::Id type, const ProcInOption *procOption);

	util::StackAllocator& getAllocator();
	const ProcRegistrarTable& getRegistrarTable();

	SQLType::Id getType();

	template<typename T> void decode(T &target);

	ProcPlan::Node::Id resolvePlanNodeId() const;
	const ProcPlan::Node& resolvePlanNode() const;
	const ProcPlan::Node* findPlanNode() const;

	SQLExprs::SyntaxExprRewriter createPlanRewriter();

private:
	typedef util::ObjectCoder::AllocCoder<util::StackAllocator> AllocCoder;
	typedef SQLExprs::Expression::Coder<AllocCoder> Coder;

	struct JsonStream {
		explicit JsonStream(const picojson::value &value);

		typedef JsonUtils::InStream JsonInStream;
		typedef JsonInStream::ObjectScope JsonInStreamScope;

		JsonInStream stream_;
		JsonInStreamScope objectScope_;
		util::AbstractObjectInStream::Wrapper<
				JsonInStreamScope::Stream> wrappedStream_;
	};

	void setUpStream();
	Coder makeCoder(SQLExprs::ExprFactoryContext &cxt);

	SQLValues::ValueContext &valueCxt_;
	const ProcRegistrarTable &table_;
	SQLType::Id type_;

	const ProcInOption *procOption_;
	util::AbstractObjectInStream *stream_;

	util::LocalUniquePtr<JsonStream> jsonStream_;
};

class DQLProcs::OptionOutput {
public:
	OptionOutput(const ProcOutOption *procOption, SQLType::Id type);

	util::StackAllocator& getAllocator();
	SQLType::Id getType();

	template<typename T> void encode(const T &target);

	ProcPlan::Node& resolvePlanNode();
	ProcPlan::Node* findPlanNode();

	SQLExprs::SyntaxExprRewriter createPlanRewriter();

private:
	typedef SQLExprs::Expression::Coder<util::ObjectCoder> Coder;

	Coder makeCoder();

	util::LocalUniquePtr<SQLValues::ValueContext> valueCxt_;
	const ProcOutOption *procOption_;
	SQLType::Id type_;
};

class DQLProcs::Option {
public:
	Option();

	void importFrom(OptionInput &in);
	void exportTo(OptionOutput &out);

	SQLOps::OpCode toCode(SQLOps::OpCodeBuilder &builder) const;

	static bool isInputIgnorable(const SQLOps::OpCode &code);

private:
	typedef BasicSubOption<CommonOption> Common;

	static SubOption* createSpecific(
			util::StackAllocator &alloc, SQLType::Id type);
	template<typename T>
	static SubOption* createSpecificAs(util::StackAllocator &alloc);

	Common *common_;
	SubOption *specific_;
};

class DQLProcs::SubOption {
public:
	virtual void importFrom(OptionInput &in) = 0;
	virtual void exportTo(OptionOutput &out) const = 0;

	virtual void toCode(
			SQLOps::OpCodeBuilder &builder, SQLOps::OpCode &code) const = 0;
};

template<typename T>
class DQLProcs::BasicSubOption : public DQLProcs::SubOption {
public:
	explicit BasicSubOption(util::StackAllocator &alloc);

	virtual void importFrom(OptionInput &in);
	virtual void exportTo(OptionOutput &out) const;

	virtual void toCode(
			SQLOps::OpCodeBuilder &builder, SQLOps::OpCode &code) const;

private:
	T optionValue_;
};

class DQLProcs::CommonOption {
public:
	explicit CommonOption(util::StackAllocator &alloc);

	void fromPlanNode(OptionInput &in);
	void toPlanNode(OptionOutput &out) const;

	void toCode(SQLOps::OpCodeBuilder &builder, SQLOps::OpCode &code) const;

	UTIL_OBJECT_CODER_PARTIAL_OBJECT;
	UTIL_OBJECT_CODER_MEMBERS(
			UTIL_OBJECT_CODER_OPTIONAL(id_, 0U),
			UTIL_OBJECT_CODER_OPTIONAL(limit_, DEFAULT_TUPLE_LIMIT),
			UTIL_OBJECT_CODER_ENUM(
					phase_, SQLType::AGG_PHASE_ALL_PIPE, SQLType::Coder()),
			outputInfo_);

private:
	static const int64_t DEFAULT_TUPLE_LIMIT;

	uint32_t id_;
	int64_t limit_;
	SQLType::AggregationPhase phase_;
	TupleInfo *outputInfo_;
};

class DQLProcs::GroupOption {
public:
	explicit GroupOption(util::StackAllocator &alloc);

	void fromPlanNode(OptionInput &in);
	void toPlanNode(OptionOutput &out) const;

	void toCode(SQLOps::OpCodeBuilder &builder, SQLOps::OpCode &code) const;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_PARTIAL_OBJECT;

	UTIL_OBJECT_CODER_MEMBERS(
			columnList_, groupColumns_, nestLevel_, pred_);

private:
	Expression *columnList_;
	ColumnPosList groupColumns_;
	uint32_t nestLevel_;
	Expression *pred_;
};

class DQLProcs::JoinOption {
public:
	explicit JoinOption(util::StackAllocator &alloc);

	void fromPlanNode(OptionInput &in);
	void toPlanNode(OptionOutput &out) const;

	void toCode(SQLOps::OpCodeBuilder &builder, SQLOps::OpCode &code) const;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_PARTIAL_OBJECT;

	UTIL_OBJECT_CODER_MEMBERS(
			columnList_,
			UTIL_OBJECT_CODER_ENUM(joinType_, SQLType::Coder()),
			UTIL_OBJECT_CODER_ENUM(joinOp_, SQLType::Coder()),
			joinColumns_, pred_, joinCond_);

private:
	Expression *columnList_;
	SQLType::JoinType joinType_;
	ExprType joinOp_;
	ColumnPosList joinColumns_;
	Expression *pred_;
	Expression *joinCond_;
};

class DQLProcs::LimitOption {
public:
	explicit LimitOption(util::StackAllocator &alloc);

	void fromPlanNode(OptionInput &in);
	void toPlanNode(OptionOutput &out) const;

	void toCode(SQLOps::OpCodeBuilder &builder, SQLOps::OpCode &code) const;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_PARTIAL_OBJECT;

	UTIL_OBJECT_CODER_MEMBERS(offset_);

private:
	int64_t offset_;
};

class DQLProcs::ScanOption {
public:
	explicit ScanOption(util::StackAllocator &alloc);

	void fromPlanNode(OptionInput &in);
	void toPlanNode(OptionOutput &out) const;

	void toCode(SQLOps::OpCodeBuilder &builder, SQLOps::OpCode &code) const;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_PARTIAL_OBJECT;

	UTIL_OBJECT_CODER_MEMBERS(location_, columnList_, pred_);

private:
	static SQLOps::ContainerLocation getLocation(
			util::StackAllocator &alloc, const ProcPlan::Node &node);

	SQLOps::ContainerLocation location_;
	Expression *columnList_;
	Expression *pred_;
};

class DQLProcs::SelectOption {
public:
	explicit SelectOption(util::StackAllocator &alloc);

	void fromPlanNode(OptionInput &in);
	void toPlanNode(OptionOutput &out) const;

	void toCode(SQLOps::OpCodeBuilder &builder, SQLOps::OpCode &code) const;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_PARTIAL_OBJECT;

	UTIL_OBJECT_CODER_MEMBERS(output_);

private:
	Expression *output_;
};

class DQLProcs::SortOption {
public:
	explicit SortOption(util::StackAllocator &alloc);

	void fromPlanNode(OptionInput &in);
	void toPlanNode(OptionOutput &out) const;

	void toCode(SQLOps::OpCodeBuilder &builder, SQLOps::OpCode &code) const;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_PARTIAL_OBJECT;

	UTIL_OBJECT_CODER_MEMBERS(
			UTIL_OBJECT_CODER_OPTIONAL(subOffset_, 0),
			UTIL_OBJECT_CODER_OPTIONAL(subLimit_, -1),
			orderColumns_,
			output_,
			UTIL_OBJECT_CODER_OPTIONAL(forWindow_, false),
			windowOption_);

private:
	static void planExprToOptions(
			const ProcPlan::Node::Expr &expr, SortColumnList &orderColumns,
			ProcPlan::Node::ExprList *windowOption);

	int64_t subOffset_;
	int64_t subLimit_;
	SortColumnList orderColumns_;
	Expression *output_;
	bool forWindow_;
	Expression *windowOption_;
};

class DQLProcs::UnionOption {
public:
	explicit UnionOption(util::StackAllocator &alloc);

	void fromPlanNode(OptionInput &in);
	void toPlanNode(OptionOutput &out) const;

	void toCode(SQLOps::OpCodeBuilder &builder, SQLOps::OpCode &code) const;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_PARTIAL_OBJECT;

	UTIL_OBJECT_CODER_MEMBERS(
			UTIL_OBJECT_CODER_ENUM(opType_, SQLType::Coder()));

private:
	SQLType::UnionType opType_;
};

struct DQLProcs::SortColumn {
	SortColumn();

	UTIL_OBJECT_CODER_MEMBERS(
			column_,
			UTIL_OBJECT_CODER_ENUM(order_, SQLType::Coder()));

	uint32_t column_;
	SQLType::OrderDirection order_;
};

struct DQLProcs::CompilerUtils {
	static SQLExprs::PlanPartitioningInfo* toPlanPartitioningInfo(
			const SQLTableInfo::PartitioningInfo *src,
			SQLExprs::PlanPartitioningInfo &dest);
	static SQLExprs::PlanPartitioningInfo toPlanPartitioningInfo(
			const SQLTableInfo::PartitioningInfo &src);

	static SQLCompiler::NarrowingKey toCompilerNarrowingKey(
			const SQLExprs::PlanNarrowingKey &src);

	static void applyCompileOption(
			SQLExprs::ExprContext &cxt,
			const SQLCompiler::CompileOption &option);
	static void applyCompileOption(
			SQLValues::ValueContext &cxt,
			const SQLCompiler::CompileOption &option);
};

#endif
