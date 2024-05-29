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

#ifndef SQL_OPERATOR_SCAN_H_
#define SQL_OPERATOR_SCAN_H_

#include "sql_operator.h"
#include "sql_operator_utils.h"
#include "sql_utils_container.h"

struct SQLScanOps {
	typedef SQLOps::TupleListReader TupleListReader;
	typedef SQLOps::TupleListWriter TupleListWriter;

	typedef SQLOps::SummaryTuple SummaryTuple;
	typedef SQLOps::ReadableTuple ReadableTuple;
	typedef SQLOps::WritableTuple WritableTuple;

	typedef SQLOps::ColumnTypeList ColumnTypeList;
	typedef SQLValues::TupleColumnList TupleColumnList;

	typedef SQLOps::DigestTupleListReader DigestTupleListReader;
	typedef SQLOps::DigestReadableTuple DigestReadableTuple;

	typedef SQLOps::OpCode OpCode;
	typedef SQLOps::OpNode OpNode;
	typedef SQLOps::OpPlan OpPlan;

	typedef SQLOps::OpContext OpContext;
	typedef SQLOps::OpCodeBuilder OpCodeBuilder;

	typedef SQLOps::ProjectionCode ProjectionCode;

	typedef SQLOps::Projection Projection;
	typedef SQLOps::ProjectionFactoryContext ProjectionFactoryContext;

	typedef SQLContainerUtils::ScanCursor ScanCursor;
	typedef SQLContainerUtils::ScanCursorAccessor ScanCursorAccessor;
	typedef SQLContainerUtils::IndexScanInfo IndexScanInfo;

	class Registrar;

	class BaseScanContainerOperator;

	class Scan;
	class ScanSemiFiltering;
	class ScanUnique;
	class ScanNoIndex;
	class ScanContainerFull;
	class ScanContainerRange;
	class ScanContainerIndex;
	class ScanContainerMeta;
	class ScanContainerVisited;
	class Select;
	class SelectPipe;
	class Limit;

	class OutputProjection;

	class AggrPipeProjection;
	class AggrOutputProjection;
	class MultiOutputProjection;

	class FilterProjection;

	class LimitProjection;
	class LimitContext;

	class SubLimitProjection;
	class SubLimitContext;
};

class SQLScanOps::Registrar : public SQLOps::OpProjectionRegistrar {
public:
	virtual void operator()() const;

private:
	static const SQLOps::OpProjectionRegistrar REGISTRAR_INSTANCE;
};


class SQLScanOps::BaseScanContainerOperator : public SQLOps::Operator {
protected:
	ScanCursorAccessor& getScanCursorAccessor(OpContext &cxt) const;
	util::AllocUniquePtr<ScanCursor>::ReturnType getScanCursor(
			OpContext &cxt) const;

	static void unbindScanCursor(OpContext *cxt, ScanCursor &cursor);
	static uint32_t getScanOutpuIndex(OpContext &cxt);
};


class SQLScanOps::Scan :
		public SQLScanOps::BaseScanContainerOperator {
public:
	virtual void compile(OpContext &cxt) const;

	static bool generateMainScanPlan(
			OpContext &cxt, ScanCursorAccessor &accessor,
			const OpCode &baseCode);

	static void generateEmptyPlan(OpContext &cxt);

	static void generateNoIndexedScanPlan(
			OpContext &cxt, const OpCode &baseCode);

private:
	enum {
		INPUT_JOIN_SCAN = 0,
		INPUT_JOIN_DRIVING = 1
	};

	struct ProjectionSet {
		ProjectionSet();

		OpCode arrangedCode_;
		SQLOpUtils::ProjectionPair projections_;
		const Projection *distinctProj_;
		const Projection *midInProj_;
	};

	static bool generateCheckedScanPlan(
			OpContext &cxt, ScanCursorAccessor &accessor,
			const OpCode &baseCode);

	static void generateIndexJoinPlan(OpContext &cxt, const OpCode &baseCode);

	static void generateIndexJoinDrivingNode(
			OpCodeBuilder &builder, const OpCode &baseCode,
			const SQLValues::CompColumnList *keyColumnList,
			const SQLExprs::Expression *baseSemiPred,
			SQLExprs::Expression *&semiPred, const OpNode *&drivingNode,
			OpPlan &plan);

	static bool generateIndexScanNodes(
			OpCodeBuilder &builder, ScanCursorAccessor &accessor,
			const OpCode &baseCode, const SQLExprs::IndexSelector &selector,
			const ProjectionSet &projSet, OpPlan &plan);
	static void generateNoIndexedScanNodes(
			bool indexPlanned, const OpCode &baseCode,
			const ProjectionSet &projSet, OpPlan &plan);

	static void setUpMainScanPlanBuilder(
			OpCodeBuilder &builder, const SQLExprs::IndexSelector *selector,
			bool indexPlanned, const OpCode &baseCode, ProjectionSet &projSet);
	static const SQLExprs::IndexSelector* selectIndex(
			OpContext &cxt, ScanCursorAccessor &accessor, const OpCode &baseCode,
			const OpPlan &plan, bool &indexPlanned);

	static bool isForMetaScan(const SQLOps::ContainerLocation &location);
	static bool isDrivingInputFound(const OpCode &baseCode);

	static TupleList::Info getInputInfo(
			OpContext &cxt, SQLOps::ColumnTypeList &typeList);
};

class SQLScanOps::ScanSemiFiltering :
		public SQLScanOps::BaseScanContainerOperator {
public:
	virtual void compile(OpContext &cxt) const;
};

class SQLScanOps::ScanUnique :
		public SQLScanOps::BaseScanContainerOperator {
public:
	virtual void compile(OpContext &cxt) const;

	static bool generateUniqueScanPlan(
			OpContext &cxt, ScanCursorAccessor &accessor,
			const OpCode &baseCode);

private:
	static bool detectRowDuplicatable(
			ScanCursorAccessor &accessor, bool &duplicatable);
};

class SQLScanOps::ScanNoIndex :
		public SQLScanOps::BaseScanContainerOperator {
public:
	virtual void compile(OpContext &cxt) const;

private:
	static bool detectIndexLoss(ScanCursorAccessor &accessor, bool &indexLost);
};

class SQLScanOps::ScanContainerFull :
		public SQLScanOps::BaseScanContainerOperator {
public:
	virtual void execute(OpContext &cxt) const;
};

class SQLScanOps::ScanContainerRange :
		public SQLScanOps::BaseScanContainerOperator {
public:
	virtual void execute(OpContext &cxt) const;
};

class SQLScanOps::ScanContainerIndex :
		public SQLScanOps::BaseScanContainerOperator {
public:
	virtual void execute(OpContext &cxt) const;

private:
	static void bindIndexCondition(
			TupleListReader &reader, const TupleColumnList &columnList,
			const SQLExprs::IndexSelector &selector,
			SQLExprs::IndexConditionList &condList,
			SQLValues::ValueSetHolder &valuesHolder);
};

class SQLScanOps::ScanContainerMeta :
		public SQLScanOps::BaseScanContainerOperator {
public:
	virtual void compile(OpContext &cxt) const;
	virtual void execute(OpContext &cxt) const;
};

class SQLScanOps::ScanContainerVisited :
		public SQLScanOps::BaseScanContainerOperator {
public:
	virtual void execute(OpContext &cxt) const;
};


class SQLScanOps::Select : public SQLOps::Operator {
public:
	virtual void compile(OpContext &cxt) const;
};

class SQLScanOps::SelectPipe : public SQLOps::Operator {
public:
	virtual void execute(OpContext &cxt) const;
};


class SQLScanOps::Limit : public SQLOps::Operator {
public:
	virtual void compile(OpContext &cxt) const;
};


class SQLScanOps::OutputProjection : public SQLOps::Projection {
public:
	OutputProjection(
			ProjectionFactoryContext &cxt, const ProjectionCode &code);

	virtual void initializeProjectionAt(OpContext &cxt) const;
	virtual void updateProjectionContextAt(OpContext &cxt) const;

	virtual void project(OpContext &cxt) const;

	virtual void projectBy(
			OpContext &cxt, const ReadableTuple &tuple) const;
	virtual void projectBy(
			OpContext &cxt, const DigestTupleListReader &reader) const;
	virtual void projectBy(
			OpContext &cxt, const SummaryTuple &tuple) const;
	virtual void projectBy(
			OpContext &cxt, TupleListReader &reader, size_t index) const;

private:
	SQLOpUtils::ExpressionListWriter writer_;
};

class SQLScanOps::AggrPipeProjection : public SQLOps::Projection {
public:
	virtual void project(OpContext &cxt) const;
};

class SQLScanOps::AggrOutputProjection : public SQLOps::Projection {
public:
	virtual void project(OpContext &cxt) const;
};

class SQLScanOps::MultiOutputProjection : public SQLOps::Projection {
public:
	virtual void project(OpContext &cxt) const;
};

class SQLScanOps::FilterProjection : public SQLOps::Projection {
public:
	virtual void project(OpContext &cxt) const;
};

class SQLScanOps::LimitProjection : public SQLOps::Projection {
public:
	virtual void initializeProjectionAt(OpContext &cxt) const;
	virtual void updateProjectionContextAt(OpContext &cxt) const;

	virtual void project(OpContext &cxt) const;

	static std::pair<int64_t, int64_t> getLimits(
			const SQLOps::Projection &proj);
};

class SQLScanOps::LimitContext {
public:
	enum Acceptance {
		ACCEPTABLE_CURRENT_AND_AFTER,
		ACCEPTABLE_CURRENT_ONLY,
		ACCEPTABLE_AFTER_ONLY,
		ACCEPTABLE_NONE
	};

	LimitContext(int64_t limit, int64_t offset);

	bool isAcceptable() const;
	Acceptance accept();
	int64_t update();

private:
	int64_t restLimit_;
	int64_t restOffset_;

	int64_t prevRest_;
};

class SQLScanOps::SubLimitProjection : public SQLOps::Projection {
public:
	virtual void initializeProjectionAt(OpContext &cxt) const;

	virtual void project(OpContext &cxt) const;
};

class SQLScanOps::SubLimitContext {
public:
	typedef SQLValues::TupleComparator::WithAccessor<
			std::equal_to<SQLValues::ValueComparator::PredArgType>,
			false, false, false, false,
			SQLValues::ValueAccessor::ByReadableTuple,
			SQLValues::ValueAccessor::ByKeyArrayTuple> TupleEq;

	SubLimitContext(
			util::StackAllocator &alloc, int64_t limit, int64_t offset,
			const TupleEq &pred);

	bool accept();

	void clear();

	SQLValues::ArrayTuple& getKey();

	const TupleEq& getPredicate() const;

private:

	const LimitContext initial_;
	LimitContext cur_;
	SQLValues::ArrayTuple key_;
	TupleEq pred_;
};

#endif
