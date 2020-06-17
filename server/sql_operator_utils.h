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

#ifndef SQL_OPERATOR_UTILS_H_
#define SQL_OPERATOR_UTILS_H_

#include "sql_operator.h"
#include "sql_expression_utils.h"

class SQLExecutionManager;
class ResourceSet;

class SQLOps::ExtOpContext : public SQLValues::ExtValueContext {
public:
	virtual void checkCancelRequest() = 0;
	virtual void transfer(TupleList::Block &block, uint32_t id) = 0;
	virtual void finishRootOp() = 0;

	virtual const Event* getEvent() = 0;
	virtual EventContext* getEventContext() = 0;
	virtual SQLExecutionManager* getExecutionManager() = 0;

	virtual const ResourceSet* getResourceSet() = 0;

	virtual bool isOnTransactionService() = 0;
	virtual double getStoreMemoryAgingSwapRate() = 0;
	virtual void getConfig(OpConfig &config) = 0;
};

class SQLOps::OpCodeBuilder {
public:
	struct Source;

	explicit OpCodeBuilder(const Source &source);

	static Source ofAllocator(util::StackAllocator &alloc);
	static Source ofContext(OpContext &cxt);

	ProjectionFactoryContext& getProjectionFactoryContext();
	SQLExprs::ExprFactoryContext& getExprFactoryContext();
	SQLExprs::ExprRewriter& getExprRewriter();

	void applyJoinType(SQLType::JoinType type);
	static void applyJoinTypeTo(
			SQLType::JoinType type, SQLExprs::ExprFactoryContext &cxt);

	OpConfig& createConfig();

	ContainerLocation& createContainerLocation();
	SQLExprs::IndexConditionList& createIndexConditionList();

	SQLValues::CompColumnList& createCompColumnList();

	Projection& createProjection(
			SQLOpTypes::ProjectionType projType,
			const ProjectionCode &code = ProjectionCode());
	Expression& createExpression(
			SQLExprs::ExprType exprType,
			const SQLExprs::ExprCode &code = SQLExprs::ExprCode());

	Expression& createConstExpr(const TupleValue &value);
	Expression& createColumnExpr(uint32_t input, uint32_t column);

	Projection& toNormalProjectionByExpr(
			const Expression *src, SQLType::AggregationPhase aggrPhase);
	Projection& toGroupingProjectionByExpr(
			const Expression *src, SQLType::AggregationPhase aggrPhase);

	Projection& createIdenticalProjection(bool unified, uint32_t index);
	Projection& createProjectionByUsage(bool unified);

	Projection& rewriteProjection(const Projection &src);

	Projection& createFilteredProjection(Projection &src, OpCode &code);
	Projection& createFilteredProjection(
			Projection &src, const Expression *filterPred);

	Projection& createLimitedProjection(Projection &src, OpCode &code);

	std::pair<Projection*, Projection*> arrangeProjections(
			OpCode &code, bool withFilter, bool withLimit);

	OpCode toExecutable(const OpCode &src);

	void addColumnUsage(const OpCode &code, bool withId);

	static bool isInputUnified(const OpCode &code);

	static const Expression* findFilterPredicate(const Projection &src);

	static const Projection* findOutputProjection(const OpCode &code);
	static const Projection* findOutputProjection(const Projection &src);

	static void resolveColumnTypeList(
			const OpCode &code, ColumnTypeList &typeList);
	static void resolveColumnTypeList(
			const Projection &proj, ColumnTypeList &typeList);

private:
	void setUpExprFactoryContext(OpContext *cxt);

	util::StackAllocator& getAllocator();

	const ProjectionFactory& getProjectionFactory();
	const SQLExprs::ExprFactory& getExprFactory();

	ProjectionFactoryContext factoryCxt_;
	SQLExprs::ExprRewriter rewriter_;
};

struct SQLOps::OpCodeBuilder::Source {
	explicit Source(util::StackAllocator &alloc);

	util::StackAllocator &alloc_;
	OpContext *cxt_;
};

#endif
