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

#ifndef SQL_OPERATOR_GROUP_H_
#define SQL_OPERATOR_GROUP_H_

#include "sql_operator.h"

#include "sql_utils_algorithm.h"

struct SQLGroupOps {
	typedef SQLOps::TupleListReader TupleListReader;
	typedef SQLOps::TupleListWriter TupleListWriter;

	typedef SQLOps::ReadableTuple ReadableTuple;
	typedef SQLOps::WritableTuple WritableTuple;
	typedef SQLOps::TupleColumn TupleColumn;

	typedef SQLOps::OpCode OpCode;
	typedef SQLOps::OpNode OpNode;
	typedef SQLOps::OpPlan OpPlan;

	typedef SQLOps::OpContext OpContext;
	typedef SQLOps::OpCodeBuilder OpCodeBuilder;

	typedef SQLOps::Projection Projection;

	typedef uint64_t UnionFlags;

	typedef SQLAlgorithmUtils::HeapQueue<
			SQLValues::ArrayTupleRef,
			SQLValues::TupleGreater> TupleHeapQueue;

	class Registrar;

	class Group;
	class GroupDistinct;
	class GroupFinish;
	class Union;
	class UnionAll;
	class UnionSorted;

	class GroupProjection;

	class UnionActionBase;
	class UnionDistinctAction;
	class UnionIntersectAction;
	class UnionExceptAction;
	class UnionCompensateAction;

	template<typename Action> class SortedUnionProjection;
	typedef SortedUnionProjection<UnionDistinctAction> UnionDistinctProjection;
	typedef SortedUnionProjection<UnionIntersectAction> UnionIntersectProjection;
	typedef SortedUnionProjection<UnionExceptAction> UnionExceptProjection;
	typedef SortedUnionProjection<
			UnionCompensateAction> UnionCompensateProjection;

	class SortedUnionContext;
};

class SQLGroupOps::Registrar : public SQLOps::OpProjectionRegistrar {
public:
	virtual void operator()() const;

private:
	static const SQLOps::OpProjectionRegistrar REGISTRAR_INSTANCE;
};


class SQLGroupOps::Group : public SQLOps::Operator {
public:
	virtual void compile(OpContext &cxt) const;

private:
	void setUpGroupProjections(
			OpCodeBuilder &builder,
			std::pair<Projection*, Projection*> &projections) const;
};

class SQLGroupOps::GroupDistinct : public SQLOps::Operator {
public:
	virtual void compile(OpContext &cxt) const;
};

class SQLGroupOps::Union : public SQLOps::Operator {
public:
	virtual void compile(OpContext &cxt) const;

private:
	void setUpUnionProjections(
			OpCodeBuilder &builder, SQLType::UnionType unionType,
			const SQLValues::CompColumnList *keyColumnList,
			std::pair<Projection*, Projection*> &projections) const;
};

class SQLGroupOps::UnionAll : public SQLOps::Operator {
public:
	virtual void execute(OpContext &cxt) const;
};

class SQLGroupOps::UnionSorted : public SQLOps::Operator {
public:
	virtual void execute(OpContext &cxt) const;

private:
	class UnionHeapQueue;
};

class SQLGroupOps::UnionSorted::UnionHeapQueue {
public:
	UnionHeapQueue(
			util::StackAllocator &alloc, const SQLValues::TupleGreater &pred);
	~UnionHeapQueue();

	TupleHeapQueue& build(OpContext &cxt);

private:
	static SQLValues::TupleGreater setUpPredicate(
			const SQLValues::TupleGreater &pred);

	util::StackAllocator &alloc_;
	TupleHeapQueue queue_;
	util::Vector<SQLValues::ArrayTuple*> list_;
};


class SQLGroupOps::GroupProjection : public SQLOps::Projection {
public:
	virtual void initializeProjectionAt(OpContext &cxt) const;

	virtual void project(OpContext &cxt) const;

private:
	enum {
		CHAIN_FINISH,
		CHAIN_PIPE
	};

	bool isFinishOnly() const;
};

class SQLGroupOps::UnionActionBase {
public:
	bool onDifferent(UnionFlags &flags) const;
	bool onFinish(UnionFlags &flags) const;
	bool onNonMatch(UnionFlags &flags, const UnionFlags &initialFlags) const;

	bool onTuple(UnionFlags &flags, uint32_t index) const;

	UnionFlags getInitialFlags(uint32_t inputCount) const;
};

class SQLGroupOps::UnionDistinctAction : public SQLGroupOps::UnionActionBase {
public:
	bool onNonMatch(UnionFlags &flags, const UnionFlags &initialFlags) const;
};

class SQLGroupOps::UnionIntersectAction : public SQLGroupOps::UnionActionBase {
public:
	bool onNonMatch(UnionFlags &flags, const UnionFlags &initialFlags) const;
	bool onTuple(UnionFlags &flags, uint32_t index) const;
	UnionFlags getInitialFlags(uint32_t inputCount) const;
};

class SQLGroupOps::UnionExceptAction : public SQLGroupOps::UnionActionBase {
public:
	bool onDifferent(UnionFlags &flags) const;
	bool onFinish(UnionFlags &flags) const;
	bool onNonMatch(UnionFlags &flags, const UnionFlags &initialFlags) const;
	bool onTuple(UnionFlags &flags, uint32_t index) const;
};

class SQLGroupOps::UnionCompensateAction :
		public SQLGroupOps::UnionActionBase {
public:
	bool onDifferent(UnionFlags &flags) const;
	bool onFinish(UnionFlags &flags) const;
	bool onNonMatch(UnionFlags &flags, const UnionFlags &initialFlags) const;
	bool onTuple(UnionFlags &flags, uint32_t index) const;
};

template<typename Action>
class SQLGroupOps::SortedUnionProjection : public SQLOps::Projection {
public:
	virtual void initializeProjectionAt(OpContext &cxt) const;

	virtual void project(OpContext &cxt) const;

private:
	bool isFinishOnly() const;

	Action action_;
};

class SQLGroupOps::SortedUnionContext {
public:
	SortedUnionContext(util::StackAllocator &alloc, UnionFlags initialFlags);

	UnionFlags getInitialFlags();
	UnionFlags& getFlags();

	SQLValues::ArrayTuple& getKey();
	SQLValues::ArrayTuple& getLastTuple();

private:
	SQLValues::ArrayTuple key_;
	SQLValues::ArrayTuple lastTuple_;
	UnionFlags initialFlags_;
	UnionFlags flags_;
};

#endif
