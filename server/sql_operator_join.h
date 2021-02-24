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

#ifndef SQL_OPERATOR_JOIN_H_
#define SQL_OPERATOR_JOIN_H_

#include "sql_operator.h"
#include "sql_utils_algorithm.h"

struct SQLJoinOps {
	typedef SQLOps::TupleListReader TupleListReader;
	typedef SQLOps::TupleListWriter TupleListWriter;

	typedef SQLOps::SummaryTuple SummaryTuple;
	typedef SQLOps::WritableTuple WritableTuple;
	typedef SQLOps::TupleColumn TupleColumn;

	typedef SQLOps::OpCode OpCode;
	typedef SQLOps::OpNode OpNode;
	typedef SQLOps::OpPlan OpPlan;

	typedef SQLOps::OpContext OpContext;
	typedef SQLOps::OpCodeBuilder OpCodeBuilder;

	typedef SQLOps::Projection Projection;

	typedef SQLValues::SummaryTupleSet SummaryTupleSet;

	class Registrar;

	class Join;
	class JoinOuter;
	class JoinGraceHash;
	class JoinSorted;
	class JoinNested;
	class JoinCompMerge;
	class JoinOuterNested;
	class JoinOuterCompMerge;
	class JoinHash;

	struct JoinOuterContext;
	struct JoinHashContext;

	struct JoinInputOrdinals;
};

class SQLJoinOps::Registrar : public SQLOps::OpRegistrar {
public:
	virtual void operator()() const;

private:
	static const SQLOps::OpRegistrar REGISTRAR_INSTANCE;
};

class SQLJoinOps::Join : public SQLOps::Operator {
public:
	virtual void compile(OpContext &cxt) const;

	static bool isOnlyEqKeyFound(const SQLValues::CompColumnList *keyList);
	static int32_t detectDrivingInput(
			OpContext &cxt, const OpCode &code, bool *small, bool *empty);

	static JoinInputOrdinals getOrdinals(const OpCode &code);
	static bool checkJoinReady(OpContext &cxt, const JoinInputOrdinals &ordinals);

	static SQLValues::CompColumnList resolveKeyList(
			util::StackAllocator &alloc, const OpCode &code);
	static SQLOps::CompColumnListPair resolveSideKeyList(const OpCode &code);

	static SQLOps::CompColumnListPair* toSideKeyList(
			util::StackAllocator &alloc, const SQLValues::CompColumnList &src);
	static SQLValues::CompColumnList toSingleSideKeyList(
			util::StackAllocator &alloc, const SQLValues::CompColumnList &src,
			bool first);

	static void exrtactJoinKeyColumns(
			const SQLExprs::Expression *expr, SQLValues::CompColumnList &dest);
};

class SQLJoinOps::JoinOuter : public SQLOps::Operator {
public:
	virtual void compile(OpContext &cxt) const;
};

class SQLJoinOps::JoinGraceHash : public SQLOps::Operator {
public:
	virtual void compile(OpContext &cxt) const;

private:
	typedef std::pair<const Projection*, const Projection*> ProjectionRefPair;

	uint32_t getBucketCount(OpContext &cxt) const;

	static Projection& createBucketProjection(
			OpCodeBuilder &builder, bool first);
	static Projection& createUnionProjection(
			OpContext &cxt, const ProjectionRefPair &joinProjections);

	static ProjectionRefPair createJoinProjections(
			OpCodeBuilder &builder, const ProjectionRefPair &src,
			int32_t aggrMode);
	static ProjectionRefPair createMergeProjections(
			OpCodeBuilder &builder, const ProjectionRefPair &src,
			int32_t aggrMode);

	static int32_t setUpAggregationBuilding(
			OpCodeBuilder &builder, const ProjectionRefPair &src,
			SQLType::AggregationPhase srcPhase,
			SQLType::AggregationPhase &mergePhase);
};

class SQLJoinOps::JoinSorted : public SQLOps::Operator {
public:
	virtual void compile(OpContext &cxt) const;
};

class SQLJoinOps::JoinNested : public SQLOps::Operator {
public:
	virtual void compile(OpContext &cxt) const;
	virtual void execute(OpContext &cxt) const;
};

class SQLJoinOps::JoinCompMerge : public SQLOps::Operator {
public:
	virtual void compile(OpContext &cxt) const;
	virtual void execute(OpContext &cxt) const;

private:
	typedef SQLValues::TupleListReaderSource ReaderSourceType;
	typedef SQLValues::TupleRangeComparator TupleRangeComparator;
	typedef TupleRangeComparator::WithAccessor<
			true, SQLValues::ValueAccessor::ByReader> Comparator;
};

class SQLJoinOps::JoinOuterNested : public SQLOps::Operator {
public:
	virtual void compile(OpContext &cxt) const;
	virtual void execute(OpContext &cxt) const;
};

class SQLJoinOps::JoinOuterCompMerge : public SQLOps::Operator {
public:
	virtual void compile(OpContext &cxt) const;
	virtual void execute(OpContext &cxt) const;

private:
	typedef SQLValues::TupleListReaderSource ReaderSourceType;
	typedef SQLValues::TupleRangeComparator TupleRangeComparator;
	typedef TupleRangeComparator::WithAccessor<
			true, SQLValues::ValueAccessor::ByReader> Comparator;
};

class SQLJoinOps::JoinHash : public SQLOps::Operator {
public:
	struct TupleChain {
		TupleChain(const SummaryTuple &tuple, TupleChain *next);

		SummaryTuple tuple_;
		TupleChain *next_;
	};

	typedef SQLValues::TupleListReaderSource ReaderSourceType;

	typedef SQLValues::TupleNullChecker::WithAccessor<
			SQLValues::ValueAccessor::ByReader> NullChecker;
	typedef SQLValues::TupleDigester::WithAccessor<
			SQLValues::ValueAccessor::ByReader> TupleDigester;

	typedef SQLValues::TupleComparator::WithAccessor<
			std::equal_to<SQLValues::ValueComparator::PredArgType>,
			false, false, false, false,
			SQLValues::ValueAccessor::ByReader,
			SQLValues::ValueAccessor::BySummaryTuple> SelfTupleEq;
	typedef SQLValues::TupleComparator::WithAccessor<
			std::equal_to<SQLValues::ValueComparator::PredArgType>,
			false, true, false, false,
			SQLValues::ValueAccessor::ByReader,
			SQLValues::ValueAccessor::BySummaryTuple> PromoTupleEq;

	typedef SQLValues::DigestHasher MapHasher;
	typedef std::equal_to<int64_t> MapPred;
	typedef SQLAlgorithmUtils::HashMultiMap<
			int64_t, JoinHash::TupleChain, MapHasher, MapPred> Map;

	virtual void compile(OpContext &cxt) const;
	virtual void execute(OpContext &cxt) const;

	static TupleDigester createDigester(
			util::StackAllocator &alloc,
			const SQLValues::CompColumnList *bothKeyList,
			const SQLValues::CompColumnList *sideKeyList);

private:
	static JoinHashContext& prepareHashContext(
			OpContext &cxt, const SQLValues::CompColumnList &keyList,
			const SQLOps::CompColumnListPair &sideKeyList,
			TupleListReader &reader,
			const util::Vector<TupleColumn> &innerColumnList);

	static TupleChain* find(
			JoinHashContext &hashCxt, TupleListReader &reader);
};

struct SQLJoinOps::JoinOuterContext {
	JoinOuterContext() : matched_(false) {}
	bool matched_;
};

struct SQLJoinOps::JoinHashContext {
	JoinHashContext(
			util::StackAllocator &alloc,
			const SQLValues::CompColumnList &bothKeyList,
			const SQLValues::CompColumnList &drivingKeyList,
			util::AllocUniquePtr<SummaryTupleSet> &tupleSet);

	SQLValues::CompColumnList bothKeyList_;
	SQLValues::CompColumnList drivingKeyList_;

	JoinHash::Map map_;
	JoinHash::TupleChain *nextChain_;

	JoinHash::NullChecker nullChecker_;
	JoinHash::TupleDigester digester_;
	JoinHash::PromoTupleEq tupleEq_;

	util::AllocUniquePtr<SummaryTupleSet> tupleSet_;
};

struct SQLJoinOps::JoinInputOrdinals {
public:
	explicit JoinInputOrdinals(bool reversed) : reversed_(reversed) {}

	uint32_t driving() const { return reversed_ ? 1 : 0; }
	uint32_t inner() const { return reversed_ ? 0 : 1; }

private:
	bool reversed_;
};

#endif
