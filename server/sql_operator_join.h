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

struct SQLJoinOps {
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

	class Registrar;

	class JoinCompMergeContext;

	class Join;
	class JoinOuter;
	class JoinSorted;
	class JoinNested;
	class JoinCompMerge;
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
};

class SQLJoinOps::JoinOuter : public SQLOps::Operator {
public:
	virtual void compile(OpContext &cxt) const;
};

class SQLJoinOps::JoinSorted : public SQLOps::Operator {
public:
	virtual void compile(OpContext &cxt) const;
};

class SQLJoinOps::JoinNested : public SQLOps::Operator {
public:
	virtual void execute(OpContext &cxt) const;

private:
	enum {
		IN_LEFT = 0,
		IN_RIGHT = 1
	};
};

class SQLJoinOps::JoinCompMergeContext : public SQLOps::OpContext {
public:
};

class SQLJoinOps::JoinCompMerge : public SQLOps::Operator {
public:
	virtual void execute(OpContext &cxt) const;

private:
	enum {
		IN_LEFT = 0,
		IN_RIGHT = 1,
		IN_INSIDE_MAIN = 0,
		IN_INSIDE_SUB = 1
	};
};

#endif
