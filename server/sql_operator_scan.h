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
#include "sql_utils_container.h"

struct SQLScanOps {
	typedef SQLOps::TupleListReader TupleListReader;
	typedef SQLOps::TupleListWriter TupleListWriter;

	typedef SQLOps::ReadableTuple ReadableTuple;
	typedef SQLOps::WritableTuple WritableTuple;

	typedef SQLOps::OpCode OpCode;
	typedef SQLOps::OpNode OpNode;
	typedef SQLOps::OpPlan OpPlan;

	typedef SQLOps::OpContext OpContext;
	typedef SQLOps::OpCodeBuilder OpCodeBuilder;

	typedef SQLOps::ProjectionCode ProjectionCode;

	typedef SQLOps::Projection Projection;
	typedef SQLOps::ProjectionFactoryContext ProjectionFactoryContext;

	typedef SQLContainerUtils::ScanCursor ScanCursor;

	class Registrar;

	class BaseScanContainerOperator;

	class Scan;
	class ScanContainerFull;
	class ScanContainerIndex;
	class ScanContainerUpdates;
	class ScanContainerMeta;
	class Select;
	class SelectPipe;
	class Limit;

	class OutputProjection;

	class AggrPipeProjection;
	class AggrOutputProjection;

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
	ScanCursor& getScanCursor(OpContext &cxt, bool forCompiling = false) const;
	static void unbindScanCursor(OpContext *cxt, ScanCursor &cursor);
	static uint32_t getScanOutpuIndex(OpContext &cxt);
};


class SQLScanOps::Scan :
		public SQLScanOps::BaseScanContainerOperator {
public:
	virtual void compile(OpContext &cxt) const;

private:
	bool genScanIndex(OpContext &cxt) const;
	TupleList::Info getInputInfo(
			OpContext &cxt, SQLOps::ColumnTypeList &typeList) const;
};

class SQLScanOps::ScanContainerFull :
		public SQLScanOps::BaseScanContainerOperator {
public:
	virtual void execute(OpContext &cxt) const;
};

class SQLScanOps::ScanContainerIndex :
		public SQLScanOps::BaseScanContainerOperator {
public:
	virtual void execute(OpContext &cxt) const;
};

class SQLScanOps::ScanContainerUpdates :
		public SQLScanOps::BaseScanContainerOperator {
public:
	virtual void execute(OpContext &cxt) const;
};

class SQLScanOps::ScanContainerMeta :
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

	virtual void project(OpContext &cxt) const;

	virtual void projectBy(
			OpContext &cxt, const ReadableTuple &tuple) const;
	virtual void projectBy(
			OpContext &cxt, TupleListReader &reader, size_t index) const;

private:
	void projectInternal(OpContext &cxt, const ReadableTuple *srcTuple) const;

	util::Vector<SQLValues::ValueWriter*> writerList_;
	bool columExprOnly_;
};

class SQLScanOps::AggrPipeProjection : public SQLOps::Projection {
public:
	static SQLValues::ArrayTuple& getTuple(OpContext &cxt);

	virtual void initializeProjectionAt(OpContext &cxt) const;
	virtual void clearProjection(OpContext &cxt) const;

	virtual void project(OpContext &cxt) const;
};

class SQLScanOps::AggrOutputProjection : public SQLOps::Projection {
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

	virtual void project(OpContext &cxt) const;

	static std::pair<int64_t, int64_t> getLimits(
			const SQLOps::Projection &proj);
};

class SQLScanOps::LimitContext {
public:
	LimitContext(int64_t limit, int64_t offset);

	bool accept(bool &limited);

private:
	int64_t restLimit_;
	int64_t restOffset_;
};

class SQLScanOps::SubLimitProjection : public SQLOps::Projection {
public:
	virtual void initializeProjectionAt(OpContext &cxt) const;

	virtual void project(OpContext &cxt) const;
};

class SQLScanOps::SubLimitContext {
public:
	SubLimitContext(
			util::StackAllocator &alloc, int64_t limit, int64_t offset);

	bool accept();

	void clear();

	SQLValues::ArrayTuple& getKey();

private:
	const LimitContext initial_;
	LimitContext cur_;
	SQLValues::ArrayTuple key_;
};

#endif
