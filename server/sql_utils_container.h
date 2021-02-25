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

#ifndef SQL_UTILS_CONTAINER_H_
#define SQL_UTILS_CONTAINER_H_

#include "sql_operator.h"

class ResourceSet;
class Query;

struct SQLContainerUtils {
	typedef SQLExprs::Expression Expression;

	typedef SQLOps::ColumnTypeList ColumnTypeList;
	typedef SQLOps::OpContext OpContext;
	typedef SQLOps::Projection Projection;

	class ScanCursor;

	struct ContainerUtils;
};

class SQLContainerUtils::ScanCursor {
public:
	struct Source;
	class LatchTarget;

	static util::AllocUniquePtr<ScanCursor>::ReturnType create(
			util::StdAllocator<void, void> &alloc, const Source &source);
	virtual ~ScanCursor();

	virtual void unlatch() throw() = 0;
	virtual void close() throw() = 0;

	virtual bool scanFull(OpContext &cxt, const Projection &proj) = 0;
	virtual bool scanIndex(
			OpContext &cxt, const Projection &proj,
			const SQLExprs::IndexConditionList &condList) = 0;
	virtual bool scanUpdates(OpContext &cxt, const Projection &proj) = 0;
	virtual bool scanMeta(
			OpContext &cxt, const Projection &proj, const Expression *pred) = 0;

	virtual void getIndexSpec(
			OpContext &cxt, SQLExprs::IndexSelector &selector) = 0;
	virtual bool isIndexLost() = 0;

	virtual uint32_t getOutputIndex() = 0;
	virtual void setOutputIndex(uint32_t index) = 0;
};

struct SQLContainerUtils::ScanCursor::Source {
	Source(
			util::StackAllocator &alloc,
			const SQLOps::ContainerLocation &location,
			const ColumnTypeList *columnTypeList);

	util::StackAllocator &alloc_;
	SQLOps::ContainerLocation location_;
	const ColumnTypeList *columnTypeList_;
	int64_t indexLimit_;
};

class SQLContainerUtils::ScanCursor::LatchTarget :
		public SQLValues::BaseLatchTarget {
public:
	LatchTarget(ScanCursor *cursor = NULL);

	virtual void unlatch() throw();
	virtual void close() throw();

private:
	ScanCursor *cursor_;
};

struct SQLContainerUtils::ContainerUtils {
	static size_t findMaxStringLength(const ResourceSet *resourceSet);

	static bool toTupleColumnType(
			uint8_t src, bool nullable, TupleColumnType &dest,
			bool failOnUnknown);
	static int32_t toSQLColumnType(TupleColumnType type);

	static bool predicateToMetaTarget(
			SQLValues::ValueContext &cxt, const SQLExprs::Expression *pred,
			uint32_t partitionIdColumn, uint32_t containerNameColumn,
			uint32_t containerIdColumn, PartitionId partitionCount,
			PartitionId &partitionId, bool &placeholderAffected);

	static SQLExprs::SyntaxExpr tqlToPredExpr(
			util::StackAllocator &alloc, const Query &query);
};

#endif
