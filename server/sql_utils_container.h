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

class TransactionContext;
class FullContainerKey;
class DataStoreV4;

struct SQLContainerUtils {
	typedef SQLValues::TupleColumnList TupleColumnList;

	typedef SQLExprs::Expression Expression;

	typedef SQLOps::ContainerLocation ContainerLocation;

	typedef SQLOps::ColumnTypeList ColumnTypeList;
	typedef SQLOps::OpStore OpStore;
	typedef SQLOps::OpContext OpContext;
	typedef SQLOps::Projection Projection;

	typedef SQLOps::ExtOpContext ExtOpContext;

	class ScanCursor;
	class ScanCursorAccessor;

	struct IndexScanInfo;
	struct ContainerUtils;
};

class SQLContainerUtils::ScanCursor {
public:
	class Holder;

	virtual ~ScanCursor();

	virtual bool scanFull(OpContext &cxt, const Projection &proj) = 0;
	virtual bool scanRange(OpContext &cxt, const Projection &proj) = 0;
	virtual bool scanIndex(OpContext &cxt, const IndexScanInfo &info) = 0;
	virtual bool scanMeta(
			OpContext &cxt, const Projection &proj, const Expression *pred) = 0;
	virtual bool scanVisited(OpContext &cxt, const Projection &proj) = 0;

	virtual uint32_t getOutputIndex() = 0;
	virtual void setOutputIndex(uint32_t index) = 0;

	virtual ScanCursorAccessor& getAccessor() = 0;

protected:
	ScanCursor();

private:
	ScanCursor(const ScanCursor&);
	ScanCursor& operator=(const ScanCursor&);
};

class SQLContainerUtils::ScanCursor::Holder {
public:
	virtual ~Holder();
	virtual util::AllocUniquePtr<ScanCursor>::ReturnType attach() = 0;

protected:
	Holder();

private:
	Holder(const Holder&);
	Holder& operator=(const Holder&);
};

class SQLContainerUtils::ScanCursorAccessor :
		public SQLValues::BaseLatchTarget {
public:
	struct Source;

	static util::AllocUniquePtr<ScanCursorAccessor>::ReturnType create(
			const Source &source);
	virtual ~ScanCursorAccessor();

	virtual util::AllocUniquePtr<ScanCursor::Holder>::ReturnType createCursor(
			const OpStore::ResourceRef &accessorRef) = 0;

	virtual void unlatch() throw() = 0;
	virtual void close() throw() = 0;

	virtual void getIndexSpec(
			ExtOpContext &cxt, const TupleColumnList &inColumnList,
			SQLExprs::IndexSelector &selector) = 0;
	virtual bool isIndexLost() = 0;
	virtual bool isRowDuplicatable() = 0;
	virtual bool isRangeScanFinished() = 0;

	virtual void setIndexSelection(const SQLExprs::IndexSelector &selector) = 0;
	virtual const SQLExprs::IndexSelector& getIndexSelection() = 0;

	virtual void setRowIdFiltering() = 0;
	virtual bool isRowIdFiltering() = 0;

protected:
	ScanCursorAccessor();

private:
	ScanCursorAccessor(const ScanCursorAccessor&);
	ScanCursorAccessor& operator=(const ScanCursorAccessor&);
};

struct SQLContainerUtils::ScanCursorAccessor::Source {
	Source(
			SQLValues::VarAllocator &varAlloc,
			const SQLOps::ContainerLocation &location,
			const ColumnTypeList *columnTypeList,
			const OpContext::Source &cxtSrc);

	SQLValues::VarAllocator &varAlloc_;
	SQLOps::ContainerLocation location_;
	const ColumnTypeList *columnTypeList_;
	int64_t indexLimit_;
	int64_t memLimit_;
	std::pair<uint64_t, uint64_t> partialExecSizeRange_;
	bool partialExecCountBased_;
	OpContext::Source cxtSrc_;
};

struct SQLContainerUtils::IndexScanInfo {
	IndexScanInfo(
			const SQLExprs::IndexConditionList &condList,
			const uint32_t *inputIndex);

	const SQLExprs::IndexConditionList *condList_;
	const uint32_t *inputIndex_;
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
			uint32_t containerIdColumn, uint32_t partitionNameColumn,
			PartitionId partitionCount, PartitionId &partitionId,
			bool &placeholderAffected);
	static FullContainerKey* predicateToContainerKey(
			SQLValues::ValueContext &cxt, TransactionContext &txn,
			DataStoreV4 &dataStore, const Expression *pred,
			const ContainerLocation &location, PartitionId partitionCount,
			bool forCore, util::String &dbNameStr, bool &fullReduced,
			PartitionId &reducedPartitionId);

	static SQLExprs::SyntaxExpr tqlToPredExpr(
			util::StackAllocator &alloc, const Query &query);
};

#endif
