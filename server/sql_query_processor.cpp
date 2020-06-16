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
#include "query_processor.h"
#include "sql_tuple.h"
#include "sql_utils.h"
#include "sql_compiler.h"
#include "nosql_utils.h"

void QueryProcessor::assignDistributedTarget(
		TransactionContext &txn, BaseContainer &container,
		const Query &query, ResultSet &resultSet) {
	if (resultSet.getLargeInfo() == NULL || static_cast<const ResultSet&>(
			resultSet).getDistributedTarget() != NULL) {
		return;
	}

	util::StackAllocator &alloc = txn.getDefaultAllocator();

	util::Vector< std::pair<
			TupleList::TupleColumnType, util::String> > columnInfoList(alloc);

	const util::String emptyColumnName(alloc);
	const uint32_t columnCount = container.getColumnNum();
	for (uint32_t i = 0; i < columnCount; i++) {
		const ColumnInfo &columnInfo = container.getColumnInfo(i);
		TupleList::TupleColumnType columnType =
			NoSQLUtils::convertNoSQLTypeToTupleType(
						columnInfo.getColumnType());
		if (!TupleColumnTypeUtils::isAny(columnType) && !columnInfo.isNotNull()) {
			columnType |= TupleList::TYPE_MASK_NULLABLE;
		}
		columnInfoList.push_back(std::make_pair(
				columnType, emptyColumnName));
	}

	SQLTableInfo tableInfo = *resultSet.getLargeInfo();
	resultSet.setLargeInfo(NULL);

	tableInfo.columnInfoList_ = columnInfoList;

	bool uncovered;
	const bool reduced = SQLCompiler::reducePartitionedTargetByTQL(
			alloc, tableInfo, query, uncovered,
			resultSet.getDistributedTarget(), NULL);
	resultSet.setDistributedTargetStatus(uncovered, reduced);
}
