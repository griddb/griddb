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
#include "sql_service_handler.h"
#include "sql_service.h"
#include "sql_execution_manager.h"
#include "sql_execution.h"
#include "nosql_db.h"

UTIL_TRACER_DECLARE(SQL_SERVICE);

void UpdateTableCacheHandler::operator()(
		EventContext &ec, Event &ev) {

	SQLExecutionManager *executionManager
			= resourceSet_->getSQLExecutionManager();

	try {

		util::StackAllocator &alloc = ec.getAllocator();
		EventByteInStream in(ev.getInStream());
		
		DatabaseId dbId;
		util::String dbName(alloc);
		util::String tableName(alloc);
		
		TablePartitioningVersionId versionId;
		in >> dbId;
		
		decodeStringData<util::String>(in, dbName);
		decodeStringData<util::String>(in, tableName);
		
		in >> versionId;
		
		NoSQLDB *db = executionManager->getDB();
		if (db) {

			NoSQLStore *store = db->getNoSQLStore(
					dbId, dbName.c_str());
			
			if (store) {
				store->removeCache(tableName.c_str());
			}
		}
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(SQL_SERVICE, e, "");
	}
}
