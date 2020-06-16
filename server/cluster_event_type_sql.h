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
#ifndef CLUSTER_EVENT_TYPE_SQL_H_
#define CLUSTER_EVENT_TYPE_SQL_H_

enum GSSQLEventType {
	SQL_EXECUTE_TQL_DIRECT = QUERY_TQL,
	SQL_EXECUTE_QUERY = 400,
	SQL_CANCEL_QUERY,


	SQL_NOTIFY_CLIENT = 4000,  
	SQL_TIMER_NOTIFY_CLIENT,  
	SQL_COLLECT_TIMEOUT_CONTEXT,  
	SQL_SCAN_CANCELLABLE_EVENT

};

#endif
