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
#ifndef SQL_EXECUTION_COMMON_H_
#define SQL_EXECUTION_COMMON_H_

#include "sql_tuple.h"

struct SQLFetchContext {

	SQLFetchContext() {}
	SQLFetchContext(
			TupleList::Reader *reader,
			TupleList::Column *columnList,
			size_t columnSize,
			bool isCompleted) :
					reader_(reader),
					columnList_(columnList),
					columnSize_(columnSize),
					isCompleted_(isCompleted) {
	}

	TupleList::Reader *reader_;
	TupleList::Column *columnList_;
	size_t columnSize_;
	bool isCompleted_;
};

#endif
