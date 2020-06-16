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
/*!
 * @file sql_parser_define.h
 * @brief Macros for SQLParser
*/

#ifndef SQL_PARSER_DEFINE_H_
#define SQL_PARSER_DEFINE_H_

#include "util/type.h"
#include "util/container.h"

#define SQL_PARSER_NEW SQL_PARSER_NEW_BY_ALLOCATOR(cxt)
#define SQL_PARSER_ALLOC_NEW(alloc) ALLOC_NEW(alloc)
#define SQL_PARSER_NEW_BY_ALLOCATOR(cxt) ALLOC_NEW(cxt.getDefaultAllocator())
#define SQL_PARSER_ALLOCATOR (cxt.getDefaultAllocator())
#define SQL_PARSER_ALLOCATOR_ (cxt_.getDefaultAllocator())

#define SQL_PARSER_ALLOC_DELETE(alloc, x) sqlParserCallDestructor(x)
#define SQL_PARSER_DELETE(x) sqlParserCallDestructor(x)

template<typename T>
inline void sqlParserCallDestructor(T *obj) {
	obj->~T();
}

#define SQL_PARSER_XArray util::XArray
#define SQL_PARSER_DEREF_ALLOCATOR(alloc) (*alloc)



#define SQL_PARSER_SAFE_DELETE(x) if ((x)!=NULL) { SQL_PARSER_DELETE(x); (x)=NULL; }
#define SQL_PARSER_ALLOC_SAFE_DELETE(alloc, x) if ((x)!=NULL) { SQL_PARSER_ALLOC_DELETE(alloc, x); (x)=NULL; }


#define SQL_MAX_COLUMN_VALUE_NUM 256
#define SQL_MAX_COLUMN_NAME_LEN 256

#define SQL_PARSER_TR_EPSI 1.0E-8


#endif /* SQL_PARSER_DEFINE_H */
