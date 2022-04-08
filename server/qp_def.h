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
	@file
	@brief Macros for query processor
*/

#ifndef QP_DEF_H_
#define QP_DEF_H_

#include <stddef.h>

#define QP_NEW QP_NEW_BY_TXN(txn)
#define QP_ALLOC_NEW(alloc) ALLOC_NEW(alloc)
#define QP_NEW_BY_TXN(txn) ALLOC_NEW(txn.getDefaultAllocator())
#define QP_ALLOCATOR (txn.getDefaultAllocator())
#define QP_ALLOCATOR_ (txn_.getDefaultAllocator())

#define QP_ALLOC_DELETE(alloc, x) callDestructor(x)
#define QP_DELETE(x) callDestructor(x)

template <typename T>
inline void callDestructor(T *obj) {
	obj->~T();
}

#define QP_XArray util::XArray
#define QP_DEREF_ALLOCATOR(alloc) (*alloc)


#define QP_SAFE_DELETE(x) \
	if ((x) != NULL) {    \
		QP_DELETE(x);     \
		(x) = NULL;       \
	}
#define QP_ALLOC_SAFE_DELETE(alloc, x) \
	if ((x) != NULL) {                 \
		QP_ALLOC_DELETE(alloc, x);     \
		(x) = NULL;                    \
	}

#define MAX_COLUMN_VALUE_NUM 256
#define MAX_COLUMN_NAME_LEN 256
enum EvalMode { EVAL_MODE_NORMAL, EVAL_MODE_PRINT, EVAL_MODE_CONTRACT };

#define USE_LOCAL_TIMEZONE (false)


#define QP_TR_EPSI 1.0E-8

#endif
