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
#ifndef _BACKEND_H_
#define _BACKEND_H_

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

#include "sqliteInt.h"
int sqlite3gsHashOpen(void **ppHash, int hashSize);
int sqlite3gsHashSet(void *pHash, void **keys, int *sizes, int nKey, i64 value);
int sqlite3gsHashSearch(void *pHash, void **keys, int *sizes, int nKey, void **ppCur, i64 *value);
int sqlite3gsHashGetNext(void **ppCur, i64 *value);
void sqlite3gsHashCursorClose(void *pCursor);
void sqlite3gsHashClose(void *pHash);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* _BACKEND_H_ */
