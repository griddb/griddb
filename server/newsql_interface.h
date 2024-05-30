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
#ifndef NEWSQL_INTERFACE_H_
#define NEWSQL_INTERFACE_H_


enum SQLExtendedType {
	GS_TYPE_STRING_NULL = 20,	
	GS_TYPE_BOOL_NULL,
	GS_TYPE_BYTE_NULL,
	GS_TYPE_SHORT_NULL,
	GS_TYPE_INTEGER_NULL,
	GS_TYPE_LONG_NULL,
	GS_TYPE_FLOAT_NULL,
	GS_TYPE_DOUBLE_NULL,
	GS_TYPE_TIMESTAMP_NULL,
	GS_TYPE_GEOMETRY_NULL,
	GS_TYPE_BLOB_NULL
};

struct SQLStatement;
struct SQLCursor;
struct GSBlobTag;

static const int GS_STATUS_OK = 0;
static const int GS_STATUS_NG = 1;

extern const char * const SQL_PRAGMA_VALUE_TRUE;
extern const char * const SQL_PRAGMA_VALUE_FALSE;

typedef int32_t SQLPragmaType;
static const SQLPragmaType PRAGMA_USE_HASH = 0;
static const SQLPragmaType PRAGMA_USE_TMPDB = 1;
static const SQLPragmaType PRAGMA_HASH_SIZE = 2;
static const SQLPragmaType PRAGMA_USE_PARTIAL_TQL_EXECUTED = 3;
static const SQLPragmaType PRAGMA_EXPERIMENTAL_SHOW_SYSTEM = 4;
static const SQLPragmaType PRAGMA_CACHE_SIZE = 5;
static const SQLPragmaType PRAGMA_PAGE_SIZE = 6;
static const SQLPragmaType PRAGMA_LOOKASIDE_COUNT = 7;
static const SQLPragmaType PRAGMA_LOOKASIDE_SIZE = 8;
static const SQLPragmaType PRAGMA_SHOW_PROFILE = 9;
static const SQLPragmaType PRAGMA_TYPE_MAX = 10;

#ifdef __cplusplus
extern "C" {
#endif

int gsCursorCreate(SQLStatement *pSqlStatement, int iTable, SQLCursor **pCur);

int gsCursorClose(SQLCursor *pCur);

int gsCursorFirst(SQLCursor *pCur, int *pRes);

int gsCursorNext(SQLCursor *pCur, int *pRes);

int gsCursorLast(SQLCursor *pCur, int *pRes);

int gsCursorPosition(SQLCursor *pCur, i64 *pos);
	
int gsCursorPosition2(SQLCursor *pCur, int *blockNo, i64 *pos);

int gsCursorMove(SQLCursor *pCur, i64 *pos, int *pRes);

int gsCursorMove2(SQLCursor *pCur, int blockNo, i64 *pos, int *pRes);

int gsColumnType(SQLStatement *pSqlStatement, int iTable, int iCol);

int gsColumnCount(SQLStatement *pSqlStatement, int iTable);

int64_t gsRowCount(SQLStatement *pSqlStatement, int iTable);


int gsSetConnectionEnv(SQLStatement *pSqlStatement, int type, int flag);

int gsGetConnectionEnv(SQLStatement *pSqlStatement, int type);




int gsGetValueString(SQLCursor *pCur, int iCol, char **value, size_t *size);

int gsGetValueBool(SQLCursor *pCur, int iCol, int8_t *value);

int gsGetValueByte(SQLCursor *pCur, int iCol, int8_t *value);

int gsGetValueShort(SQLCursor *pCur, int iCol, int16_t *value);

int gsGetValueInteger(SQLCursor *pCur, int iCol, int *value);

int gsGetValueLong(SQLCursor *pCur, int iCol, int64_t *value);

int gsGetValueFloat(SQLCursor *pCur, int iCol, float *value);

int gsGetValueDouble(SQLCursor *pCur, int iCol, double *value);

int gsGetValueTimestamp(SQLCursor *pCur, int iCol, int64_t *value);

int gsGetValueBlob(SQLCursor *pCur, int iCol, struct GSBlobTag *value);


int gsGetValueStringNull(SQLCursor *pCur, int iCol, char **value, size_t *size, int *pRes);

int gsGetValueBoolNull(SQLCursor *pCur, int iCol, int8_t *value, int *pRes);

int gsGetValueByteNull(SQLCursor *pCur, int iCol, int8_t *value, int *pRes);

int gsGetValueShortNull(SQLCursor *pCur, int iCol, int16_t *value, int *pRes);

int gsGetValueIntegerNull(SQLCursor *pCur, int iCol, int *value, int *pRes);

int gsGetValueLongNull(SQLCursor *pCur, int iCol, int64_t *value, int *pRes);

int gsGetValueFloatNull(SQLCursor *pCur, int iCol, float *value, int *pRes);

int gsGetValueDoubleNull(SQLCursor *pCur, int iCol, double *value, int *pRes);

int gsGetValueTimestampNull(SQLCursor *pCur, int iCol, int64_t *value, int *pRes);

int gsGetValueBlobNull(SQLCursor *pCur, int iCol, struct GSBlobTag *value, int *pRes);


#ifdef __cplusplus
}
#endif

#endif
