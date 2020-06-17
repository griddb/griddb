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
#ifndef REWRITE_H_
#define REWRITE_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "sqliteInt.h"


/*
	cmd ::= select(X).
	- withは非対応
 */
Select* gssqlSetUpSelect(Parse *parse, Select *select);

/*
	cmd ::= with(C) DELETE FROM fullname(X) indexed_opt(I) where_opt(W)
		orderby_opt(O) limit_opt(L).
	- with、orderby_opt、limit_optは非対応
 */
Select* gssqlSetUpDelete(Parse *parse, SrcList *tableList, Expr *where);

/*
	cmd ::= with(C) UPDATE orconf(R) fullname(X) indexed_opt(I) SET setlist(Y)
		where_opt(W) orderby_opt(O) limit_opt(L).
	- with、orderby_opt、limit_optは非対応
 */
Select* gssqlSetUpUpdate(Parse *parse, SrcList *tableList, ExprList *changes,
		Expr *where, int onError);

/*
	cmd ::= with(W) insert_cmd(R) INTO fullname(X) inscollist_opt(F) select(S).
	- inscollist_opt: カラム順序制御構文
	- DEFAULT VALUESは非対応
 */
Select* gssqlSetUpInsert(Parse *parse, SrcList *tableList, Select *select,
		IdList *columnList, int onError);

void gssqlSelect(Parse *parse, Select *select);


struct RewriterTableColumn;
struct RewriterTableColumnList;
struct RewriterTableOption;

typedef struct RewriterTableColumn RewriterTableColumn;
typedef struct RewriterTableColumnList RewriterTableColumnList;
typedef struct RewriterTableOption RewriterTableOption;

#define REWRITER_TABLE_COLUMN_PRIMARY_KEY 1 << 0

/*
	cmd ::= CREATE DATABASE nm(X).
*/
void gssqlCreateDatabase(Parse *parse, Token *name);

/*
	cmd ::= DROP DATABASE nm(X).
*/
void gssqlDropDatabase(Parse *parse, Token *name);

/*
	cmd ::= CREATE TABLE ifnotexists(E) nm(Y) dbnm(Z)
		LP mod_columnlist(L) RP mod_table_options(F).
	- 既存のパース処理と置き換え
 */
void gssqlCreateTable(Parse *parse, Token *name1, Token *name2,
		RewriterTableColumnList *columnList, RewriterTableOption *option,
		int noErr);

/*
	mod_columnlist(A) ::= mod_column(C).
	mod_columnlist(A) ::= mod_columnlist(L) COMMA mod_column(C).
 */
RewriterTableColumnList* gssqlAppendTableColumn(
		Parse *parse, RewriterTableColumnList *columnList,
		RewriterTableColumn *column);

void gssqlDeleteTableColumnList(
		sqlite3 *db, RewriterTableColumnList *columnList);

/*
	mod_column(A) ::= nm(X) ids(T) mod_carglist(L).
 */
RewriterTableColumn* gssqlCreateTableColumn(
		Parse *parse, Token *name, Token *type, int option);

void gssqlDeleteTableColumn(
		sqlite3 *db, RewriterTableColumn *column);

/*
	mod_carglist(A) ::= .
	mod_carglist(A) ::= mod_carglist PRIMARY KEY.
 */
int gssqlUpdateTableColumnOption(Parse *parse, int base, int extra);

/*
	mod_table_options(A) ::= .
	mod_table_options(A) ::= PARTITION BY HASH nm(C) PARTITIONS INTEGER(P).
 */
RewriterTableOption* gssqlCreateTableOption(
		Parse *parse, Token *partitionColumn, Token *partitionCount);

void gssqlDeleteTableOption(sqlite3 *db, RewriterTableOption *option);

/*
	cmd ::= DROP TABLE ifexists(E) fullname(X).
 */
void gssqlDropTable(Parse *parse, SrcList *name, int noErr);

/*
	cmd ::= CREATE INDEX ifnotexists(NE) nm(X) ON nm(Y) dbnm(Z)
		LP idxlist(L) RP.
 */
void gssqlCreateIndex(Parse *parse,
		Token *indexName, Token *tableName1, Token *tableName2,
		ExprList *columnList, int ifNotExist);

/*
	cmd ::= DROP INDEX ifexists(E) fullname(X).
 */
void gssqlDropIndex(Parse *parse, SrcList *name, int noErr);

/*
	cmd ::= CREATE USER nm(X).
	cmd ::= CREATE USER nm(X) IDENTIFIED BY STRING(Y).
 */
void gssqlCreateUser(Parse *parse, Token *userName, Token *password);

/*
	cmd ::= SET PASSWORD FOR nm(X) EQ STRING(Y).
	cmd ::= SET PASSWORD STRING(Y).
 */
void gssqlSetPassword(Parse *parse, Token *userName, Token *password);

/*
	cmd ::= DROP USER nm(X).
 */
void gssqlDropUser(Parse *parse, Token *userName);

/*
	cmd ::= GRANT ALL ON nm(X) TO nm(Y).
 */
void gssqlGrant(Parse *parse, Token *dbName, Token *userName);

/*
	cmd ::= REVOKE ALL ON nm(X) FROM nm(Y).
 */
void gssqlRevoke(Parse *parse, Token *dbName, Token *userName);


/*
	cmd ::= PRAGMA nm(X) dbnm(Z).
	cmd ::= PRAGMA nm(X) dbnm(Z) EQ nmnum(Y).
	cmd ::= PRAGMA nm(X) dbnm(Z) LP nmnum(Y) RP.
	cmd ::= PRAGMA nm(X) dbnm(Z) EQ minus_num(Y).
	cmd ::= PRAGMA nm(X) dbnm(Z) LP minus_num(Y) RP.
 */
void gssqlExecutePragma(
		Parse *parse, Token *key1, Token *key2, Token *value, int minusFlag);

#ifdef __cplusplus
}
#endif

#endif 
