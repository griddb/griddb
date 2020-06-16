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
#include "sqliteInt.h"

#ifdef GD_ENABLE_NEWSQL_SERVER

#include "c_utility.h"

/*
** This is GridDB allocator.
*/
#if SQLITE_OS_WIN
#define Thread  __declspec(thread)
#else
#include <pthread.h>
#define Thread __thread
#endif

Thread CUtilVariableSizeAllocator *allocator = NULL;
Thread CUtilException *exception = NULL;

static void *sqlite3gsMemMalloc(int nByte){
  GSResult grc = GS_RESULT_OK;
  void *p = NULL;

  grc = cutilAllocateVariableSize(exception, allocator, (size_t)nByte, &p);
  if (!GS_SUCCEEDED(grc)) {
    p = NULL;
  }

  return p;
}

static void sqlite3gsMemFree(void *pPrior){
  cutilDeallocateVariableSize(allocator, pPrior);
}

static int sqlite3gsMemSize(void *pPrior){
  return pPrior ? (int)cutilGetVariableSizeElementCapacity(allocator, pPrior) : 0;
}

static void *sqlite3gsMemRealloc(void *pPrior, int nByte){
  int n = sqlite3gsMemSize(pPrior);
	void *pNew = sqlite3gsMemMalloc(nByte);

  if (pNew == NULL) {return NULL;}
	memcpy(pNew, pPrior, (size_t)n);
	sqlite3gsMemFree(pPrior);
  if( pNew==0 ){
    sqlite3_log(SQLITE_NOMEM,
      "failed memory resize %u to %u bytes",
      sqlite3gsMemSize(pPrior), nByte);
  }
  return pNew;
}

/*
** Round up a request size to the next valid allocation size.
*/
static int sqlite3gsMemRoundup(int n){
  /* Trial implementation */
  return n;
}

/*
** Initialize this module.
*/
static int sqlite3gsMemInit(void *NotUsed){
  UNUSED_PARAMETER(NotUsed);
  if (allocator == NULL) {
    /* you should call sqlite3_mem_param_set */
    return SQLITE_MISUSE;
  }
  return SQLITE_OK;
}

/*
** Deinitialize this module.
*/
static void sqlite3gsMemShutdown(void *NotUsed){
  UNUSED_PARAMETER(NotUsed);
  /* Do nothing */
  return;
}

/*
** This routine is the only routine in this file with external linkage.
**
** Populate the low-level memory allocation function pointers in
** sqlite3GlobalConfig.m with pointers to the routines in this file.
*/
void sqlite3gsMemSet(void){
  static const sqlite3_mem_methods gsMethods = {
     sqlite3gsMemMalloc,
     sqlite3gsMemFree,
     sqlite3gsMemRealloc,
     sqlite3gsMemSize,
     sqlite3gsMemRoundup,
     sqlite3gsMemInit,
     sqlite3gsMemShutdown,
     0
  };
  sqlite3_config(SQLITE_CONFIG_MALLOC, &gsMethods);
}

int sqlite3gs_allocator_set(CUtilException *ex, CUtilVariableSizeAllocator *alc) {
  if (alc == NULL) {
    return SQLITE_ERROR;
  }
  allocator = alc;
  exception = ex;
  return SQLITE_OK;
}
#endif /* GD_ENABLE_NEWSQL_SERVER */
