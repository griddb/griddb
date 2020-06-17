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
#include "c_utility_cpp.h"
#include "gs_error.h"
#include "backend.hpp"
#include <iostream>
#include <stdexcept>
#include <assert.h>
#include <cstring>
#include <cstdlib>
#include <cstdio>

#include <list>
#include <iterator>

extern "C" {
#include "vdbeInt.h"
#include "vdbe.h"
#include "sqliteInt.h"
}

#if SQLITE_OS_WIN
#define Thread  __declspec(thread)
#else
#include <pthread.h>
#define Thread __thread
#endif

extern "C" {
extern Thread CUtilVariableSizeAllocator *allocator;
}
Thread CUtilRawVariableSizeAllocator *varAlloc = NULL;

/**********************************************************************/
/*! 
 * @brief Hash table
 */

/*
Table of prime numbers 2^n+a, 2<=n<=30.
*/
static const unsigned int primes[] = {
  8 - 1,
  16 - 3,
  32 - 1,
  64 - 3,
  128 - 1,
  256 - 5,
  512 - 3,
  1024 - 3,
  2048 - 9,
  4096 - 3,
  8192 - 1,
  16384 - 3,
  32768 - 19,
  65536 - 15,
  131072 - 1,
  262144 - 5,
  524288 - 1,
  1048576 - 3,
  2097152 - 9,
  4194304 - 3,
  8388608 - 15,
  16777216 - 3,
  33554432 - 39,
  67108864 - 5,
  134217728 - 39,
  268435456 - 57,
  536870912 - 3,
  1073741824 - 35,
};

template <typename S>
class HashCursor{
 typedef std::list<S, util::StdAllocator<S, CUtilRawVariableSizeAllocator> > NodeList;
 public:
  /*! 
   * @brief Constructor. 
   *        Set cursor to the head of list. And, memory the tail.
   * 
   * @param values Values list
   */
  HashCursor(NodeList *values){
    assert(varAlloc != NULL);
    it = (*values).begin();
    end = (*values).end();
  }
  virtual ~HashCursor(){}

  /*! 
   * @brief Return a value this cursor pointing to. 
   * 
   * @return Value or NULL.
   */
  S getValue() {
    assert(it != end);
    return *it;
  }

  /*! 
   * @brief Move cursor to the next. 
   * 
   * @return  0 : Succeeded 
   * @return  1 : Succeeded and last entry
   */
  int next(){
    if (it == end) {
      assert(0);
      throw "Internal logic error. No hash entry. Already pointing last.";
    }

    it++;
    if (it == end) {
      /* If last */
      return 1;
    }
    return 0;
  }

 protected:
  typename NodeList::iterator it;
  typename NodeList::iterator end;
};

template <typename T>
class TempHash{
 typedef std::list<T, util::StdAllocator<T, CUtilRawVariableSizeAllocator> > NodeList;
 private:
  unsigned int hash_num_;
  struct node{
    void *key;
    size_t size;
    struct node *next;
    NodeList *values;
  }**hash_;
 public:
  /*! 
   * @brief Constructor
   */
  TempHash(int32_t hashSize){
    assert(varAlloc != NULL);
	if(hashSize > 0) {
		hash_num_ = hashSize;
	}
	else {
	    hash_num_ = 4194301; /* The biggest prime less than 4M(=1<<22) */
	}
    hash_ = reinterpret_cast<struct node**>(varAlloc->allocate(sizeof(struct node*) * hash_num_));
    memset(hash_, 0, sizeof(struct node*) * hash_num_);
  }

  virtual ~TempHash(){
    for (unsigned int i = 0; i < hash_num_; i++) {
      if (hash_[i] != NULL) {
        node *p = hash_[i], *q;
        while (p != NULL) {
          q = p->next;
          p->values->clear();
          varAlloc->deallocate(p->key);
          ALLOC_VAR_SIZE_DELETE(*varAlloc, p->values);
          ALLOC_VAR_SIZE_DELETE(*varAlloc, p);
          p = q;
        }
      }
    }
    varAlloc->deallocate(hash_);
  } 

  /*! 
   * @brief Search for value object by keys
   * 
   * @param keys Multi keys
   * @param sizes Size of each key
   * @param nKey The number of keys
   * @param ppCur Cursor of the found list. NULL if not found 
   * 
   * @return If succeeded SQLITE_OK else SQLITE_NEWSQL_ERROR.
   */
  void search(void **keys, int *sizes, int nKey, void **ppCur, T *value){
    size_t size = 0;
    void *key = NULL;
    node *found = NULL;

    /* Generate key */
    generateKey(keys, sizes, nKey, &key, &size);

    unsigned int h = calcHash(key, size);
    node *n = hash_[h];

    found = searchValues(n, key, size);
    varAlloc->deallocate(key);

    if (found == NULL) {
      /* If not found */
      *ppCur = NULL;
      return;
    }

    HashCursor<T> *pCursor = ALLOC_VAR_SIZE_NEW(*varAlloc) HashCursor<T>(found->values);
    *ppCur = pCursor;

    *value = pCursor->getValue();
  }

  /*! 
   * @brief Put value into hash map
   * 
   * @param keys Multi keys
   * @param sizes Size of each key
   * @param nKey The number of keys
   * @param value Value object 
   */
  void set(void **keys, int *sizes, int nKey, T value){
    void *key = NULL;
    size_t size = 0;
    
    /* Generate key */
    generateKey(keys, sizes, nKey, &key, &size);

    /* Insert */
    unsigned int h = calcHash(key, size);
    node *n = hash_[h];
    if(n == NULL){
      n = ALLOC_VAR_SIZE_NEW(*varAlloc) node;
      n->next = NULL;
      n->key = key;
      n->size = size;
      n->values = ALLOC_VAR_SIZE_NEW(*varAlloc) NodeList (*varAlloc);
      n->values->push_back(value);
      hash_[h] = n;
    }else{
      /* Search */
      node *values = searchValues(n, key, size);
      if (values != NULL) {
        /* The same key is already inserted */
        values->values->push_back(value);
        varAlloc->deallocate(key);
      }else{
        node *nn = ALLOC_VAR_SIZE_NEW(*varAlloc) node;
        nn->key = key;
        nn->size = size;
        nn->next = n;
        nn->values = ALLOC_VAR_SIZE_NEW(*varAlloc) NodeList (*varAlloc);
        nn->values->push_back(value);
        hash_[h] = nn;
      }
    }
  }

 protected:
  /*! 
   * @brief Naive hash function
   * 
   * @param key Hash key
   * @param size Key size
   * 
   * @return Hashed value
   */
  unsigned int calcHash(void *key, size_t size){
    unsigned int i=0;
    u64 h = 0;
    u64 *k = (u64*)key;
    int mod = size % sizeof(u64);

    for (i = 0; i < size/sizeof(u64); i++) {
      h += ((h << 5) + h) + *k;
      k++;
    }
    if (mod != 0) {
      u64 a = 0;
      memcpy(&a, k, mod);
      h += (h << 5) + h + a;
    }
    return (unsigned int)(h % hash_num_);
  }

  node *searchValues(node *n, void *key, size_t size) {
    node *nn = n;
    while(nn != NULL) {
      if (nn->size == size && (size == 0 || memcmp(nn->key, key, size) == 0)) {
        return nn;
      }
      nn = nn->next;
    }
    return NULL;
  }

  /*! 
   * @brief Generate hash key from multi keys
   * 
   * @param keys Multi keys
   * @param sizes Size of each key
   * @param nKey The number of keys
   * @param key Generated key
   * @return 
   */
  void generateKey(void **keys, int *sizes, int nKey, void **key, size_t *size) {
    int i = 0;
    size_t sz = 0;
    char *pTail = 0;

    for (i = 0; i < nKey; i++) {
      sz += sizes[i];
    }

    if (sz == 0) {
      *key = NULL;
      return;
    }

    pTail = reinterpret_cast<char*>(varAlloc->allocate(sizeof(char) * sz));

    /* Set output */
    *key = pTail;
    *size = sz;
    for (i = 0; i < nKey; i++) {
      if (sizes[i] != 0) {
        memcpy(pTail, keys[i], sizes[i]);
        pTail += sizes[i];
      }
    }
  }
};


extern "C" {
  /**
  ** @brief Create the new hash handler.
  **
  ** @param[out] ppHash hash handler
  **
  ** @retval SQLITE_OK If successful.
  ** @retval SQLITE_NEWSQL_ERROR If failed.
  */
  int sqlite3gsHashOpen(void **ppHash, int hashSize) {
    try {
      varAlloc = CUtilVariableSizeAllocatorWrapper::unwrap(::allocator);
      *ppHash = reinterpret_cast<void*>(ALLOC_VAR_SIZE_NEW(*varAlloc) TempHash<i64>(hashSize));
    } catch(std::exception& e) {
      LocalException::set(GS_EXCEPTION_CONVERT(e, ""));
      return SQLITE_NEWSQL_ERROR;
    }
    return SQLITE_OK;
  }

  int sqlite3gsHashSet(void *pHash, void **keys, int *sizes, int nKey, i64 value) {
    TempHash<i64> *h = reinterpret_cast< TempHash<i64>* >(pHash);
    try {
      h->set(keys, sizes, nKey, value);
    } catch(std::exception& e) {
      LocalException::set(GS_EXCEPTION_CONVERT(e, ""));
      return SQLITE_NEWSQL_ERROR;
    }
    return SQLITE_OK;
  }

  /**
  ** @brief Search a hash list by keys. 
  **        If found, generate a cursor and return first value.
  **
  ** @param[out] ppCur Cursor of hash list if found. NULL if not found.
  ** @param[out] value First value in the list.
  **
  ** @retval SQLITE_OK If successful.
  ** @retval SQLITE_NEWSQL_ERROR If failed.
  */
  int sqlite3gsHashSearch(void *pHash, void **keys, int *sizes, int nKey, void **ppCur, i64 *value) {
    TempHash<i64> *h = reinterpret_cast< TempHash<i64>* >(pHash);
    try {
      h->search(keys, sizes, nKey, ppCur, value);
    } catch(std::exception& e) {
      LocalException::set(GS_EXCEPTION_CONVERT(e, ""));
      return SQLITE_NEWSQL_ERROR;
    }
    return SQLITE_OK;
  }

  /**
  ** @brief Get next hash value. 
  **        If the cursor is already pointing last one, close this cursor.
  **
  ** @param[out] ppCur Cursor of hash list if found. NULL if not found.
  ** @param[out] value Next value in the list.
  **
  ** @retval SQLITE_OK If successful.
  ** @retval SQLITE_NEWSQL_ERROR If failed.
  */
  int sqlite3gsHashGetNext(void **ppCur, i64 *value) {
    try {
      HashCursor<i64> *c = reinterpret_cast< HashCursor<i64>* >(*ppCur);    
      int ret = c->next();
      if (ret == 0) {
        *value = c->getValue();
      } else {
        sqlite3gsHashCursorClose(*ppCur);
        *ppCur = NULL;
      }
    } catch(std::exception& e) {
      LocalException::set(GS_EXCEPTION_CONVERT(e, ""));
      return SQLITE_NEWSQL_ERROR;
    }
    return SQLITE_OK;
  }

  void sqlite3gsHashCursorClose(void *pCur) {
    if (pCur != NULL) {
      HashCursor<i64> *c = reinterpret_cast< HashCursor<i64>* >(pCur);
      ALLOC_VAR_SIZE_DELETE(*varAlloc, c);
    }
  }

  void sqlite3gsHashClose(void *pHash) {
    if (pHash != NULL) {
      TempHash<i64> *h = reinterpret_cast< TempHash<i64>* >(pHash);
      ALLOC_VAR_SIZE_DELETE(*varAlloc, h);
    }
  }
}
