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
	@brief Definition of TqlFunc and FunctionMap
*/

#ifndef FUNCTION_MAP_H_
#define FUNCTION_MAP_H_

#include "util/allocator.h"

#include "collection.h"
#include "expression.h"
#include "time_series.h"

class FunctionMap;
class ContainerRowWrapper;

/*!
 * @brief Class for TQL function object (functor)
 */
class TqlFunc {
public:
	/*!
	 * @brief Default function call operator without priori argument evaluation
	 *
	 * @param args Argument list
	 * @param txn Transaction Context
	 *
	 * @return Result as expression
	 */
	virtual Expr *operator()(
		ExprList &args, TransactionContext &txn, ObjectManager &) = 0;

	/*!
	 * @brief Custom function call operator with priori argument evaluation
	 *
	 * @param args Argument list
	 * @param column_values Values bound to column
	 * @param function_map Function map
	 * @param mode Evaluation mode described in EvalMode
	 * @param txn Transaction Context
	 * @param argsAfterEval Evaluation result of args
	 *
	 * @return Result as expression
	 */
	virtual Expr *operator()(ExprList &args, ContainerRowWrapper *column_values,
		FunctionMap *function_map, EvalMode mode, TransactionContext &txn,
		ObjectManager &objectManager, ExprList &argsAfterEval);

	/*!
	 * @brief Check if the function can use an index.
	 * @return In default, cannot use index.
	 */
	virtual uint8_t checkUsableIndex() {
		return static_cast<uint8_t>(-1);
	}

	/*!
	 * @brief Check if the function needs to be called by his own call operator.
	 * @return True if custom function call operator must be called
	 */
	virtual bool needsCustomEvaluation() {
		return false;
	}

	/*!
	 * @brief If function has internal valuables, returns a clone object of
	 * "this"
	 * @return Cloned object or Constant object
	 */
	virtual TqlFunc *clone(util::StackAllocator &) {
		return this;
	}

	/*!
	 * @brief If clone() generates a new object, delete that
	 */
	virtual void destruct() { /* DO NOTHING */
	}

	virtual ~TqlFunc() {}
};

/*!
 * @brief Class for GIS function object (functor)
 */
class TqlGisFunc : public TqlFunc {
public:
	/*!
	 * @brief Get parameter to index
	 *
	 * @param txn The transaction context
	 * @param args Argument list
	 * @param[out] outSearchType search type
	 * @param[out] outParam1 parameter1 (RtreeMap::SearchContext::rect1)
	 * @param[out] outParam2 parameter2 (RtreeMap::SearchContext::rect2)
	 */
	virtual void getIndexParam(TransactionContext &txn, ExprList &args,
		GeometryOperator &outSearchType, const void *&outParam1,
		const void *&outParam2, ColumnId &indexColumnId) = 0;

	virtual ~TqlGisFunc() {}
};

/*!
 * @brief Hash table
 */
template <typename T>
class OpenHash {
public:
	OpenHash() {
		memset(hashList_, 0, sizeof(hashList_));
	}

	virtual ~OpenHash() {
		for (int i = 0; i < hash_num; i++) {
			if (hashList_[i]) {
				node *p = hashList_[i], *q;
				while (p) {
					q = p->next;
					delete (p->f);
					delete p;
					p = q;
				}
			}
		}
	}

	/*!
	 * @brief Search for function object by name
	 *
	 * @param name Function name
	 * @param alloc allocator
	 *
	 * @return Found functor or NULL
	 */
	T *findFunction(const char *name, util::StackAllocator &alloc) {
		unsigned int h = hash(name);
		node *n = hashList_[h];
		while (n != NULL) {
			if (strcmp(name, n->str) == 0) {
				return n->f->clone(alloc);
			}
			n = n->next;
		}
		return NULL;
	}

	/*!
	 * @brief Register a function into the map
	 *
	 * @param name Function name
	 */
	template <typename CHILD>
	void RegisterFunction(const char *name) {
		CHILD *functor = new CHILD();
		insert(name, functor);
	}

	/*!
	 * @brief Register a function into the map
	 *
	 * @param name Function name
	 * @param functor Functor object
	 */
	void RegisterFunction(const char *name, T *functor) {
		insert(name, functor);
	}

protected:
	static const int hash_bits = 8;
	static const int hash_num = 1 << hash_bits;
	static const int hash_name_len = 32;
	struct node {
		char str[hash_name_len];
		T *f;
		struct node *next;
	} * hashList_[hash_num];

	/*!
	 * @brief Naive hash function
	 *
	 * @param str Function name
	 *
	 * @return Hashed value
	 */
	unsigned int hash(const char *str) {
		int i = 0;
		unsigned int h = 0;
		while (str[i] != 0) {
			h += (3 * str[i] + i * 13);
			i++;
		}
		return (h & (hash_num - 1));
	}

	/*!
	 * @brief Put functor into hash map
	 *
	 * @param name Functor name
	 * @param functor Functor object
	 */
	void insert(const char *name, T *functor) {
		unsigned int h = hash(name);
		node *n = hashList_[h];
		if (n == NULL) {
			n = new node();
			n->next = NULL;
		}
		else {
			node *nn = new node();
			nn->next = n;
			n = nn;
		}
		n->f = functor;
		size_t len = strlen(name);
		memcpy(
			n->str, name, (len >= hash_name_len - 2) ? hash_name_len - 2 : len);
		n->str[hash_name_len - 1] = '\0';
		hashList_[h] = n;
	}
};
/*!
 * @brief Hash based map for TQL function
*/
class FunctionMap : public OpenHash<TqlFunc> {
public:
	FunctionMap(bool forgis = false);

	static FunctionMap *getInstance() {
		return &map_;
	}
	static FunctionMap *getInstanceForWkt() {
		return &gismap_;
	}

private:
	static FunctionMap map_;
	static FunctionMap gismap_;
};

#include "aggregation_func.h"
#include "selection_func.h"
#include "special_id_map.h"

#endif /* FUNCTION_MAP_H_*/
