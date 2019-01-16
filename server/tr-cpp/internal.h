/*
	Copyright (c) 2011 TOSHIBA CORPORATION

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
	@brief TR: R-Tree implementation
*/

#ifndef _TR_INTERNAL_H
#define _TR_INTERNAL_H

#include "util/type.h"
#include "TrTree.h"
#include "data_type.h"
#include <stdio.h>

#define _USE_MATH_DEFINES
#include <math.h>
#ifndef INFINITY
#define INFINITY 1e300
#endif

/* machine-memory page size */
#define TR_PAGESIZE 512
#define TR_CHILD_COUNT \
	((int32_t)((TR_PAGESIZE - 2 * sizeof(intptr_t)) / sizeof(TrChildTag)))

/* balance criteria for node splitting */
#define TR_CHILD_COUNT_MAX TR_CHILD_COUNT
#define TR_CHILD_COUNT_MIN (TR_CHILD_COUNT / 2)

/* format magic */
#define TR_MAGIC "TR\0\1"

/* data structure definitions */
struct _TrNodeTag;

typedef struct _TrChildTag {
	TrRectTag rect;
	OId nodeOId;
} TrChildTag, *TrChild;

typedef struct _TrNodeTag {
	int32_t count;
	int32_t level; /* 0 = leaf, positive = node */
	TrChildTag children[TR_CHILD_COUNT];
} TrNodeTag, *TrNode;
#define TrNode_leaf_p(n) ((n)->level == 0)

typedef struct _TrIndexTag {
	int32_t count;
	OId rootNodeOId;
} TrIndexTag;

/* prototype declarations */
/* rect.c */
void TrRect_init(TrRect r);
TrRectTag TrRect_null(void);
double TrRect_extent_volume(TrRect r);
TrRectTag TrRect_surround(TrRect r1, TrRect r2);
int32_t TrRect_overlap_p(TrRect r1, TrRect r2);
int32_t TrRect_contained_p(TrRect r1, TrRect r2);
void TrRect_print(TrRect r, int32_t indent);

/* node.c */
OId TrNode_new(TransactionContext &txn, ObjectManager &objectManager);
void TrNode_destroy(
	TransactionContext &txn, ObjectManager &objectManager, OId nodeOId,
	uint64_t &removeNum);
void TrNode_init(TrNode n);
void TrNode_free(
	TransactionContext &txn, ObjectManager &objectManager, OId nOId);
TrRectTag TrNode_surround(
	TransactionContext &txn, ObjectManager &objectManager, OId nOId);
int32_t TrNode_add_child(TransactionContext &txn, ObjectManager &objectManager,
	OId nOId, TrChild b, OId *nnOId);
void TrNode_delete_child(TrNode n, int32_t i);

/* node-split.c */
void TrNode_split(TransactionContext &txn, ObjectManager &objectManager,
	OId nOId, TrChild c, OId *newNodeOId);
void TrNode_print(TransactionContext &txn, ObjectManager &objectManager,
	OId nodeOId, int32_t indent);

/* node-search.c */
void TrNode_all(TransactionContext &txn, ObjectManager &objectManager,
	OId nodeOId, TrHitCallback cb, void *cbarg);
void TrNode_search(TransactionContext &txn, ObjectManager &objectManager,
	OId nodeOId, TrCheckCallback ccb, void *ccbarg, TrHitCallback hcb,
	void *hcbarg);
void TrNode_search_rect(TransactionContext &txn, ObjectManager &objectManager,
	OId nodeOId, TrRect r, TrHitCallback cb, void *cbarg);
void TrNode_search_quad(TransactionContext &txn, ObjectManager &objectManager,
	OId nodeOId, TrPv3Key *qkey, TrHitCallback hcb, void *hcbarg);
void TrNode_all_dump(
	TransactionContext &txn, ObjectManager &objectManager, OId nOId);
void TrNode_dump(TransactionContext &txn, ObjectManager &objectManager,
	OId nodeOId, int32_t depth);

/* node-insert.c */
int32_t TrNode_insert(TransactionContext &txn, ObjectManager &objectManager,
	OId *nodeOId, TrRect r, OId dataOId);
int32_t TrNode_insert0(TransactionContext &txn, ObjectManager &objectManager,
	OId *nodeOId, TrRect r, OId childOId, int32_t level);

/* node-delete.c */
int32_t TrNode_delete(TransactionContext &txn, ObjectManager &objectManager,
	OId *n, TrRect r, void *data);
int32_t TrNode_delete_cmp(TransactionContext &txn, ObjectManager &objectManager,
	OId *root, TrRect r, TrDataCmpCallback dccb, void *arg);

#define TrUtil_print_indent(indent)                \
	do {                                           \
		int32_t i;                                 \
		for (i = 0; i < indent; i++) printf("  "); \
	} while (0)

#endif

#define UNDEF_OID_CHECK(id)                                                \
	do {                                                                   \
		if (id == UNDEF_OID) {                                             \
			UTIL_THROW_USER_ERROR(                                         \
				GS_ERROR_TQ_INTERNAL_LIBRARY_MISUSE, "OId is undefined."); \
		}                                                                  \
	} while (0)
#define NULL_PTR_CHECK(p)                                              \
	do {                                                               \
		if (p == NULL) {                                               \
			UTIL_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_LIBRARY_MISUSE, \
				"Tr internal function is called with NULL pointer.");  \
		}                                                              \
	} while (0)
